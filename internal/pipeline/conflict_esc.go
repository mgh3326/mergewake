package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// The retry wait of an ESC no destination took: it doubles from the first
// to the cap. Constants, not config keys: internal/config/** is a
// deploy_approval path.
const (
	conflictEscRetryFirst = time.Minute
	conflictEscRetryMax   = 30 * time.Minute
	// conflictEscErrorMax bounds the error kept in the state file and audit.
	conflictEscErrorMax = 512
)

// conflictEscReceipt is one head's delivered ESC. A head with a receipt is
// never sent again.
type conflictEscReceipt struct {
	EventID string    `json:"event_id"`
	Lane    string    `json:"lane"`
	Via     string    `json:"via"`
	At      time.Time `json:"at"`
}

// conflictEscPending is a head whose ESC no destination has taken yet.
type conflictEscPending struct {
	EventID  string    `json:"event_id"`
	Attempts int       `json:"attempts"`
	Next     time.Time `json:"next"`
	// Pinned is the lane of a delivery that failed without saying whether
	// the event landed. Only that lane is tried again, under the same event
	// id, until it accepts (panewire answers a repeat as delivered) or
	// refuses outright.
	Pinned string `json:"pinned,omitempty"`
	// PinnedVia is the rule that chose Pinned.
	PinnedVia string `json:"pinned_via,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// conflictEscState is <state_dir>/<repo>/conflict-esc/<pr>.json. Receipts are
// kept for the life of the file, so a head that returns, a PR that is
// reopened or a restart never re-sends.
type conflictEscState struct {
	Delivered map[string]conflictEscReceipt `json:"delivered,omitempty"`
	Pending   map[string]conflictEscPending `json:"pending,omitempty"`
}

func (s *Service) conflictEscFile(repo string, n int) string {
	return filepath.Join(s.state(repo), "conflict-esc", strconv.Itoa(n)+".json")
}

func readConflictEsc(file string) (conflictEscState, error) {
	var st conflictEscState
	if _, e := fsutil.ReadJSON(file, &st); e != nil {
		return st, fmt.Errorf("%s: %w", file, e)
	}
	if st.Delivered == nil {
		st.Delivered = map[string]conflictEscReceipt{}
	}
	if st.Pending == nil {
		st.Pending = map[string]conflictEscPending{}
	}
	return st, nil
}

// conflictEscID is the event id of one (repo, PR, head) conflict: nothing but
// that key goes into it, so every attempt, replay and destination of the same
// head carries the same id.
func conflictEscID(repo string, n int, head string) string {
	return fmt.Sprintf("mergewake-esc-%s-%d-conflict-%s", repo, n, head)
}

// mergeabilityKnown reports whether GitHub has computed the PR's
// mergeability. UNKNOWN is neither a conflict nor a resolution.
func mergeabilityKnown(pr github.Pull) bool {
	return conflicting(pr) || (pr.Mergeable != nil && pr.MergeableState != "unknown")
}

// conflictEscRefused reports whether a delivery failed before panewire
// recorded anything for the lane, so the next destination can be tried
// without the same event landing twice. Any other failure may have landed.
func conflictEscRefused(err error) bool {
	var ce *integrations.CommandError
	return errors.As(err, &ce) && strings.Contains(ce.Stderr, "emit: invalid --lane")
}

// EscalateConflict sends the one ESC of a PR head that conflicts with its
// base (hk 3712; docs/config.md, "Conflict ESC"). The pr role calls it for
// every evaluated open PR before review and settle, and NotifyPR calls it
// before its draft and gate returns, so a conflicting head is told whether
// its review is held, pending, quota-stopped or not started, and also as a
// draft. It never changes review, gate or merge.
//
// The ESC goes to the PR body's Mergewake-Notify-Lane, else the hk task's
// lane, else the desk, as a `decision`. A destination that refuses the lane
// outright moves to the next; any other failure retries the same lane under
// the same event id after a doubling wait. The head is recorded delivered
// only once a lane took it. Unknown mergeability neither sends nor clears.
// A dry run, or notify switched off, does nothing.
func (s *Service) EscalateConflict(ctx context.Context, r config.Repo, pr github.Pull, o Options) error {
	if !s.Config.Daemon.PR.Notify.On() || !o.Reporting() || !escalable(r, pr) || !mergeabilityKnown(pr) {
		return nil
	}
	file := s.conflictEscFile(r.Name, pr.Number)
	head := pr.Head.SHA
	if !conflicting(pr) {
		return s.resolveConflictEsc(file, head)
	}
	defer lockPath(file)()
	unlock, e := fsutil.Lock(file+".lock", true)
	if e != nil {
		return e
	}
	defer unlock()
	st, e := readConflictEsc(file)
	if e != nil {
		return e
	}
	if _, done := st.Delivered[head]; done {
		return nil
	}
	p, waiting := st.Pending[head]
	now := s.now().UTC()
	if waiting && now.Before(p.Next) {
		return nil
	}
	// The observation may be old by now: send only for a PR GitHub still
	// reports open, on this base and head, and conflicting.
	fresh, e := s.GH.Pull(github.FreshEvidence(ctx), r.Name, pr.Number)
	if e != nil {
		return e
	}
	if !escalable(r, fresh) || fresh.Head.SHA != head || !conflicting(fresh) {
		return nil
	}
	p.EventID = conflictEscID(r.Name, pr.Number, head)
	lane, via, tried, err := s.deliverConflictEsc(ctx, r, fresh, p.EventID, &p)
	if lane != "" {
		delete(st.Pending, head)
		st.Delivered[head] = conflictEscReceipt{EventID: p.EventID, Lane: lane, Via: via, At: now}
		if e := fsutil.WriteJSON(file, st); e != nil {
			return e
		}
		return s.audit(r.Name, "conflict-esc", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "lane": lane, "via": via, "event_id": p.EventID}, false)
	}
	if err == nil {
		// No destination at all: not even a desk lane is configured.
		err = errors.New("no destination: no body lane, no task lane and no desk lane configured")
	}
	var route *conflictEscRouteError
	if errors.As(err, &route) {
		// A route source could not be read (hk or GitHub): unknown, not
		// absent. No lane took the ESC and no wait starts; the next
		// observation reads the route again.
		if p.Pinned != "" || waiting {
			st.Pending[head] = p
			if e := fsutil.WriteJSON(file, st); e != nil {
				return errors.Join(route.err, e)
			}
		}
		return route.err
	}
	p.Attempts++
	p.Next = now.Add(conflictEscBackoff(p.Attempts))
	p.LastError = clip(integrations.Detail(err), conflictEscErrorMax)
	st.Pending[head] = p
	if e := fsutil.WriteJSON(file, st); e != nil {
		return e
	}
	return s.audit(r.Name, "conflict-esc-failed", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "event_id": p.EventID, "tried": tried, "pinned": p.Pinned, "attempts": p.Attempts, "next": p.Next, "error": p.LastError}, false)
}

// escalable reports whether pr is one the ESC is for: open, not merged, and
// against the repo's base branch. Drafts are.
func escalable(r config.Repo, pr github.Pull) bool {
	return pr.State == "open" && !pr.Merged && (r.Branch == "" || pr.Base.Ref == r.Branch)
}

// resolveConflictEsc drops the pending ESC of a head GitHub now reports
// mergeable, unless a lane may already hold it (Pinned): that one is still
// retried there if the head conflicts again. A delivered head stays
// delivered.
func (s *Service) resolveConflictEsc(file, head string) error {
	st, e := readConflictEsc(file)
	if e != nil {
		return e
	}
	if p, ok := st.Pending[head]; !ok || p.Pinned != "" {
		return nil
	}
	defer lockPath(file)()
	unlock, e := fsutil.Lock(file+".lock", true)
	if e != nil {
		return e
	}
	defer unlock()
	if st, e = readConflictEsc(file); e != nil {
		return e
	}
	if p, ok := st.Pending[head]; !ok || p.Pinned != "" {
		return nil
	}
	delete(st.Pending, head)
	return fsutil.WriteJSON(file, st)
}

// conflictEscBackoff is the wait after the given number of failed attempts.
func conflictEscBackoff(attempts int) time.Duration {
	d := conflictEscRetryFirst
	for i := 1; i < attempts && d < conflictEscRetryMax; i++ {
		d *= 2
	}
	return min(d, conflictEscRetryMax)
}

// conflictEscRouteError is a route source that could not be read.
type conflictEscRouteError struct{ err error }

func (e *conflictEscRouteError) Error() string { return e.err.Error() }

// deliverConflictEsc offers the ESC to each destination in order until one
// takes it, and returns that lane and its rule. A pinned lane is offered
// alone first. The route is read lazily: a body lane that takes the ESC
// costs no hk or commit read. tried lists the lanes offered. A failure that
// may have landed pins its lane and stops; a route read failure stops with a
// *conflictEscRouteError.
func (s *Service) deliverConflictEsc(ctx context.Context, r config.Repo, pr github.Pull, id string, p *conflictEscPending) (lane, via string, tried []string, err error) {
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	offer := func(l, v string) (bool, error) {
		tried = append(tried, l)
		text := fmt.Sprintf("ESC [mergewake] %s#%d head=%s conflict with %s: rebase or merge base, then push | lane=%s via %s",
			r.Name, pr.Number, short7(pr.Head.SHA), pr.Base.Ref, l, v)
		e := s.Notifier.Notify(reportCtx, integrations.Event{ID: id, Text: text, Lane: l, Type: "decision"})
		if e == nil {
			p.Pinned, p.PinnedVia = "", ""
			return true, nil
		}
		if conflictEscRefused(e) {
			if p.Pinned == l {
				p.Pinned, p.PinnedVia = "", ""
			}
			return false, e
		}
		p.Pinned, p.PinnedVia = l, v
		return false, e
	}
	if pinned, pinnedVia := p.Pinned, p.PinnedVia; pinned != "" {
		ok, e := offer(pinned, pinnedVia)
		if ok {
			return pinned, pinnedVia, tried, nil
		}
		if p.Pinned != "" {
			return "", "", tried, e
		}
		err = e
	}
	sources := []func() (string, string, error){
		func() (string, string, error) {
			if v := trailer(pr.Body, "Mergewake-Notify-Lane"); laneName.MatchString(v) {
				return v, "notify-lane trailer", nil
			}
			return "", "", nil
		},
		func() (string, string, error) { return s.conflictEscTaskLane(ctx, r, pr) },
		func() (string, string, error) { return s.deskLane(), "desk", nil },
	}
	seen := map[string]bool{}
	for _, l := range tried {
		seen[l] = true
	}
	for _, source := range sources {
		l, v, e := source()
		if e != nil {
			return "", "", tried, &conflictEscRouteError{e}
		}
		if l == "" || seen[l] {
			continue
		}
		seen[l] = true
		ok, e := offer(l, v)
		if ok {
			return l, v, tried, nil
		}
		err = e
		if p.Pinned != "" {
			return "", "", tried, err
		}
	}
	return "", "", tried, err
}

// conflictEscTaskLane is the current lane of the hk task the PR names
// (Mergewake-Task, body then head commit). Empty when the PR names no task
// or the task no valid lane; a read error is returned, never read as absence.
func (s *Service) conflictEscTaskLane(ctx context.Context, r config.Repo, pr github.Pull) (string, string, error) {
	task, e := s.prTask(ctx, r, pr)
	if e != nil || task == "" {
		return "", "", e
	}
	l, ok := s.Sink.(interface {
		Lane(context.Context, string) (string, error)
	})
	if !ok {
		return "", "", nil
	}
	lane, e := l.Lane(ctx, task)
	if e != nil {
		return "", "", e
	}
	if !laneName.MatchString(lane) {
		return "", "", nil
	}
	return lane, "task " + task, nil
}

// deskLane is the desk: notifier.fallback_lane, else notifier.lane.
func (s *Service) deskLane() string {
	if s.Config.Notifier.FallbackLane != "" {
		return s.Config.Notifier.FallbackLane
	}
	return s.Config.Notifier.Lane
}
