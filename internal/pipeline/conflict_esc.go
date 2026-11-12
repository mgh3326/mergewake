package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
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
	// Pinned is the lane the ESC was last offered to, written before the
	// offer: a delivery that failed without saying whether the event landed,
	// or a process that died before it wrote the receipt, leaves it set. Only
	// that lane is tried again, under the same event id, until it accepts
	// (panewire answers a repeat as delivered) or refuses outright; the route
	// is not read again while it is set.
	Pinned string `json:"pinned,omitempty"`
	// PinnedVia is the rule that chose Pinned.
	PinnedVia string `json:"pinned_via,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// conflictEscState is <state_dir>/<repo>/conflict-esc/<pr>.json. Receipts are
// kept for the life of the file, so a head that returns, a PR that is
// reopened or a restart never re-sends.
type conflictEscState struct {
	Delivered          map[string]conflictEscReceipt `json:"delivered,omitempty"`
	Pending            map[string]conflictEscPending `json:"pending,omitempty"`
	OwnerHead          string                        `json:"owner_head,omitempty"`
	OwnerSince         time.Time                     `json:"owner_since,omitzero"`
	OwnerPending       *conflictEscPending           `json:"owner_pending,omitempty"`
	OwnerPendingHead   string                        `json:"owner_pending_head,omitempty"`
	OwnerPendingReason string                        `json:"owner_pending_reason,omitempty"`
	OwnerSuppressed    map[string]bool               `json:"owner_suppressed,omitempty"`
	AuthorSuppressed   map[string]bool               `json:"author_suppressed,omitempty"`
	Suppressed         map[string]bool               `json:"suppressed,omitempty"`
	OwnerSent          bool                          `json:"owner_sent,omitempty"`
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
	if st.OwnerSuppressed == nil {
		st.OwnerSuppressed = map[string]bool{}
	}
	if st.AuthorSuppressed == nil {
		st.AuthorSuppressed = map[string]bool{}
	}
	if st.Suppressed == nil {
		st.Suppressed = map[string]bool{}
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

// EscalateConflict sends the one ESC of a PR head that conflicts with its
// base (hk 3712; docs/config.md, "Conflict ESC"). The pr role calls it for
// every evaluated open PR before review and settle, and NotifyPR calls it
// before its draft and gate returns, so a conflicting head is told whether
// its review is held, pending, quota-stopped or not started, and also as a
// draft. It never changes review, gate or merge.
//
// The ESC first goes to the PR author's findings route. The owner lane gets
// one notice for the PR if that route is gone, or after the same conflicting
// head persists for the configured window. A destination that refuses the
// lane outright is not silently rerouted; delivery retries the pinned route
// under the same event id. Unknown mergeability neither sends nor clears. A
// dry run, or notify switched off, does nothing.
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
	p, waiting := st.Pending[head]
	now := s.now().UTC()
	_, authorDelivered := st.Delivered[head]
	if st.OwnerSent && (authorDelivered || st.Suppressed[head]) {
		if st.OwnerHead == head && now.Sub(st.OwnerSince) >= s.Config.Daemon.PR.Notify.ConflictOwnerWindow() && !st.OwnerSuppressed[head] {
			st.OwnerSuppressed[head] = true
			if e = fsutil.WriteJSON(file, st); e != nil {
				return e
			}
			return s.audit(r.Name, "conflict-esc-owner-suppressed", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "reason": "owner was already notified for PR"}, false)
		}
		return nil
	}
	ownerTimerDue := st.OwnerHead == head && now.Sub(st.OwnerSince) >= s.Config.Daemon.PR.Notify.ConflictOwnerWindow()
	if waiting && now.Before(p.Next) && !ownerTimerDue {
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
	if st.OwnerHead != head {
		st.OwnerHead, st.OwnerSince = head, now
		if st.OwnerPending != nil && st.OwnerPendingHead != head {
			st.OwnerPending = nil
			st.OwnerPendingHead, st.OwnerPendingReason = "", ""
		}
		if e = fsutil.WriteJSON(file, st); e != nil {
			return e
		}
	}
	ownerDue := now.Sub(st.OwnerSince) >= s.Config.Daemon.PR.Notify.ConflictOwnerWindow()
	route := findingsRoute{}
	authorGone := false
	if waiting && p.Pinned != "" {
		route.lane, route.via = p.Pinned, p.PinnedVia
		authorGone = p.PinnedVia == "owner lane (author route gone)"
	} else {
		route, e = s.findingsRoute(ctx, r, fresh)
		if e != nil {
			if ownerDue && !st.OwnerSent {
				return s.sendConflictOwner(ctx, file, &st, r, pr.Number, head, "author route unreadable after conflict window", now)
			}
			return fmt.Errorf("conflict author route: %w", e)
		}
		authorGone = route.via == "unknown author; desk"
	}
	ownerDue = authorGone || ownerDue
	var err error
	ownerDeliveredThisCall := false
	shouldSendAuthor := !authorDelivered && !(authorGone && st.OwnerSent) && (!waiting || !now.Before(p.Next))
	if shouldSendAuthor {
		p.EventID = conflictEscID(r.Name, pr.Number, head)
		lane, via := route.lane, route.via
		if authorGone {
			lane, via = s.ownerLane(r), "owner lane (author route gone)"
		}
		p.Pinned, p.PinnedVia = lane, via
		st.Pending[head] = p
		if err = fsutil.WriteJSON(file, st); err != nil {
			return err
		}
		text := fmt.Sprintf("ESC [mergewake] %s#%d head=%s conflict with %s: rebase or merge base, then push | lane=%s via %s",
			r.Name, pr.Number, short7(fresh.Head.SHA), fresh.Base.Ref, lane, via)
		if err = s.Notifier.Notify(ctx, integrations.Event{ID: p.EventID, Text: text, Lane: lane, Type: "decision"}); err != nil {
			p.Attempts++
			p.Next = now.Add(conflictEscBackoff(p.Attempts))
			p.LastError = clip(integrations.Detail(err), conflictEscErrorMax)
			st.Pending[head] = p
			if writeErr := fsutil.WriteJSON(file, st); writeErr != nil {
				return errors.Join(err, writeErr)
			}
			return errors.Join(err, s.audit(r.Name, "conflict-esc-failed", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "event_id": p.EventID, "lane": lane, "error": p.LastError}, false))
		}
		delete(st.Pending, head)
		st.Delivered[head] = conflictEscReceipt{EventID: p.EventID, Lane: lane, Via: via, At: now}
		if via == "owner lane (author route gone)" {
			st.OwnerSent = true
			ownerDeliveredThisCall = true
		}
		if err = fsutil.WriteJSON(file, st); err != nil {
			return err
		}
		if err = s.audit(r.Name, "conflict-esc", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "lane": lane, "via": via, "event_id": p.EventID}, false); err != nil {
			return err
		}
	} else if authorDelivered && !st.AuthorSuppressed[head] {
		st.AuthorSuppressed[head] = true
		if err = fsutil.WriteJSON(file, st); err != nil {
			return err
		}
		if err = s.audit(r.Name, "conflict-esc-suppressed", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "reason": "author already notified for head"}, false); err != nil {
			return err
		}
	} else if authorGone && st.OwnerSent && !authorDelivered && !st.Suppressed[head] {
		st.Suppressed[head] = true
		if err = fsutil.WriteJSON(file, st); err != nil {
			return err
		}
		if err = s.audit(r.Name, "conflict-esc-suppressed", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "reason": "owner already notified while author route was gone"}, false); err != nil {
			return err
		}
	}
	if ownerDue && !st.OwnerSent {
		return s.sendConflictOwner(ctx, file, &st, r, pr.Number, head, map[bool]string{true: "author route gone", false: "conflict persisted"}[authorGone], now)
	}
	if ownerDue && st.OwnerSent && !ownerDeliveredThisCall && !st.OwnerSuppressed[head] {
		st.OwnerSuppressed[head] = true
		if err = fsutil.WriteJSON(file, st); err != nil {
			return err
		}
		return s.audit(r.Name, "conflict-esc-owner-suppressed", map[string]any{"repo": r.Name, "pr": pr.Number, "head": head, "reason": "owner was already notified for PR"}, false)
	}
	return nil
}

func (s *Service) sendConflictOwner(ctx context.Context, file string, st *conflictEscState, r config.Repo, pr int, head, reason string, now time.Time) error {
	if st.OwnerPending == nil {
		st.OwnerPending = &conflictEscPending{EventID: fmt.Sprintf("mergewake-esc-%s-%d-owner-%s", r.Name, pr, short7(head)), Pinned: s.ownerLane(r), PinnedVia: "owner follow-up"}
		st.OwnerPendingHead, st.OwnerPendingReason = head, reason
		if err := fsutil.WriteJSON(file, st); err != nil {
			return err
		}
	}
	p := st.OwnerPending
	if now.Before(p.Next) {
		return nil
	}
	head, reason = st.OwnerPendingHead, st.OwnerPendingReason
	text := fmt.Sprintf("ESC [mergewake] %s#%d remains conflicted at head=%s | lane=%s via %s", r.Name, pr, short7(head), p.Pinned, p.PinnedVia)
	if err := s.Notifier.Notify(ctx, integrations.Event{ID: p.EventID, Text: text, Lane: p.Pinned, Type: "decision"}); err != nil {
		p.Attempts++
		p.Next = now.Add(conflictEscBackoff(p.Attempts))
		p.LastError = clip(integrations.Detail(err), conflictEscErrorMax)
		if writeErr := fsutil.WriteJSON(file, st); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		return errors.Join(err, s.audit(r.Name, "conflict-esc-failed", map[string]any{"repo": r.Name, "pr": pr, "head": head, "event_id": p.EventID, "lane": p.Pinned, "kind": "owner", "error": p.LastError}, false))
	}
	eventID, lane := p.EventID, p.Pinned
	st.OwnerPending = nil
	st.OwnerPendingHead, st.OwnerPendingReason = "", ""
	st.OwnerSent = true
	if err := fsutil.WriteJSON(file, st); err != nil {
		return err
	}
	return s.audit(r.Name, "conflict-esc-owner", map[string]any{"repo": r.Name, "pr": pr, "head": head, "lane": lane, "event_id": eventID, "reason": reason}, false)
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
	changed := false
	if p, ok := st.Pending[head]; ok && p.Pinned == "" {
		delete(st.Pending, head)
		changed = true
	}
	if st.OwnerHead == head && !st.OwnerSent {
		st.OwnerHead, st.OwnerSince = "", time.Time{}
		changed = true
	}
	if st.OwnerPending != nil && st.OwnerPendingHead == head {
		st.OwnerPending = nil
		st.OwnerPendingHead, st.OwnerPendingReason = "", ""
		changed = true
	}
	if !changed {
		return nil
	}
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

// deskLane is the desk: notifier.fallback_lane, else notifier.lane.
func (s *Service) deskLane() string {
	if s.Config.Notifier.FallbackLane != "" {
		return s.Config.Notifier.FallbackLane
	}
	return s.Config.Notifier.Lane
}
