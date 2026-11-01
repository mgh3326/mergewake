package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

var laneName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

// NotifyPR emits one lane event when the merge-gate verdict for a PR head
// becomes ready or blocked: READY once every required check is green, no
// review thread is unresolved, the review -done marker exists, the base is an
// ancestor of the head, and the PR is mergeable; BLOCKED with the
// closed-vocabulary reasons that hold it. Pending checks emit nothing. Each
// (PR, head, verdict) notifies once across restarts, except that a READY PR
// still open and READY on the same head is notified again every
// daemon.pr.notify.renotify_after, at most renotify_max times: a READY nobody
// merged must not vanish. A merged or closed PR is never notified here.
//
// The pr role notifies under shadow too — telling a lane is not merging — while
// dry-run only prints the decision and writes no keys or events.
func (s *Service) NotifyPR(ctx context.Context, r config.Repo, n int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "pr-notify-error", map[string]any{"pr": n, "error": integrations.Detail(retErr)}, o.DryRun))
		}
	}()
	if !s.Config.Daemon.PR.Notify.On() {
		return nil
	}
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if pr.State != "open" || pr.Draft || pr.Base.Ref != r.Branch {
		return nil
	}
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return e
	}
	v, e := s.mergeGate(ctx, r, pr, p, head)
	if e != nil {
		return e
	}
	record := struct {
		PR      int      `json:"pr"`
		Head    string   `json:"head"`
		State   string   `json:"state"`
		Reasons []string `json:"reasons,omitempty"`
		Lane    string   `json:"lane,omitempty"`
		Via     string   `json:"via,omitempty"`
		Sent    bool     `json:"sent"`
		Deduped bool     `json:"deduped,omitempty"`
		Error   string   `json:"error,omitempty"`
	}{PR: n, Head: pr.Head.SHA, State: v.State, Reasons: v.Reasons}
	if v.State == "pending" || o.DryRun {
		return s.audit(r.Name, "pr-notify", record, o.DryRun)
	}
	summary := pr.Title
	if v.State == "blocked" {
		summary = strings.Join(v.Reasons, "; ")
	}
	if n := v.Review.Findings.DismissedBlocking; v.State == "ready" && n > 0 {
		// A dismissed critical/high no longer holds the gate; the desk still
		// checks the dismissal's evidence before merging.
		summary += fmt.Sprintf(" | %d critical/high dismissed: check the evidence", n)
	}
	lane, via, sent, e := s.laneEvent(ctx, r, pr, v.State, summary)
	if e != nil {
		if !errors.Is(e, errNotifyDelivery) {
			return e
		}
		// The decision to stop retrying a failing delivery is logged once per
		// (PR, head, verdict): laneEvent dedupes further attempts.
		record.Via, record.Error = "delivery-failed", e.Error()
		return s.audit(r.Name, "pr-notify", record, false)
	}
	if lane == "" && via == "" {
		// A skip or failure decision for this verdict was already logged.
		return nil
	}
	record.Lane, record.Via, record.Sent, record.Deduped = lane, via, sent, !sent
	return s.audit(r.Name, "pr-notify", record, false)
}

// errNotifyDelivery marks a lane event the notifier refused. The first
// failure is logged with the captured stderr and recorded so the loop does
// not retry — and re-log — the same verdict every pass.
var errNotifyDelivery = errors.New("notify delivery failed")

// laneEvent resolves the owning lane and emits one deduped lane event for the
// verdict: READY, BLOCKED or MERGED. The sent key covers the state, PR, head
// and summary so a changed reason set re-notifies while an identical verdict
// stays quiet. A PR with no owning lane routes to the configured fallback
// lane; a delivery the notifier fails is retried once on that lane, then
// parked with one logged decision per (PR, head, verdict) instead of failing
// every loop. An empty via means a decision was already logged and the
// caller stays silent.
func (s *Service) laneEvent(ctx context.Context, r config.Repo, pr github.Pull, state, summary string) (string, string, bool, error) {
	lane, via, e := s.notifyLane(ctx, r, pr)
	if e != nil {
		return "", "", false, e
	}
	sha7 := pr.Head.SHA
	if len(sha7) > 7 {
		sha7 = sha7[:7]
	}
	textFor := func(l, v string) string {
		shown := l
		if shown == "" {
			shown = "-"
		}
		return fmt.Sprintf("[mergewake] %s %s#%d head=%s | %s | lane=%s via %s",
			strings.ToUpper(state), r.Name, pr.Number, sha7, summary, shown, v)
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		return "", "", false, e
	}
	// The verdict key dedupes per (PR, head, verdict): a BLOCKED reason set is
	// part of the verdict, while a retitled READY is the same verdict.
	key := fmt.Sprintf("%s %d %s", state, pr.Number, pr.Head.SHA)
	if state == "blocked" {
		key += " " + summary
	}
	if sent.has(key) {
		if state == "ready" {
			return s.renotifyReady(ctx, r, pr, key, lane, via, textFor)
		}
		return lane, via, false, nil
	}
	if sent.has("decided " + key) {
		return "", "", false, nil
	}
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	// A changed BLOCKED reason set is a new verdict under the same head: fold
	// the summary into the event ID so a notifier deduping on ID cannot drop it
	// while the sent key records delivery.
	id := fmt.Sprintf("mergewake-%s-%s-%d-%s", state, r.Name, pr.Number, pr.Head.SHA)
	if state == "blocked" {
		sum := sha256.Sum256([]byte(summary))
		id += "-" + hex.EncodeToString(sum[:4])
	}
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   id,
		Text: textFor(lane, via),
		Lane: lane,
		Type: eventType(state),
	}); e != nil {
		fallback := s.Config.Notifier.FallbackLane
		if fallback != "" && fallback != lane {
			// The resolved lane may not exist on this host; the fallback lane
			// gets one try under a distinct ID before the verdict is parked.
			if fb := s.Notifier.Notify(reportCtx, integrations.Event{
				ID:   id + "-fallback",
				Text: textFor(fallback, via+"+fallback"),
				Lane: fallback,
				Type: eventType(state),
			}); fb == nil {
				if ae := sent.add(key); ae != nil {
					return "", "", false, ae
				}
				if state == "ready" {
					if ae := s.startReadyClock(r, key, fallback); ae != nil {
						return "", "", false, ae
					}
				}
				return fallback, via + "+fallback", true, nil
			}
		}
		if ae := sent.add("decided " + key); ae != nil {
			return "", "", false, errors.Join(e, ae)
		}
		return "", "", false, fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(e))
	}
	if e = sent.add(key); e != nil {
		return "", "", false, e
	}
	if state == "ready" {
		if e = s.startReadyClock(r, key, lane); e != nil {
			return "", "", false, e
		}
	}
	return lane, via, true, nil
}

// eventType is the panewire notification type a verdict is declared with, so
// a READY or BLOCKED stays unread on the receiving lane until it is acked.
func eventType(state string) string {
	switch state {
	case "ready", "blocked":
		return state
	}
	return ""
}

// readyClock is when a READY verdict was last sent, to which lane, and how
// many reminders followed it.
type readyClock struct {
	Lane    string    `json:"lane"`
	First   time.Time `json:"first"`
	Last    time.Time `json:"last"`
	Repeats int       `json:"repeats"`
}

func (s *Service) readyClocksFile(repo string) string {
	return filepath.Join(s.state(repo), "pr-ready-renotify.json")
}

func (s *Service) loadReadyClocks(repo string) (map[string]readyClock, error) {
	clocks := map[string]readyClock{}
	b, e := os.ReadFile(s.readyClocksFile(repo))
	if errors.Is(e, fs.ErrNotExist) {
		return clocks, nil
	}
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(b, &clocks); e != nil {
		return nil, fmt.Errorf("%s: %w", s.readyClocksFile(repo), e)
	}
	return clocks, nil
}

// saveReadyClocks keeps the clocks touched in the last week.
func (s *Service) saveReadyClocks(repo string, clocks map[string]readyClock) error {
	cutoff := s.now().Add(-7 * 24 * time.Hour)
	for k, c := range clocks {
		if c.Last.Before(cutoff) {
			delete(clocks, k)
		}
	}
	b, e := json.Marshal(clocks)
	if e != nil {
		return e
	}
	return fsutil.Atomic(s.readyClocksFile(repo), b, 0600)
}

// startReadyClock records a first READY send for key.
func (s *Service) startReadyClock(r config.Repo, key, lane string) error {
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		return e
	}
	now := s.now().UTC()
	clocks[key] = readyClock{Lane: lane, First: now, Last: now}
	return s.saveReadyClocks(r.Name, clocks)
}

// renotifyReady repeats an already sent READY for a PR that is still open and
// READY on the same head, once renotify_after has passed since the last send
// and at most renotify_max times. A READY sent before the clock existed
// starts its clock now, so an upgrade does not burst reminders. A failed
// reminder counts against the bound, so delivery trouble cannot loop.
func (s *Service) renotifyReady(ctx context.Context, r config.Repo, pr github.Pull, key, lane, via string, textFor func(l, v string) string) (string, string, bool, error) {
	every, max := s.Config.Daemon.PR.Notify.Renotify()
	if every <= 0 || max <= 0 {
		return lane, via, false, nil
	}
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		return "", "", false, e
	}
	now := s.now().UTC()
	c, ok := clocks[key]
	if !ok {
		clocks[key] = readyClock{Lane: lane, First: now, Last: now}
		return lane, via, false, s.saveReadyClocks(r.Name, clocks)
	}
	if c.Repeats >= max || now.Sub(c.Last) < every {
		return lane, via, false, nil
	}
	c.Repeats++
	c.Last = now
	clocks[key] = c
	if e = s.saveReadyClocks(r.Name, clocks); e != nil {
		return "", "", false, e
	}
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	rvia := fmt.Sprintf("%s; reminder %d/%d, unmerged for %s", via, c.Repeats, max, now.Sub(c.First).Round(time.Minute))
	e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-ready-%s-%d-%s-reminder-%d", r.Name, pr.Number, pr.Head.SHA, c.Repeats),
		Text: textFor(lane, rvia),
		Lane: lane,
		Type: "ready",
	})
	if e != nil {
		return "", "", false, fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(e))
	}
	return lane, rvia, true, nil
}

// notifyLane resolves the destination lane without guessing: the handoffkeep
// task's lane via a Mergewake-Task trailer (PR body, then head commit message),
// an explicit Mergewake-Notify-Lane trailer in the same order, then the
// configured notifier lane. The returned rule names the choosing source and is
// recorded in the event text; a sink read error retries rather than reroutes.
func (s *Service) notifyLane(ctx context.Context, r config.Repo, pr github.Pull) (string, string, error) {
	message := ""
	fetched := false
	headMessage := func() (string, error) {
		if !fetched {
			fetched = true
			c, e := s.GH.Commit(ctx, r.Name, pr.Head.SHA)
			if e != nil {
				return "", e
			}
			message = c.Commit.Message
		}
		return message, nil
	}
	task := ""
	if t := trailer(pr.Body, "Mergewake-Task"); integrations.TaskID.MatchString(t) {
		task = t
	}
	if task == "" {
		msg, e := headMessage()
		if e != nil {
			return "", "", e
		}
		if t := trailer(msg, "Mergewake-Task"); integrations.TaskID.MatchString(t) {
			task = t
		}
	}
	if task != "" {
		if l, ok := s.Sink.(interface {
			Lane(context.Context, string) (string, error)
		}); ok {
			lane, e := l.Lane(ctx, task)
			if e != nil {
				return "", "", e
			}
			if laneName.MatchString(lane) {
				return lane, "task " + task, nil
			}
		}
	}
	notify := trailer(pr.Body, "Mergewake-Notify-Lane")
	if notify == "" {
		msg, e := headMessage()
		if e != nil {
			return "", "", e
		}
		notify = trailer(msg, "Mergewake-Notify-Lane")
	}
	if laneName.MatchString(notify) {
		return notify, "notify-lane trailer", nil
	}
	if laneName.MatchString(r.Lane) {
		return r.Lane, "repo lane", nil
	}
	fallback := s.Config.Notifier.FallbackLane
	if fallback == "" {
		fallback = s.Config.Notifier.Lane
	}
	if fallback != "" {
		return fallback, "fallback", nil
	}
	return "", "no-lane", nil
}
