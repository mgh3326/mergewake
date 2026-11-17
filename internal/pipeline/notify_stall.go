package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

const prIdleCause = "idle"
const prCheckRedCause = "check_red"

type prActivityClock struct {
	Since        time.Time `json:"since"`
	Head         string    `json:"head"`
	LastActivity time.Time `json:"last_activity"`
	Sent         bool      `json:"sent,omitempty"`
}

func redCheckClock(prior prActivityClock, exists bool, head string, now time.Time) prActivityClock {
	if !exists || prior.Head != head {
		return prActivityClock{Since: now, Head: head}
	}
	return prior
}

func (s *Service) notifyPRStalls(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy) error {
	file := filepath.Join(s.state(r.Name), fmt.Sprintf("pr-%d-stall-state.json", pr.Number))
	var clocks map[string]prActivityClock
	if _, err := fsutil.ReadJSON(file, &clocks); err != nil {
		return err
	}
	if clocks == nil {
		clocks = map[string]prActivityClock{}
	}
	now := s.now().UTC()
	states, err := s.GH.CheckStates(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, name := range p.RequiredChecks {
		cause := prCheckRedCause + ":" + name
		if states[name] != github.Red {
			continue
		}
		active[cause] = true
		entry := redCheckClock(clocks[cause], clocks[cause].Head != "", pr.Head.SHA, now)
		if now.Sub(entry.Since) >= s.Config.Daemon.PR.Notify.CheckRedWindow() && !entry.Sent {
			if err := s.sendPRStallAt(ctx, r, pr, cause, s.Config.Daemon.PR.Notify.CheckRedWindow(), now.Sub(entry.Since), entry.Since); err != nil {
				return err
			}
			entry.Sent = true
		}
		clocks[cause] = entry
	}
	for cause := range clocks {
		if strings.HasPrefix(cause, prCheckRedCause+":") && !active[cause] {
			delete(clocks, cause)
		}
	}
	return fsutil.WriteJSON(file, clocks)
}

func (s *Service) notifyPRIdle(ctx context.Context, r config.Repo, pr github.Pull) error {
	file := filepath.Join(s.state(r.Name), fmt.Sprintf("pr-%d-stall-state.json", pr.Number))
	clocks := map[string]prActivityClock{}
	if _, err := fsutil.ReadJSON(file, &clocks); err != nil {
		return err
	}
	now := s.now().UTC()
	last := parsedTime(pr.CreatedAt)
	if updated := parsedTime(pr.UpdatedAt); updated != nil && (last == nil || updated.After(*last)) {
		last = updated
	}
	if last == nil {
		return nil
	}
	previous, hasPrevious := clocks[prIdleCause]
	fb, err := s.GH.ReviewFeedback(ctx, r.Name, pr.Number)
	if err != nil {
		return err
	}
	for _, thread := range fb.Threads {
		for _, comment := range thread.Comments {
			if !comment.CreatedAt.IsZero() && comment.CreatedAt.After(*last) {
				t := comment.CreatedAt
				last = &t
			}
		}
	}
	history, err := s.GH.PullReviewActivity(ctx, r.Name, pr.Number)
	if err != nil {
		return err
	}
	for _, submitted := range history {
		if submitted.After(*last) {
			t := submitted
			last = &t
		}
	}
	entry, ok := clocks[prIdleCause]
	if !ok || entry.Head != pr.Head.SHA {
		entry = prActivityClock{Head: pr.Head.SHA, LastActivity: *last, Since: *last}
	} else if hasPrevious && previous.Head == pr.Head.SHA && previous.LastActivity.After(*last) {
		entry = previous
	} else if last.After(entry.LastActivity) {
		entry = prActivityClock{Head: pr.Head.SHA, LastActivity: *last, Since: *last}
	}
	if now.Sub(entry.Since) >= s.Config.Daemon.PR.Notify.IdleWindow() && !entry.Sent {
		if err := s.sendPRStall(ctx, r, pr, prIdleCause, s.Config.Daemon.PR.Notify.IdleWindow(), now.Sub(entry.Since)); err != nil {
			return err
		}
		entry.Sent = true
	}
	clocks[prIdleCause] = entry
	return fsutil.WriteJSON(file, clocks)
}

func (s *Service) sendPRStall(ctx context.Context, r config.Repo, pr github.Pull, cause string, threshold, elapsed time.Duration) error {
	return s.sendPRStallAt(ctx, r, pr, cause, threshold, elapsed, s.now().Add(-elapsed))
}

func (s *Service) sendPRStallAt(ctx context.Context, r config.Repo, pr github.Pull, cause string, threshold, elapsed time.Duration, since time.Time) error {
	lane, via, err := s.notifyLane(ctx, r, pr)
	if err != nil {
		return err
	}
	text := fmt.Sprintf("[mergewake] STALLED %s#%d head=%s | cause=%s | elapsed=%s (threshold %s) | lane=%s via %s", r.Name, pr.Number, short7(pr.Head.SHA), cause, elapsed.Round(time.Minute), threshold, lane, via)
	if err := s.Notifier.Notify(ctx, integrations.Event{ID: fmt.Sprintf("mergewake-pr-stalled-%s-%d-%s-%s-%d", r.Name, pr.Number, pr.Head.SHA, cause, since.UnixNano()), Text: text, Lane: lane, Type: eventType("blocked")}); err != nil {
		return fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(err))
	}
	return s.audit(r.Name, "pr-notify", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "state": "stalled", "cause": cause, "lane": lane, "via": via, "sent": true}, false)
}

// reviewStallAfter is how long one head may wait for its review before the
// design session hears about it once.
const reviewStallAfter = 60 * time.Minute

// splitInternalWaits drops the reasons only mergewake itself can clear and
// reports whether it dropped any. review_missing is mergewake's own queue
// state, unless the review was blocked with a detail (a missing task trailer,
// say): that waits on the author and stays. Every other reason is either the
// author's to fix or needs a human decision, and is sent.
func splitInternalWaits(reasons []string, block string) (acted []string, waiting bool) {
	for _, reason := range reasons {
		if reason == "review_missing" && block == "" {
			waiting = true
			continue
		}
		acted = append(acted, reason)
	}
	return acted, waiting
}

// reviewWait is when a head was first seen waiting for its review.
type reviewWait struct {
	First time.Time `json:"first"`
}

func (s *Service) reviewWaitsFile(repo string) string {
	return filepath.Join(s.state(repo), "pr-review-wait.json")
}

// reviewWaitSince returns when this (PR, head) was first seen with an
// internal review wait, recording it on the first sight. The clock survives
// daemon restarts; entries untouched for a week are dropped.
func (s *Service) reviewWaitSince(r config.Repo, pr github.Pull) (time.Time, error) {
	file := s.reviewWaitsFile(r.Name)
	defer lockPath(file)()
	waits := map[string]reviewWait{}
	if found, e := fsutil.ReadJSON(file, &waits); found && e != nil {
		return time.Time{}, fmt.Errorf("%s: %w", file, e)
	} else if e != nil {
		return time.Time{}, e
	}
	now := s.now().UTC()
	key := fmt.Sprintf("%d %s", pr.Number, pr.Head.SHA)
	if w, ok := waits[key]; ok {
		return w.First, nil
	}
	for k, w := range waits {
		if w.First.Before(now.Add(-7 * 24 * time.Hour)) {
			delete(waits, k)
		}
	}
	waits[key] = reviewWait{First: now}
	return now, fsutil.WriteJSON(file, waits)
}

// NotifyReviewStall sends the one review-stall alert for a head whose review
// did not run this pass, naming the wait that kept it: held, quota-stopped,
// pending or a skipped poll. It runs where a review reached no verdict, so it
// sends the alert and stops: findings delivery and Land never run for a review
// that posted nothing. The wait is read from wait, the error the review
// returned; the once-per-head sent key keeps a delivered alert from repeating,
// and a delivery the notifier refuses is retried on the next pass.
func (s *Service) NotifyReviewStall(ctx context.Context, r config.Repo, pr github.Pull, o Options, wait error) error {
	if !s.Config.Daemon.PR.Notify.On() || !o.Reporting() {
		return nil
	}
	named := reviewWaitName(wait)
	if named == "" {
		return nil
	}
	since, e := s.reviewWaitSince(r, pr)
	if e != nil {
		return e
	}
	record := map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "state": "blocked", "reasons": []string{"review_stalled"}, "wait": named}
	lane, via, sent, e := s.reviewStalled(ctx, r, pr, s.reviewJobState(r, pr), named, since)
	if e != nil {
		if !errors.Is(e, errNotifyDelivery) {
			return e
		}
		// A refused delivery is no review failure: the next pass sends it
		// again, and the row names the reason it did not land.
		record["via"], record["error"] = "delivery-failed", e.Error()
		return s.audit(r.Name, "pr-notify", record, false)
	}
	if !sent {
		return nil
	}
	record["lane"], record["via"], record["sent"] = lane, via, true
	return s.audit(r.Name, "pr-notify", record, false)
}

// reviewWaitName names the wait a RetryReview error reports, with the reason
// its audit row recorded when the error carries one: "held (round-hold)",
// "quota-stopped (pool <pool>)", "pending" or "skipped". Empty when err keeps
// no head out of review.
func reviewWaitName(err error) string {
	var text waitText
	kind := ""
	switch {
	case errors.Is(err, ErrReviewHeld):
		kind = "held"
	case errors.Is(err, ErrReviewQuotaStopped):
		kind = "quota-stopped"
	case errors.Is(err, ErrReviewPending):
		kind = "pending"
	case errors.Is(err, github.ErrPollSkipped):
		kind = "skipped"
	default:
		return ""
	}
	if errors.As(err, &text) && text != "" {
		return string(text)
	}
	return kind
}

// reviewStalled sends the one "review stalled" alert for a head that has
// waited more than reviewStallAfter, to notifyLane's lane, and never again for
// that head. progress names how far the head's review got, wait the wait that
// kept it from running and is empty on the queue wait NotifyPR reports. sent
// is false while the wait is young or already alerted.
func (s *Service) reviewStalled(ctx context.Context, r config.Repo, pr github.Pull, progress, wait string, since time.Time) (string, string, bool, error) {
	waited := s.now().Sub(since)
	if waited <= reviewStallAfter {
		return "", "", false, nil
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		return "", "", false, e
	}
	key := fmt.Sprintf("stalled %d %s", pr.Number, pr.Head.SHA)
	// "decided" covers a state file from a version that parked a refused
	// delivery instead of retrying it.
	if sent.has(key) || sent.has("decided "+key) {
		return "", "", false, nil
	}
	lane, via, e := s.notifyLane(ctx, r, pr)
	if e != nil {
		return "", "", false, e
	}
	shown := lane
	if shown == "" {
		shown = "-"
	}
	stalled := fmt.Sprintf("waited %s for its review", waited.Round(time.Minute))
	if wait != "" {
		stalled += ", " + wait
	}
	text := fmt.Sprintf("[mergewake] BLOCKED %s#%d head=%s | review stalled: %s | %s | lane=%s via %s",
		r.Name, pr.Number, short7(pr.Head.SHA), stalled, progress, shown, via)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-review-stalled-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA),
		Text: text,
		Lane: lane,
		Type: eventType("blocked"),
	}); e != nil {
		// No sent key: the next pass retries the alert rather than losing it.
		return "", "", false, fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(e))
	}
	if e = sent.add(key); e != nil {
		return "", "", false, e
	}
	return lane, via, true, nil
}

// reviewState says how far the head's review got: passes posted and the
// reviewer job recorded for it, when there is one.
func (s *Service) reviewState(r config.Repo, pr github.Pull, v gateVerdict) string {
	return fmt.Sprintf("passes posted %d/%d, %s", v.HeadPasses, v.PassesNeeded, s.reviewJobState(r, pr))
}

// reviewJobState names the reviewer jobs recorded for the PR's current head,
// one "<label> (pass <n>)" each, as a pass that reached no verdict can.
func (s *Service) reviewJobState(r config.Repo, pr github.Pull) string {
	labels := s.reviewerJobLabels(r, pr)
	if len(labels) == 0 {
		return "no reviewer job known"
	}
	return "reviewer job " + strings.Join(labels, ", ")
}

// reviewerJobLabels names the reviewer jobs recorded for the PR's current
// head, one "<label> (pass <n>)" each.
func (s *Service) reviewerJobLabels(r config.Repo, pr github.Pull) []string {
	paths, _ := filepath.Glob(filepath.Join(s.state(r.Name), "reviewer-jobs", fmt.Sprintf("%d-p*.json", pr.Number)))
	var labels []string
	for _, path := range paths {
		var rj reviewerJob
		if found, e := fsutil.ReadJSON(path, &rj); e == nil && found && rj.Head == pr.Head.SHA && rj.Label != "" {
			labels = append(labels, fmt.Sprintf("%s (pass %d)", rj.Label, rj.Pass))
		}
	}
	return labels
}
