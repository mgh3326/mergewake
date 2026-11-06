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
)

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

// reviewStalled sends the one "review stalled" alert for a head that has
// waited more than reviewStallAfter, to notifyLane's lane, and never again for
// that head. sent is false while the wait is young or already alerted.
func (s *Service) reviewStalled(ctx context.Context, r config.Repo, pr github.Pull, v gateVerdict, since time.Time) (string, string, bool, error) {
	waited := s.now().Sub(since)
	if waited <= reviewStallAfter {
		return "", "", false, nil
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		return "", "", false, e
	}
	key := fmt.Sprintf("stalled %d %s", pr.Number, pr.Head.SHA)
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
	text := fmt.Sprintf("[mergewake] BLOCKED %s#%d head=%s | review stalled: waited %s for its review | %s | lane=%s via %s",
		r.Name, pr.Number, short7(pr.Head.SHA), waited.Round(time.Minute), s.reviewState(r, pr, v), shown, via)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-review-stalled-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA),
		Text: text,
		Lane: lane,
		Type: eventType("blocked"),
	}); e != nil {
		if ae := sent.add("decided " + key); ae != nil {
			return "", "", false, errors.Join(e, ae)
		}
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
	state := fmt.Sprintf("passes posted %d/%d", v.HeadPasses, v.PassesNeeded)
	paths, _ := filepath.Glob(filepath.Join(s.state(r.Name), "reviewer-jobs", fmt.Sprintf("%d-p*.json", pr.Number)))
	var labels []string
	for _, path := range paths {
		var rj reviewerJob
		if found, e := fsutil.ReadJSON(path, &rj); e == nil && found && rj.Head == pr.Head.SHA && rj.Label != "" {
			labels = append(labels, fmt.Sprintf("%s (pass %d)", rj.Label, rj.Pass))
		}
	}
	if len(labels) == 0 {
		return state + ", no reviewer job known"
	}
	return state + ", reviewer job " + strings.Join(labels, ", ")
}
