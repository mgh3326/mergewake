package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// ciStarter is an optional GitHub capability: whether any check or status
// exists for a head. Without it CI counts as started.
type ciStarter interface {
	CIStarted(ctx context.Context, repo, sha string) (bool, error)
}

// CheckStalls raises the review-stall alarm. A head is waiting while its PR is
// open, not a draft, CI has started (running or done) and no review pass has
// been claimed for it. The clock starts when this host first sees the head in
// that state and is kept across restarts in review-wait.json; once a head has
// waited longer than daemon.pr.stall_after, one lane event goes to the operator
// lane (notifier.lane), recorded in review-stall-sent so it fires once per
// (PR, head). It catches a review loop that is alive but skipping reviews.
//
// A dry run decides and records nothing; shadow mode still alarms, like the
// pr role's other lane events.
func (s *Service) CheckStalls(ctx context.Context, r config.Repo, prs []github.Pull, o Options) error {
	window := s.Config.Daemon.PR.StallWindow()
	if window <= 0 || o.DryRun {
		return nil
	}
	file := filepath.Join(s.state(r.Name), "review-wait.json")
	var waiting map[string]time.Time
	if _, e := fsutil.ReadJSON(file, &waiting); e != nil {
		// A damaged clock restarts; an alarm is late, never wrong.
		waiting = nil
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "review-stall-sent"), 255)
	if e != nil {
		return e
	}
	now := s.now()
	next := map[string]time.Time{}
	var errs []error
	for _, pr := range prs {
		if pr.Draft || (pr.Base.Ref != "" && pr.Base.Ref != r.Branch) {
			continue
		}
		key := fmt.Sprintf("%d %s", pr.Number, pr.Head.SHA)
		since, known := waiting[key]
		// A read that fails leaves the clock where it was.
		keep := func() {
			if known {
				next[key] = since
			}
		}
		if cs, ok := s.GH.(ciStarter); ok {
			started, e := cs.CIStarted(ctx, r.Name, pr.Head.SHA)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d stall check: %w", pr.Number, e))
				keep()
				continue
			}
			if !started {
				continue
			}
		}
		claimed, e := s.GH.ReviewClaimed(ctx, r.Name, pr.Number, pr.Head.SHA)
		if e != nil {
			if !errors.Is(e, github.ErrPollSkipped) {
				errs = append(errs, fmt.Errorf("PR %d stall check: %w", pr.Number, e))
			}
			keep()
			continue
		}
		if claimed {
			continue
		}
		if !known {
			since = now
		}
		next[key] = since
		waited := now.Sub(since)
		if waited <= window || sent.has(key) {
			continue
		}
		sha := short7(pr.Head.SHA)
		text := fmt.Sprintf("[mergewake] STALLED review %s#%d head=%s | no review started for %s (threshold %s); the pr role is alive but has not begun a review",
			r.Name, pr.Number, sha, waited.Round(time.Minute), window)
		nctx, cancel := reportContext(ctx)
		e = s.Notifier.Notify(nctx, integrations.Event{
			ID:   fmt.Sprintf("mergewake-stall-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA),
			Text: text,
			Lane: s.Config.Notifier.Lane,
		})
		cancel()
		if e == nil {
			e = sent.add(key)
		}
		if e != nil {
			// Not recorded as sent: the next poll tries again.
			errs = append(errs, fmt.Errorf("PR %d stall alarm: %w", pr.Number, e))
		}
		if ae := s.audit(r.Name, "review-stall", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "waited": waited.String(), "sent": e == nil}, false); ae != nil {
			errs = append(errs, ae)
		}
	}
	if e := fsutil.WriteJSON(file, next); e != nil {
		errs = append(errs, e)
	}
	return errors.Join(errs...)
}
