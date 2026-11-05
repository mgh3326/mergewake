package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// panewireHeld is panewire's active-job refusal: the hub keeps one live
// reservation per (task, role), and another reviewer job of the task holds it.
const panewireHeld = 74

// panewireWaitTimeout is panewire's `spawn --wait` deadline exit: the job was
// spawned and still runs.
const panewireWaitTimeout = 85

// reviewerJob ties one review pass of a PR head to the reviewer job it spawned,
// so a later run resumes that job instead of spawning another under a new
// label. It is written before the spawn and removed when the pass reaches a
// terminal outcome. Args is the exact panewire argument list spawned, --role
// included; Recorded is when the job was first spawned and bounds how long a
// live job is waited for.
type reviewerJob struct {
	PR       int       `json:"pr"`
	Head     string    `json:"head"`
	LeaseSHA string    `json:"lease_sha,omitempty"`
	Pass     int       `json:"pass"`
	Label    string    `json:"label"`
	Checkout string    `json:"checkout"`
	Args     []string  `json:"args"`
	Recorded time.Time `json:"recorded"`
}

func (s *Service) reviewerJobPath(repo string, pr, pass int) string {
	return filepath.Join(s.state(repo), "reviewer-jobs", fmt.Sprintf("%d-p%d.json", pr, pass))
}

// reviewerJob reads the record of this pass for the PR head. Records of the
// PR left by another head are dropped first.
func (x *panewireExecutor) reviewerJob(pass int) (reviewerJob, bool, error) {
	s, repo, pr := x.s, x.repo.Name, x.pr
	paths, e := filepath.Glob(filepath.Join(s.state(repo), "reviewer-jobs", fmt.Sprintf("%d-p*.json", pr.Number)))
	if e != nil {
		return reviewerJob{}, false, e
	}
	var found reviewerJob
	ok := false
	for _, path := range paths {
		var rj reviewerJob
		if _, e = fsutil.ReadJSON(path, &rj); e != nil {
			return reviewerJob{}, false, fmt.Errorf("reviewer job record %s: %w", filepath.Base(path), e)
		}
		if rj.Head != pr.Head.SHA {
			if e = os.Remove(path); e != nil && !errors.Is(e, fs.ErrNotExist) {
				return reviewerJob{}, false, e
			}
			continue
		}
		if path == s.reviewerJobPath(repo, pr.Number, pass) && rj.PR == pr.Number && rj.Pass == pass && rj.Label != "" && len(rj.Args) > 0 {
			found, ok = rj, true
		}
	}
	return found, ok, nil
}

func (x *panewireExecutor) saveReviewerJob(rj reviewerJob) error {
	return fsutil.WriteJSON(x.s.reviewerJobPath(x.repo.Name, rj.PR, rj.Pass), rj)
}

// dropReviewerJob removes the record of a pass that reached a terminal outcome.
func (s *Service) dropReviewerJob(repo string, pr, pass int) error {
	if e := os.Remove(s.reviewerJobPath(repo, pr, pass)); e != nil && !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	return nil
}

// withWaitTimeout returns the recorded spawn arguments with --wait-timeout
// bounded by wait; every other argument is unchanged, so the hub treats the
// replay as the same spawn request and re-attaches a wait to its operation.
func withWaitTimeout(args []string, wait time.Duration) []string {
	out := slices.Clone(args)
	if i := slices.Index(out, "--wait-timeout"); i >= 0 && i+1 < len(out) {
		secs := int(wait.Seconds())
		if recorded, e := strconv.Atoi(out[i+1]); e == nil && recorded < secs {
			secs = recorded
		}
		out[i+1] = strconv.Itoa(secs)
	}
	return out
}

// reviewerStaleAfter is how long a recorded job may stay live before the desk
// is told: review.panewire.stale_after, by default twice review.timeout.
func (s *Service) reviewerStaleAfter() (time.Duration, error) {
	if v := s.Config.Review.Panewire.StaleAfter; v != "" {
		return config.Duration(v)
	}
	timeout, e := config.Duration(s.Config.Review.Timeout)
	return 2 * timeout, e
}

// reviewerStale holds a pass whose recorded job stayed live past the bound. No
// new reviewer is spawned while that job holds the task's reviewer
// reservation; the desk is told once per (PR, head, pass), with the
// reviewer-stale audit row.
func (x *panewireExecutor) reviewerStale(ctx context.Context, rj reviewerJob, state string, bound time.Duration) error {
	s, r, pr := x.s, x.repo, x.pr
	held := fmt.Errorf("%w: pass %d reviewer job %s still %s after %s; held, no new reviewer while it holds the reservation", ErrReviewUnavailable, rj.Pass, rj.Label, state, bound)
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "reviewer-stale-sent"), math.MaxInt)
	if e != nil {
		return errors.Join(held, e)
	}
	key := fmt.Sprintf("%d %s %d", pr.Number, pr.Head.SHA, rj.Pass)
	if sent.has(key) {
		return held
	}
	auditErr := s.audit(r.Name, "reviewer-stale", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "pass": rj.Pass, "label": rj.Label, "state": state, "recorded": rj.Recorded, "bound": bound.String()}, false)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-reviewer-stale-%s-%d-%s-%d", r.Name, pr.Number, pr.Head.SHA, rj.Pass),
		Lane: s.Config.Notifier.Lane, Type: "automation-failed",
		Text: fmt.Sprintf("[mergewake] reviewer job %s for %s#%d head=%s pass=%d is still %s after %s; the pass holds until it finishes or is closed", rj.Label, r.Name, pr.Number, short7(pr.Head.SHA), rj.Pass, state, bound),
	})
	if e == nil {
		e = sent.add(key)
	}
	return errors.Join(held, auditErr, e)
}
