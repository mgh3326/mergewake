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
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// panewireHeld is panewire's active-job refusal: the hub keeps one live
// reservation per (task, role), and another reviewer job of the task holds it.
const panewireHeld = 74

// panewireCloseRefused is panewire's exit status for a job close it refuses.
const panewireCloseRefused = 5

// panewireQuotaTimeout is panewire's quota gate deadline exit: the spawn waited
// for quota and gave up before it placed a pane.
const panewireQuotaTimeout = 124

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
	PR           int       `json:"pr"`
	Head         string    `json:"head"`
	LeaseSHA     string    `json:"lease_sha,omitempty"`
	Pass         int       `json:"pass"`
	Label        string    `json:"label"`
	Checkout     string    `json:"checkout"`
	Args         []string  `json:"args"`
	Recorded     time.Time `json:"recorded"`
	Replacements int       `json:"replacements,omitempty"`
	// Supersedes is the label of the dead reviewer this record replaced, when
	// its request never landed (replaceDeadSpawn); empty otherwise.
	Supersedes string `json:"supersedes,omitempty"`
	// Spawned is the spawn's stdout, kept when the spawn did not wait: the
	// pass that later collects the result closes the pane from its placement.
	Spawned string `json:"spawned,omitempty"`
	// NudgedAt is when mergewake asked this reviewer, whose pane finished with
	// no result, to write its output now. Nil when it was never nudged; a
	// record written before the field existed reads as not nudged.
	NudgedAt *time.Time `json:"nudged_at,omitempty"`
}

func (s *Service) reviewerJobPath(repo string, pr, pass int) string {
	return filepath.Join(s.state(repo), "reviewer-jobs", fmt.Sprintf("%d-p%d.json", pr, pass))
}

// recordedJob is one record file of the PR and its path.
type recordedJob struct {
	path string
	rj   reviewerJob
}

func (x *panewireExecutor) recordedJobs() ([]recordedJob, error) {
	s, repo, pr := x.s, x.repo.Name, x.pr
	paths, e := filepath.Glob(filepath.Join(s.state(repo), "reviewer-jobs", fmt.Sprintf("%d-p*.json", pr.Number)))
	if e != nil {
		return nil, e
	}
	var out []recordedJob
	for _, path := range paths {
		var rj reviewerJob
		if _, e = fsutil.ReadJSON(path, &rj); e != nil {
			return nil, fmt.Errorf("reviewer job record %s: %w", filepath.Base(path), e)
		}
		out = append(out, recordedJob{path, rj})
	}
	return out, nil
}

// ownJob picks the record of this pass for the PR head.
func (x *panewireExecutor) ownJob(recs []recordedJob, pass int) (reviewerJob, bool) {
	for _, rec := range recs {
		rj := rec.rj
		if rj.Head == x.pr.Head.SHA && rec.path == x.s.reviewerJobPath(x.repo.Name, x.pr.Number, pass) && rj.PR == x.pr.Number && rj.Pass == pass && rj.Label != "" && len(rj.Args) > 0 {
			return rj, true
		}
	}
	return reviewerJob{}, false
}

// ownReviewerJob reads the record of this pass for the PR head and changes
// nothing: records of other heads are left for the pass that runs.
func (x *panewireExecutor) ownReviewerJob(pass int) (reviewerJob, bool, error) {
	recs, e := x.recordedJobs()
	if e != nil {
		return reviewerJob{}, false, e
	}
	rj, ok := x.ownJob(recs, pass)
	return rj, ok, nil
}

// reviewerJob reads the record of this pass for the PR head. Records of the
// PR left by another head are dropped first, after their jobs are closed.
// Only a pass that acts calls it.
func (x *panewireExecutor) reviewerJob(ctx context.Context, pass int) (reviewerJob, bool, error) {
	x.jobMu.Lock()
	defer x.jobMu.Unlock()
	recs, e := x.recordedJobs()
	if e != nil {
		return reviewerJob{}, false, e
	}
	for _, rec := range recs {
		if rec.rj.Head == x.pr.Head.SHA {
			continue
		}
		if e = x.closeMovedHeadJob(ctx, rec.rj); e != nil {
			return reviewerJob{}, false, e
		}
		if e = os.Remove(rec.path); e != nil && !errors.Is(e, fs.ErrNotExist) {
			return reviewerJob{}, false, e
		}
	}
	rj, ok := x.ownJob(recs, pass)
	return rj, ok, nil
}

// movedHeadJobs is a snapshot of the older-head records this run closed.
func (x *panewireExecutor) movedHeadJobs() []reviewerJob {
	x.mu.Lock()
	defer x.mu.Unlock()
	return slices.Clone(x.movedHead)
}

// closeReviewerJob closes a reviewer job with the given outcome so its hub
// reservation is released, as best effort: the daemon's node credential often cannot close a
// job whose pane ran on another host (no local events, no claim for the lane); the close
// carries reap.hub's service credential (integrations.HubArgv) when that is set. The owner lane
// is the hk record's, else the --owner of the recorded spawn arguments, else
// the configured one. A close that cannot run or fails is audited as
// reviewer-close-skipped and reported as not closed, never as an error: the
// hub frees the reservation itself when the job ends, and a spawn it still
// holds is refused as reviewer-held. Only a cancelled context or a failed
// audit write is an error.
//
// A close refused because the job already has a terminal record counts as
// closed: the job has ended, so nothing is owed (alreadyClosedRefusal).
func (x *panewireExecutor) closeReviewerJob(ctx context.Context, rj reviewerJob, job integrations.JobRecord, outcome, reason string) (bool, error) {
	s, cfg := x.s, x.s.Config.Review.Panewire
	lane := reviewerLane(job, rj)
	if lane == "" {
		lane = cfg.Owner
	}
	skipped := "no owner lane"
	if lane != "" {
		argv := integrations.HubArgv(s.Config.Reap.Hub, "job", "close", rj.Label, "--lane", lane, "--outcome", outcome, "--reason", reason)
		_, e := s.Runner.Run(ctx, cfg.Command, argv, "", "")
		if e == nil {
			return true, nil
		}
		if ctx.Err() != nil {
			return false, e
		}
		if alreadyClosedRefusal(e) {
			return true, s.audit(x.repo.Name, "reviewer-close-skipped", map[string]any{"pr": x.pr.Number, "label": rj.Label, "head": rj.Head, "reason": "already closed"}, false)
		}
		skipped = fleetevent.Redact(integrations.Detail(e))
	}
	return false, s.audit(x.repo.Name, "reviewer-close-skipped", map[string]any{"pr": x.pr.Number, "label": rj.Label, "head": rj.Head, "reason": skipped}, false)
}

// alreadyClosedRefusal reports whether panewire refused a job close because the
// job already has a terminal record: exit 5 with that text on stderr. Other
// refusals with the same exit (owner_mismatch, job_unknown, ...) leave the job owed.
func alreadyClosedRefusal(e error) bool {
	return exitStatus(e) == panewireCloseRefused && strings.Contains(integrations.Detail(e), "already has a terminal record")
}

// reviewerLane is the owner lane a reviewer's hk job closes under: the hk
// record's, else the --owner of the recorded spawn arguments, else "".
func reviewerLane(job integrations.JobRecord, rj reviewerJob) string {
	if i := slices.Index(rj.Args, "--owner"); job.OwnerLane == "" && i >= 0 && i+1 < len(rj.Args) {
		return rj.Args[i+1]
	}
	return job.OwnerLane
}

// closeMovedHeadJob closes the reviewer job of a record left by an older head
// of the PR, so its hub reservation does not refuse every spawn for the new
// head. A job with no hk record holds nothing and is left alone. A terminal job
// with no pane holds nothing either. A terminal job with a pane holds no
// reservation, so no job close is sent, but its pane is still closed from the
// record's host and pane. A job that cannot be closed from here is audited and
// treated the same: the caller drops the record. A closed job is remembered for
// this run, so a spawn refused as held can close it once more and retry.
func (x *panewireExecutor) closeMovedHeadJob(ctx context.Context, rj reviewerJob) error {
	s, pr := x.s, x.pr
	job, e := x.job(ctx, rj.Label)
	switch {
	case errors.Is(e, integrations.ErrJobNotFound), errors.Is(e, errJobRecords):
		return nil
	case e != nil:
		return fmt.Errorf("reviewer job %s of head %s: %w", rj.Label, short7(rj.Head), e)
	case terminalJob(job.State) && (job.Host == "" || job.PaneID == ""):
		return nil
	}
	res, e := x.closeReplaced(ctx, rj, job, "PR head moved")
	if e != nil || !res.JobClosed {
		return e
	}
	x.mu.Lock()
	if !slices.ContainsFunc(x.movedHead, func(m reviewerJob) bool { return m.Label == rj.Label }) {
		x.movedHead = append(x.movedHead, rj)
	}
	x.mu.Unlock()
	return s.audit(x.repo.Name, "reviewer-head-moved", map[string]any{"pr": pr.Number, "old_head": rj.Head, "new_head": pr.Head.SHA, "label": rj.Label}, false)
}

// closeCollectedJob closes the hk job of a reviewer whose findings mergewake
// collected and accepted but which never ran `panewire done`, so the hub frees
// its reservation at once instead of holding it for the grace period. A job
// with no hk record, or one already terminal, holds nothing and is left alone.
// It is best effort: closeReviewerJob audits a close that cannot run, and the
// close runs on reportContext so a review deadline that already passed cannot
// skip it. The caller never fails the pass on its error; it logs it.
func (x *panewireExecutor) closeCollectedJob(ctx context.Context, rj reviewerJob, job integrations.JobRecord) error {
	if job.State == "" || terminalJob(job.State) {
		return nil
	}
	c, cancel := reportContext(ctx)
	defer cancel()
	_, e := x.closeReviewerJob(c, rj, job, "completed", "result collected by mergewake")
	return e
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

// deadReviewerLabel is the marker replaceDeadSpawn leaves when it cannot
// replace a dead spawn in the same run: the next run's fresh spawn for the
// same head and pass must not reuse the label, whose stamp the clock can
// still reproduce. It lives outside reviewer-jobs/, so the record globs never
// match or read it, and it is removed once the replacement's record is
// written, or when the head moves. Replacements is the dead record's
// lost-result budget, carried so the replacement neither spends nor resets it.
type deadReviewerLabel struct {
	Head         string `json:"head"`
	Label        string `json:"label"`
	Replacements int    `json:"replacements"`
}

func (s *Service) deadLabelPath(repo string, pr, pass int) string {
	return filepath.Join(s.state(repo), "reviewer-dead", fmt.Sprintf("%d-p%d.json", pr, pass))
}

func (s *Service) saveDeadLabel(repo string, pr, pass int, d deadReviewerLabel) error {
	return fsutil.WriteJSON(s.deadLabelPath(repo, pr, pass), d)
}

func (s *Service) dropDeadLabel(repo string, pr, pass int) error {
	if e := os.Remove(s.deadLabelPath(repo, pr, pass)); e != nil && !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	return nil
}

// deadLabel reads the marker kept for this pass of the current head. A marker
// of another head is stale — the head moved — and is removed; the pass then
// spawns as if none was kept.
func (x *panewireExecutor) deadLabel(pass int) (deadReviewerLabel, bool, error) {
	var d deadReviewerLabel
	ok, e := fsutil.ReadJSON(x.s.deadLabelPath(x.repo.Name, x.pr.Number, pass), &d)
	if e != nil || !ok {
		return d, false, e
	}
	if d.Head != x.pr.Head.SHA {
		return d, false, x.s.dropDeadLabel(x.repo.Name, x.pr.Number, pass)
	}
	return d, true, nil
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

// placeOnly returns args without --wait and --wait-timeout: the spawn
// places the reviewer and returns.
func placeOnly(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--wait":
		case "--wait-timeout":
			i++
		default:
			out = append(out, args[i])
		}
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

// RecordedReviewerJobs lists the reviewer jobs recorded for repo, in record
// file order, for the daemon's reviewer watch. It reads only the state
// directory. A record that cannot be read is skipped: the pass that owns it
// reports it.
func (s *Service) RecordedReviewerJobs(repo string) []daemon.WatchedJob {
	paths, e := filepath.Glob(filepath.Join(s.state(repo), "reviewer-jobs", "*-p*.json"))
	if e != nil {
		return nil
	}
	var out []daemon.WatchedJob
	for _, path := range paths {
		var rj reviewerJob
		if _, e = fsutil.ReadJSON(path, &rj); e != nil || rj.Label == "" || rj.PR <= 0 {
			continue
		}
		out = append(out, daemon.WatchedJob{Repo: repo, PR: rj.PR, Pass: rj.Pass, Label: rj.Label})
	}
	return out
}

// ReviewerJobView reads one reviewer job's hk record by label. It makes no
// GitHub request.
func (s *Service) ReviewerJobView(ctx context.Context, label string) (daemon.JobView, error) {
	jr, ok := s.Sink.(integrations.JobReader)
	if !ok {
		return daemon.JobView{}, errJobRecords
	}
	rec, e := jr.Job(ctx, label)
	if e != nil {
		return daemon.JobView{}, e
	}
	return daemon.JobView{State: rec.State, HasResult: len(rec.Result) > 0}, nil
}

// IsJobNotFound reports a reviewer job with no hk record yet.
func IsJobNotFound(e error) bool { return errors.Is(e, integrations.ErrJobNotFound) }
