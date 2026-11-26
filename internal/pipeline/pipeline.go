// Package pipeline coordinates policy, GitHub, local reviewers, and adapters.
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

type GitHub interface {
	Pull(context.Context, string, int) (github.Pull, error)
	PullReviewActivity(context.Context, string, int) ([]time.Time, error)
	Files(context.Context, string, int) ([]policy.File, error)
	Diff(context.Context, string, int) (string, error)
	Merge(context.Context, string, int, string) error
	Head(context.Context, string, string) (string, error)
	CreateRef(context.Context, string, string, string) error
	RecoveryActive(context.Context, string) (bool, error)
	Check(context.Context, string, string, []string) (github.CI, error)
	CheckStates(context.Context, string, string, []string) (map[string]github.CI, error)
	CheckEvidence(context.Context, string, string, []string) ([]github.CheckEvidence, error)
	Reviewed(context.Context, string, int, string, string) (bool, error)
	ReviewPasses(context.Context, string, int, string, string) ([]github.PassReceipt, error)
	ReviewHistory(context.Context, string, int, string) ([]github.ReviewedPass, error)
	ReviewClaimed(context.Context, string, int, string) (bool, error)
	ReviewLease(context.Context, string, string, time.Duration) (string, bool, error)
	ClaimReview(context.Context, string, string, string, time.Duration) (bool, error)
	ReviewFeedback(context.Context, string, int) (github.ReviewFeedback, error)
	Unresolved(context.Context, string, int) (int, error)
	PostReview(context.Context, string, int, string, []github.Finding, github.PassReceipt) error
	IssueComments(context.Context, string, int) ([]github.IssueComment, error)
	PostIssueComment(context.Context, string, int, string) error
	Commit(context.Context, string, string) (github.Commit, error)
	PRsForCommit(context.Context, string, string) ([]github.Pull, error)
	MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error)
	Ancestor(context.Context, string, string, string) (bool, error)
	CommitsSince(context.Context, string, string, string) ([]github.Commit, error)
	ReadFile(context.Context, string, string, string) ([]byte, error)
	Revert(context.Context, string, string, string, string, int) (string, error)
}
type Service struct {
	Config   config.Config
	GH       GitHub
	Runner   integrations.Runner
	Sink     integrations.Sink
	Notifier integrations.Notifier
	Out      io.Writer
	// Feed posts events to Discord; nil means off. Only the daemon sets it.
	Feed *Feed
	// Now is the clock for stamps and age decisions; nil means time.Now.
	Now func() time.Time
	// patches caches each PR head's patch id (see patchIDOf), guarded by
	// patchMu.
	patches map[string]string
	// baseDrops caches successful base-drop verdicts by base/head and
	// merge-base/head, guarded by its keyed path lock.
	baseDrops map[string]baseDropResult
	// ancestorOnBase and cleanMerged cache the base-merge checks of base_merge.go
	// that came back true (see baseMergeMu).
	ancestorOnBase, cleanMerged map[string]bool
	// reapBinds caches each merged PR's task/job binding for reap (see
	// reapBinding), guarded by reapBindMu.
	reapBinds map[string]reapBinding
	// Log receives the pipeline's warnings and notes; nil discards them.
	Log *slog.Logger
	// OutcomesChanged, when set, tells the outcome publisher that the ledger
	// has new outcomes (see OutcomePublisher.Nudge). It must not block. Nil
	// means nobody is told; the publisher's timer finds them.
	OutcomesChanged func()
	// hostScans shares each configured host's pane scan across evaluations for
	// authorScanTTL, guarded by hostScanMu (see hostScan).
	hostScans      map[string]*hostScanEntry
	reapRetryState *reapRetryState
}
type Options struct {
	DryRun, Shadow, Observed bool
}

// Acting reports whether a command may write remotely or mutate the host:
// dry runs decide only, and shadow mode observes without acting.
func (o Options) Acting() bool { return !o.DryRun && !o.Shadow }

// Reporting reports whether a command may tell people what it decided: lane
// events, findings deliveries and feed posts. Shadow mode still reports,
// since telling a lane is not acting; only a dry run stays silent.
func (o Options) Reporting() bool { return !o.DryRun }

func (s *Service) Print(v any) error {
	printMu.Lock()
	defer printMu.Unlock()
	return json.NewEncoder(s.Out).Encode(v)
}
func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) state(repo string) string { return filepath.Join(s.Config.StateDir, repo) }
func (s *Service) audit(repo, command string, value any, dry bool) error {
	record := struct {
		Time    time.Time `json:"time"`
		Command string    `json:"command"`
		Value   any       `json:"value"`
	}{s.now().UTC(), command, value}
	b, e := json.Marshal(record)
	if e != nil {
		return e
	}
	if e = s.Print(record); e != nil {
		return e
	}
	if dry {
		return nil
	}
	return appendLine(filepath.Join(s.state(repo), "decisions.jsonl"), b)
}
func (s *Service) Policy(ctx context.Context, r config.Repo, sha string) (policy.Policy, error) {
	var p policy.Policy
	b, e := s.GH.ReadFile(ctx, r.Name, sha, r.Policy)
	if e != nil {
		return p, e
	}
	if e = config.Decode(bytes.NewReader(b), &p); e != nil {
		return p, e
	}
	return p, p.Validate()
}

// distinctPasses counts passes 1..needed, one receipt per pass number, whatever
// reviewer family supplied them (decision 462). Two different receipts for one
// pass number make the set incomplete; identical duplicates add no coverage.
func distinctPasses(receipts []github.PassReceipt, needed int) (int, bool) {
	passes := map[int]github.PassReceipt{}
	for _, r := range receipts {
		if r.Pass < 1 || r.Pass > needed {
			continue
		}
		if prior, ok := passes[r.Pass]; ok && prior != r {
			return len(passes), false
		}
		passes[r.Pass] = r
	}
	if len(passes) < needed {
		return len(passes), false
	}
	return len(passes), true
}

// Land runs the merge gate once for a single PR (the same gate the pr role's
// notice reports) and, when the verdict is ready and the repository policy
// opted in with enabled, squash-merges pinned to the gated head. A dry run
// prints the verdict only.
func (s *Service) Land(ctx context.Context, r config.Repo, n int, o Options) (retErr error) {
	ctx = github.FreshEvidence(ctx)
	defer func() {
		if retErr == nil {
			return
		}
		// A hold is a verdict, not a failure: it writes no land-error row.
		// (Writing one would also hide a state-write failure behind the
		// hold in the joined error.)
		var held *MergeHeldError
		var waiting *WaitingOnWatchError
		if errors.As(retErr, &held) || errors.As(retErr, &waiting) {
			return
		}
		retErr = errors.Join(retErr, s.audit(r.Name, "land-error", map[string]any{"pr": n, "error": retErr.Error()}, o.DryRun))
	}()
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	// A pending attempt that watch owns is watch's to settle: Land neither
	// settles it nor runs the post-merge steps, on any pass kind.
	pending, e := s.readMergeAttempt(r.Name, n)
	if e != nil {
		return e
	}
	if pending != nil && watchOwns(pending, pr.Head.Ref) {
		return s.waitOnWatch(r, n)
	}
	// A pending unknown-outcome merge settles before the PR's state is read
	// further: a merged PR is not merged again, and landed content holds it.
	// A non-acting (dry-run or shadow) pass settles without clearing state:
	// a merged attempt stays pending and its post-merge steps (task event,
	// merge hook, MERGED notice) run only on an acting pass. A landed hold
	// still notifies on every pass kind: telling a lane is reporting, which
	// shadow passes do (Options.Reporting), not acting.
	st, e := s.settleLocked(ctx, r, pr, !o.Acting(), true)
	var waiting *WaitingOnWatchError
	if errors.As(e, &waiting) {
		return s.waitOnWatch(r, n)
	}
	if e != nil {
		return e
	}
	switch st.Kind {
	case settleMerged:
		if !o.Acting() {
			return nil
		}
		if st.Posted {
			return s.finishMergeObserved(ctx, r, pr, o)
		}
		return s.finishMerge(ctx, r, pr, o)
	case settleLanded:
		return s.holdSettledMerge(ctx, r, pr, st, o.DryRun)
	}
	if !o.Observed {
		if e = s.ObservePR(ctx, r, pr, o); e != nil {
			return e
		}
	}
	if pr.State != "open" || pr.Draft || pr.Base.Ref != r.Branch {
		return errors.New("land requires an open, ready PR against the configured branch")
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
	d := policy.Decision{Reasons: v.Reasons, Shadow: !p.Enabled || o.Shadow}
	d.Action = v.State
	if v.State == "ready" {
		d.Action = "merge"
	}
	if e = s.audit(r.Name, "land", struct {
		PR         int    `json:"pr"`
		Head       string `json:"head"`
		Base       string `json:"base"`
		State      string `json:"state"`
		SameFamily bool   `json:"same_family,omitempty"`
		// Review is the patch id, the review rounds and how the findings
		// stand, so `land --dry-run` shows why a head is or is not ready.
		Review reviewRecord `json:"review"`
		policy.Decision
	}{n, pr.Head.SHA, head, v.State, v.SameFamily, v.Review, d}, o.DryRun); e != nil {
		return e
	}
	if e = s.recordDecision(r.Name, n, d.Action, d.Shadow, v.SameFamily, o.DryRun); e != nil {
		return e
	}
	if v.State != "ready" || d.Shadow || o.DryRun {
		return nil
	}
	if e = s.mergePinned(ctx, r, pr); errors.Is(e, errWaitingOnWatch) {
		return s.waitOnWatch(r, n)
	} else if e != nil {
		return e
	}
	return s.finishMerge(ctx, r, pr, o)
}

// errWaitingOnWatch is mergePinned's refusal when a watch-owned attempt became
// pending after Land's check: no merge is sent and Land waits on watch.
var errWaitingOnWatch = errors.New("pending merge attempt belongs to watch")

// waitOnWatch reports that PR n's pending merge attempt belongs to watch, which
// settles it. The typed result keeps this wait out of the clean-settle cache.
func (s *Service) waitOnWatch(r config.Repo, n int) error {
	if e := s.Print(map[string]any{"action": "land-waiting-on-watch", "repo": r.Name, "pr": n, "reason": "pending merge attempt belongs to watch; watch settles it"}); e != nil {
		return e
	}
	return &WaitingOnWatchError{}
}

// finishMerge runs the post-merge steps shared by a fresh merge and a merge
// that settled an unknown-outcome attempt: the merge task event (which also
// runs the context's merge hook), a fresh PR read and observation, the
// best-effort MERGED lane notice and the outcome nudge. The merge already
// landed, so the notice failure is audited rather than returned; any other
// error is returned as it is after a normal merge.
func (s *Service) finishMerge(ctx context.Context, r config.Repo, pr github.Pull, o Options) error {
	s.postMergeEvent(ctx, r, pr)
	return s.finishMergeObserved(ctx, r, pr, o)
}

// postMergeEvent sends the merge task event, which also runs the merge hook.
func (s *Service) postMergeEvent(ctx context.Context, r config.Repo, pr github.Pull) {
	s.postTaskEvent(ctx, r, pr, integrations.SourceMerge, nil, s.now())
}

// finishMergeObserved is finishMerge after the task event: the fresh PR read,
// the observation, the MERGED notice and the nudge.
func (s *Service) finishMergeObserved(ctx context.Context, r config.Repo, pr github.Pull, o Options) error {
	merged, e := s.GH.Pull(ctx, r.Name, pr.Number)
	if e != nil {
		return e
	}
	if e = s.ObservePR(ctx, r, merged, o); e != nil {
		return e
	}
	// The merge already landed and is observed: the lane event is best-effort,
	// its failure audited rather than returned.
	if s.Config.Daemon.PR.Notify.On() {
		landed := merged
		landed.Head.SHA = pr.Head.SHA
		if e = s.RetryMergedCandidate(ctx, r, landed, pr.Title); e != nil {
			_ = s.audit(r.Name, "land-notify-error", map[string]any{"pr": pr.Number, "error": e.Error()}, false)
		}
	}
	// The merge is observed; publishing its outcome is the daemon's publisher's
	// job, so a slow publish never holds the repository's merge lock.
	s.nudgeOutcomes()
	return nil
}

// mergeLock is the lock of one repository's merges. It is two locks taken in
// this fixed order: the in-process mutex, which orders the goroutines of the
// daemon, then the flock under the repository's state dir, which also excludes
// another process (the daemon and a `mergewake land` run from a shell). Held
// across the settle and the write-attempt → PUT → result sequence, never while
// waiting on GitHub for anything else.
func (s *Service) mergeLock(repo string) (func(), error) {
	dir := s.state(repo)
	unlockProc := lockPath("merge:" + dir)
	unlockFile, e := lockFile(filepath.Join(dir, "merges.lock"))
	if e != nil {
		unlockProc()
		return nil, e
	}
	return func() { unlockFile(); unlockProc() }, nil
}

// lockMerges takes the repository's merge lock and books the wait as lock_wait.
func (s *Service) lockMerges(ctx context.Context, repo string) (func(), error) {
	daemon.PhasesOf(ctx).Enter(daemon.PhaseLockWait)
	return s.mergeLock(repo)
}

// settleLocked settles pr's pending unknown attempt. With no attempt pending,
// which is the normal case, it returns settleNone and takes no lock: nothing
// is in flight to wait for. An attempt is written under the lock before its
// PUT, so one in flight is visible here; only then does it take the lock and
// settle under it, so no write-attempt of another caller is in flight while it
// decides and clears. A dry (non-acting: dry-run or shadow) settle clears
// nothing and takes no lock, so it writes no state.
func (s *Service) settleLocked(ctx context.Context, r config.Repo, pr github.Pull, dry, leaveWatch bool) (settlement, error) {
	if dry {
		return s.settleMerge(ctx, r, pr, dry)
	}
	pending, e := s.readMergeAttempt(r.Name, pr.Number)
	if e != nil || pending == nil {
		return settlement{}, e
	}
	unlock, e := s.lockMerges(ctx, r.Name)
	if e != nil {
		return settlement{}, e
	}
	// The lock is taken: the settle and everything after it in Land are back
	// in PhaseSettle, not lock_wait.
	daemon.PhasesOf(ctx).Enter(daemon.PhaseSettle)
	defer unlock()
	// The first read only decides whether taking the lock is necessary. Watch
	// may have written or replaced the attempt while Land waited, so ownership
	// must be checked again under the lock before settling it.
	pending, e = s.readMergeAttempt(r.Name, pr.Number)
	if e != nil {
		return settlement{}, e
	}
	if leaveWatch && pending != nil && watchOwns(pending, pr.Head.Ref) {
		return settlement{}, &WaitingOnWatchError{}
	}
	return s.settleMerge(ctx, r, pr, dry)
}

// mergePinned rechecks a PR and merges it pinned to the head Land gated. It
// holds the repository's merge lock from the PR re-read to the end of GH.Merge
// and nothing else is under that lock.
//
// Each line is inside for one reason: a merge moves the base, so the next
// merge must read the base the previous one left, and must read it after the
// previous merge returned. The base head, the policy at it and the gate against
// it are therefore read under the lock. The PR re-read sits beside them so
// that the "PR changed during decision" check is as fresh as the base: a PR
// read before a long wait for the lock would be checked against a stale head.
// GH.Merge is the step being serialised.
func (s *Service) mergePinned(ctx context.Context, r config.Repo, pr github.Pull) error {
	unlock, e := s.lockMerges(ctx, r.Name)
	if e != nil {
		return e
	}
	defer unlock()
	daemon.PhasesOf(ctx).Enter(daemon.PhaseSettle)
	// Recheck everything immediately before the SHA-bound merge: the head must
	// be unchanged and must still gate ready under the current base and policy.
	freshPR, e := s.GH.Pull(ctx, r.Name, pr.Number)
	if e != nil {
		return e
	}
	// A watch attempt written after Land's check is watch's to settle, not
	// this merge's: the settle below would clear it.
	pending, e := s.readMergeAttempt(r.Name, pr.Number)
	if e != nil {
		return e
	}
	if pending != nil && watchOwns(pending, freshPR.Head.Ref) {
		return errWaitingOnWatch
	}
	st, e := s.settleMerge(ctx, r, freshPR, false)
	if e != nil {
		return e
	}
	switch st.Kind {
	case settleMerged:
		return nil
	case settleLanded:
		return s.holdSettledMerge(ctx, r, freshPR, st, false)
	}
	if freshPR.Head.SHA != pr.Head.SHA || freshPR.State != "open" || freshPR.Draft || freshPR.Base.Ref != r.Branch {
		return errors.New("PR changed during decision")
	}
	fresh, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, fresh)
	if e != nil {
		return e
	}
	if !p.Enabled {
		return errors.New("landing policy changed during decision")
	}
	v, e := s.mergeGate(ctx, r, freshPR, p, fresh)
	if e != nil {
		return e
	}
	if v.State != "ready" {
		return errors.New("gate changed during decision")
	}
	return s.mergeOnce(ctx, r, pr.Number, pr.Head.SHA, fresh, mergeRolePR)
}
