package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// mergeAttempt is a squash PUT whose outcome came back unknown (see
// github.IsUnknownOutcome). GitHub may have merged it anyway, so it stays in
// the PR's state file until settleMerge reads the result, and a daemon restart
// does not forget it.
type mergeAttempt struct {
	PR   int       `json:"pr"`
	Head string    `json:"head"`
	Base string    `json:"base"`
	At   time.Time `json:"at"`
	// Role is the path that wrote the attempt: mergeRolePR or mergeRoleWatch.
	// A file without it was written before the field existed and is a PR attempt.
	Role string `json:"role,omitempty"`
	// Posted is set by the orphan sweep before it sends the merge's task event
	// and hook, so a later pass finishing the same orphan does not send them again.
	Posted bool `json:"posted,omitempty"`
}

// The writers of a merge attempt. The orphan sweep and Land leave a watch
// attempt to settlePendingRevert, which records the revert it belongs to.
const (
	mergeRolePR    = "pr"
	mergeRoleWatch = "watch"
)

// watchOwns reports whether the pending attempt a belongs to watch: it was
// written by the watch path, or it is role-less (written before the field
// existed) on the recovery branch. headRef is the PR's head branch; pass ""
// when the PR is not read yet, which decides only a role=watch attempt.
func watchOwns(a *mergeAttempt, headRef string) bool {
	return a.Role == mergeRoleWatch || (a.Role == "" && headRef == github.RecoveryBranch)
}

type settleKind int

const (
	// settleNone: no unknown attempt is pending, or it did not land. The
	// normal gate decides whether the PR merges.
	settleNone settleKind = iota
	// settleMerged: the attempt's merge is on the PR. It is never sent again.
	settleMerged
	// settleLanded: the attempt's content is on the base but GitHub left the PR
	// open. The PR is held for a person to close.
	settleLanded
)

type settlement struct {
	Kind   settleKind
	Posted bool
	// Commit is the base commit that carries the content, for settleLanded.
	Commit string
}

func (s *Service) mergeAttemptFile(repo string, n int) string {
	return filepath.Join(s.state(repo), fmt.Sprintf("pr-%d-merge-attempt.json", n))
}

func (s *Service) readMergeAttempt(repo string, n int) (*mergeAttempt, error) {
	var a mergeAttempt
	ok, e := fsutil.ReadJSON(s.mergeAttemptFile(repo, n), &a)
	if e != nil || !ok {
		return nil, e
	}
	return &a, nil
}

func (s *Service) clearMergeAttempt(repo string, n int) error {
	e := os.Remove(s.mergeAttemptFile(repo, n))
	if errors.Is(e, fs.ErrNotExist) {
		return nil
	}
	return e
}

// mergeOnce sends the squash of PR n pinned to head, against the base branch
// head base. The attempt is on disk before the PUT, so a crash mid-call leaves
// it for settleMerge. A definite answer clears it; an unknown outcome keeps it,
// rewritten after the PUT returns so no clear can have erased it.
//
// The caller holds the repository's merge lock, which covers this whole
// write-attempt → PUT → result sequence. A pending attempt is never overwritten:
// it is settled first. role names the writer (mergeRolePR or mergeRoleWatch).
func (s *Service) mergeOnce(ctx context.Context, r config.Repo, n int, head, base, role string) error {
	pending, e := s.readMergeAttempt(r.Name, n)
	if e != nil {
		return e
	}
	if pending != nil {
		return fmt.Errorf("PR #%d has an unsettled merge attempt; settle it before merging again", n)
	}
	a := mergeAttempt{PR: n, Head: head, Base: base, At: s.now().UTC(), Role: role}
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, n), a); e != nil {
		return e
	}
	e = s.GH.Merge(ctx, r.Name, n, head)
	if github.IsUnknownOutcome(e) {
		if we := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, n), a); we != nil {
			return errors.Join(e, we)
		}
		return e
	}
	if ce := s.clearMergeAttempt(r.Name, n); ce != nil {
		return errors.Join(e, ce)
	}
	return e
}

// settleMerge settles the pending unknown attempt on pr, if there is one,
// before pr can be merged again. A merged PR is recorded as merged. An open PR
// whose base took, after the attempt's base, a commit whose subject ends with
// (#<n>) has its content landed: GitHub's squash of this PR carries that
// subject. Any other open PR did not merge, and the attempt is dropped. A dry
// run reports the settlement and leaves the state file alone.
func (s *Service) settleMerge(ctx context.Context, r config.Repo, pr github.Pull, dry bool) (settlement, error) {
	return s.settleMergeKeeping(ctx, r, pr, dry, false)
}

// settleMergeKeeping is settleMerge. With keepMerged a merged attempt is not
// cleared: its caller clears it once the post-merge steps have succeeded.
func (s *Service) settleMergeKeeping(ctx context.Context, r config.Repo, pr github.Pull, dry, keepMerged bool) (settlement, error) {
	a, e := s.readMergeAttempt(r.Name, pr.Number)
	if e != nil || a == nil {
		return settlement{}, e
	}
	fresh := github.FreshEvidence(ctx)
	now, e := s.GH.Pull(fresh, r.Name, pr.Number)
	if e != nil {
		return settlement{}, e
	}
	if now.MergedAt != nil {
		return settlement{Kind: settleMerged, Posted: a.Posted}, s.endMergeAttempt(r, pr.Number, "merged", "", dry, keepMerged)
	}
	if now.State != "open" {
		return settlement{}, s.endMergeAttempt(r, pr.Number, "closed-unmerged", "", dry, false)
	}
	main, e := s.GH.Head(fresh, r.Name, r.Branch)
	if e != nil {
		return settlement{}, e
	}
	var landed []github.Commit
	if main != a.Base {
		if landed, e = s.GH.CommitsSince(fresh, r.Name, a.Base, main); e != nil {
			return settlement{}, e
		}
	}
	suffix := fmt.Sprintf("(#%d)", pr.Number)
	for _, c := range landed {
		subject, _, _ := strings.Cut(c.Commit.Message, "\n")
		if strings.HasSuffix(strings.TrimSpace(subject), suffix) {
			return settlement{Kind: settleLanded, Commit: c.SHA}, s.endMergeAttempt(r, pr.Number, "landed", c.SHA, dry, false)
		}
	}
	return settlement{}, s.endMergeAttempt(r, pr.Number, "not-merged", "", dry, false)
}

// endMergeAttempt audits how a pending attempt was settled and drops it. A
// landed attempt stays pending, so every later pass keeps holding the PR, and
// its settlement is audited on each pass. A kept attempt stays on disk too.
func (s *Service) endMergeAttempt(r config.Repo, n int, state, commit string, dry, keep bool) error {
	if e := s.audit(r.Name, "merge-settled", map[string]any{"pr": n, "state": state, "commit": commit}, dry); e != nil {
		return e
	}
	if dry || keep || state == "landed" {
		return nil
	}
	return s.clearMergeAttempt(r.Name, n)
}

// SettleOrphanedMergeAttempts settles pending merge attempts for PRs absent
// from the open list (a merged PR is closed, so the pr sweep never enqueues
// it). A shadow pass stays read-only and does nothing. A watch attempt is left
// to watch's settlePendingRevert, which records the revert it belongs to. On
// merged it runs finishOrphanMerge: one merge task event, one merge hook call,
// one MERGED notice and the outcome nudge, and the attempt is cleared only
// once those have succeeded, so a failed pass is retried on the next sweep. On
// closed-unmerged or not-merged the attempt is dropped as settleMerge does. A
// landed attempt is held as Land holds it.
func (s *Service) SettleOrphanedMergeAttempts(ctx context.Context, r config.Repo, open map[int]bool, o Options) error {
	if !o.Acting() {
		return nil
	}
	ents, e := os.ReadDir(s.state(r.Name))
	if errors.Is(e, fs.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	var nums []int
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasPrefix(name, "pr-") || !strings.HasSuffix(name, "-merge-attempt.json") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, "pr-"), "-merge-attempt.json")
		n, err := strconv.Atoi(mid)
		if err != nil || n <= 0 {
			continue
		}
		if open != nil && open[n] {
			continue
		}
		nums = append(nums, n)
	}
	if len(nums) == 0 {
		return nil
	}
	sort.Ints(nums)
	fresh := github.FreshEvidence(ctx)
	var errs []error
	for _, n := range nums {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		a, err := s.readMergeAttempt(r.Name, n)
		if err != nil {
			errs = append(errs, fmt.Errorf("PR %d: %w", n, err))
			continue
		}
		if a == nil || watchOwns(a, "") {
			continue
		}
		pr, err := s.GH.Pull(fresh, r.Name, n)
		if err != nil {
			errs = append(errs, fmt.Errorf("PR %d: %w", n, err))
			continue
		}
		if open != nil && open[pr.Number] {
			continue
		}
		if watchOwns(a, pr.Head.Ref) {
			continue
		}
		st, err := s.settleOrphan(fresh, r, pr)
		if err != nil {
			errs = append(errs, fmt.Errorf("PR %d settle: %w", n, err))
			continue
		}
		switch st.Kind {
		case settleMerged:
			if err := s.finishOrphanMerge(fresh, r, pr, o); err != nil {
				errs = append(errs, fmt.Errorf("PR %d finish merge: %w", n, err))
			}
		case settleLanded:
			if err := s.holdSettledMerge(fresh, r, pr, st, o.DryRun); err != nil {
				var held *MergeHeldError
				if errors.As(err, &held) {
					if pe := s.Print(map[string]any{"action": "land-held", "repo": r.Name, "pr": n, "reason": held.Reason}); pe != nil {
						errs = append(errs, fmt.Errorf("PR %d land held: %w", n, pe))
					}
					continue
				}
				errs = append(errs, fmt.Errorf("PR %d land held: %w", n, err))
			}
		}
	}
	return errors.Join(errs...)
}

// settleOrphan is settleLocked for an orphaned attempt: a merged attempt is
// kept on disk until finishOrphanMerge has succeeded.
func (s *Service) settleOrphan(ctx context.Context, r config.Repo, pr github.Pull) (settlement, error) {
	pending, e := s.readMergeAttempt(r.Name, pr.Number)
	if e != nil || pending == nil {
		return settlement{}, e
	}
	unlock, e := s.lockMerges(ctx, r.Name)
	if e != nil {
		return settlement{}, e
	}
	daemon.PhasesOf(ctx).Enter(daemon.PhaseSettle)
	defer unlock()
	return s.settleMergeKeeping(ctx, r, pr, false, true)
}

// finishOrphanMerge runs the post-merge steps for an orphan whose merge
// settled. The task event and the merge hook are sent once: the attempt is
// marked Posted under the merge lock before the post, and a later pass skips
// the post. Everything after it (a fresh read, the observation, the MERGED
// notice, which has its own pr-notify-sent key, and the nudge) is safe to
// repeat. The attempt is cleared only when all of it has succeeded.
func (s *Service) finishOrphanMerge(ctx context.Context, r config.Repo, pr github.Pull, o Options) error {
	unlock, e := s.lockMerges(ctx, r.Name)
	if e != nil {
		return e
	}
	a, e := s.readMergeAttempt(r.Name, pr.Number)
	if e != nil || a == nil {
		unlock()
		return e
	}
	post := !a.Posted
	if post {
		a.Posted = true
		if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, pr.Number), a); e != nil {
			unlock()
			return e
		}
	}
	unlock()
	if post {
		s.postMergeEvent(ctx, r, pr)
	}
	if e := s.finishMergeObserved(ctx, r, pr, o); e != nil {
		return e
	}
	return s.clearMergeAttempt(r.Name, pr.Number)
}

// MergeHeldError is the deliberate refusal of a merge whose content already
// landed on the base while GitHub left the PR open. The hold is a verdict, not
// a failure: the caller shows it as held and does not count it as an error.
type MergeHeldError struct {
	Reason string
}

func (e *MergeHeldError) Error() string { return e.Reason }

// holdSettledMerge refuses to merge pr again: its content already landed on
// the base. It sends one notice to the repository's owner lane and returns the
// typed hold. A notify or state-write error is returned as it is, so it still
// counts as a failure.
func (s *Service) holdSettledMerge(ctx context.Context, r config.Repo, pr github.Pull, st settlement, dry bool) error {
	reason := fmt.Sprintf("merge of head %s had an unknown outcome and its content landed on %s as %s; GitHub left the PR open, so a person closes it",
		short7(pr.Head.SHA), r.Branch, short7(st.Commit))
	held := &MergeHeldError{Reason: reason}
	if dry {
		return held
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "merge-held-reported"), 127)
	if e != nil {
		return e
	}
	key := strconv.Itoa(pr.Number) + ":" + pr.Head.SHA + ":" + st.Commit
	if sent.has(key) {
		return held
	}
	nctx, cancel := reportContext(ctx)
	defer cancel()
	if e := s.Notifier.Notify(nctx, integrations.Event{
		ID:   "mergewake-merge-held-" + strconv.Itoa(pr.Number) + "-" + short7(st.Commit),
		Type: "blocked",
		Text: "mergewake: " + r.Name + " PR #" + strconv.Itoa(pr.Number) + " held: " + reason,
		Lane: s.ownerLane(r),
	}); e != nil {
		return e
	}
	if e := sent.add(key); e != nil {
		return e
	}
	return held
}
