package pipeline

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/prstatus"
	"github.com/mgh3326/mergewake/internal/tracing"
)

// gateVerdict is the merge-gate outcome for one PR head. State is "ready",
// "blocked" or "pending"; pending verdicts emit nothing and never merge.
// Reasons use a closed vocabulary: check_failed:<name>, check_cancelled:<name>, check_absent:<name> (a required check
// with no run or status on the head, once the head has had time to start it), threads_open:<n>,
// blocking_findings:<n>, triage_unanswered:<n>, finding_outside_diff:<n> (critical or high findings a pass listed in its review body because their
// path or line is not in the diff), review_missing and conflict. The two graded reasons replace threads_open when the policy has a
// review_gate.land: graded or the PR takes the fast lane.
type gateVerdict struct {
	State      string
	Reasons    []string
	SameFamily bool
	Review     reviewRecord
	// HeadPasses is how many distinct review passes of the trusted identity are
	// posted for this head; PassesNeeded is how many its class asks for. Both
	// feed the review-stalled notice and nothing else.
	HeadPasses, PassesNeeded int
	// Status is what the gate saw, kept for the PR status snapshot. It never
	// feeds a decision.
	Status gateEvidence
	// Outside is every outside-diff finding the gate folded from the review
	// history, each in its current state; nil when the pass did not evaluate
	// them. The notify path audits the state changes and lists the open ones.
	Outside []outsideFinding
	// Holds names the block-class threads holding the head under
	// blocking_findings, and PassClean says the head's review pass is complete
	// and no holding finding was posted on the head itself. Both feed the
	// BLOCKED notice only; they never feed a decision.
	Holds     []holdingThread
	PassClean bool
}

// gateEvidence is the part of a gate pass the PR status snapshot carries
// beyond the verdict: required check states, the head's review passes and
// done marker, the open threads, and the base comparison (nil until made).
type gateEvidence struct {
	Checks    []prstatus.Check
	Passes    []github.PassReceipt
	Done      bool
	Threads   prstatus.Threads
	Findings  []prstatus.Finding
	BaseFresh *bool
}

// reviewRecord is what the gate knows about a PR's review, printed by
// `land --dry-run` and in the land audit record: the patch the head carries,
// the patches reviewed so far, how many review rounds that took, where this
// head's review came from when it did not run on this head, and how the
// findings stand.
type reviewRecord struct {
	// BaseBehind marks a base that is not an ancestor of the head while GitHub
	// still merges the PR as is. It blocks nothing.
	BaseBehind      bool         `json:"base_behind,omitempty"`
	ChecksCancelled []string     `json:"checks_cancelled,omitempty"`
	ChecksSkipped   []string     `json:"checks_skipped,omitempty"`
	Block           string       `json:"review_block,omitempty"`
	PatchID         string       `json:"patch_id,omitempty"`
	Rounds          int          `json:"rounds"`
	Reviewed        []string     `json:"reviewed_patch_ids,omitempty"`
	Inherited       string       `json:"inherited_from,omitempty"`
	Findings        dispositions `json:"dispositions"`
	Hold            string       `json:"hold,omitempty"`
}

// dispositions counts the PR's review threads by how they stand: blocking and
// triage findings still holding the gate, findings answered by a reply,
// findings resolved, and advisory ones that never hold it. DismissedBlocking
// counts the critical/high findings among Answered that were dismissed rather
// than fixed: they no longer hold the gate, and the desk checks their evidence.
type dispositions struct {
	OpenBlocking      int `json:"open_blocking"`
	OpenTriage        int `json:"open_triage"`
	Answered          int `json:"answered"`
	Resolved          int `json:"resolved"`
	Advisory          int `json:"advisory"`
	DismissedBlocking int `json:"dismissed_blocking,omitempty"`
}

// reviewRounds counts the distinct patches reviewed: a round is one patch, so
// a head that inherits a review adds none. Passes recorded before patch ids
// existed count by head.
func reviewRounds(history []github.ReviewedPass) (rounds int, patches []string) {
	seen := map[string]bool{}
	for _, p := range history {
		key := p.Patch
		if key == "" {
			key = "head:" + p.Head
		} else if !slices.Contains(patches, p.Patch) {
			patches = append(patches, p.Patch)
		}
		if !seen[key] {
			seen[key] = true
			rounds++
		}
	}
	return rounds, patches
}

// settlingVerdict holds the verdict quiet for one reason while the evidence
// settles; it never merges. Like the CI-pending branch it reads the review
// record best-effort: a failed read costs only the record.
func (s *Service) settlingVerdict(ctx context.Context, r config.Repo, pr github.Pull, v gateVerdict, reason string) gateVerdict {
	v.State = "pending"
	v.Reasons = []string{reason}
	daemon.PhasesOf(ctx).Settling(reason)
	s.readReviewRecord(ctx, r, pr, &v)
	return v
}

// headSettleTries and headSettleWait bound how long one gate pass waits for
// GitHub's GraphQL head to catch up with the REST head (about 4 s).
var headSettleTries, headSettleWait = 3, 2 * time.Second

// settledFeedback re-reads the review feedback past the evidence cache until
// its head matches pr's head or the bound runs out; the last read is returned.
func (s *Service) settledFeedback(ctx context.Context, repo string, pr github.Pull) (github.ReviewFeedback, error) {
	var fb github.ReviewFeedback
	var e error
	for i := 0; i < headSettleTries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return fb, ctx.Err()
			case <-time.After(headSettleWait):
			}
		}
		if fb, e = s.GH.ReviewFeedback(github.FreshEvidence(ctx), repo, pr.Number); e != nil || fb.HeadSHA == pr.Head.SHA {
			return fb, e
		}
	}
	return fb, nil
}

func (s *Service) readReviewRecord(ctx context.Context, r config.Repo, pr github.Pull, v *gateVerdict) {
	if history, e := s.GH.ReviewHistory(ctx, r.Name, pr.Number, s.Config.Review.Login); e == nil {
		v.Review.PatchID = s.patchIDOf(ctx, r.Name, pr.Number, pr.Head.SHA)
		v.Review.Rounds, v.Review.Reviewed = reviewRounds(history)
	}
}

// cancelledCheckWait is how long a cancelled required check may sit without a
// newer run before the gate stops waiting for it and blocks.
const cancelledCheckWait = 30 * time.Minute

// checkSnapshotter is the part of the GitHub client that returns the required
// checks' states together with when each cancelled run completed, from one
// read. Evidence without it cannot age a cancellation, so the gate keeps
// waiting.
type checkSnapshotter interface {
	CheckSnapshot(ctx context.Context, repo, sha string, required []string) (map[string]github.CI, map[string]time.Time, error)
}

// absentGrace is how long a head may go without any run or status for a
// required check before the gate calls that check absent. It counts from the
// head commit's committer date, so a commit pushed long after it was made
// reaches the grace sooner than the push time alone would.
const absentGrace = 20 * time.Minute

// checkJudgment is what the required checks say about a head. Failed,
// Expired and Absent hold check_failed:<name>, check_cancelled:<name> and
// check_absent:<name> reasons that block; Waiting holds the check_cancelled:<name>
// reasons still inside the wait; Cancelled lists every required check whose
// latest run is cancelled; Skipped lists the skipped or neutral ones, which
// hold nothing; Pending is set while any check is incomplete, absent but not
// yet due, or waiting on a rerun.
type checkJudgment struct {
	Failed, Expired, Absent, Waiting, Cancelled, Skipped []string
	Pending                                              bool
	// States holds each required check's state as read.
	States map[string]github.CI
}

// absentDue reports whether the absent required checks are to be named now
// rather than waited for: the head commit is absentGrace old, or the only absent
// check is the one missing one, every other required check has completed (green,
// red or skipped), and at least one exists, so the workflow demonstrably ran
// without it. A second absent check has not completed, so with two or more absent
// checks none is named before the grace. Cancelled and unfinished checks do not
// count as completed.
func absentDue(states map[string]github.CI, required []string, headAge time.Duration) bool {
	if headAge >= absentGrace {
		return true
	}
	absent, done := 0, 0
	for _, name := range required {
		switch states[name] {
		case github.Green, github.Red, github.Skipped:
			done++
		case github.Absent:
			absent++
		default:
			return false
		}
	}
	return absent == 1 && done > 0
}

// judgeChecks folds the required checks' states into what the gate acts on.
// A cancelled check holds no verdict on the head: it waits for a newer run,
// and blocks once its completion is cancelledCheckWait old. An absent check
// waits quietly until absentIsDue, then blocks. Every gate judges checks
// through it.
func judgeChecks(states map[string]github.CI, required []string, cancelledAt map[string]time.Time, now time.Time, absentIsDue bool) checkJudgment {
	j := checkJudgment{States: make(map[string]github.CI, len(required))}
	for _, name := range required {
		j.States[name] = states[name]
		switch states[name] {
		case github.Red:
			j.Failed = append(j.Failed, "check_failed:"+name)
		case github.Green:
		case github.Skipped:
			j.Skipped = append(j.Skipped, name)
		case github.Cancelled:
			j.Cancelled = append(j.Cancelled, name)
			reason := "check_cancelled:" + name
			if at := cancelledAt[name]; !at.IsZero() && now.Sub(at) >= cancelledCheckWait {
				j.Expired = append(j.Expired, reason)
			} else {
				j.Waiting = append(j.Waiting, reason)
				j.Pending = true
			}
		case github.Absent:
			if absentIsDue {
				j.Absent = append(j.Absent, "check_absent:"+name)
			} else {
				j.Pending = true
			}
		default:
			j.Pending = true
		}
	}
	return j
}

// judgeHead reads the required checks of sha and judges them. A verdict that
// would block on an expired cancellation or an absent check is confirmed
// against a fresh read first, since a polled read can predate the run that
// replaced it or the run that started late. A conflicting PR never reports an
// absent check: GitHub runs no workflow for it, and the gate says conflict.
func (s *Service) judgeHead(ctx context.Context, repo, sha string, required []string, conflict bool) (checkJudgment, error) {
	j, e := s.readChecks(ctx, repo, sha, required, conflict)
	if e != nil || len(j.Expired) == 0 && len(j.Absent) == 0 {
		return j, e
	}
	return s.readChecks(github.FreshEvidence(ctx), repo, sha, required, conflict)
}

// absentIsDue decides whether an absent required check is named now. It reads
// the head commit only when a check is absent and the cheap rule (every other
// check completed) does not already settle it.
func (s *Service) absentIsDue(ctx context.Context, repo, sha string, states map[string]github.CI, required []string, conflict bool) (bool, error) {
	if conflict || !slices.ContainsFunc(required, func(n string) bool { return states[n] == github.Absent }) {
		return false, nil
	}
	if absentDue(states, required, 0) {
		return true, nil
	}
	c, e := s.GH.Commit(ctx, repo, sha)
	if e != nil {
		return false, e
	}
	if c.Commit.Committer.Date.IsZero() {
		return false, nil
	}
	return absentDue(states, required, s.now().Sub(c.Commit.Committer.Date)), nil
}

func (s *Service) readChecks(ctx context.Context, repo, sha string, required []string, conflict bool) (checkJudgment, error) {
	var (
		states map[string]github.CI
		at     map[string]time.Time
		e      error
	)
	if snap, ok := s.GH.(checkSnapshotter); ok {
		states, at, e = snap.CheckSnapshot(ctx, repo, sha, required)
	} else {
		states, e = s.GH.CheckStates(ctx, repo, sha, required)
	}
	if e != nil {
		return checkJudgment{}, e
	}
	due, e := s.absentIsDue(ctx, repo, sha, states, required, conflict)
	if e != nil {
		return checkJudgment{}, e
	}
	return judgeChecks(states, required, at, s.now(), due), nil
}

// mergeGate evaluates the repo session's merge gate once for head H:
// every required check green or skipped on H (a cancelled one waits for a rerun, then blocks), no unresolved review threads (or, under a
// graded policy, no unfixed blocking or unanswered triage findings), the
// review completion marker mergewake/reviews/<pr>/<H>-done present, the
// base branch an ancestor of H, and the PR mergeable. A review covers a patch,
// not a SHA: a head whose patch id equals a reviewed head's inherits that
// head's passes, so merging the base into a head costs a CI run, not a review. A pending required
// check waits silently unless a sibling already failed or the PR conflicts
// (its checks never run then); mergeable unknown
// also waits when nothing else holds.
func (s *Service) mergeGate(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, baseHead string) (gateVerdict, error) {
	defer daemon.PhasesOf(ctx).Gate()()
	v, err := s.mergeGateResult(ctx, r, pr, p, baseHead)
	if err == nil {
		tracing.Verdict(ctx, v.State, v.Reasons)
		s.writeGate(ctx, r, pr, baseHead, v)
	}
	return v, err
}

func (s *Service) mergeGateResult(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, baseHead string) (gateVerdict, error) {
	v := gateVerdict{State: "ready"}
	j, e := s.judgeHead(ctx, r.Name, pr.Head.SHA, p.RequiredChecks, conflicting(pr))
	if e != nil {
		return v, e
	}
	v.Reasons = append(v.Reasons, j.Failed...)
	v.Reasons = append(v.Reasons, j.Expired...)
	v.Reasons = append(v.Reasons, j.Absent...)
	v.Review.ChecksCancelled, v.Review.ChecksSkipped = j.Cancelled, j.Skipped
	for _, name := range p.RequiredChecks {
		state := j.States[name]
		if state == "" {
			state = github.Pending
		}
		v.Status.Checks = append(v.Status.Checks, prstatus.Check{Name: name, State: string(state)})
	}
	if j.Pending {
		// A sibling failure is already final for this head, and so is a
		// conflict: GitHub runs no pull_request workflow for a conflicting PR,
		// so its checks never appear. Otherwise the incomplete evidence stays
		// quiet.
		if conflicting(pr) {
			v.Reasons = append(v.Reasons, "conflict")
		}
		v.State = "blocked"
		if len(v.Reasons) == 0 {
			v.State = "pending"
		}
		v.Reasons = append(v.Reasons, j.Waiting...)
		// The review record stays readable while CI runs.
		s.readReviewRecord(ctx, r, pr, &v)
		return v, nil
	}
	// The decision-head review feedback and the file list come first: the
	// PR's class and size decide whether findings are graded or counted.
	fb, e := s.GH.ReviewFeedback(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if fb.HeadSHA != pr.Head.SHA {
		// GitHub's REST pull head and the GraphQL head disagree for a few
		// seconds after a push or update-branch, and a cached feedback read
		// can still carry the old head. Re-read past the cache a few times in
		// this pass before deferring the verdict to the next one.
		if fb, e = s.settledFeedback(ctx, r.Name, pr); e != nil {
			return v, e
		}
		if fb.HeadSHA != pr.Head.SHA {
			return s.settlingVerdict(ctx, r, pr, v, "head_moving"), nil
		}
	}
	login := policy.NormalizeAuthor(s.Config.Review.Login)
	files, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if len(files) != pr.ChangedFiles {
		// changed_files and the files list disagree just after a push.
		return s.settlingVerdict(ctx, r, pr, v, "files_settling"), nil
	}
	class := p.ClassifyGuarded(files, r.Policy).Class
	// The PR totals decide the fast lane only while the file list backs them:
	// any mismatch is unverified size, so the lane is refused.
	fastLane := linesVerified(files, pr) && p.FastLaneEligible(class, pr.Additions+pr.Deletions)
	gate := policy.ReviewGate{}
	if p.ReviewGate != nil {
		gate = *p.ReviewGate
	}
	fixes := s.fixVerifier(ctx, r.Name, pr.Head.SHA)
	deferRule := s.deferTaskRule(ctx, r, pr)
	var holding map[string]bool
	_, _, v.Review.Findings, holding = gradeThreadsDetailedWithDefer(fb, gate.WithOwnLogin(login), login, class, fastLane, fixes.verified, deferRule, s.Config.Review.FleetAuthors)
	if deferRule != nil && deferRule.err != nil {
		return v, fmt.Errorf("defer task lookup: %w", deferRule.err)
	}
	if fixes.err != nil {
		// A fix claim GitHub could not check holds the verdict; it neither
		// passes nor blocks on a guess.
		return v, fixes.err
	}
	if (p.ReviewGate != nil && p.ReviewGate.Land == "graded") || fastLane {
		// Graded: findings are weighed by severity instead of counted.
		blocking, triage := gradeThreadsWithDefer(fb, gate.WithOwnLogin(login), login, class, fastLane, fixes.verified, deferRule, s.Config.Review.FleetAuthors)
		v.Status.Threads = prstatus.Threads{Blocking: blocking, Other: triage + v.Review.Findings.Advisory}
		v.Holds = holdingThreads(fb, holding)
		if blocking > 0 {
			v.Reasons = append(v.Reasons, "blocking_findings:"+strconv.Itoa(blocking))
		}
		if triage > 0 {
			v.Reasons = append(v.Reasons, "triage_unanswered:"+strconv.Itoa(triage))
		}
	} else {
		open, e := s.GH.Unresolved(ctx, r.Name, pr.Number)
		if e != nil {
			return v, e
		}
		held := open
		if held > 0 {
			v.Reasons = append(v.Reasons, "threads_open:"+strconv.Itoa(held))
		}
		v.Status.Threads = prstatus.Threads{Blocking: held, Other: v.Review.Findings.Advisory}
		// Every unresolved thread holds the counted gate.
		holding = map[string]bool{}
		for _, t := range fb.Threads {
			if !t.IsResolved {
				holding[t.ID] = true
			}
		}
	}
	v.Status.Findings, v.Status.Threads.Unresolved = openFindings(fb, pr.User.Login, holding)
	// The -done marker alone is forgeable by anyone with push access, so the
	// gate also requires the head-bound passes the trusted review identity
	// posted: one for routine changes, two distinct passes for blocking ones.
	needed := 1
	if class == "blocking" {
		needed = 2
	}
	history, e := s.GH.ReviewHistory(ctx, r.Name, pr.Number, s.Config.Review.Login)
	if e != nil {
		return v, e
	}
	receipts := passesFor(history, pr.Head.SHA)
	v.Status.Passes = receipts
	posted, complete := distinctPasses(receipts, needed)
	v.HeadPasses, v.PassesNeeded = posted, needed
	for _, receipt := range receipts {
		v.SameFamily = v.SameFamily || receipt.SameFamily
	}
	v.Review.PatchID = s.patchIDOf(ctx, r.Name, pr.Number, pr.Head.SHA)
	v.Review.Rounds, v.Review.Reviewed = reviewRounds(history)
	done, e := s.GH.ReviewClaimed(ctx, r.Name, pr.Number, pr.Head.SHA+"-done")
	if errors.Is(e, github.ErrPollSkipped) {
		// The done-marker probe is inside its poll interval: hold the verdict
		// quiet this round rather than erroring every loop.
		v.State = "blocked"
		if len(v.Reasons) == 0 {
			v.State = "pending"
		}
		return v, nil
	}
	if e != nil {
		return v, e
	}
	v.Status.Done = done
	reviewed := done && complete
	if !reviewed {
		// A verdict covers a patch: an earlier head with the same patch id, such
		// as this head before the base was merged into it, already carries the
		// review.
		if from, inherited, ok := inheritedPasses(history, v.Review.PatchID, pr.Head.SHA, needed); ok {
			reviewed = true
			v.Review.Inherited = from
			for _, receipt := range inherited {
				v.SameFamily = v.SameFamily || receipt.SameFamily
			}
		}
	}
	v.PassClean = reviewed && !holdsPostedOn(v.Holds, pr.Head.SHA)
	s.checkRound5Gate(ctx, r, pr, fb, v.Review.Rounds, reviewed, &v)
	records, legacyOutside, e := s.outsideGate(ctx, r, pr, history, v.Review.PatchID, v.Review.Inherited, reviewed, fixes)
	if e != nil {
		return v, e
	}
	v.Outside = records
	openOutside := 0
	for _, f := range records {
		if f.State != outsideOpen {
			continue
		}
		openOutside++
		v.Status.Findings = append(v.Status.Findings, prstatus.Finding{
			ID:       f.Ref,
			Location: f.Path + ":" + strconv.Itoa(f.Line),
			Severity: f.Severity,
			Blocking: true,
			Summary:  outsideTitle(f.Body),
		})
	}
	if n := openOutside + legacyOutside; n > 0 {
		v.Reasons = append(v.Reasons, "finding_outside_diff:"+strconv.Itoa(n))
	}
	if !reviewed {
		v.Reasons = append(v.Reasons, "review_missing")
		if ledger, err := s.ledger().Read(); err == nil {
			v.Review.Block = ledger.Latest[outcomes.Key(r.Name, pr.Number)].ReviewBlocked[pr.Head.SHA]
		}
	}
	fresh, e := s.GH.Ancestor(ctx, r.Name, baseHead, pr.Head.SHA)
	if e != nil {
		return v, e
	}
	v.Status.BaseFresh = &fresh
	if !fresh {
		// A base that moved on never blocks: a conflict-free PR lands on its
		// own head, post-merge CI checks the composed result, and the stale
		// base is recorded for information.
		v.Review.BaseBehind = true
	}
	if conflicting(pr) {
		v.Reasons = append(v.Reasons, "conflict")
	}
	switch {
	case len(v.Reasons) > 0:
		v.State = "blocked"
	case pr.Mergeable == nil || pr.MergeableState == "unknown":
		v.State = "pending"
	}
	return v, nil
}

// outsideBlocking counts the critical and high findings the review passes of
// this head listed in their bodies because the path is not in the diff. Only a
// receipt for the current patch counts: the base can move under a fixed head
// and put the path into the diff, and the finding was about the old patch. A
// head that inherited its review (same patch id) carries the inherited head's
// too. When the current patch id is unknown (the diff could not be read), the
// head's own receipts count, so a failed read never lifts the hold.
func outsideBlocking(history []github.ReviewedPass, head, patch, inherited string) int {
	n := 0
	for _, p := range history {
		switch {
		case p.Head == head && (patch == "" || p.Patch == patch):
			n += p.OutsideBlocking
		case inherited != "" && p.Head == inherited && p.Patch == patch:
			n += p.OutsideBlocking
		}
	}
	return n
}

// outsideDismissed reports a dismissal of this head's outside-diff findings: a
// PR comment from a person (not a bot, not the review identity) that is a
// dismissal with a reason, as dismissReply reads it, and names the head by at
// least seven characters of its SHA, so it does not carry to a later head.
func outsideDismissed(comments []github.IssueComment, head, reviewLogin string) bool {
	if len(head) < 7 {
		return false
	}
	for _, c := range comments {
		who := policy.NormalizeAuthor(c.User.Login)
		if who == "" || who == policy.NormalizeAuthor(reviewLogin) || c.User.Type == "Bot" || strings.HasSuffix(c.User.Login, "[bot]") {
			continue
		}
		if dismissReply.MatchString(c.Body) && strings.Contains(c.Body, head[:7]) {
			return true
		}
	}
	return false
}

// conflicting reports whether GitHub says the PR cannot merge into its base.
func conflicting(pr github.Pull) bool {
	return (pr.Mergeable != nil && !*pr.Mergeable) || pr.MergeableState == "dirty"
}

// linesVerified reports whether the PR's additions and deletions equal the
// sums over its file list.
func linesVerified(files []policy.File, pr github.Pull) bool {
	adds, dels := 0, 0
	for _, f := range files {
		adds += f.Additions
		dels += f.Deletions
	}
	return adds == pr.Additions && dels == pr.Deletions
}

// postedOn is the head a review comment was written against. GitHub moves a
// comment's commit forward to each new head while its line is unchanged, so a
// finding resolved after a fix would otherwise stay held on every later head.
func postedOn(c github.ReviewComment) string {
	if c.OriginalCommitSHA != "" {
		return c.OriginalCommitSHA
	}
	return c.CommitSHA
}

// holdingThread is one block-class thread holding the head, as the BLOCKED
// notice names it. It never feeds a decision.
type holdingThread struct {
	Ref       string // thread URL, or the thread id when GitHub gives none
	Author    string // normalized login of the finding's author
	Severity  string // "" when the finding carries no readable severity
	Location  string // path:line
	Posted    string // the head the finding was posted on
	ReplyBy   string // normalized login of the latest reply, "" when none
	ReplyBody string // body of the latest reply
}

// holdingThreads names the threads in holding, in thread order.
func holdingThreads(fb github.ReviewFeedback, holding map[string]bool) []holdingThread {
	var out []holdingThread
	for _, t := range fb.Threads {
		if !holding[t.ID] {
			continue
		}
		h := holdingThread{Ref: t.ID, Location: t.Path + ":" + strconv.Itoa(t.Line)}
		if len(t.Comments) > 0 {
			first := t.Comments[0]
			if first.URL != "" {
				h.Ref = first.URL
			}
			h.Author = policy.NormalizeAuthor(first.Author)
			h.Severity = github.ThreadSeverity(first.Author, first.Body)
			h.Posted = postedOn(first)
		}
		if n := len(t.Comments); n > 1 {
			h.ReplyBy = policy.NormalizeAuthor(t.Comments[n-1].Author)
			h.ReplyBody = t.Comments[n-1].Body
		}
		out = append(out, h)
	}
	return out
}

// holdsPostedOn reports whether a holding thread's finding was posted on head.
func holdsPostedOn(holds []holdingThread, head string) bool {
	for _, h := range holds {
		if h.Posted == head {
			return true
		}
	}
	return false
}

// gradeThreads folds review threads into the two counts the graded gate holds
// on. A block-class finding (mergewake critical/high, CodeRabbit Critical or
// Major) blocks, whoever wrote it and at any age, until it is resolved or
// carries a block disposition: a reply that names the fixing commit or
// dismisses it with a reason (blockDisposition). Neither needs a new head. A
// triage finding (codex P2, or a severity the rules cannot read) holds until it
// has a disposition: a reply from someone other than its author (a fix SHA, a
// dismissal with its reason, a follow-up task) or a resolution. Answered counts
// either whether or not it is resolved. Advisory findings never block, and the
// fast lane drops triage except for unreadable review.login evidence. Outdated
// findings clear as ReviewGate.OutdatedCleared allows. Reviewers still
// requesting changes, and unresolved threads with no comment to grade, count as
// blocking findings.
func gradeThreads(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck, fleet ...[]string) (blocking, triage int) {
	blocking, triage, _, _ = gradeThreadsDetailed(fb, g, login, class, fastLane, verify, fleet...)
	return blocking, triage
}

func gradeThreadsWithDefer(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck, deferRule *deferTaskRule, fleet ...[]string) (blocking, triage int) {
	blocking, triage, _, _ = gradeThreadsDetailedWithDefer(fb, g, login, class, fastLane, verify, deferRule, fleet...)
	return blocking, triage
}

// gradeThreadsDetailed is gradeThreads together with how every graded thread
// stands, for the review record, and the ids of the unresolved threads that
// count as blocking findings.
func gradeThreadsDetailed(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck, fleet ...[]string) (blocking, triage int, d dispositions, holding map[string]bool) {
	return gradeThreadsDetailedWithDefer(fb, g, login, class, fastLane, verify, nil, fleet...)
}

func gradeThreadsDetailedWithDefer(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck, deferRule *deferTaskRule, fleet ...[]string) (blocking, triage int, d dispositions, holding map[string]bool) {
	holding = map[string]bool{}
	blocking = fb.ChangesRequested
	d.OpenBlocking = fb.ChangesRequested
	for _, t := range fb.Threads {
		if len(t.Comments) == 0 {
			if !t.IsResolved {
				blocking++
				d.OpenBlocking++
				holding[t.ID] = true
			}
			continue
		}
		first := t.Comments[0]
		author := policy.NormalizeAuthor(first.Author)
		f := policy.ReviewFinding{
			Author:    author,
			Severity:  github.ThreadSeverity(first.Author, first.Body),
			Path:      t.Path,
			Line:      t.Line,
			Resolved:  t.IsResolved,
			Outdated:  t.IsOutdated,
			OwnReview: login != "" && author == login,
			CommitSHA: postedOn(first),
		}
		f.Rechecked = !f.OwnReview && fb.RecheckedAuthors[author]
		grade := g.Grade(f, class)
		if g.OutdatedCleared(f, grade) {
			continue
		}
		switch grade {
		case policy.GradeBlock:
			switch kind := blockDisposition(t, f.OwnReview, verify, fleet...); {
			case f.Resolved:
				d.Resolved++
			case kind != "":
				d.Answered++
				if kind == "dismiss" {
					d.DismissedBlocking++
				}
			default:
				blocking++
				d.OpenBlocking++
				holding[t.ID] = true
			}
		case policy.GradeTriage:
			// The fast lane drops rule-graded triage, but a trusted-review
			// finding whose severity cannot be read still needs an answer.
			unreadable := f.OwnReview && !policy.KnownSeverity(f.Severity)
			switch {
			case f.Resolved:
				d.Resolved++
			case answeredWithDefer(t, f.OwnReview, deferRule, fleet...):
				d.Answered++
			case !fastLane || unreadable:
				triage++
				d.OpenTriage++
			}
		case policy.GradeAdvisory:
			d.Advisory++
		}
	}
	return blocking, triage, d, holding
}

var htmlComment = regexp.MustCompile(`(?s)<!--.*?-->`)

// openFindings lists the unresolved review threads of fb for the status
// snapshot, with the count of them all: the list is capped, the count is not.
// holding names the threads that hold the gate. Everything comes from the
// feedback the gate pass already read.
func openFindings(fb github.ReviewFeedback, prAuthor string, holding map[string]bool) ([]prstatus.Finding, *int) {
	out := []prstatus.Finding{}
	open := 0
	author := policy.NormalizeAuthor(prAuthor)
	for _, t := range fb.Threads {
		if t.IsResolved {
			continue
		}
		open++
		f := prstatus.Finding{ID: t.ID, Location: t.Path + ":" + strconv.Itoa(t.Line), Blocking: holding[t.ID]}
		if len(t.Comments) > 0 {
			first := t.Comments[0]
			f.Severity = github.ThreadSeverity(first.Author, first.Body)
			f.Summary = firstLine(first.Body)
			f.URL = first.URL
			for _, c := range t.Comments[1:] {
				if author != "" && policy.NormalizeAuthor(c.Author) == author {
					f.AuthorReplied = true
				}
			}
		}
		out = append(out, f)
	}
	return out, &open
}

// firstLine is the first non-empty line of a finding body, without HTML
// comment markers, cut to prstatus.MaxSummary characters.
func firstLine(body string) string {
	for _, line := range strings.Split(htmlComment.ReplaceAllString(body, ""), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			if r := []rune(line); len(r) > prstatus.MaxSummary {
				return string(r[:prstatus.MaxSummary])
			}
			return line
		}
	}
	return ""
}

var (
	// fixReply opens with a fix.
	fixReply = regexp.MustCompile(`(?is)^\W*(?:disposition:\s*)?fix(?:ed|es)?\b`)
	// commitToken is a commit SHA, abbreviated or full, named in a reply.
	commitToken = regexp.MustCompile(`\b[0-9a-f]{7,40}\b`)
	// dismissReply opens with a dismissal and gives a reason after it.
	dismissReply = regexp.MustCompile(`(?is)^\W*(?:disposition:\s*)?dismiss(?:ed|al)?\b\W*\w`)
	deferIntent  = regexp.MustCompile(`(?i)^\W*disposition:\s*defer\b`)
	deferReply   = regexp.MustCompile(`(?i)^\W*disposition:\s*defer\s+` + deferTaskRef)
)

// deferTaskRef names the follow-up hk task after "defer": a bare number, or
// the same number with "hk " / "hk:" or "#" in front, case-insensitive. Group 1
// is the task id. Every parser of a defer reply uses this one pattern.
const deferTaskRef = `(?:hk(?::|\s+)|#)?([0-9]+)\b`

type deferTaskRule struct {
	own       string
	ownTask   func() (string, error)
	ownLoaded bool
	exists    func(string) (bool, error)
	seen      map[string]bool
	failed    map[string]error
	err       error
}

func (s *Service) deferTaskRule(ctx context.Context, r config.Repo, pr github.Pull) *deferTaskRule {
	jobs, ok := s.Sink.(interface {
		TaskJob(context.Context, string) (string, string, error)
	})
	rule := &deferTaskRule{seen: map[string]bool{}, failed: map[string]error{}, ownTask: func() (string, error) {
		return s.prTask(ctx, r, pr)
	}}
	if ok {
		rule.exists = func(id string) (bool, error) {
			_, _, err := jobs.TaskJob(ctx, id)
			return err == nil, err
		}
	}
	return rule
}

func (r *deferTaskRule) isOwnTask(id string) (bool, error) {
	if r == nil {
		return true, nil
	}
	if !r.ownLoaded && r.ownTask != nil {
		own, err := r.ownTask()
		r.ownLoaded = true
		if err != nil {
			return false, err
		}
		r.own = own
	}
	return id == r.own, nil
}

func (r *deferTaskRule) taskExists(id string) (bool, error) {
	if r == nil || r.exists == nil {
		return false, nil
	}
	if r.seen == nil {
		r.seen = map[string]bool{}
	}
	if r.failed == nil {
		r.failed = map[string]error{}
	}
	if exists, ok := r.seen[id]; ok {
		return exists, nil
	}
	if err, ok := r.failed[id]; ok {
		return false, err
	}
	exists, err := r.exists(id)
	if err != nil {
		if isTaskNotFound(err) {
			r.seen[id] = false
			return false, nil
		}
		r.failed[id] = err
		return false, err
	}
	r.seen[id] = exists
	return exists, nil
}

// TaskJob's public adapter wraps hk's not_found answer with this stable reason.
// It is a missing disposition target, not a failed lookup of the verdict inputs.
func isTaskNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "handoffkeep task not_found:")
}

func answeredWithDefer(t github.ReviewThread, ownReview bool, rule *deferTaskRule, fleet ...[]string) bool {
	if len(t.Comments) == 0 {
		return false
	}
	author := policy.NormalizeAuthor(t.Comments[0].Author)
	answered := false
	var lookupErr error
	for i := 1; i < len(t.Comments); i++ {
		c := t.Comments[i]
		if !answeredBy(c, author, ownReview, fleet...) {
			continue
		}
		if !deferIntent.MatchString(c.Body) {
			answered = true
			lookupErr = nil
			continue
		}
		match := deferReply.FindStringSubmatch(c.Body)
		if match == nil {
			continue
		}
		if rule == nil || rule.exists == nil || answered {
			continue
		}
		own, err := rule.isOwnTask(match[1])
		if err != nil {
			lookupErr = errors.Join(lookupErr, err)
			continue
		}
		if own {
			continue
		}
		exists, err := rule.taskExists(match[1])
		if err != nil {
			lookupErr = errors.Join(lookupErr, err)
			continue
		}
		if exists {
			answered = true
			lookupErr = nil
		}
	}
	if !answered && lookupErr != nil && rule != nil {
		rule.err = errors.Join(rule.err, lookupErr)
	}
	return answered
}

// fixCheck reports whether sha, named in a fix reply, is a real fix for a
// finding posted on head posted (fixVerifier.verified).
type fixCheck func(sha, posted string) bool

// blockDisposition returns "fix" or "dismiss" when a reply that counts as an
// answer (answeredBy) carries one of the two dispositions fleet-review allows
// a critical/high finding: a fix naming a commit that verify accepts, or a
// dismissal with a reason. Anything else — an acknowledgement, a defer, a bare
// "dismissed", a fix naming no commit or one that is not in the PR after the
// finding — leaves the finding blocking. The latest answer decides: a later
// reply that is not a disposition (say, "that fix is wrong") supersedes an
// earlier fix.
func blockDisposition(t github.ReviewThread, ownReview bool, verify fixCheck, fleet ...[]string) string {
	author := policy.NormalizeAuthor(t.Comments[0].Author)
	posted := postedOn(t.Comments[0])
	for i := len(t.Comments) - 1; i > 0; i-- {
		c := t.Comments[i]
		if !answeredBy(c, author, ownReview, fleet...) {
			continue
		}
		switch {
		case fixReply.MatchString(c.Body):
			for _, sha := range commitToken.FindAllString(c.Body, 8) {
				if verify != nil && verify(sha, posted) {
					return "fix"
				}
			}
		case dismissReply.MatchString(c.Body):
			return "dismiss"
		}
		return ""
	}
	return ""
}

// fixVerifier checks fix claims against GitHub for one PR head. A named
// commit counts only when it exists, comes after the head the finding was
// posted on (that head is its strict ancestor) and is in the PR head's
// history. A commit GitHub does not know is no fix; any other lookup failure
// is kept in err, for the caller to hold its decision rather than guess.
// Answers are cached for the pass.
type fixVerifierState struct {
	ctx        context.Context
	gh         GitHub
	repo, head string
	cache      map[[2]string]bool
	err        error
}

func (s *Service) fixVerifier(ctx context.Context, repo, head string) *fixVerifierState {
	return &fixVerifierState{ctx: ctx, gh: s.GH, repo: repo, head: head, cache: map[[2]string]bool{}}
}

func (v *fixVerifierState) verified(sha, posted string) bool {
	key := [2]string{sha, posted}
	if ok, seen := v.cache[key]; seen {
		return ok
	}
	ok, e := v.check(sha, posted)
	if e != nil {
		v.err = errors.Join(v.err, e)
		return false
	}
	v.cache[key] = ok
	return ok
}

func (v *fixVerifierState) check(sha, posted string) (bool, error) {
	if posted == "" || v.head == "" {
		return false, nil
	}
	c, e := v.gh.Commit(v.ctx, v.repo, sha)
	var api *github.APIError
	if errors.As(e, &api) && (api.Code == 404 || api.Code == 422) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	full := c.SHA
	if full == "" || full == posted || !strings.HasPrefix(full, sha) {
		return false, nil
	}
	after, e := v.gh.Ancestor(v.ctx, v.repo, posted, full)
	if e != nil || !after {
		return false, e
	}
	if full == v.head {
		return true, nil
	}
	return v.gh.Ancestor(v.ctx, v.repo, full, v.head)
}

// answered reports a human reply from someone other than the finding's author.
// mergewake posts its own findings under review.login, which is usually the
// operator's account, so for an own-review finding a reply from that same
// login also counts when the reply is not itself a mergewake finding (it
// carries no severity marker).
func answered(t github.ReviewThread, ownReview bool, fleet ...[]string) bool {
	author := policy.NormalizeAuthor(t.Comments[0].Author)
	for _, c := range t.Comments[1:] {
		if answeredBy(c, author, ownReview, fleet...) {
			return true
		}
	}
	return false
}

// answeredBy reports whether reply c, on a finding by author, is a human
// answer under the rules of answered.
func answeredBy(c github.ReviewComment, author string, ownReview bool, fleet ...[]string) bool {
	who := policy.NormalizeAuthor(c.Author)
	if who == "" || strings.Contains(c.Body, "<!-- mergewake-severity:") {
		return false
	}
	for _, authors := range fleet {
		for _, login := range authors {
			if who == policy.NormalizeAuthor(login) && (who != author || ownReview) {
				return true
			}
		}
	}
	if c.AuthorType == "Bot" || strings.HasSuffix(c.Author, "[bot]") {
		return false
	}
	return who != author || (ownReview && !strings.Contains(c.Body, "<!-- mergewake-severity:"))
}
