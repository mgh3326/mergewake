package pipeline

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/prstatus"
	"github.com/mgh3326/mergewake/internal/tracing"
)

// gateVerdict is the merge-gate outcome for one PR head. State is "ready",
// "blocked" or "pending"; pending verdicts emit nothing and never merge.
// Reasons use a closed vocabulary: check_failed:<name>, check_cancelled:<name>, threads_open:<n>,
// blocking_findings:<n>, triage_unanswered:<n>, review_missing, base_stale (the base moved on, GitHub reports the PR behind and the policy sets
// require_up_to_date) and conflict. The two graded reasons replace threads_open when the policy has a
// review_gate.land: graded or the PR takes the fast lane.
type gateVerdict struct {
	State      string
	Reasons    []string
	SameFamily bool
	Review     reviewRecord
	// Status is what the gate saw, kept for the PR status snapshot. It never
	// feeds a decision.
	Status gateEvidence
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

// checkJudgment is what the required checks say about a head. Failed and
// Expired hold check_failed:<name> and check_cancelled:<name> reasons that
// block; Waiting holds the check_cancelled:<name> reasons still inside the
// wait; Cancelled lists every required check whose latest run is cancelled;
// Skipped lists the skipped or neutral ones, which hold nothing; Pending is
// set while any check is incomplete or waiting on a rerun.
type checkJudgment struct {
	Failed, Expired, Waiting, Cancelled, Skipped []string
	Pending                                      bool
	// States holds each required check's state as read.
	States map[string]github.CI
}

// judgeChecks folds the required checks' states into what the gate acts on.
// A cancelled check holds no verdict on the head: it waits for a newer run,
// and blocks once its completion is cancelledCheckWait old. Every gate judges
// checks through it.
func judgeChecks(states map[string]github.CI, required []string, cancelledAt map[string]time.Time, now time.Time) checkJudgment {
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
		default:
			j.Pending = true
		}
	}
	return j
}

// judgeHead reads the required checks of sha and judges them. A verdict that
// would block on an expired cancellation is confirmed against a fresh read
// first, since a polled read can predate the run that replaced it.
func (s *Service) judgeHead(ctx context.Context, repo, sha string, required []string) (checkJudgment, error) {
	j, e := s.readChecks(ctx, repo, sha, required)
	if e != nil || len(j.Expired) == 0 {
		return j, e
	}
	return s.readChecks(github.FreshEvidence(ctx), repo, sha, required)
}

func (s *Service) readChecks(ctx context.Context, repo, sha string, required []string) (checkJudgment, error) {
	if snap, ok := s.GH.(checkSnapshotter); ok {
		states, at, e := snap.CheckSnapshot(ctx, repo, sha, required)
		if e != nil {
			return checkJudgment{}, e
		}
		return judgeChecks(states, required, at, s.now()), nil
	}
	states, e := s.GH.CheckStates(ctx, repo, sha, required)
	if e != nil {
		return checkJudgment{}, e
	}
	return judgeChecks(states, required, nil, s.now()), nil
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
	v, err := s.mergeGateResult(ctx, r, pr, p, baseHead)
	if err == nil {
		tracing.Verdict(ctx, v.State, v.Reasons)
	}
	return v, err
}

func (s *Service) mergeGateResult(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, baseHead string) (gateVerdict, error) {
	v := gateVerdict{State: "ready"}
	j, e := s.judgeHead(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if e != nil {
		return v, e
	}
	v.Reasons = append(v.Reasons, j.Failed...)
	v.Reasons = append(v.Reasons, j.Expired...)
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
	var holding map[string]bool
	_, _, v.Review.Findings, holding = gradeThreadsDetailed(fb, gate.WithOwnLogin(login), login, class, fastLane, fixes.verified, s.Config.Review.FleetAuthors)
	if fixes.err != nil {
		// A fix claim GitHub could not check holds the verdict; it neither
		// passes nor blocks on a guess.
		return v, fixes.err
	}
	if (p.ReviewGate != nil && p.ReviewGate.Land == "graded") || fastLane {
		// Graded: findings are weighed by severity instead of counted.
		blocking, triage := gradeThreads(fb, gate.WithOwnLogin(login), login, class, fastLane, fixes.verified, s.Config.Review.FleetAuthors)
		v.Status.Threads = prstatus.Threads{Blocking: blocking, Other: triage + v.Review.Findings.Advisory}
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
	_, complete := distinctPasses(receipts, needed)
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
		// A base that moved on blocks only when the policy requires an
		// up-to-date branch and GitHub reports the PR "behind". Otherwise a
		// conflict-free PR lands as is, post-merge CI checks the composed
		// result, and the stale base is recorded for information.
		if p.RequireUpToDate && pr.MergeableState == "behind" {
			v.Reasons = append(v.Reasons, "base_stale")
		} else {
			v.Review.BaseBehind = true
		}
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

// gradeThreads folds review threads into the two counts the graded gate holds
// on. A block-class finding (mergewake critical/high, CodeRabbit Critical)
// blocks, whoever wrote it and at any age, until it is resolved or carries a
// block disposition: a reply that names the fixing commit or dismisses it with
// a reason (blockDisposition). Neither needs a new head. A triage finding
// (CodeRabbit Major) holds until it has a disposition: a reply from someone
// other than its author (a fix SHA, a dismissal with its reason, a follow-up
// task) or a resolution. Answered counts either whether or not it is resolved. Advisory findings never block, and the fast lane drops triage
// except for unreadable review.login evidence. Outdated findings clear as
// ReviewGate.OutdatedCleared allows. Reviewers still requesting changes, and
// unresolved threads with no comment to grade, count as blocking findings.
func gradeThreads(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck, fleet ...[]string) (blocking, triage int) {
	blocking, triage, _, _ = gradeThreadsDetailed(fb, g, login, class, fastLane, verify, fleet...)
	return blocking, triage
}

// gradeThreadsDetailed is gradeThreads together with how every graded thread
// stands, for the review record, and the ids of the unresolved threads that
// count as blocking findings.
func gradeThreadsDetailed(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck, fleet ...[]string) (blocking, triage int, d dispositions, holding map[string]bool) {
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
			case answered(t, f.OwnReview, fleet...):
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
)

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
