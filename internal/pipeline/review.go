package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/tracing"
)

var ErrReviewUnavailable = errors.New("review unavailable")

const familyRequired = "author family required via --author-family, a Mergewake-Author-Family trailer in the PR body or head commit, or a known commit author"

var familyValue = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

var coAuthorTrailer = regexp.MustCompile(`(?im)^Co-Authored-By:\s*([^<>\r\n]+?)\s*<([^<>\r\n]*)>`)

var lensFocus = map[string]string{
	"correctness": "correctness and regressions: logic errors, broken edge cases, and behaviour the diff changes without covering evidence",
	"state-risk":  "state, migration, security and deploy risk: irreversible mutations, data loss, secret or auth exposure, unsafe rollout and recovery gaps",
}

// singleLens names the lens of a pass in a repo that runs one pass: it covers
// the whole review, so it has no narrower focus than the other lenses.
const singleLens = "default"

// claimRef names the server-side ref claiming one pass for one head. Pass 1
// keeps the original layout; the completion marker is <sha>-done.
func claimRef(n int, sha string, pass int) string {
	base := fmt.Sprintf("mergewake/reviews/%d/%s", n, sha)
	if pass > 1 {
		base += "-p" + strconv.Itoa(pass)
	}
	return base
}
func doneRef(n int, sha string) string { return fmt.Sprintf("mergewake/reviews/%d/%s-done", n, sha) }

// A minute beyond the configured invocation timeout allows posting/cleanup to
// finish before another host may take over an interrupted pass.
func (s *Service) reviewLeaseTTL() (time.Duration, error) {
	timeout, err := config.Duration(s.Config.Review.Timeout)
	return timeout + time.Minute, err
}

// familyFromIdentity maps a commit identity's name and email to a canonical
// model family by matching alias tokens (claude → anthropic, devin →
// cognition, ...). Human identities hold no listed token and return "".
func familyFromIdentity(name, email string) string {
	for _, tok := range strings.FieldsFunc(name+" "+email, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if f, ok := config.FamilyToken(tok); ok {
			return f
		}
	}
	return ""
}

// inferAuthorFamily classifies the head commit when neither the PR body nor
// its commit message declares a family: Co-Authored-By trailers first
// (agent-authored commits name the tool there), then the author and committer
// identities.
func inferAuthorFamily(c github.Commit) string {
	for _, m := range coAuthorTrailer.FindAllStringSubmatch(c.Commit.Message, -1) {
		if f := familyFromIdentity(m[1], m[2]); f != "" {
			return f
		}
	}
	if f := familyFromIdentity(c.Commit.Author.Name, c.Commit.Author.Email); f != "" {
		return f
	}
	return familyFromIdentity(c.Commit.Committer.Name, c.Commit.Committer.Email)
}

type passPlan struct {
	pass         int
	lens         string
	sameFamily   bool
	replacements int
}

// Review runs the required diff-only passes for the PR head: one for routine
// changes, two for blocking paths. Panewire runs each pass in a reviewer pane. A pass the executor did not produce fails the run with
// ErrReviewUnavailable; RetryReview backs off and runs the review again.
func (s *Service) Review(ctx context.Context, r config.Repo, n int, family string, o Options) error {
	unlock, e := s.lockReview(ctx, r.Name, n)
	if e != nil {
		return e
	}
	defer unlock()
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if !o.Observed {
		if e = s.ObservePR(ctx, r, pr, o); e != nil {
			return e
		}
	}
	if pr.State != "open" || pr.Draft {
		return errors.New("review requires an open, ready PR")
	}
	needed, settled, e := s.reviewPassesNeeded(ctx, r, pr)
	if e != nil {
		return e
	}
	if !settled {
		// changed_files and the files list disagree just after a push: no
		// usable data this round. The retry loop treats this sentinel as a
		// skip, not a failure.
		return fmt.Errorf("PR file list is settling: %w", github.ErrPollSkipped)
	}
	history, e := s.GH.ReviewHistory(ctx, r.Name, n, s.Config.Review.Login)
	if e != nil {
		return e
	}
	receipts := passesFor(history, pr.Head.SHA)
	// A review covers a patch, not a SHA. A head whose patch id equals a
	// reviewed head's, such as the same patch after the base was merged into it,
	// inherits that review: it gets its completion marker and no pass is
	// scheduled. An unreadable diff leaves the patch unknown, which equals
	// nothing, so the head is reviewed on its own.
	var diff, patch string
	var src diffResult
	if _, own := distinctPasses(receipts, needed); !own {
		var diffErr error
		if src, diffErr = s.reviewDiff(ctx, r, pr); diffErr == nil && src.text != "" {
			diff = src.text
			patch = s.rememberPatch(patchKey(r.Name, n, pr.Head.SHA), reviewPatchID(diff))
		}
		if from, _, ok := inheritedPasses(history, patch, pr.Head.SHA, needed); ok {
			if e = s.recordReviewBlock(r.Name, n, pr.Head.SHA, "", o.DryRun); e != nil {
				return e
			}
			if o.Acting() {
				if e = s.markDone(ctx, r.Name, n, pr.Head.SHA); e != nil {
					return e
				}
			}
			return s.Print(map[string]any{"action": "inherited-review", "head": pr.Head.SHA, "inherited_from": from, "patch_id": patch, "needed": needed})
		}
	}
	familySource := "flag"
	// A completed head needs no author identity; missing passes do.
	_, complete := distinctPasses(receipts, needed)
	if !complete {
		if b, blocked := s.reviewBlockFor(ctx, r, pr); blocked {
			return s.blockReview(ctx, r, pr, b, o)
		}
		state, err := s.ledger().Read()
		if err != nil {
			return err
		}
		if state.Latest[outcomes.Key(r.Name, n)].ReviewBlocked[pr.Head.SHA] == blockSecretLike {
			return s.blockReview(ctx, r, pr, secretLikeBlock, o)
		}
	}
	if e = s.recordReviewBlock(r.Name, n, pr.Head.SHA, "", o.DryRun); e != nil {
		return e
	}
	if !complete {
		if family == "" {
			f, e := s.prTrailer(ctx, r, pr, "Mergewake-Author-Family", familyValue.MatchString)
			if e != nil {
				return e
			}
			if f != "" {
				family, familySource = f, "trailer"
			}
		}
		profileTrailer, e := s.prTrailer(ctx, r, pr, "Mergewake-Author-Profile", nil)
		if e != nil {
			return e
		}
		author := strings.SplitN(profileTrailer, "@", 2)
		if len(author) == 2 {
			if profile, ok := s.Config.Review.Profiles[author[0]]; ok {
				if family != "" && config.CanonicalFamily(family) != config.CanonicalFamily(profile.Family) {
					return errors.New("author family conflicts with configured profile")
				}
				family, familySource = profile.Family, "profile"
			}
		}
		if family == "" {
			c, e := s.headCommit(ctx, r, pr)
			if e != nil {
				return e
			}
			if inferred := inferAuthorFamily(c); inferred != "" {
				family, familySource = inferred, "commit"
			}
		}
		if family == "" && s.Config.Review.DefaultFamily != "" {
			family, familySource = s.Config.Review.DefaultFamily, "default"
		}
		family = config.CanonicalFamily(family)
		if family == "" {
			if s.fleetAuthor(pr.User.Login) {
				return s.blockReview(ctx, r, pr, reviewBlock{blockUnknownFamily, "Add `Mergewake-Author-Family: <family>` to the PR body. The head commit trailer also works."}, o)
			}
			return fmt.Errorf("%w: %s", ErrReviewUnavailable, familyRequired)
		}
	}
	posted := map[int]github.PassReceipt{}
	for _, receipt := range receipts {
		posted[receipt.Pass] = receipt
	}
	missing, pending := []int{}, []int{}
	for i := 1; i <= needed; i++ {
		if _, ok := posted[i]; ok {
			continue
		}
		ttl, e := s.reviewLeaseTTL()
		if e != nil {
			return e
		}
		leaseSHA, claimed, e := s.GH.ReviewLease(ctx, r.Name, claimRef(n, pr.Head.SHA, i), ttl)
		if e != nil {
			return e
		}
		if claimed {
			x := &panewireExecutor{s: s, repo: r, pr: pr}
			job, own, err := x.ownReviewerJob(i)
			if err != nil {
				return err
			}
			if own && job.LeaseSHA != "" && job.LeaseSHA == leaseSHA {
				missing = append(missing, i)
				continue
			}
			pending = append(pending, i)
		} else {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		action := "review-pending"
		if len(pending) == 0 {
			action = "already-reviewed"
			if o.Acting() {
				if e = s.markDone(ctx, r.Name, n, pr.Head.SHA); e != nil {
					return e
				}
			}
		}
		return s.Print(map[string]any{"action": action, "head": pr.Head.SHA, "passes": len(posted), "needed": needed})
	}
	timeout, e := config.Duration(s.Config.Review.Timeout)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	grade := s.Config.Review.DefaultGrade
	if grade == "" {
		grade = "B"
	}
	g, e := s.prTrailer(ctx, r, pr, "Mergewake-Grade", nil)
	if e != nil {
		return e
	}
	if g != "" {
		grade = g
	}
	if !config.ValidGrade(grade) {
		return errors.New("invalid PR review grade")
	}
	var plans []*passPlan
	for _, pass := range missing {
		plans = append(plans, &passPlan{pass: pass})
	}
	lens := func(plan *passPlan) string {
		if needed == 2 {
			if plan.pass == 1 {
				return "correctness"
			}
			return "state-risk"
		}
		return singleLens
	}

	if !o.Acting() {
		out := []map[string]any{}
		for _, plan := range plans {
			plan.lens = lens(plan)
			row := map[string]any{"pass": plan.pass, "lens": plan.lens, "executor": "panewire"}
			out = append(out, row)
		}
		plan := map[string]any{"action": "review", "head": pr.Head.SHA, "grade": grade, "needed": needed, "pending_claims": pending, "passes": out, "dry_run": true}
		return s.Print(plan)
	}
	if diff == "" {
		if src, e = s.reviewDiff(ctx, r, pr); e != nil {
			return e
		}
		diff = src.text
		if diff != "" {
			patch = s.rememberPatch(patchKey(r.Name, n, pr.Head.SHA), reviewPatchID(diff))
		}
	}
	if len(diff) == 0 || len(diff) > reviewDiffBudget {
		return errors.New("empty or oversized diff; manual review required")
	}
	// Findings already answered on this PR, and its operator decisions, keep the
	// reviewer from re-flagging settled points. The context is best effort: a
	// feedback read that fails costs the context, not the review.
	var prContext reviewContext
	// The round decides what the reviewer sees: the whole diff in round one, the
	// interdiff since the last reviewed head afterwards.
	rp := s.planRound(ctx, r, pr, diff)
	var contractErr error
	if rp.contract, rp.contractSource, contractErr = s.acceptance(ctx, r, pr); contractErr != nil {
		// Without the contract every finding stays in scope, as before.
		rp.contract, rp.contractSource = "", ""
	}
	// The read runs on its own short deadline, a child of ctx, so a slow or
	// paginating read cannot spend the time the gate and reviewer need.
	feedbackCtx, cancelFeedback := context.WithTimeout(ctx, feedbackTimeout(timeout))
	feedback, contextErr := s.GH.ReviewFeedback(feedbackCtx, r.Name, n)
	cancelFeedback()
	if contextErr != nil {
		feedback = github.ReviewFeedback{}
	}
	prContext = buildRoundContext(pr.Body, feedback, rp, s.Config.Review.Login)
	// Findings already on the PR, and those posted by earlier passes of this
	// run, are never posted again.
	known := knownThreads(feedback)
	x := &panewireExecutor{s: s, repo: r, pr: pr, diff: rp.diff, delta: rp.base != "" && !rp.empty, round: rp.round, context: prContext.text, authorFamily: family}
	// The review event is posted when the run ends, so a slow hk never
	// spends the reviewers' deadline.
	var reviewStarted *time.Time
	// reviews records each pass this run posted, for the event.
	var reviews []integrations.ReviewRecord
	defer func() {
		if reviewStarted != nil {
			// The passes finish in any order; the event lists them by pass.
			slices.SortStableFunc(reviews, func(a, b integrations.ReviewRecord) int { return a.Pass - b.Pass })
			s.postTaskEventWith(ctx, r, pr, integrations.SourceReview, nil, *reviewStarted, reviews)
		}
	}()
	// Every missing pass of the head runs at once: no pass reads another's
	// result, and each owns its claim ref, reviewer job record and outcome.
	// mu guards what the passes of this run share: reviewStarted, reviews and
	// known.
	var mu sync.Mutex
	runOne := func(plan *passPlan) error {
		passStarted := s.now()
		// A GitHub ref claims this PR head across hosts before any reviewer is
		// invoked. One claim covers every candidate of the pass.
		claim := func() (string, bool, error) {
			ttl, e := s.reviewLeaseTTL()
			if e != nil {
				return "", false, e
			}
			ref := claimRef(n, pr.Head.SHA, plan.pass)
			claimer, ok := s.GH.(interface {
				ClaimReviewWithSHA(context.Context, string, string, string, time.Duration) (string, bool, error)
			})
			if !ok {
				return "", false, errors.New("GitHub client cannot return review lease identity")
			}
			sha, acquired, e := claimer.ClaimReviewWithSHA(ctx, r.Name, ref, pr.Head.SHA, ttl)
			if e != nil {
				return "", false, e
			}
			if acquired {
				mu.Lock()
				if reviewStarted == nil {
					now := s.now()
					reviewStarted = &now
				}
				mu.Unlock()
				return sha, true, nil
			}
			sha, active, e := s.GH.ReviewLease(ctx, r.Name, ref, ttl)
			if e != nil {
				return "", false, e
			}
			if !active {
				return "", false, nil
			}
			return sha, false, nil
		}
		passCtx, passSpan := tracing.Span(ctx, "mergewake.review.pass")
		tracing.Data(passSpan, "pass", plan.pass)
		out, e := x.runPass(passCtx, plan, claim)
		if e == nil && out.reviewer != "" {
			mu.Lock()
			if reviewStarted == nil {
				now := s.now()
				reviewStarted = &now
			}
			mu.Unlock()
		}
		privateNames := []string{s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane, s.Config.Review.Panewire.Lane, s.Config.Review.Panewire.Owner}
		for _, repo := range s.Config.Repos {
			privateNames = append(privateNames, repo.Lane)
		}
		tracing.Reviewer(passSpan, out.reviewer, privateNames...)
		tracing.Finish(passSpan)
		if e != nil {
			if errors.Is(e, errSecretLikeBrief) {
				return errors.Join(s.blockReview(ctx, r, pr, secretLikeBlock, o), e)
			}
			var lost *reviewResultLost
			if errors.As(e, &lost) {
				auditErr := s.audit(r.Name, "review-result-lost", map[string]any{"pr": n, "head": pr.Head.SHA, "pass": plan.pass, "lens": plan.lens, "label": lost.label, "record": lost.record, "profile": lost.profile, "report": lost.report, "collect_error": lost.cause.Error()}, false)
				noticeErr := s.notifyReviewResultLost(ctx, r, pr, plan, lost)
				return errors.Join(e, auditErr, noticeErr)
			}
			// A reviewer cut off by the review deadline is recorded like a
			// pass nobody produced.
			if ctx.Err() != nil || errors.Is(e, context.DeadlineExceeded) {
				s.reviewerEvents(r.Name, n, plan.pass, out.skips)
				s.reviewMissingEvent(r.Name, n, plan.pass, "review_deadline", out.skips, "")
			}
			if errors.Is(e, errPassPending) {
				return errPassPending
			}
			return e
		}
		skips := out.skips
		// The whole review shares one deadline: a pass nobody produced by then is
		// a deadline, not a missing reviewer.
		if out.reviewer == "" && ctx.Err() != nil {
			s.reviewerEvents(r.Name, n, plan.pass, skips)
			s.reviewMissingEvent(r.Name, n, plan.pass, "review_deadline", skips, "")
			return errors.Join(fmt.Errorf("%w: no reviewer produced pass %d (%s)", ErrReviewUnavailable, plan.pass, describeSkips(skips)), ctx.Err())
		}
		s.reviewerEvents(r.Name, n, plan.pass, skips)
		selected := out.reviewer
		if selected == "" {
			s.reviewMissingEvent(r.Name, n, plan.pass, "review_missing", skips, "")
			e = s.audit(r.Name, "review-skipped", map[string]any{"pr": n, "head": pr.Head.SHA, "pass": plan.pass, "skipped": skips}, false)
			return errors.Join(fmt.Errorf("%w: no reviewer produced pass %d (%s)", ErrReviewUnavailable, plan.pass, describeSkips(skips)), e)
		}
		// A finding the other pass kept first is dropped here, as when the
		// passes ran one after the other.
		mu.Lock()
		shape := shapeFindings(out.findings, rp, known)
		findings := shape.kept
		known = append(known, knownFromFindings(findings)...)
		mu.Unlock()
		plan.sameFamily = out.family == "" || out.family == family
		receipt := github.PassReceipt{Pass: plan.pass, Lens: plan.lens, Reviewer: selected, Family: out.family, SameFamily: plan.sameFamily, AnsweredContext: prContext.answered, Executor: out.executor, Fallback: out.fallback, Round: rp.round, Delta: rp.base, Quiet: rp.quiet, Patch: patch, DiffSource: src.source, DiffSummarized: strings.Join(src.summarized, "\n")}
		if e = s.recordReview(r.Name, n, pr.Head.SHA, findings, receipt, o.DryRun); e != nil {
			return e
		}
		fresh, e := s.GH.Pull(ctx, r.Name, n)
		if e != nil {
			return e
		}
		if fresh.Head.SHA != pr.Head.SHA {
			return errors.New("PR head changed during review; run once for the new head")
		}
		current, e := s.GH.ReviewPasses(ctx, r.Name, n, pr.Head.SHA, s.Config.Review.Login)
		if e != nil {
			return e
		}
		alreadyPosted := false
		for _, p := range current {
			if p.Pass == plan.pass {
				alreadyPosted = true
				break
			}
		}
		if alreadyPosted {
			_ = s.dropReviewerJob(r.Name, n, plan.pass)
			return nil
		}
		if e = s.GH.PostReview(ctx, r.Name, n, pr.Head.SHA, findings, receipt); e != nil {
			// A review GitHub stored before the call failed is a posted pass: a
			// later run finds its receipt and skips it, so the event of this run
			// is the only one that can carry its record.
			if stored, readErr := s.GH.ReviewPasses(ctx, r.Name, n, pr.Head.SHA, s.Config.Review.Login); readErr == nil && slices.ContainsFunc(stored, func(p github.PassReceipt) bool { return p.Pass == plan.pass }) {
				mu.Lock()
				reviews = append(reviews, passRecord(pr, receipt, findings, rp.contractSource, passStarted, s.now()))
				mu.Unlock()
			}
			return e
		}
		mu.Lock()
		reviews = append(reviews, passRecord(pr, receipt, findings, rp.contractSource, passStarted, s.now()))
		mu.Unlock()
		// The pass is posted: its reviewer job record, if any, is spent. A
		// record left behind is never resumed, since a posted pass is not run
		// again for this head.
		_ = s.dropReviewerJob(r.Name, n, plan.pass)
		event := map[string]any{"pr": n, "head": pr.Head.SHA, "pass": plan.pass, "lens": plan.lens, "reviewer": selected, "family": out.family, "same_family": plan.sameFamily, "findings": len(findings), "author_family": family, "answered_context": prContext.answered}
		if prContext.truncated {
			event["context_truncated"] = true
		}
		event["round"] = rp.round
		if rp.base != "" {
			event["delta_base"] = rp.base
		}
		if rp.quiet {
			event["quiet"] = true
		}
		if rp.note != "" {
			event["round_note"] = rp.note
		}
		if prContext.prior > 0 {
			event["prior_context"] = prContext.prior
		}
		if rp.contract != "" {
			event["contract"] = rp.contractSource
		}
		if contractErr != nil {
			event["contract_error"] = contractErr.Error()
		}
		if shape.duplicates > 0 {
			event["duplicates_dropped"] = shape.duplicates
		}
		if shape.advisory > 0 {
			event["advisory"] = shape.advisory
		}
		if shape.budget > 0 {
			event["budget_dropped"] = shape.budget
		}
		if contextErr != nil {
			event["context_error"] = contextErr.Error()
		}
		if out.executor != "" {
			event["executor"] = out.executor
		}
		if receipt.Fallback != "" {
			event["fallback"] = receipt.Fallback
		}
		if src.source != "" {
			event["diff_source"] = src.source
		}
		if len(src.summarized) > 0 {
			event["diff_summarized"] = len(src.summarized)
		}
		if familySource == "commit" || familySource == "default" {
			event["author_family_inferred"] = familySource
		}
		if len(skips) > 0 {
			event["skipped"] = skips
		}
		return s.audit(r.Name, "review", event, false)
	}
	for _, plan := range plans {
		plan.lens = lens(plan)
	}
	errs := make([]error, len(plans))
	var wg sync.WaitGroup
	for i, plan := range plans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = runOne(plan)
		}()
	}
	wg.Wait()
	// Each pass's outcome stands alone: a pass that failed does not undo a
	// pass that posted, and the passes that did not post are retried. A pass
	// still held by its reviewer or another host is not a failure.
	var failed []error
	held := -1
	for i, e := range errs {
		switch {
		case errors.Is(e, errPassPending):
			if held < 0 {
				held = i
			}
		case e != nil:
			failed = append(failed, e)
		}
	}
	switch {
	case len(failed) == 1:
		return failed[0]
	case len(failed) > 1:
		return errors.Join(failed...)
	case held >= 0:
		return s.Print(map[string]any{"action": "review-pending", "head": pr.Head.SHA, "pass": plans[held].pass})
	}
	if len(posted)+len(plans) >= needed {
		return s.markDone(ctx, r.Name, n, pr.Head.SHA)
	}
	return nil
}

// feedbackReadLimit caps the optional thread read; a quarter of the review
// timeout applies when that is shorter. A var so tests can shorten it.
var feedbackReadLimit = 20 * time.Second

func feedbackTimeout(review time.Duration) time.Duration {
	return min(feedbackReadLimit, review/4)
}

// reviewSkip records a candidate that did not produce the pass, and why.
type reviewSkip struct {
	Reviewer string `json:"reviewer"`
	Reason   string `json:"reason"`
	Reset    string `json:"reset,omitempty"`
	Note     string `json:"note,omitempty"`
	// Host is where the reviewer ran (headless) or was asked to run (panewire).
	Host string `json:"host,omitempty"`
	// Profile is the launch profile the reviewer ran as: the reviewer's hk job
	// record names it for a spawned panewire pane, the configured name for a
	// headless reviewer. Empty before any profile was chosen.
	Profile string `json:"profile,omitempty"`
	// ExitCode, StderrTail and Cause say why a reviewer process failed; see
	// withCause. StderrTail is redacted and bounded.
	ExitCode   *int   `json:"exit_code,omitempty"`
	StderrTail string `json:"stderr_tail,omitempty"`
	Cause      string `json:"cause,omitempty"`
}

func describeSkips(skips []reviewSkip) string {
	parts := make([]string, 0, len(skips))
	for _, sk := range skips {
		part := sk.Reviewer + ": " + sk.Reason
		if sk.Reset != "" {
			part += " until " + sk.Reset
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

// markDone creates the completion ref once every required pass is posted; the
// daemon skips only done heads. A lost create race is tolerated when the ref
// exists on recheck.
func (s *Service) markDone(ctx context.Context, repo string, n int, sha string) error {
	done, e := s.GH.ReviewClaimed(ctx, repo, n, sha+"-done")
	if e != nil && !errors.Is(e, github.ErrPollSkipped) {
		return e
	}
	if done {
		return nil
	}
	if e = s.GH.CreateRef(ctx, repo, doneRef(n, sha), sha); e != nil {
		exists, check := s.GH.ReviewClaimed(ctx, repo, n, sha+"-done")
		if check != nil || !exists {
			return errors.Join(e, check)
		}
	}
	return nil
}

var hunk = regexp.MustCompile(`^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,[0-9]+)? @@`)

// parseFindings validates a reviewer's result. A malformed document (not JSON,
// unknown fields, more than 50 findings, an invalid severity) rejects the whole
// result. A single bad finding does not: an empty or over-long body is dropped
// and reported, and every other finding is kept with the anchor GitHub can
// give it: its line when that is a new-side line of the diff, else its file
// when the path is in the diff, else the review body.
func parseFindings(b []byte, diff string) ([]github.Finding, []droppedFinding, error) {
	// Summary is the job result's short summary that workers write on
	// completion (hk job result summary); it carries no findings and is not
	// used here, but its presence must not make a finished review look lost.
	var v struct {
		Findings []github.Finding `json:"findings"`
		Summary  string           `json:"summary"`
	}
	if e := configJSON(b, &v); e != nil {
		return nil, nil, e
	}
	if v.Findings == nil || len(v.Findings) > 50 {
		return nil, nil, errors.New("review must contain a findings array of at most 50 items")
	}
	valid := map[string]map[int]bool{}
	file := ""
	line := 0
	inside := false
	for _, s := range strings.Split(diff, "\n") {
		if strings.HasPrefix(s, "diff --git ") {
			inside = false
			file = ""
			continue
		}
		if !inside && strings.HasPrefix(s, "+++ ") {
			file = strings.TrimPrefix(s, "+++ ")
			if strings.HasPrefix(file, `"`) {
				v, e := strconv.Unquote(file)
				if e != nil {
					return nil, nil, e
				}
				file = v
			}
			file = strings.TrimPrefix(file, "b/")
			valid[file] = map[int]bool{}
			continue
		}
		if m := hunk.FindStringSubmatch(s); len(m) == 2 {
			line, _ = strconv.Atoi(m[1])
			inside = true
			continue
		}
		if inside && len(s) > 0 && (s[0] == '+' || s[0] == ' ') {
			valid[file][line] = true
			line++
		}
	}
	var kept []github.Finding
	var dropped []droppedFinding
	for _, f := range v.Findings {
		switch f.Severity {
		case "", "blocking", "major", "minor", "critical", "high", "medium", "low":
		default:
			return nil, nil, errors.New("invalid finding severity")
		}
		// A bad body costs that finding, not the pass.
		switch {
		case strings.TrimSpace(f.Body) == "":
			dropped = append(dropped, droppedFinding{f.Path, f.Line, "empty body"})
			continue
		case len(f.Body) > maxFindingBody:
			dropped = append(dropped, droppedFinding{f.Path, f.Line, "body over 4000 bytes"})
			continue
		}
		lines, inDiff := valid[f.Path]
		switch {
		case inDiff && lines[f.Line]:
			f.Anchor = github.AnchorLine
		case inDiff:
			f.Anchor = github.AnchorFile
		default:
			f.Anchor = github.AnchorOutside
		}
		kept = append(kept, f)
	}
	return kept, dropped, nil
}

// maxFindingBody is the longest finding body a pass accepts.
const maxFindingBody = 4000

// droppedFinding is a finding parseFindings left out of the pass.
type droppedFinding struct {
	Path   string
	Line   int
	Reason string
}

func configJSON(b []byte, v any) error {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("trailing data after JSON")
	}
	return nil
}

func (s *Service) notifyReviewResultLost(ctx context.Context, r config.Repo, pr github.Pull, plan *passPlan, lost *reviewResultLost) error {
	sent, err := loadSentKeys(filepath.Join(s.state(r.Name), "review-result-lost-sent"), math.MaxInt)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%d %s %d", pr.Number, pr.Head.SHA, plan.pass)
	if sent.has(key) {
		return nil
	}
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	err = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-review-result-lost-%s-%d-%s-%d", r.Name, pr.Number, pr.Head.SHA, plan.pass),
		Lane: s.Config.Notifier.Lane, Type: "automation-failed",
		Text: fmt.Sprintf("[mergewake] reviewer finished but its result was lost for %s#%d head=%s pass=%d lens=%s reviewer=%s report=%s", r.Name, pr.Number, pr.Head.SHA, plan.pass, plan.lens, lost.label, lost.report),
	})
	if err != nil {
		return err
	}
	return sent.add(key)
}
