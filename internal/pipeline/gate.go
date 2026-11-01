package pipeline

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// gateVerdict is the merge-gate outcome for one PR head. State is "ready",
// "blocked" or "pending"; pending verdicts emit nothing and never merge.
// Reasons use a closed vocabulary: check_failed:<name>, threads_open:<n>,
// blocking_findings:<n>, triage_unanswered:<n>, review_missing, base_stale and
// conflict. The two graded reasons replace threads_open when the policy has a
// review_gate.land: graded or the PR takes the fast lane.
type gateVerdict struct {
	State      string
	Reasons    []string
	SameFamily bool
	Review     reviewRecord
}

// reviewRecord is what the gate knows about a PR's review, printed by
// `land --dry-run` and in the land audit record: the patch the head carries,
// the patches reviewed so far, how many review rounds that took, where this
// head's review came from when it did not run on this head, and how the
// findings stand.
type reviewRecord struct {
	PatchID   string       `json:"patch_id,omitempty"`
	Rounds    int          `json:"rounds"`
	Reviewed  []string     `json:"reviewed_patch_ids,omitempty"`
	Inherited string       `json:"inherited_from,omitempty"`
	Findings  dispositions `json:"dispositions"`
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

// mergeGate evaluates the repo session's merge gate once for head H:
// every required check green on H, no unresolved review threads (or, under a
// graded policy, no unfixed blocking or unanswered triage findings), the
// review completion marker mergewake/reviews/<pr>/<H>-done present, the
// base branch an ancestor of H, and the PR mergeable. A review covers a patch,
// not a SHA: a head whose patch id equals a reviewed head's inherits that
// head's passes, so merging the base into a head costs a CI run, not a review. A pending required
// check waits silently unless a sibling already failed; mergeable unknown
// also waits when nothing else holds.
func (s *Service) mergeGate(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, baseHead string) (gateVerdict, error) {
	v := gateVerdict{State: "ready"}
	states, e := s.GH.CheckStates(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if e != nil {
		return v, e
	}
	pending := false
	for _, name := range p.RequiredChecks {
		switch states[name] {
		case github.Red:
			v.Reasons = append(v.Reasons, "check_failed:"+name)
		case github.Green:
		default:
			pending = true
		}
	}
	if pending {
		// A sibling failure is already final for this head; otherwise the
		// incomplete evidence stays quiet.
		v.State = "blocked"
		if len(v.Reasons) == 0 {
			v.State = "pending"
		}
		// The review record stays readable while CI runs; a failed read costs
		// only the record.
		if history, e := s.GH.ReviewHistory(ctx, r.Name, pr.Number, s.Config.Review.Login); e == nil {
			v.Review.PatchID = s.patchIDOf(ctx, r.Name, pr.Number, pr.Head.SHA)
			v.Review.Rounds, v.Review.Reviewed = reviewRounds(history)
		}
		return v, nil
	}
	// The decision-head review feedback and the file list come first: the
	// PR's class and size decide whether findings are graded or counted.
	fb, e := s.GH.ReviewFeedback(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if fb.HeadSHA != pr.Head.SHA {
		return v, errors.New("review feedback is not bound to the decision head")
	}
	login := policy.NormalizeAuthor(s.Config.Review.Login)
	files, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if len(files) != pr.ChangedFiles {
		return v, errors.New("incomplete PR file list")
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
	_, _, v.Review.Findings = gradeThreadsDetailed(fb, gate.WithOwnLogin(login), login, class, fastLane, fixes.verified)
	if fixes.err != nil {
		// A fix claim GitHub could not check holds the verdict; it neither
		// passes nor blocks on a guess.
		return v, fixes.err
	}
	if (p.ReviewGate != nil && p.ReviewGate.Land == "graded") || fastLane {
		// Graded: findings are weighed by severity instead of counted.
		blocking, triage := gradeThreads(fb, gate.WithOwnLogin(login), login, class, fastLane, fixes.verified)
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
	}
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
	}
	fresh, e := s.GH.Ancestor(ctx, r.Name, baseHead, pr.Head.SHA)
	if e != nil {
		return v, e
	}
	if !fresh {
		v.Reasons = append(v.Reasons, "base_stale")
	}
	if (pr.Mergeable != nil && !*pr.Mergeable) || pr.MergeableState == "dirty" {
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
func gradeThreads(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck) (blocking, triage int) {
	blocking, triage, _ = gradeThreadsDetailed(fb, g, login, class, fastLane, verify)
	return blocking, triage
}

// gradeThreadsDetailed is gradeThreads together with how every graded thread
// stands, for the review record.
func gradeThreadsDetailed(fb github.ReviewFeedback, g policy.ReviewGate, login, class string, fastLane bool, verify fixCheck) (blocking, triage int, d dispositions) {
	blocking = fb.ChangesRequested
	d.OpenBlocking = fb.ChangesRequested
	for _, t := range fb.Threads {
		if len(t.Comments) == 0 {
			if !t.IsResolved {
				blocking++
				d.OpenBlocking++
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
			switch kind := blockDisposition(t, f.OwnReview, verify); {
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
			}
		case policy.GradeTriage:
			// The fast lane drops rule-graded triage, but a trusted-review
			// finding whose severity cannot be read still needs an answer.
			unreadable := f.OwnReview && !policy.KnownSeverity(f.Severity)
			switch {
			case f.Resolved:
				d.Resolved++
			case answered(t, f.OwnReview):
				d.Answered++
			case !fastLane || unreadable:
				triage++
				d.OpenTriage++
			}
		case policy.GradeAdvisory:
			d.Advisory++
		}
	}
	return blocking, triage, d
}

var (
	// fixReply opens with a fix.
	fixReply = regexp.MustCompile(`(?is)^\W*fix(?:ed|es)?\b`)
	// commitToken is a commit SHA, abbreviated or full, named in a reply.
	commitToken = regexp.MustCompile(`\b[0-9a-f]{7,40}\b`)
	// dismissReply opens with a dismissal and gives a reason after it.
	dismissReply = regexp.MustCompile(`(?is)^\W*dismiss(?:ed|al)?\b\W*\w`)
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
func blockDisposition(t github.ReviewThread, ownReview bool, verify fixCheck) string {
	author := policy.NormalizeAuthor(t.Comments[0].Author)
	posted := postedOn(t.Comments[0])
	for i := len(t.Comments) - 1; i > 0; i-- {
		c := t.Comments[i]
		if !answeredBy(c, author, ownReview) {
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
func answered(t github.ReviewThread, ownReview bool) bool {
	author := policy.NormalizeAuthor(t.Comments[0].Author)
	for _, c := range t.Comments[1:] {
		if answeredBy(c, author, ownReview) {
			return true
		}
	}
	return false
}

// answeredBy reports whether reply c, on a finding by author, is a human
// answer under the rules of answered.
func answeredBy(c github.ReviewComment, author string, ownReview bool) bool {
	who := policy.NormalizeAuthor(c.Author)
	if who == "" || c.AuthorType == "Bot" || strings.HasSuffix(c.Author, "[bot]") {
		return false
	}
	return who != author || (ownReview && !strings.Contains(c.Body, "<!-- mergewake-severity:"))
}
