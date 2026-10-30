package pipeline

import (
	"context"
	"errors"
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
}

// mergeGate evaluates the repo session's merge gate once for head H:
// every required check green on H, no unresolved review threads (or, under a
// graded policy, no unfixed blocking or unanswered triage findings), the
// review completion marker mergewake/reviews/<pr>/<H>-done present, the
// base branch an ancestor of H, and the PR mergeable. A pending required
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
	fastLane := p.FastLaneEligible(class, pr.Additions+pr.Deletions)
	if (p.ReviewGate != nil && p.ReviewGate.Land == "graded") || fastLane {
		// Graded: findings are weighed by severity instead of counted.
		gate := policy.ReviewGate{}
		if p.ReviewGate != nil {
			gate = *p.ReviewGate
		}
		blocking, triage := gradeThreads(fb, gate.WithOwnLogin(login), login, class, pr.Head.SHA, fastLane)
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
		// A review.login finding on the current head stays held while it was
		// resolved without a new head: anyone who can click resolve — including
		// the PR author — would otherwise clear the trusted review's findings.
		// Clearing needs a new head and a new review pass.
		held := open
		if login != "" {
			for _, t := range fb.Threads {
				if t.IsResolved && len(t.Comments) > 0 && postedOn(t.Comments[0]) == pr.Head.SHA && policy.NormalizeAuthor(t.Comments[0].Author) == login {
					held++
				}
			}
		}
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
	receipts, e := s.GH.ReviewPasses(ctx, r.Name, pr.Number, pr.Head.SHA, s.Config.Review.Login)
	if e != nil {
		return v, e
	}
	_, complete := distinctPasses(receipts, needed)
	for _, receipt := range receipts {
		v.SameFamily = v.SameFamily || receipt.SameFamily
	}
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
	if !done || !complete {
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
// on. A block-class finding blocks until a new head resolves it, whoever wrote
// it: unresolved counts, and so does one posted on this head that someone
// resolved without a new head. A triage finding blocks until someone other than
// its author replies; resolving or answering it needs no new head. Advisory
// findings never block, and the fast lane drops triage except for unreadable
// review.login evidence. Outdated findings clear as ReviewGate.OutdatedCleared
// allows. Reviewers still requesting changes, and unresolved threads with no
// comment to grade, count as blocking findings.
func gradeThreads(fb github.ReviewFeedback, g policy.ReviewGate, login, class, head string, fastLane bool) (blocking, triage int) {
	blocking = fb.ChangesRequested
	for _, t := range fb.Threads {
		if len(t.Comments) == 0 {
			if !t.IsResolved {
				blocking++
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
			if !f.Resolved || f.CommitSHA == head {
				blocking++
			}
		case policy.GradeTriage:
			// The fast lane drops rule-graded triage, but a trusted-review
			// finding whose severity cannot be read still needs an answer.
			unreadable := f.OwnReview && !policy.KnownSeverity(f.Severity)
			if (!fastLane || unreadable) && !f.Resolved && !answered(t) {
				triage++
			}
		}
	}
	return blocking, triage
}

// answered reports a human reply from someone other than the finding's author.
func answered(t github.ReviewThread) bool {
	author := policy.NormalizeAuthor(t.Comments[0].Author)
	for _, c := range t.Comments[1:] {
		who := policy.NormalizeAuthor(c.Author)
		if who != "" && who != author && c.AuthorType != "Bot" && !strings.HasSuffix(c.Author, "[bot]") {
			return true
		}
	}
	return false
}
