package pipeline

import (
	"context"
	"errors"
	"strconv"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// gateVerdict is the merge-gate outcome for one PR head. State is "ready",
// "blocked" or "pending"; pending verdicts emit nothing and never merge.
// Reasons use a closed vocabulary: check_failed:<name>, threads_open:<n>,
// review_missing, base_stale and conflict.
type gateVerdict struct {
	State      string
	Reasons    []string
	SameFamily bool
}

// mergeGate evaluates the repo session's merge gate once for head H:
// every required check green on H, zero unresolved review threads, the
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
	open, e := s.GH.Unresolved(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	// A review.login finding on the current head stays held while it was
	// resolved without a new head: anyone who can click resolve — including
	// the PR author — would otherwise clear the trusted review's findings.
	// Clearing needs a new head and a new review pass.
	fb, e := s.GH.ReviewFeedback(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if fb.HeadSHA != pr.Head.SHA {
		return v, errors.New("review feedback is not bound to the decision head")
	}
	login := policy.NormalizeAuthor(s.Config.Review.Login)
	held := open
	if login != "" {
		for _, t := range fb.Threads {
			if t.IsResolved && len(t.Comments) > 0 && t.Comments[0].CommitSHA == pr.Head.SHA && policy.NormalizeAuthor(t.Comments[0].Author) == login {
				held++
			}
		}
	}
	if held > 0 {
		v.Reasons = append(v.Reasons, "threads_open:"+strconv.Itoa(held))
	}
	// The -done marker alone is forgeable by anyone with push access, so the
	// gate also requires the head-bound passes the trusted review identity
	// posted: one for routine changes, two distinct passes for blocking ones.
	files, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return v, e
	}
	if len(files) != pr.ChangedFiles {
		return v, errors.New("incomplete PR file list")
	}
	needed := 1
	if p.ClassifyGuarded(files, r.Policy).Class == "blocking" {
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
