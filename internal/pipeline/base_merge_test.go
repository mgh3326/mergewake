package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// mergeRunner answers the git calls of a clean-merge check from a fixed tree:
// head's tree is the one the merge of its parents makes, or the merge conflicts.
// Every other command, git included, goes to the runner it wraps.
type mergeRunner struct {
	integrations.Runner
	headTree, mergedTree string
	conflict             bool
}

func (m *mergeRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command != "git" || len(args) < 3 {
		return m.Runner.Run(ctx, command, args, input, dir)
	}
	switch sub := args[2]; {
	case sub == "cat-file", sub == "fetch":
		return nil, nil
	case sub == "rev-parse" && strings.HasSuffix(args[len(args)-1], "^{tree}"):
		return []byte(m.headTree + "\n"), nil
	case sub == "merge-tree":
		if m.conflict {
			return []byte("conflicted\n"), errors.New("exit status 1")
		}
		return []byte(m.mergedTree + "\n"), nil
	}
	return m.Runner.Run(ctx, command, args, input, dir)
}

// baseMergedAtB makes head B a base merge of reviewed head A: its first parent is
// A and its second is C, the PR base. B's diff changed, as a conflict resolution
// or a base edit beside the PR's hunks does, so B has no equal patch to inherit.
func baseMergedAtB(t *testing.T) (*Service, *fakeGH, *fakeRunner, func(Options) error) {
	t.Helper()
	s, g, runner, review, _ := reviewedAtA(t, toolDiff("aaaaaaa..bbbbbbb", 1, "resolved"))
	g.parents2 = map[string][]string{shaB: {shaA, shaC}}
	return s, g, runner, review
}

// Merging the base into a reviewed head with changed text, a conflict
// resolution, is still reviewed, but it adds no round: the PR has one reviewed
// patch so far, not two.
func TestRoundsSkipBaseMergeWithChangedPatch(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", conflict: true}
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 2 || len(g.passes[shaB]) != 1 {
		t.Fatalf("the base merge was not reviewed: calls=%d passes=%d", runner.calls, len(g.passes[shaB]))
	}
	g.enabled = true
	if v := gateAt(t, s, g, shaB); v.Review.Rounds != 1 || len(v.Review.Reviewed) != 1 {
		t.Fatalf("base merge counted as a round: rounds=%d reviewed=%v", v.Review.Rounds, v.Review.Reviewed)
	}
}

// A merge whose first parent has no review is a new patch the reviewer has not
// seen, so it counts as a round as before.
func TestRoundsCountMergeOfUnreviewedParent(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	g.parents2 = map[string][]string{shaB: {shaC, shaA}}
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 2 {
		t.Fatalf("the merge was not reviewed: calls=%d", runner.calls)
	}
	g.enabled = true
	if v := gateAt(t, s, g, shaB); v.Review.Rounds != 2 {
		t.Fatalf("merge of an unreviewed parent did not count: rounds=%d", v.Review.Rounds)
	}
}

// A clean base merge of a reviewed head inherits that review: its completion
// marker is written and no pass is scheduled.
func TestReviewInheritsCleanBaseMerge(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", mergedTree: "tree-m"}
	var out bytes.Buffer
	s.Out = &out
	claims := g.claimCount
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 || g.postCount != 1 || len(g.passes[shaB]) != 0 {
		t.Fatalf("a clean base merge was reviewed: calls=%d posts=%d", runner.calls, g.postCount)
	}
	if !g.refs[doneRef(7, shaB)] || g.claimCount != claims+1 {
		t.Fatalf("the inherited head has no completion marker: refs=%v claims=%d", g.refs, g.claimCount-claims)
	}
	var got map[string]any
	if e := json.Unmarshal(out.Bytes(), &got); e != nil || got["action"] != "inherited-review" || got["inherited_from"] != shaA {
		t.Fatalf("%v %s", e, out.String())
	}
	g.enabled = true
	// The marker the inherited review wrote is what makes the head reviewed here;
	// the verdict reads no inheritance from it, and the head adds no round.
	if v := gateAt(t, s, g, shaB); v.Review.Rounds != 1 || reasons(v) != "" {
		t.Fatalf("%+v", v)
	}
}

// A base merge that conflicts is new text, so it is reviewed rather than
// inherited.
func TestReviewSchedulesConflictedBaseMerge(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", conflict: true}
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 2 || g.postCount != 2 || len(g.passes[shaB]) != 1 {
		t.Fatalf("a conflicted base merge was not reviewed: calls=%d posts=%d", runner.calls, g.postCount)
	}
}
