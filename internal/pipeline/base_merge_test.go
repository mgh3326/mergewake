package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// mergeRunner answers the git calls of a clean-merge check from a fixed tree:
// head's tree is the one the merge of its parents makes, or the merge conflicts.
// Every other command, git included, goes to the runner it wraps.
type mergeRunner struct {
	integrations.Runner
	headTree, mergedTree string
	conflict             bool
	results              map[[2]string]bool
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
		clean := !m.conflict
		if m.results != nil && len(args) >= 6 {
			clean = m.results[[2]string{args[4], args[5]}]
		}
		if !clean {
			return []byte("conflicted\n"), errors.New("exit status 1")
		}
		return []byte(m.mergedTree + "\n"), nil
	}
	return m.Runner.Run(ctx, command, args, input, dir)
}

func TestReviewInheritsChainPastPartialReceipt(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	g.blocking = true
	g.passes[shaA] = append(g.passes[shaA], github.PassReceipt{Pass: 2})
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", mergedTree: "tree-m"}
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	// B inherits A. Leave a stale partial receipt on C before reviewing D.
	g.passes[shaC] = []github.PassReceipt{{Pass: 1}}
	g.parents2[shaC] = []string{shaB, shaC}
	g.parents2[shaD] = []string{shaC, shaA}
	g.pull.Head.SHA = shaD
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", mergedTree: "tree-m", results: map[[2]string]bool{{shaB, shaC}: true, {shaC, shaA}: true}}
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 || len(g.passes[shaD]) != 0 {
		t.Fatalf("chain with a partial receipt did not inherit: calls=%d passes=%d", runner.calls, len(g.passes[shaD]))
	}
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

func TestCountedHistoryUsesFirstParentsRecordedPasses(t *testing.T) {
	s, g, _, _, r := setup(t)
	first, merge := shaA, shaD
	g.head = shaD
	g.pull.Head.SHA = merge
	g.blocking = true
	g.files = []policy.File{{Path: "tool", Kind: "modified"}}
	g.passes = map[string][]github.PassReceipt{
		first: {{Pass: 1, Lens: "correctness", Patch: "old-policy"}},
		shaB:  {{Pass: 1, Lens: "correctness", Patch: "next"}},
		merge: {{Pass: 1, Lens: "correctness", Patch: "merge"}},
	}
	g.parents2 = map[string][]string{merge: {first, shaC}}
	g.refs = map[string]bool{doneRef(g.pull.Number, first): true}
	history := []github.ReviewedPass{
		{Head: first, PassReceipt: g.passes[first][0]},
		{Head: shaB, PassReceipt: g.passes[shaB][0]},
		{Head: merge, PassReceipt: g.passes[merge][0]},
	}
	got, err := s.countedHistory(context.Background(), r, g.pull, history)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("countedHistory retained base merge under changed two-pass policy: got %d records, want 2", len(got))
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

// A base merge of a head that was itself a clean base merge inherits the review
// of the reviewed head behind the chain: the chain adds no pass and no round.
func TestReviewInheritsBaseMergeOfInheritedMerge(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", mergedTree: "tree-m"}
	var out bytes.Buffer
	s.Out = &out
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 || len(g.passes[shaB]) != 0 {
		t.Fatalf("the clean base merge B was reviewed: calls=%d", runner.calls)
	}
	out.Reset()
	g.parents2[shaD] = []string{shaB, shaC}
	g.pull.Head.SHA = shaD
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 || len(g.passes[shaD]) != 0 {
		t.Fatalf("a base merge of an inherited merge was reviewed again: calls=%d passes=%d", runner.calls, len(g.passes[shaD]))
	}
	var got map[string]any
	if e := json.Unmarshal(out.Bytes(), &got); e != nil || got["action"] != "inherited-review" || got["inherited_from"] != shaA {
		t.Fatalf("%v %s", e, out.String())
	}
}

// A base merge behind an inherited merge that conflicts is new text: the chain
// stops there, so the head is reviewed as before.
func TestReviewSchedulesBaseMergeOfConflictedMerge(t *testing.T) {
	s, g, runner, review := baseMergedAtB(t)
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", mergedTree: "tree-m", results: map[[2]string]bool{{shaA, shaC}: false, {shaB, shaC}: true}}
	g.parents2[shaD] = []string{shaB, shaC}
	g.pull.Head.SHA = shaD
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 2 || len(g.passes[shaD]) != 1 {
		t.Fatalf("a base merge behind a conflicted merge was not reviewed: calls=%d passes=%d", runner.calls, len(g.passes[shaD]))
	}
}

// hunkBody is one "@@" hunk holding lines (each starting with ' ', '+', '-' or
// '\'); the hunk counts are derived from them.
func hunkBody(lines ...string) string {
	oldN, newN := 0, 0
	for _, l := range lines {
		switch l[0] {
		case ' ':
			oldN++
			newN++
		case '-':
			oldN++
		case '+':
			newN++
		}
	}
	var b strings.Builder
	b.WriteString("@@ -1," + itoa(oldN) + " +1," + itoa(newN) + " @@\n")
	for _, l := range lines {
		b.WriteString(l + "\n")
	}
	return b.String()
}

// A clean base merge that splits an unchanged gap in an elided file changes the
// digest (the position of the + lines is part of it), so its patch id differs
// from the reviewed head's. It still inherits that review through
// cleanBaseMergePasses, which checks the merge itself: no pass is scheduled.
func TestReviewInheritsCleanBaseMergeThatSplitsElidedGap(t *testing.T) {
	// The unchanged text alone is over reviewDiffBudget, so the file is elided.
	const head = "diff --git a/big.txt b/big.txt\nindex 1111111..2222222 100644\n--- a/big.txt\n+++ b/big.txt\n"
	bigCtx := " " + strings.Repeat("c", reviewDiffBudget+1)
	before := head + hunkBody(bigCtx, "+first", longCtx, "+second", longCtx)
	after := "diff --git a/big.txt b/big.txt\nindex 3333333..4444444 100644\n--- a/big.txt\n+++ b/big.txt\n" +
		hunkBody(bigCtx, "+first", longCtx, longCtx, longCtx) + hunkBody(longCtx, longCtx, longCtx, "+second", longCtx)
	a, _ := summarizeLocalDiff(before, reviewDiffBudget)
	b, _ := summarizeLocalDiff(after, reviewDiffBudget)
	if reviewPatchID(a) == reviewPatchID(b) {
		t.Fatal("the gap split did not change the patch id, so the inheritance below proves nothing")
	}
	s, g, runner, review, _ := reviewedAtADiffs(t, before, after)
	g.parents2 = map[string][]string{shaB: {shaA, shaC}}
	s.Runner = &mergeRunner{Runner: s.Runner, headTree: "tree-m", mergedTree: "tree-m"}
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 || g.postCount != 1 || len(g.passes[shaB]) != 0 {
		t.Fatalf("a clean base merge that split an elided gap was reviewed: calls=%d posts=%d", runner.calls, g.postCount)
	}
}
