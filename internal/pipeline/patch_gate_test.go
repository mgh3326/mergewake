package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// toolDiff is a one-file PR diff. index and start vary with the base the head
// was built on, never with the patch itself.
func toolDiff(index string, start int, added string) string {
	return "diff --git a/tool b/tool\nindex " + index + " 100644\n--- a/tool\n+++ b/tool\n@@ -" + itoa(start) + ",3 +" + itoa(start) + ",3 @@\n keep\n-old\n+" + added + "\n end\n"
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestPatchIDIgnoresBaseAndTracksPatch(t *testing.T) {
	base := patchID(toolDiff("1111111..2222222", 1, "new"))
	if base == "" || len(base) != 40 {
		t.Fatalf("patch id %q", base)
	}
	if got := patchID(toolDiff("aaaaaaa..bbbbbbb", 41, "new")); got != base {
		t.Fatalf("a base merged in changed the patch id: %s != %s", got, base)
	}
	if got := patchID(strings.ReplaceAll(toolDiff("1111111..2222222", 1, "new"), "\n", "\r\n")); got != base {
		t.Fatalf("line endings changed the patch id: %s", got)
	}
	if got := patchID(toolDiff("1111111..2222222", 1, "newer")); got == base {
		t.Fatal("a different patch kept the patch id")
	}
	if patchID("") != "" || patchID("not a diff\n") != "" {
		t.Fatal("no diff must give no patch id")
	}
	// File order does not matter in stable mode.
	a := toolDiff("1111111..2222222", 1, "new")
	b := strings.ReplaceAll(a, "tool", "other")
	if patchID(a+b) != patchID(b+a) {
		t.Fatal("file order changed the patch id")
	}
}

// The reference values were computed with `git patch-id --stable` over the
// same text.
// The reference values were computed with `git patch-id --stable` over the
// same text.
func TestPatchIDMatchesGit(t *testing.T) {
	const (
		tool = "diff --git a/tool b/tool\nindex 1111111..2222222 100644\n--- a/tool\n+++ b/tool\n@@ -1,3 +1,3 @@\n keep\n-old\n+new\n end\n"
		doc  = "diff --git a/docs/a.md b/docs/a.md\nnew file mode 100644\nindex 0000000..3333333\n--- /dev/null\n+++ b/docs/a.md\n@@ -0,0 +1,2 @@\n+first line\n+\ta tabbed  line\n"
	)
	for name, tt := range map[string]struct{ diff, want string }{
		"one file": {toolDiff("1111111..2222222", 1, "new"), "c2497266b0d291e5aa5cb66c30c72841f8771bd6"},
		"shifted":  {toolDiff("aaaaaaa..bbbbbbb", 41, "new"), "c2497266b0d291e5aa5cb66c30c72841f8771bd6"},
		"new file": {doc, "571679184b816b18396f1ba9d61db893287e2c86"},
		"text+new": {tool + doc, "1960eb7efb53fdfde3cbd11507e5e0d420f6475c"},
	} {
		if got := patchID(tt.diff); got != tt.want {
			t.Errorf("%s: patch id %s, git says %s", name, got, tt.want)
		}
	}
}

// A binary diff has no content to hash, so its blob ids tell two binary changes
// to the same file apart, while a longer abbreviation does not.
func TestPatchIDBinaryUsesBlobIDs(t *testing.T) {
	bin := func(index string) string {
		return "diff --git a/bin b/bin\nindex " + index + " 100644\nBinary files a/bin and b/bin differ\n"
	}
	one := patchID(bin("4444444..5555555"))
	if one == "" || one != patchID(bin("44444444..55555555")) {
		t.Fatalf("abbreviation length changed the patch id: %s", one)
	}
	if one == patchID(bin("4444444..6666666")) {
		t.Fatal("a different binary change kept the patch id")
	}
}

// A whitespace-only edit keeps the git patch id but is a different reviewed
// patch; a base merged in keeps both.
func TestReviewPatchIDSeesWhitespace(t *testing.T) {
	a := toolDiff("1111111..2222222", 1, "\tnew")
	b := toolDiff("1111111..2222222", 1, "    new")
	if patchID(a) != patchID(b) {
		t.Fatal("git patch id must ignore whitespace")
	}
	if reviewPatchID(a) == reviewPatchID(b) {
		t.Fatal("a whitespace-only edit inherited the review")
	}
	if reviewPatchID(a) != reviewPatchID(toolDiff("aaaaaaa..bbbbbbb", 41, "\tnew")) {
		t.Fatal("a base merged in changed the review patch id")
	}
	if reviewPatchID("") != "" {
		t.Fatal("no diff must give no patch id")
	}
}

// A rename with no hunk ends at the next file: reordering files keeps the id.
func TestPatchIDHunklessFilesStayOrderIndependent(t *testing.T) {
	rename := "diff --git a/old b/new\nsimilarity index 100%\nrename from old\nrename to new\n"
	mode := "diff --git a/run b/run\nold mode 100644\nnew mode 100755\n"
	text := toolDiff("1111111..2222222", 1, "new")
	want := patchID(rename + mode + text)
	for _, order := range []string{rename + text + mode, mode + rename + text, text + mode + rename} {
		if got := patchID(order); got != want {
			t.Fatalf("file order changed the id: %s != %s\n%s", got, want, order)
		}
	}
}

// Only the passes recorded for this patch count toward inheriting it.
func TestInheritedPassesNeedMatchingReceipts(t *testing.T) {
	pass := func(head string, n int, family, patch string) github.ReviewedPass {
		return github.ReviewedPass{Head: head, PassReceipt: github.PassReceipt{Pass: n, Family: family, Lens: map[int]string{1: "correctness", 2: "state-risk"}[n], Patch: patch}}
	}
	history := []github.ReviewedPass{pass(shaA, 1, "anthropic", "p1"), pass(shaA, 2, "openai", "")}
	if _, _, ok := inheritedPasses(history, "p1", shaB, 2); ok {
		t.Fatal("a pass without this patch id completed the required pair")
	}
	history = []github.ReviewedPass{pass(shaA, 1, "anthropic", "p1"), pass(shaA, 2, "openai", "p2")}
	if _, _, ok := inheritedPasses(history, "p1", shaB, 2); ok {
		t.Fatal("a pass for another patch completed the required pair")
	}
	history = []github.ReviewedPass{pass(shaA, 1, "anthropic", "p1"), pass(shaA, 2, "openai", "p1")}
	if from, _, ok := inheritedPasses(history, "p1", shaB, 2); !ok || from != shaA {
		t.Fatalf("both passes carry the patch: %v %s", ok, from)
	}
	if _, _, ok := inheritedPasses(history, "", shaB, 2); ok {
		t.Fatal("an unknown patch id inherited a review")
	}
}

// reviewedAtA reviews head A of PR 7 for real, then moves the PR to head B whose
// diff is diffB, as if the base had been merged into it.
func reviewedAtA(t *testing.T, diffB string) (*Service, *fakeGH, *fakeRunner, func(Options) error, string) {
	t.Helper()
	s, g, _, _, r := setup(t)
	runner := s.Runner.(*fakeRunner)
	diffA := toolDiff("1111111..2222222", 1, "new")
	g.diffs = map[string]string{shaA: diffA, shaB: diffB}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if g.postCount != 1 || runner.calls != 1 {
		t.Fatalf("first head not reviewed: posts=%d calls=%d", g.postCount, runner.calls)
	}
	if got := g.passes[shaA][0].Patch; got != reviewPatchID(diffA) {
		t.Fatalf("the pass did not record its patch id: %q", got)
	}
	g.pull.Head.SHA = shaB
	g.pull.Base.SHA = shaC
	return s, g, runner, func(o Options) error { return s.Review(context.Background(), r, 7, "openai", o) }, reviewPatchID(diffA)
}

func TestReviewInheritsEqualPatch(t *testing.T) {
	s, g, runner, review, patch := reviewedAtA(t, toolDiff("aaaaaaa..bbbbbbb", 41, "new"))
	var out bytes.Buffer
	s.Out = &out
	claims := g.claimCount
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 || g.postCount != 1 {
		t.Fatalf("an equal patch was reviewed again: calls=%d posts=%d", runner.calls, g.postCount)
	}
	if !g.refs[doneRef(7, shaB)] || g.claimCount != claims+1 {
		t.Fatalf("the inherited head has no completion marker: refs=%v claims=%d", g.refs, g.claimCount-claims)
	}
	var got map[string]any
	if e := json.Unmarshal(out.Bytes(), &got); e != nil || got["action"] != "inherited-review" || got["inherited_from"] != shaA || got["patch_id"] != patch {
		t.Fatalf("%v %s", e, out.String())
	}
	// A dry run decides without writing the marker.
	delete(g.refs, doneRef(7, shaB))
	if e := review(Options{DryRun: true}); e != nil || g.refs[doneRef(7, shaB)] {
		t.Fatalf("dry run wrote a marker: %v", e)
	}
	// The gate accepts the inherited review: only CI is outstanding.
	g.refs[doneRef(7, shaB)] = true
	g.enabled = true
	v := gateAt(t, s, g, shaB)
	if v.State != "ready" || reasons(v) != "" || v.Review.Inherited != shaA || v.Review.PatchID != patch || v.Review.Rounds != 1 {
		t.Fatalf("%+v", v)
	}
}

func TestReviewSchedulesChangedPatch(t *testing.T) {
	s, g, runner, review, patch := reviewedAtA(t, toolDiff("aaaaaaa..bbbbbbb", 1, "newer"))
	if e := review(Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 2 || g.postCount != 2 || len(g.passes[shaB]) != 1 {
		t.Fatalf("a changed patch was not reviewed: calls=%d posts=%d", runner.calls, g.postCount)
	}
	g.enabled = true
	v := gateAt(t, s, g, shaB)
	if v.Review.Inherited != "" || v.Review.PatchID == patch || v.Review.Rounds != 2 || len(v.Review.Reviewed) != 2 || reasons(v) != "" {
		t.Fatalf("%+v", v)
	}
}

// Without its own review or marker, a changed patch has no review at all.
func TestGateChangedPatchNeedsReview(t *testing.T) {
	s, g, _, _, _ := reviewedAtA(t, toolDiff("aaaaaaa..bbbbbbb", 41, "newer"))
	g.enabled = true
	if v := gateAt(t, s, g, shaB); reasons(v) != "review_missing" || v.Review.Rounds != 1 {
		t.Fatalf("%+v", v)
	}
}

// base_stale never asks for a review: the stale head keeps its review, and once
// the base is merged in the new head with the same patch waits on CI alone.
func TestBaseStateNeverReReviews(t *testing.T) {
	s, g, _, _, _ := reviewedAtA(t, toolDiff("aaaaaaa..bbbbbbb", 41, "new"))
	g.enabled = true
	g.refs[doneRef(7, shaA)] = true
	g.refs[doneRef(7, shaB)] = true

	// Head A after the base moved on.
	g.pull.Head.SHA = shaA
	g.stale = true
	if v := gateAt(t, s, g, shaB); reasons(v) != "base_stale" {
		t.Fatalf("stale head: %+v", v)
	}
	// Head B has the base merged in; its CI has not finished.
	g.pull.Head.SHA = shaB
	g.stale = false
	g.ci = github.Pending
	if v := gateAt(t, s, g, shaB); v.State != "pending" || reasons(v) != "" {
		t.Fatalf("waiting on CI: %+v", v)
	}
	g.ci = github.Red
	if v := gateAt(t, s, g, shaB); reasons(v) != "check_failed:CI" {
		t.Fatalf("red CI: %+v", v)
	}
	g.ci = github.Green
	if v := gateAt(t, s, g, shaB); v.State != "ready" || reasons(v) != "" {
		t.Fatalf("green CI: %+v", v)
	}
}

func TestLandDryRunPrintsReviewRecord(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	diff := toolDiff("1111111..2222222", 1, "new")
	g.diffs = map[string]string{shaA: diff}
	markReviewed(g)
	g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Family: "anthropic", Patch: reviewPatchID(diff)}}}
	g.feedback.Threads = []github.ReviewThread{
		gradedThread("review-bot", "medium", false, shaA),
		gradedThread("review-bot", "low", false, shaA, human),
		gradedThread("review-bot", "high", true, shaA, human),
		gradedThread("review-bot", "high", false, shaA),
	}
	g.findings = 2
	var out bytes.Buffer
	s.Out = &out
	if e := s.Land(context.Background(), r, 7, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec struct {
			Command string `json:"command"`
			Value   struct {
				Review reviewRecord `json:"review"`
			} `json:"value"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Command != "land" {
			continue
		}
		found = true
		got := rec.Value.Review
		want := dispositions{OpenBlocking: 1, OpenTriage: 1, Answered: 1, Resolved: 1}
		if got.PatchID != reviewPatchID(diff) || got.Rounds != 1 || len(got.Reviewed) != 1 || got.Findings != want {
			t.Fatalf("%+v want dispositions %+v", got, want)
		}
	}
	if !found {
		t.Fatalf("no land record in %s", out.String())
	}
	if !strings.Contains(out.String(), `"patch_id"`) || !strings.Contains(out.String(), `"rounds"`) {
		t.Fatalf("dry-run output lacks patch id or rounds: %s", out.String())
	}
}

// gateAt gates the fake's current PR against baseHead.
func gateAt(t *testing.T, s *Service, g *fakeGH, baseHead string) gateVerdict {
	t.Helper()
	_, _, _, _, r := setup(t)
	p, e := s.Policy(context.Background(), r, baseHead)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.mergeGate(context.Background(), r, g.pull, p, baseHead)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
