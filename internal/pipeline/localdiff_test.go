package pipeline

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// realGitRunner runs git against a test repository and delegates every other
// command to fakeRunner.
type realGitRunner struct{ fakeRunner }

func (r *realGitRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" {
		b, e := exec.CommandContext(ctx, "git", args...).CombinedOutput()
		if e != nil {
			return b, e
		}
		return b, nil
	}
	return r.fakeRunner.Run(ctx, command, args, input, dir)
}

func gitAt(t *testing.T, dir string, args ...string) string {
	t.Helper()
	b, e := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if e != nil {
		t.Fatalf("git %v: %v\n%s", args, e, b)
	}
	return strings.TrimSpace(string(b))
}

// seedLargeDiffRepo makes a source repository with a base commit and a PR head
// that edits one file and renames another, publishes pull/7/head, and clones it
// into the checkout mergewake reviews from.
func seedLargeDiffRepo(t *testing.T) (root, base, head string) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "src")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	write := func(name, body string) {
		t.Helper()
		if e := os.WriteFile(filepath.Join(src, name), []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
	}
	gitAt(t, src, "init", "-b", "main")
	gitAt(t, src, "config", "user.email", "t@example.com")
	gitAt(t, src, "config", "user.name", "t")
	write("tool", "old\n")
	write("old", "moved\n")
	gitAt(t, src, "add", ".")
	gitAt(t, src, "commit", "-m", "base")
	base = gitAt(t, src, "rev-parse", "HEAD")
	write("tool", "new\n")
	gitAt(t, src, "mv", "old", "renamed")
	gitAt(t, src, "add", ".")
	gitAt(t, src, "commit", "-m", "head")
	head = gitAt(t, src, "rev-parse", "HEAD")
	gitAt(t, src, "update-ref", "refs/pull/7/head", "HEAD")
	root = filepath.Join(t.TempDir(), "repo")
	if b, e := exec.Command("git", "clone", src, root).CombinedOutput(); e != nil {
		t.Fatalf("clone: %v\n%s", e, b)
	}
	return root, base, head
}

// GitHub's 406 means the PR is too large for the diff media type; the review
// must continue from the checkout and name the fallback in the pass marker.
func TestReviewFallsBackToLocalDiffOn406(t *testing.T) {
	root, base, head := seedLargeDiffRepo(t)
	s, g, _, _, r := reviewSetup(t)
	runner := &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}
	s.Runner = runner
	r.Root = root
	g.diffErr = &github.APIError{Code: http.StatusNotAcceptable, Method: "GET", Path: "/repos/example/project/pulls/7"}
	g.pull.Base = github.Ref{Ref: "main", SHA: base}
	g.pull.Head = github.Ref{SHA: head, Ref: "feature"}
	g.pull.Head.Repo.FullName = r.Name
	g.pull.ChangedFiles = 2
	g.files = []policy.File{
		{Path: "tool", Kind: "modified"},
		{Path: "renamed", Previous: "old", Kind: "renamed"},
	}
	src, err := s.reviewDiff(context.Background(), r, g.pull)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"diff --git a/tool b/tool", "+new", "rename to renamed"} {
		if !strings.Contains(src.text, want) {
			t.Fatalf("local diff missing %q: %s", want, src.text)
		}
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	p := g.passes[head]
	if len(p) != 1 || p[0].DiffSource != "local" {
		t.Fatalf("pass receipt = %+v", p)
	}
	if !strings.Contains(p[0].DiffSummarized, "renamed") {
		t.Fatalf("pure rename not listed: %q", p[0].DiffSummarized)
	}
	if !strings.Contains(runner.input, "git diff "+base+"..."+head) {
		t.Fatal(runner.input)
	}
}

// Any other failure still stops the review as before: only 406 uses the local
// checkout.
func TestReviewLocalFallbackOnlyOn406(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	g.diffErr = &github.APIError{Code: http.StatusInternalServerError, Method: "GET", Path: "/repos/example/project/pulls/7"}
	r.Root = filepath.Join(t.TempDir(), "missing")
	err := s.Review(context.Background(), r, 7, "openai", Options{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("err = %v", err)
	}
	if g.postCount != 0 {
		t.Fatalf("posted %d reviews", g.postCount)
	}
}

func TestSummarizeLocalDiffElidesOverBudgetContent(t *testing.T) {
	big := strings.Repeat("x", 400)
	diff := "diff --git a/big.txt b/big.txt\n--- a/big.txt\n+++ b/big.txt\n@@ -1 +1 @@\n-old\n+" + big + "\n" +
		"diff --git a/small.txt b/small.txt\n--- a/small.txt\n+++ b/small.txt\n@@ -1 +1 @@\n-old\n+new\n"
	text, summarized := summarizeLocalDiff(diff, 300)
	if strings.Contains(text, big) {
		t.Fatal("oversized block content was kept")
	}
	if !slices.Contains(summarized, "big.txt") || slices.Contains(summarized, "small.txt") {
		t.Fatalf("summarized = %v", summarized)
	}
	for _, want := range []string{"diff --git a/big.txt b/big.txt", "sha256 ", "diff --git a/small.txt b/small.txt", "+new"} {
		if !strings.Contains(text, want) {
			t.Fatalf("local diff missing %q:\n%s", want, text)
		}
	}
}

func TestSummarizeLocalDiffListsPureRenames(t *testing.T) {
	diff := "diff --git a/old b/new\nsimilarity index 100%\nrename from old\nrename to new\n"
	text, summarized := summarizeLocalDiff(diff, 1<<20)
	if text != diff {
		t.Fatalf("a pure rename must keep its block:\n%s", text)
	}
	if !slices.Contains(summarized, "new") {
		t.Fatalf("summarized = %v", summarized)
	}
}

func TestSummarizeLocalDiffElidesBinaryContent(t *testing.T) {
	payload := strings.Repeat("A", 300)
	diff := "diff --git a/blob b/blob\nindex 1..2 100644\nGIT binary patch\nliteral 300\n" + payload + "\n"
	text, summarized := summarizeLocalDiff(diff, 200)
	if strings.Contains(text, payload) {
		t.Fatal("binary payload was kept")
	}
	if !slices.Contains(summarized, "blob") || !strings.Contains(text, "diff --git a/blob b/blob") {
		t.Fatalf("summarized = %v\ntext:\n%s", summarized, text)
	}
}

// fileBlock is one modified file whose body is size bytes of fill.
func fileBlock(path string, size int, fill string) string {
	return "diff --git a/" + path + " b/" + path + "\n--- a/" + path + "\n+++ b/" + path + "\n@@ -1 +1 @@\n-old\n+" +
		strings.Repeat(fill, size) + "\n"
}

// Within the budget the largest blocks are elided first, so a small file that
// sorts after large ones keeps its content, and the output keeps file order.
func TestSummarizeLocalDiffElidesLargestFirst(t *testing.T) {
	diff := fileBlock("a.txt", 300, "a") + fileBlock("b.txt", 900, "b") + fileBlock("c.txt", 100, "c")
	text, summarized := summarizeLocalDiff(diff, 710)
	if !strings.Contains(text, "+"+strings.Repeat("a", 300)) {
		t.Fatalf("a.txt was elided though eliding b.txt alone fits:\n%.200s", text)
	}
	if strings.Contains(text, strings.Repeat("b", 50)) || !strings.Contains(text, "+"+strings.Repeat("c", 100)) {
		t.Fatalf("b.txt kept or c.txt elided:\n%.200s", text)
	}
	if !slices.Equal(summarized, []string{"b.txt"}) {
		t.Fatalf("summarized = %v", summarized)
	}
	if i, j := strings.Index(text, "a/a.txt"), strings.Index(text, "a/c.txt"); i < 0 || j < i {
		t.Fatalf("file order not kept: a=%d c=%d", i, j)
	}
}

// An API patch under the budget is returned byte for byte, with no summarized
// paths, and a pure rename stays as the GitHub patch gave it.
func TestReviewDiffUnderBudgetIsByteIdentical(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	diff := fileBlock("tool", 4, "x") + "diff --git a/old b/new\nsimilarity index 100%\nrename from old\nrename to new\n"
	g.pull.Head = github.Ref{SHA: "under", Ref: "feature"}
	g.diffs = map[string]string{"under": diff}
	src, err := s.reviewDiff(context.Background(), r, g.pull)
	if err != nil {
		t.Fatal(err)
	}
	if src.text != diff || src.summarized != nil || src.source != "" {
		t.Fatalf("under budget changed: source=%q summarized=%v text=%q", src.source, src.summarized, src.text)
	}
}

// An API patch over the budget is summarized, not refused: the small source file
// after three large bundles keeps its content, the bundle that does not fit is
// elided and named, and the review posts a pass that lists it.
func TestReviewSummarizesOversizedAPIDiff(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	size := reviewDiffBudget / 3
	diff := fileBlock("internal/ui/static/console/shared-1.js", size, "a") +
		fileBlock("internal/ui/static/console/shared-2.js", size, "b") +
		fileBlock("internal/ui/static/console/shared-3.js", size, "c") +
		fileBlock("web/console/src/app.ts", 3, "n")
	g.pull.Base = github.Ref{Ref: "main", SHA: "base0"}
	g.pull.Head = github.Ref{SHA: "over", Ref: "feature"}
	g.pull.Head.Repo.FullName = r.Name
	g.diffs = map[string]string{"over": diff}
	src, err := s.reviewDiff(context.Background(), r, g.pull)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(src.summarized, []string{"internal/ui/static/console/shared-3.js"}) {
		t.Fatalf("summarized = %v", src.summarized)
	}
	if strings.Contains(src.text, strings.Repeat("c", 100)) || !strings.Contains(src.text, "+nnn\n") {
		t.Fatal("the elided bundle kept content, or the source file lost its content")
	}
	if len(src.text) > reviewDiffBudget {
		t.Fatalf("summarized text is %d bytes, over the budget", len(src.text))
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatalf("an oversized API diff was refused: %v", e)
	}
	p := g.passes["over"]
	if len(p) != 1 || p[0].DiffSummarized != "internal/ui/static/console/shared-3.js" || p[0].DiffSource != "" {
		t.Fatalf("pass receipt = %+v", p)
	}
}

// Headers alone over the budget cannot be summarized into the budget, so the
// review still refuses the diff, as it did before.
func TestReviewRefusesDiffWhoseHeadersAloneExceedBudget(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	g.diffs = map[string]string{"hdrs": strings.Repeat("diff --git a/x b/x\nindex 1..2 100644\n", reviewDiffBudget/30)}
	g.pull.Head = github.Ref{SHA: "hdrs", Ref: "feature"}
	err := s.Review(context.Background(), r, 7, "openai", Options{})
	if err == nil || !strings.Contains(err.Error(), "manual review required") {
		t.Fatalf("err = %v", err)
	}
}

// elidedPR is an oversized PR: a large file whose hunk is elided under the test
// budget, and a small file that always stays. index, start and ctx vary with
// the base the head was built on; added is the file's one changed line.
func elidedPR(index string, start int, ctx, added string) string {
	big := "diff --git a/big.txt b/big.txt\nindex " + index + " 100644\n--- a/big.txt\n+++ b/big.txt\n" +
		"@@ -" + itoa(start) + ",51 +" + itoa(start) + ",51 @@\n" +
		strings.Repeat(" "+ctx+" "+strings.Repeat("c", 20)+"\n", 50) + "-old\n+" + added + "\n"
	small := "diff --git a/small.txt b/small.txt\nindex 3333333..4444444 100644\n--- a/small.txt\n+++ b/small.txt\n" +
		"@@ -1,3 +1,3 @@\n keep\n-old\n+new\n end\n"
	return big + small
}

// A base merge that only shifts an elided file's context and line numbers must
// keep the review's patch id: the elided digest covers the changed lines only.
func TestElidedPatchIDIgnoresBaseMergeShift(t *testing.T) {
	const budget = 400
	before := elidedPR("1111111..2222222", 1, "keep", "new")
	after := elidedPR("aaaaaaa..bbbbbbb", 41, "kept", "new")
	a, sa := summarizeLocalDiff(before, budget)
	b, sb := summarizeLocalDiff(after, budget)
	if !slices.Contains(sa, "big.txt") || !slices.Contains(sb, "big.txt") {
		t.Fatalf("big.txt was not elided: %v %v", sa, sb)
	}
	ida, idb := reviewPatchID(a), reviewPatchID(b)
	if ida == "" || ida != idb {
		t.Fatalf("a base merge that only shifted an elided file changed the patch id: %q != %q", ida, idb)
	}
}

// One changed line in an elided file is a different patch, so the digest must
// still move with the + and - lines.
func TestElidedPatchIDTracksChangedLine(t *testing.T) {
	const budget = 400
	a, _ := summarizeLocalDiff(elidedPR("1111111..2222222", 1, "keep", "new"), budget)
	b, _ := summarizeLocalDiff(elidedPR("1111111..2222222", 1, "keep", "newer"), budget)
	if reviewPatchID(a) == reviewPatchID(b) {
		t.Fatal("a changed + line in an elided file kept the patch id")
	}
}
