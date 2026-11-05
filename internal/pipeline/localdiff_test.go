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
