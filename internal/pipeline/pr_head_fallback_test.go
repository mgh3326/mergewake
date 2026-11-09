package pipeline

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// prOrigin is a bare "origin" with main at base and feat/thing at head, and a
// clone of only main: the PR head reaches the clone through a fetch alone.
type prOrigin struct {
	work, origin, root string
	head               string
}

func seedPROrigin(t *testing.T, pullRef bool) prOrigin {
	t.Helper()
	if _, e := exec.LookPath("git"); e != nil {
		t.Skipf("git is not installed: %v", e)
	}
	dir := t.TempDir()
	o := prOrigin{work: filepath.Join(dir, "work"), origin: filepath.Join(dir, "origin.git"), root: filepath.Join(dir, "repo")}
	if e := os.MkdirAll(o.work, 0700); e != nil {
		t.Fatal(e)
	}
	gitAt(t, o.work, "init", "-b", "main")
	gitAt(t, o.work, "config", "user.email", "t@example.com")
	gitAt(t, o.work, "config", "user.name", "t")
	o.commit(t, "base")
	gitAt(t, o.work, "checkout", "-b", "feat/thing")
	o.commit(t, "head")
	o.head = gitAt(t, o.work, "rev-parse", "HEAD")
	if b, e := exec.Command("git", "init", "--bare", o.origin).CombinedOutput(); e != nil {
		t.Fatalf("init origin: %v\n%s", e, b)
	}
	gitAt(t, o.work, "push", o.origin, "main", "feat/thing")
	if pullRef {
		gitAt(t, o.work, "push", o.origin, o.head+":refs/pull/7/head")
	}
	if b, e := exec.Command("git", "clone", "--single-branch", "--branch", "main", o.origin, o.root).CombinedOutput(); e != nil {
		t.Fatalf("clone: %v\n%s", e, b)
	}
	return o
}

func (o prOrigin) commit(t *testing.T, msg string) string {
	t.Helper()
	if e := os.WriteFile(filepath.Join(o.work, "f"), []byte(msg+"\n"), 0600); e != nil {
		t.Fatal(e)
	}
	gitAt(t, o.work, "add", "f")
	gitAt(t, o.work, "commit", "-m", msg)
	return gitAt(t, o.work, "rev-parse", "HEAD")
}

// fallbackPass is one panewire pass against a real checkout.
type fallbackPass struct {
	s      *Service
	g      *fakeGH
	r      config.Repo
	runner *panewireRunner
	err    error
	// checkoutHead is the reviewer checkout's HEAD while the pane ran.
	checkoutHead string
}

func runFallbackPass(t *testing.T, o prOrigin, edit func(*fakeGH)) fallbackPass {
	t.Helper()
	p := fallbackPass{runner: &panewireRunner{findings: `{"findings":[]}`, family: "openai", realGit: true}}
	p.runner.afterRun = func() {
		p.checkoutHead = gitAt(t, arg(p.runner.spawns[0], "-c"), "rev-parse", "HEAD")
	}
	p.s, p.g, p.r = panewireSetup(t, p.runner)
	p.r.Root = o.root
	p.g.pull.Head.SHA = o.head
	if edit != nil {
		edit(p.g)
	}
	p.err = p.s.Review(context.Background(), p.r, 7, "openai", Options{})
	return p
}

// branchFetches are the fetches that name a refs/heads/ source.
func branchFetches(git [][]string) [][]string {
	var out [][]string
	for _, a := range git {
		if slices.Contains(a, "fetch") && slices.ContainsFunc(a, func(s string) bool { return strings.Contains(s, "refs/heads/") }) {
			out = append(out, a)
		}
	}
	return out
}

func worktreeAdds(git [][]string) int {
	n := 0
	for _, a := range git {
		if slices.Contains(a, "worktree") && slices.Contains(a, "add") {
			n++
		}
	}
	return n
}

func (p fallbackPass) assertReviewed(t *testing.T, head string) {
	t.Helper()
	if p.err != nil {
		t.Fatalf("review: %v (git calls %q)", p.err, p.runner.git)
	}
	if got := p.g.passes[head]; len(got) != 1 || got[0].Executor != "panewire" {
		t.Fatalf("passes=%+v", got)
	}
	if p.checkoutHead != head {
		t.Fatalf("reviewer checkout HEAD=%q, want %s", p.checkoutHead, head)
	}
}

func (p fallbackPass) assertSkipped(t *testing.T, reasons ...string) {
	t.Helper()
	if !errors.Is(p.err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want ErrReviewUnavailable", p.err)
	}
	for _, want := range reasons {
		if !strings.Contains(p.err.Error(), want) {
			t.Fatalf("err=%v, want it to name %q", p.err, want)
		}
	}
	if len(p.runner.spawns) != 0 || worktreeAdds(p.runner.git) != 0 {
		t.Fatalf("spawns=%v git=%q", p.runner.spawns, p.runner.git)
	}
	if rows := auditRows(t, p.s, p.r, "pr-head-fallback"); len(rows) != 0 {
		t.Fatalf("pr-head-fallback audit on a skip: %v", rows)
	}
}

// GitHub can lose refs/pull/<n>/head for a same-repo PR while its branch is
// intact. The pass fetches the branch into refs/remotes/origin/<branch>, the
// ref panewire builds a remote reviewer's checkout from, stands at the exact
// head, and records the fallback.
func TestPRHeadFallbackBranchAtHead(t *testing.T) {
	o := seedPROrigin(t, false)
	p := runFallbackPass(t, o, nil)
	p.assertReviewed(t, o.head)
	if got := gitAt(t, o.root, "rev-parse", "refs/remotes/origin/feat/thing"); got != o.head {
		t.Fatalf("refs/remotes/origin/feat/thing=%s want %s", got, o.head)
	}
	if n := len(branchFetches(p.runner.git)); n != 1 {
		t.Fatalf("branch fetches=%d git=%q", n, p.runner.git)
	}
	rows := auditRows(t, p.s, p.r, "pr-head-fallback")
	if len(rows) != 1 {
		t.Fatalf("pr-head-fallback rows=%v", rows)
	}
	row := rows[0]
	if row["repo"] != p.r.Name || row["pr"] != float64(7) || row["head"] != o.head || row["ref"] != "feat/thing" {
		t.Fatalf("row=%v", row)
	}
	if msg, _ := row["error"].(string); msg == "" || len(msg) > 512 {
		t.Fatalf("first error %q: want the pull/<n>/head failure, at most 512 bytes", msg)
	}
}

// The author pushed again after the reviewed head: the branch contains it,
// and the pass still reviews the exact head, not the branch tip.
func TestPRHeadFallbackBranchAdvancedPastHead(t *testing.T) {
	o := seedPROrigin(t, false)
	tip := o.commit(t, "later")
	gitAt(t, o.work, "push", o.origin, "feat/thing")
	p := runFallbackPass(t, o, nil)
	p.assertReviewed(t, o.head)
	if got := gitAt(t, o.root, "rev-parse", "refs/remotes/origin/feat/thing"); got != tip {
		t.Fatalf("refs/remotes/origin/feat/thing=%s want the branch tip %s", got, tip)
	}
	if rows := auditRows(t, p.s, p.r, "pr-head-fallback"); len(rows) != 1 {
		t.Fatalf("pr-head-fallback rows=%v", rows)
	}
}

// A branch force-pushed since the reviewed head no longer stands for it, even
// when the head commit is still in the checkout: the pass skips and names both
// failures.
func TestPRHeadFallbackForcePushedBranchSkips(t *testing.T) {
	o := seedPROrigin(t, false)
	gitAt(t, o.work, "push", o.origin, o.head+":refs/keep/head")
	gitAt(t, o.root, "fetch", "origin", "refs/keep/head:refs/keep/head")
	gitAt(t, o.work, "checkout", "-B", "feat/thing", "main")
	o.commit(t, "rewritten")
	gitAt(t, o.work, "push", "--force", o.origin, "feat/thing")
	p := runFallbackPass(t, o, nil)
	p.assertSkipped(t, "fetch PR head", "feat/thing does not contain head "+short7(o.head))
}

// A fork's branch is not origin's: the pass skips before any fetch, as it
// always has.
func TestPRHeadFallbackForkPRSkips(t *testing.T) {
	o := seedPROrigin(t, false)
	p := runFallbackPass(t, o, func(g *fakeGH) { g.pull.Head.Repo.FullName = "fork/project" })
	p.assertSkipped(t, "fork PR")
	if f := branchFetches(p.runner.git); len(f) != 0 {
		t.Fatalf("branch fetched for a fork PR: %q", f)
	}
}

// With refs/pull/<n>/head present nothing changes: one fetch, no branch
// fetch, no fallback row.
func TestPRHeadFallbackNotTakenWhenPullRefExists(t *testing.T) {
	o := seedPROrigin(t, true)
	p := runFallbackPass(t, o, nil)
	p.assertReviewed(t, o.head)
	if f := branchFetches(p.runner.git); len(f) != 0 {
		t.Fatalf("branch fetched with pull/7/head present: %q", f)
	}
	if got := gitAt(t, o.root, "rev-parse", "refs/remotes/origin/pull/7/head"); got != o.head {
		t.Fatalf("refs/remotes/origin/pull/7/head=%s want %s", got, o.head)
	}
	if rows := auditRows(t, p.s, p.r, "pr-head-fallback"); len(rows) != 0 {
		t.Fatalf("pr-head-fallback rows=%v", rows)
	}
}

// A head ref that is not a plain branch name never reaches a refspec.
func TestPRHeadFallbackRejectsInvalidRefName(t *testing.T) {
	for _, ref := range []string{"-x", "a..b", "x:y", "a b", "a*", "a^", "a~1", "a?", "a[b", `a\b`} {
		t.Run(ref, func(t *testing.T) {
			o := seedPROrigin(t, false)
			p := runFallbackPass(t, o, func(g *fakeGH) { g.pull.Head.Ref = ref })
			p.assertSkipped(t, "fetch PR head")
			if f := branchFetches(p.runner.git); len(f) != 0 {
				t.Fatalf("branch fetched for head ref %q: %q", ref, f)
			}
		})
	}
}
