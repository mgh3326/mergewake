package pipeline

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// prOrigin is a bare "origin" with main at base and feat/thing at head, and a
// clone of only main: the PR head reaches the clone through a fetch alone.
type prOrigin struct {
	work, origin, root string
	base, head         string
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
	o.base = o.commit(t, "base")
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
}

// assertNoFallback: an ineligible PR never reaches a branch fetch and leaves
// no pr-head-fallback row.
func (p fallbackPass) assertNoFallback(t *testing.T) {
	t.Helper()
	if f := branchFetches(p.runner.git); len(f) != 0 {
		t.Fatalf("branch fetched: %q", f)
	}
	if rows := auditRows(t, p.s, p.r, "pr-head-fallback"); len(rows) != 0 {
		t.Fatalf("pr-head-fallback rows=%v", rows)
	}
}

// fallbackRow is the one pr-head-fallback row an attempted fallback writes,
// whatever its outcome.
func fallbackRow(t *testing.T, s *Service, r config.Repo, result string) map[string]any {
	t.Helper()
	rows := auditRows(t, s, r, "pr-head-fallback")
	if len(rows) != 1 {
		t.Fatalf("pr-head-fallback rows=%v, want exactly one", rows)
	}
	if rows[0]["result"] != result {
		t.Fatalf("row=%v, want result %q", rows[0], result)
	}
	return rows[0]
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
	row := fallbackRow(t, p.s, p.r, "ok")
	if row["repo"] != p.r.Name || row["pr"] != float64(7) || row["head"] != o.head || row["ref"] != "feat/thing" {
		t.Fatalf("row=%v", row)
	}
	if msg, _ := row["error"].(string); msg == "" || len(msg) > 512 {
		t.Fatalf("first error %q: want the pull/<n>/head failure, at most 512 bytes", msg)
	}
}

// The author pushed again after the reviewed head. panewire would build the
// remote spec {sha: head, fetch: refs/heads/<branch>} and the remote node
// requires FETCH_HEAD to equal that sha, so a branch past the head cannot
// carry the remote reviewer: the pass skips and the next pass reviews the new
// head.
func TestPRHeadFallbackBranchAdvancedPastHead(t *testing.T) {
	o := seedPROrigin(t, false)
	tip := o.commit(t, "later")
	gitAt(t, o.work, "push", o.origin, "feat/thing")
	p := runFallbackPass(t, o, nil)
	p.assertSkipped(t, "fetch PR head", "branch feat/thing is at "+short7(tip)+", not head "+short7(o.head))
	if got := gitAt(t, o.root, "rev-parse", "refs/remotes/origin/feat/thing"); got != tip {
		t.Fatalf("refs/remotes/origin/feat/thing=%s want the branch tip %s", got, tip)
	}
	fallbackRow(t, p.s, p.r, "head-moved")
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
	p.assertSkipped(t, "fetch PR head", "branch feat/thing is at ", "not head "+short7(o.head))
	fallbackRow(t, p.s, p.r, "head-moved")
}

// A PR whose branch is gone as well: one fallback row records the attempt and
// the pass skips naming both fetch failures.
func TestPRHeadFallbackBranchFetchFailsSkips(t *testing.T) {
	o := seedPROrigin(t, false)
	p := runFallbackPass(t, o, func(g *fakeGH) { g.pull.Head.Ref = "feat/gone" })
	p.assertSkipped(t, "fetch PR head", "fetch branch feat/gone")
	if n := len(branchFetches(p.runner.git)); n != 1 {
		t.Fatalf("branch fetches=%d git=%q", n, p.runner.git)
	}
	fallbackRow(t, p.s, p.r, "fetch-failed")
}

// A local refs/remotes/origin/pull/<n>/head fetched before GitHub lost the
// ref still points at the head, and panewire prefers it over the branch: the
// remote reviewer would then fetch the missing pull/<n>/head. A taken
// fallback deletes it, so the remote spec names the branch.
func TestPRHeadFallbackDeletesStalePullRef(t *testing.T) {
	o := seedPROrigin(t, true)
	gitAt(t, o.root, "fetch", "origin", "+refs/pull/7/head:refs/remotes/origin/pull/7/head")
	gitAt(t, o.origin, "update-ref", "-d", "refs/pull/7/head")
	p := runFallbackPass(t, o, nil)
	p.assertReviewed(t, o.head)
	if b, e := exec.Command("git", "-C", o.root, "rev-parse", "--verify", "--quiet", "refs/remotes/origin/pull/7/head").CombinedOutput(); e == nil {
		t.Fatalf("refs/remotes/origin/pull/7/head still at %s after the fallback", strings.TrimSpace(string(b)))
	}
	if got := gitAt(t, o.root, "rev-parse", "refs/remotes/origin/feat/thing"); got != o.head {
		t.Fatalf("refs/remotes/origin/feat/thing=%s want %s", got, o.head)
	}
	fallbackRow(t, p.s, p.r, "ok")
}

// A fork's branch is not origin's: the pass skips before any fetch, as it
// always has.
func TestPRHeadFallbackForkPRSkips(t *testing.T) {
	o := seedPROrigin(t, false)
	p := runFallbackPass(t, o, func(g *fakeGH) { g.pull.Head.Repo.FullName = "fork/project" })
	p.assertSkipped(t, "fork PR")
	p.assertNoFallback(t)
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

// A head ref that is not a plain branch name never reaches a refspec, and
// neither does one panewire's transport would not carry: a refs/remotes/
// origin/pull/* ref is skipped in its branch discovery, and the remote node
// refuses a branch name over 200 bytes.
func TestPRHeadFallbackRejectsInvalidRefName(t *testing.T) {
	for _, ref := range []string{"-x", "a..b", "x:y", "a b", "a*", "a^", "a~1", "a?", "a[b", `a\b`, "pull/7/head", "pull/x", strings.Repeat("a", 201)} {
		t.Run(clip(ref, 16), func(t *testing.T) {
			o := seedPROrigin(t, false)
			p := runFallbackPass(t, o, func(g *fakeGH) { g.pull.Head.Ref = ref })
			p.assertSkipped(t, "fetch PR head")
			p.assertNoFallback(t)
		})
	}
}

// The fallback itself refuses a fork's branch, whatever its caller checked
// first: origin's branch of that name is not the PR's.
func TestFetchPRHeadNoFallbackForForkPR(t *testing.T) {
	o := seedPROrigin(t, false)
	runner := &panewireRunner{realGit: true}
	s, g, r := panewireSetup(t, runner)
	pr := g.pull
	pr.Head.SHA = o.head
	pr.Head.Repo.FullName = "fork/project"
	e := s.fetchPRHead(context.Background(), o.root, r.Name, pr)
	if e == nil || !strings.Contains(e.Error(), "fetch PR head") {
		t.Fatalf("err=%v, want the pull/<n>/head failure", e)
	}
	if f := branchFetches(runner.git); len(f) != 0 {
		t.Fatalf("branch fetched for a fork PR: %q", f)
	}
	if rows := auditRows(t, s, r, "pr-head-fallback"); len(rows) != 0 {
		t.Fatalf("pr-head-fallback rows=%v", rows)
	}
}

// Real branch names stay eligible for the fallback.
func TestFallbackBranchAcceptsBranchNames(t *testing.T) {
	for _, ref := range []string{"feat/thing", "wk/hk3697-yt-oplog", "lead/pr-head-fallback", "v1.2_fix", "mergewake/recovery", "fix/pull/7", "pulls", strings.Repeat("a", 200)} {
		if !fallbackBranch(ref) {
			t.Errorf("fallbackBranch(%q) = false", ref)
		}
	}
}

// pullFetchErrRunner fails the pull/<n>/head fetch with err and runs every
// other command through the wrapped runner.
type pullFetchErrRunner struct {
	integrations.Runner
	err error
}

func (r pullFetchErrRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" && slices.Contains(args, "fetch") && slices.ContainsFunc(args, func(a string) bool { return strings.HasPrefix(a, "+pull/") }) {
		return nil, r.err
	}
	return r.Runner.Run(ctx, command, args, input, dir)
}

// The first fetch error enters the audit row redacted and cut to 512 bytes:
// git's stderr can carry a credentialed remote URL and run long.
func TestPRHeadFallbackAuditErrorIsBoundedAndRedacted(t *testing.T) {
	o := seedPROrigin(t, false)
	runner := &panewireRunner{realGit: true}
	s, g, r := panewireSetup(t, runner)
	long := "fatal: couldn't find remote ref refs/pull/7/head " + fakeSecretText + "\n" + strings.Repeat("hint: the remote ref is gone; retry later\n", 40)
	s.Runner = pullFetchErrRunner{Runner: runner, err: errors.New(long)}
	pr := g.pull
	pr.Head.SHA = o.head
	if e := s.fetchPRHead(context.Background(), o.root, r.Name, pr); e != nil {
		t.Fatalf("fetchPRHead: %v", e)
	}
	msg, _ := fallbackRow(t, s, r, "ok")["error"].(string)
	if len(msg) == 0 || len(msg) > 512 {
		t.Fatalf("error is %d bytes, want 1..512: %q", len(msg), msg)
	}
	if !strings.Contains(msg, "[redacted]") {
		t.Fatalf("error %q: want the credential redacted", msg)
	}
	for _, secret := range fakeSecrets {
		if strings.Contains(msg, secret) {
			t.Fatalf("error leaks %q: %q", secret, msg)
		}
	}
}

// prHeadWithFirstFetchErr sets up a fetchPRHead call whose pull/<n>/head
// fetch fails with firstErr while every other git command runs for real.
func prHeadWithFirstFetchErr(t *testing.T, firstErr error, edit func(*github.Pull)) (*Service, config.Repo, prOrigin, github.Pull) {
	t.Helper()
	o := seedPROrigin(t, false)
	runner := &panewireRunner{realGit: true}
	s, g, r := panewireSetup(t, runner)
	s.Runner = pullFetchErrRunner{Runner: runner, err: firstErr}
	pr := g.pull
	pr.Head.SHA = o.head
	if edit != nil {
		edit(&pr)
	}
	return s, r, o, pr
}

// A fork PR or a head ref that cannot go in a refspec gets no branch
// fallback: the returned error still carries the first fetch's stderr as its
// operator detail, while its text is the one it always was.
func TestFetchPRHeadNoFallbackErrorDetail(t *testing.T) {
	ce := &integrations.CommandError{Command: "git", Err: errors.New("exit status 128"), Stderr: "fatal: couldn't find remote ref pull/7/head\n"}
	for _, tc := range []struct {
		name string
		edit func(*github.Pull)
	}{
		{"fork", func(pr *github.Pull) { pr.Head.Repo.FullName = "fork/project" }},
		{"unsafe ref", func(pr *github.Pull) { pr.Head.Ref = "a..b" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, o, pr := prHeadWithFirstFetchErr(t, ce, tc.edit)
			e := s.fetchPRHead(context.Background(), o.root, r.Name, pr)
			if e == nil {
				t.Fatal("fetchPRHead: want the pull/<n>/head failure")
			}
			if want := "fetch PR head: " + ce.Error(); e.Error() != want {
				t.Fatalf("err=%q, want %q", e.Error(), want)
			}
			if got := errorDetailOf(e); !strings.Contains(got, "fatal: couldn't find remote ref pull/7/head") {
				t.Fatalf("errorDetailOf=%q, want the first fetch's stderr", got)
			}
			if rows := auditRows(t, s, r, "pr-head-fallback"); len(rows) != 0 {
				t.Fatalf("pr-head-fallback rows=%v", rows)
			}
		})
	}
}

// The branch fallback's audit row keeps the first fetch's stderr beside the
// bounded error text.
func TestPRHeadFallbackRowDetail(t *testing.T) {
	ce := &integrations.CommandError{Command: "git", Err: errors.New("exit status 128"), Stderr: "fatal: couldn't find remote ref pull/7/head\n"}
	s, r, o, pr := prHeadWithFirstFetchErr(t, ce, nil)
	if e := s.fetchPRHead(context.Background(), o.root, r.Name, pr); e != nil {
		t.Fatalf("fetchPRHead: %v", e)
	}
	row := fallbackRow(t, s, r, "ok")
	if got, _ := row["detail"].(string); !strings.Contains(got, "fatal: couldn't find remote ref pull/7/head") {
		t.Fatalf("row detail=%q, want the first fetch's stderr", got)
	}
	if msg, _ := row["error"].(string); msg == "" || !strings.Contains(msg, "exit status 128") {
		t.Fatalf("row error=%q, want the bounded CommandError text", msg)
	}
}

// The row's detail is redacted, then bounded to 512 bytes: git's stderr can
// carry a credentialed remote URL and run long. The token straddles the cut
// so bounding before redacting would keep a fragment of it.
func TestPRHeadFallbackRowDetailBoundedAndRedacted(t *testing.T) {
	token := "ghp_FAKEfakeFAKEfakeFAKEfake0123456789"
	stderr := strings.Repeat("fatal: first fetch failed\n", 20) + token + strings.Repeat("fatal: retry later\n", 25) + "end"
	ce := &integrations.CommandError{Command: "git", Err: errors.New("exit status 128"), Stderr: stderr}
	s, r, o, pr := prHeadWithFirstFetchErr(t, ce, nil)
	if e := s.fetchPRHead(context.Background(), o.root, r.Name, pr); e != nil {
		t.Fatalf("fetchPRHead: %v", e)
	}
	detail, _ := fallbackRow(t, s, r, "ok")["detail"].(string)
	if len(detail) == 0 || len(detail) > 512 {
		t.Fatalf("detail is %d bytes, want 1..512: %q", len(detail), detail)
	}
	if !strings.Contains(detail, "[redacted]") {
		t.Fatalf("detail %q: want the credential redacted", detail)
	}
	for _, secret := range fakeSecrets {
		if strings.Contains(detail, secret) {
			t.Fatalf("detail leaks %q: %q", secret, detail)
		}
	}
	if strings.Contains(detail, "0123456789") {
		t.Fatalf("detail keeps a fragment of the token: %q", detail)
	}
}

// A first failure that is not a git CommandError carries no stderr: the
// returned error has no detail and the fallback row has no detail field.
func TestFetchPRHeadNoDetailWithoutCommandError(t *testing.T) {
	plain := errors.New("fatal: plain failure")
	t.Run("fork", func(t *testing.T) {
		s, r, o, pr := prHeadWithFirstFetchErr(t, plain, func(pr *github.Pull) { pr.Head.Repo.FullName = "fork/project" })
		e := s.fetchPRHead(context.Background(), o.root, r.Name, pr)
		if e == nil {
			t.Fatal("fetchPRHead: want the pull/<n>/head failure")
		}
		if got := errorDetailOf(e); got != "" {
			t.Fatalf("errorDetailOf=%q, want none", got)
		}
	})
	t.Run("fallback row", func(t *testing.T) {
		s, r, o, pr := prHeadWithFirstFetchErr(t, plain, nil)
		if e := s.fetchPRHead(context.Background(), o.root, r.Name, pr); e != nil {
			t.Fatalf("fetchPRHead: %v", e)
		}
		row := fallbackRow(t, s, r, "ok")
		if _, ok := row["detail"]; ok {
			t.Fatalf("row has a detail field: %v", row["detail"])
		}
	})
}

// GitHub's 406 sends the diff to the local checkout, which fetches the same
// PR head: a lost pull/<n>/head falls back to the same-repo branch there too.
func TestLocalDiffFallsBackToBranchWithoutPullRef(t *testing.T) {
	o := seedPROrigin(t, false)
	s, g, _, _, r := reviewSetup(t)
	s.Runner = &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}
	r.Root = o.root
	g.diffErr = &github.APIError{Code: http.StatusNotAcceptable, Method: "GET", Path: "/repos/example/project/pulls/7"}
	g.pull.Base = github.Ref{Ref: "main", SHA: o.base}
	g.pull.Head = github.Ref{SHA: o.head, Ref: "feat/thing"}
	g.pull.Head.Repo.FullName = r.Name
	d, err := s.reviewDiff(context.Background(), r, g.pull)
	if err != nil {
		t.Fatal(err)
	}
	if d.source != "local" || !strings.Contains(d.text, "+head") {
		t.Fatalf("diff source=%q text=%q", d.source, d.text)
	}
	fallbackRow(t, s, r, "ok")
}

// A failed fallback's operator detail starts with the first fetch's stderr and
// then carries the fallback's own git stderr after "; ": neither is lost. The
// returned text is the one the fallback always produced.
func TestFetchPRHeadFallbackFailureDetailKeepsFirstStderr(t *testing.T) {
	ce := &integrations.CommandError{Command: "git", Err: errors.New("exit status 128"), Stderr: "fatal: couldn't find remote ref pull/7/head\n"}
	s, r, o, pr := prHeadWithFirstFetchErr(t, ce, func(pr *github.Pull) { pr.Head.Ref = "feat/gone" })
	// panewireRunner's git failures carry no stderr; the branch fetch needs the
	// real runner's CommandError.
	s.Runner = pullFetchErrRunner{Runner: integrations.Exec{}, err: ce}
	e := s.fetchPRHead(context.Background(), o.root, r.Name, pr)
	if e == nil {
		t.Fatal("fetchPRHead: want the branch fallback failure")
	}
	if want := "fetch PR head: " + ce.Error() + "; fetch branch feat/gone: "; !strings.HasPrefix(e.Error(), want) {
		t.Fatalf("err=%q, want the prefix %q", e.Error(), want)
	}
	const first = "fatal: couldn't find remote ref pull/7/head"
	got := errorDetailOf(e)
	if !strings.HasPrefix(got, first+"; ") || !strings.Contains(got[len(first)+2:], "feat/gone") {
		t.Fatalf("errorDetailOf=%q, want the first fetch's stderr then the branch fetch's, joined by \"; \"", got)
	}
	fallbackRow(t, s, r, "fetch-failed")
}

// A branch that moved past the reviewed head: the detail is the first fetch's
// stderr alone, since the head-moved failure ran no git command that failed.
func TestFetchPRHeadHeadMovedDetailIsFirstStderr(t *testing.T) {
	ce := &integrations.CommandError{Command: "git", Err: errors.New("exit status 128"), Stderr: "fatal: couldn't find remote ref pull/7/head\n"}
	s, r, o, pr := prHeadWithFirstFetchErr(t, ce, nil)
	tip := o.commit(t, "later")
	gitAt(t, o.work, "push", o.origin, "feat/thing")
	e := s.fetchPRHead(context.Background(), o.root, r.Name, pr)
	if e == nil {
		t.Fatal("fetchPRHead: want the head-moved failure")
	}
	if want := "fetch PR head: " + ce.Error() + "; branch feat/thing is at " + short7(tip) + ", not head " + short7(o.head); e.Error() != want {
		t.Fatalf("err=%q, want %q", e.Error(), want)
	}
	if got, want := errorDetailOf(e), "fatal: couldn't find remote ref pull/7/head"; got != want {
		t.Fatalf("errorDetailOf=%q, want %q", got, want)
	}
	fallbackRow(t, s, r, "head-moved")
}
