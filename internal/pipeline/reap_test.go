package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

type scriptRunner struct {
	mu    sync.Mutex
	outs  map[string]string
	queue map[string][]string // consumed in order before outs, for re-reads
	errs  map[string]error
	fn    func(string) (string, bool) // dynamic answers, e.g. epoch-arg stat calls
	calls []string
}

func (s *scriptRunner) Run(_ context.Context, name string, args []string, _ string, _ string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := name + "\x00" + strings.Join(args, "\x00")
	s.calls = append(s.calls, k)
	if q := s.queue[k]; len(q) > 0 {
		s.queue[k] = q[1:]
		return []byte(q[0]), nil
	}
	if e, ok := s.errs[k]; ok {
		return nil, e
	}
	if o, ok := s.outs[k]; ok {
		return []byte(o), nil
	}
	if s.fn != nil {
		if o, ok := s.fn(k); ok {
			return []byte(o), nil
		}
	}
	// An unscripted worktree probe is a path git refuses (not a repository, or
	// a missing directory): exit 128, as git itself reports it. Only git's own
	// refusal proves a path is not a worktree.
	if name == "git" && strings.Contains(k, "\x00rev-parse\x00--absolute-git-dir\x00--git-common-dir\x00--show-toplevel") {
		return nil, gitRefusal{}
	}
	return nil, fmt.Errorf("unexpected command: %s", strings.ReplaceAll(k, "\x00", " "))
}

// gitRefusal is the error for a command git exited 128 on, as the runner
// returns it.
type gitRefusal struct{}

func (gitRefusal) Error() string { return "exit status 128" }
func (gitRefusal) ExitCode() int { return 128 }

func (s *scriptRunner) called(sub string) bool {
	for _, c := range s.calls {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

func key(name string, args ...string) string { return name + "\x00" + strings.Join(args, "\x00") }

func rfc(t time.Time) *string {
	s := t.UTC().Format(time.RFC3339)
	return &s
}

var testRepo = config.Repo{Name: "example/project", Branch: "main"}

const headSHA = "aaaa1111"

// mergedPull is the merged PR every reap test shares.
var mergedPull = github.Pull{
	Number: 12, State: "closed", Merged: true,
	Head: github.Ref{Ref: "feat/x", SHA: headSHA}, Base: github.Ref{Ref: "main"},
	MergedAt: rfc(time.Now().Add(-8 * 24 * time.Hour)), MergeCommitSHA: "abc123",
}

type reapGH struct{ fakeGH }

func (reapGH) MergedPulls(_ context.Context, repo, branch string, _ time.Time) ([]github.Pull, error) {
	if repo != "example/project" || branch != "main" {
		return nil, fmt.Errorf("bad args %s %s", repo, branch)
	}
	return []github.Pull{mergedPull}, nil
}

func (reapGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	return nil, nil
}

type openRecoveryGH struct{ reapGH }

func (openRecoveryGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	return &github.Pull{Number: 99, State: "open"}, nil
}

type revertedGH struct{ reapGH }

func (revertedGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	return &github.Pull{Number: 99, State: "closed", MergedAt: rfc(time.Now().Add(-8*24*time.Hour + time.Hour))}, nil
}

func repoConfig(dir string) config.Config {
	return config.Config{
		StateDir: dir,
		Daemon:   config.Daemon{Act: true},
		Hosts:    []reap.Host{{Name: "local", Jobs: "/jobs"}},
		Reap:     config.Reap{Enabled: true, Act: true, WorktreeAfter: "0", ScanAfter: "720h", Regenerable: config.DefaultReapRegenerable, Disposable: config.DefaultReapDisposable},
		Repos:    []config.Repo{{Name: "example/project", Branch: "main"}},
	}
}

// statAnswer answers report-mtime probes whose epoch argument varies per run.
func statAnswer(reply string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		if strings.Contains(k, "stat -c %Y") && strings.Contains(k, "report.md") {
			return reply, true
		}
		return "", false
	}
}

func svc(t *testing.T, gh GitHub, run *scriptRunner) *Service {
	t.Helper()
	if run.fn == nil {
		run.fn = statAnswer("yes\n")
	}
	s := &Service{Config: repoConfig(t.TempDir()), GH: gh, Runner: run, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: io.Discard}
	// A watch verdict on main covers the merge: the merged-PR worktree path
	// waits for one, so every test that expects a removal needs it.
	greenHead(t, s, testRepo, mergedPull.MergeCommitSHA)
	return s
}

// greenHead records a healthy watch verdict on main at head for repo.
func greenHead(t *testing.T, s *Service, r config.Repo, head string) {
	t.Helper()
	dir := s.state(r.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "last-green"), []byte(head+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// emptyComposer and draftComposer are claude screens whose input line, between
// the last two dividers, is empty or holds an unsent draft.
const (
	emptyComposer = "⏺ Done.\n\n────────\n❯ \n────────\n  ? for shortcuts\n"
	draftComposer = "⏺ Done.\n\n────────\n❯ also fix the docs\n────────\n  ? for shortcuts\n"
)

func paneJSON(cwd2 string) (agents, panes string) {
	agents = `{"result":{"agents":[` +
		`{"pane_id":"w1:p1","name":"wk-7","agent_status":"idle","agent":"claude","cwd":"/repo.wt"},` +
		`{"pane_id":"w1:p2","agent_status":"working","cwd":"` + cwd2 + `"}]}}`
	panes = `{"result":{"panes":[` +
		`{"pane_id":"w1:p1","cwd":"/repo.wt"},` +
		`{"pane_id":"w1:p2","cwd":"` + cwd2 + `"}]}}`
	return
}

func worktreeOuts(cwd2 string) map[string]string {
	agents, panes := paneJSON(cwd2)
	return map[string]string{
		key("herdr", "agent", "list"):                                agents,
		key("herdr", "pane", "list"):                                 panes,
		key("herdr", "pane", "close", "w1:p1"):                       "",
		key("herdr", "pane", "read", "w1:p1", "--source", "visible"): emptyComposer,
		key("git", "-C", "/repo.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.wt\n/repo/.git\n/repo.wt\n",
		key("git", "-C", "/repo.wt", "branch", "--show-current"):                                               "feat/x\n",
		key("git", "-C", "/repo.wt", "rev-parse", "HEAD"):                                                      headSHA + "\n",
		key("git", "-C", "/repo.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"):    "/repo/.git\n/repo/.git\n/repo\n",
		key("git", "-C", "/repo", "branch", "--show-current"):                                                  "main\n",
		key("git", "-C", "/repo", "remote", "get-url", "origin"):                                               "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                           "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.wt\nHEAD aaaa\nbranch refs/heads/feat/x\n",
		key("git", "-C", "/repo.wt", "status", "--porcelain"):                                                  "",
		key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored", "-z"):                               "",
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "feat/x", "origin/main"):                      "",
		key("git", "-C", "/repo", "worktree", "remove", "/repo.wt"):                                            "",
		key("git", "-C", "/repo", "branch", "-d", "feat/x"):                                                    "",
		key("sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", "/jobs/wk-7"):                             "yes\n",
		key("sh", "-c", `test -f "$1" && echo yes || echo no`, "sh", "/jobs/wk-7/report.md"):                   "yes\n",
	}
}

// panegoneWorktreeOuts is worktreeOuts with an empty full pane list:
// panewire's node already closed the pane, so worktree removal can proceed.
func panegoneWorktreeOuts(cwd2 string) map[string]string {
	outs := worktreeOuts(cwd2)
	outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	return outs
}

func reapLogRows(t *testing.T, s *Service, repo string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(s.reapLog(repo))
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func seedWorktreeReceipt(t *testing.T, s *Service, repo, worktree string) {
	t.Helper()
	b, _ := json.Marshal(reapReceipt{Action: "close-pane", Repo: repo, PR: 12, Host: "local", Worktree: worktree, Branch: "feat/x", Result: "closed"})
	if err := os.MkdirAll(filepath.Dir(s.reapLog(repo)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.reapLog(repo), append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A merged PR's idle worker pane is never closed: once the pane is gone the
// worktree and its branch are removed, with no screen capture or composer
// read. The pane list is empty here because panewire's node already closed
// the pane; the agent list still names its worktree.
func TestReapRemovesWorktreeWithoutClosingPane(t *testing.T) {
	outs := worktreeOuts("/repo")
	outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("herdr\x00pane\x00close") || r.called("herdr\x00pane\x00read") || r.called("pane\x00capture") {
		t.Fatalf("pane touched: %q", r.calls)
	}
	if !r.called("git\x00-C\x00/repo\x00worktree\x00remove\x00/repo.wt") {
		t.Fatal("worktree never removed")
	}
	if !r.called("git\x00-C\x00/repo\x00branch\x00-d\x00feat/x") {
		t.Fatal("branch never deleted")
	}
	for _, c := range r.calls {
		if strings.Contains(c, "--force") || strings.Contains(c, "\x00-D") {
			t.Fatal("forced mutation", c)
		}
	}
	var removed, deleted bool
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["action"] == "close-pane" {
			t.Fatalf("close-pane row written: %v", row)
		}
		removed = removed || row["result"] == "removed"
		deleted = deleted || row["result"] == "deleted"
	}
	if !removed || !deleted {
		t.Fatal("receipt log incomplete")
	}
}

func TestReapShadowAndDryRunMutateNothing(t *testing.T) {
	for name, o := range map[string]Options{"shadow": {Shadow: true}, "dry": {DryRun: true}} {
		r := &scriptRunner{outs: worktreeOuts("/repo"), fn: statAnswer("yes\n")}
		dir := t.TempDir()
		c := repoConfig(dir)
		var buf strings.Builder
		s := &Service{Config: c, GH: &reapGH{}, Runner: r, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: &buf}
		greenHead(t, s, testRepo, mergedPull.MergeCommitSHA)
		if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if r.called("pane\x00close") || r.called("worktree\x00remove") || r.called("branch\x00-d") || r.called("herdr pane read") {
			t.Fatalf("%s mutated", name)
		}
		if strings.Contains(buf.String(), "would-close") || strings.Contains(buf.String(), "would-remove") || strings.Contains(buf.String(), "would-delete") {
			t.Fatalf("%s planned a removal for a worktree in use: %s", name, buf.String())
		}
		if !strings.Contains(buf.String(), `"reason":"pane-in-use"`) {
			t.Fatalf("%s output: %s", name, buf.String())
		}
		if _, err := os.Stat(s.reapLog(c.Repos[0].Name)); o.DryRun != (err != nil) {
			t.Fatalf("%s log presence wrong", name)
		}
	}
}

func TestReapGuards(t *testing.T) {
	// pane2 sits in the same worktree: whatever it is doing, the worktree is
	// in use and waits out panewire's close. Nothing is ever closed.
	outs := worktreeOuts("/repo.wt")
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("pane\x00close") {
		t.Fatal("pane closed")
	}
	var inUse bool
	for _, row := range reapLogRows(t, s, "example/project") {
		inUse = inUse || row["reason"] == "pane-in-use"
	}
	if !inUse {
		t.Fatal("in-use worktree not kept")
	}
}

func TestReapSkipsWorktreeInUse(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo.wt")}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("in-use worktree removed")
	}
	var inUse bool
	for _, row := range reapLogRows(t, s, "example/project") {
		inUse = inUse || row["reason"] == "pane-in-use"
	}
	if !inUse {
		t.Fatal("pane-in-use not recorded")
	}
}

func TestReapSkipsUnpushedBranch(t *testing.T) {
	r := &scriptRunner{outs: panegoneWorktreeOuts("/repo"), errs: map[string]error{
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "feat/x", "origin/main"):              errors.New("exit 1"),
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "feat/x", "main"):                     errors.New("exit 1"),
		key("git", "-C", "/repo", "rev-list", "--count", "--left-right", "feat/x...feat/x@{upstream}"): errors.New("no upstream"),
		key("git", "-C", "/repo", "rev-list", "--count", "--left-right", "feat/x...origin/feat/x"):     errors.New("unknown revision"),
	}}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") || r.called("branch\x00-d") {
		t.Fatal("unpushed worktree removed")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		saw = saw || row["reason"] == "unmerged-or-unpushed"
	}
	if !saw {
		t.Fatal("unpushed skip not recorded")
	}
}

func TestReapSkipsRevertedAndRecovery(t *testing.T) {
	for name, gh := range map[string]GitHub{"reverted": &revertedGH{}, "recovery": &openRecoveryGH{}} {
		r := &scriptRunner{outs: worktreeOuts("/repo")}
		s := svc(t, gh, r)
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(name, err)
		}
		if r.called("worktree\x00remove") || r.called("branch\x00-d") {
			t.Fatal(name, "removed anyway")
		}
	}
}

func TestReapCleansReceiptedWorktree(t *testing.T) {
	agents, panes := paneJSON("/repo")
	agents = strings.ReplaceAll(agents, "/repo.wt", "/elsewhere")
	panes = strings.ReplaceAll(panes, "/repo.wt", "/elsewhere")
	r := &scriptRunner{outs: map[string]string{
		key("herdr", "agent", "list"): agents,
		key("herdr", "pane", "list"):  panes,
		key("git", "-C", "/elsewhere", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/other/.git/worktrees/elsewhere\n/other/.git\n/elsewhere\n",
		key("git", "-C", "/elsewhere", "branch", "--show-current"):                                               "feat/other\n",
		key("git", "-C", "/elsewhere", "rev-parse", "HEAD"):                                                      "bbbb2222\n",
		key("git", "-C", "/elsewhere", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo.gone", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.gone\n/repo/.git\n/repo.gone\n",
		key("git", "-C", "/repo.gone", "branch", "--show-current"):                                               "feat/x\n",
		key("git", "-C", "/repo.gone", "rev-parse", "HEAD"):                                                      headSHA + "\n",
		key("git", "-C", "/repo.gone", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"):      "/repo/.git\n/repo/.git\n/repo\n",
		key("git", "-C", "/repo", "branch", "--show-current"):                                                    "main\n",
		key("git", "-C", "/repo", "remote", "get-url", "origin"):                                                 "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                             "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.gone\nHEAD aaaa\nbranch refs/heads/feat/x\n",
		key("git", "-C", "/other", "worktree", "list", "--porcelain"):                                            "worktree /other\nHEAD bbbb\nbranch refs/heads/main\n",
		key("git", "-C", "/repo.gone", "status", "--porcelain"):                                                  "",
		key("git", "-C", "/repo.gone", "status", "--porcelain", "--ignored", "-z"):                               "",
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "feat/x", "origin/main"):                        "",
		key("git", "-C", "/repo", "worktree", "remove", "/repo.gone"):                                            "",
		key("git", "-C", "/repo", "branch", "-d", "feat/x"):                                                      "",
	}}
	s := svc(t, &reapGH{}, r)
	seedWorktreeReceipt(t, s, "example/project", "/repo.gone")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove\x00/repo.gone") {
		t.Fatal("receipted worktree not removed")
	}
}

func TestReapRejectsBadPRFlag(t *testing.T) {
	s := svc(t, &reapGH{}, &scriptRunner{})
	if err := s.Reap(context.Background(), testRepo, -1, Options{}); err == nil {
		t.Fatal("negative pr accepted")
	}
}

func seedReapRows(t *testing.T, s *Service, repo string, rows ...reapReceipt) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(s.reapLog(repo)), 0o700); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	for _, row := range rows {
		b, _ := json.Marshal(row)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(s.reapLog(repo), []byte(buf.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// failPullGH makes any single-PR fetch fail, so tests prove a receipt-named
// PR is never refetched once all its worktrees are terminal.
type failPullGH struct{ reapGH }

func (failPullGH) Pull(_ context.Context, _ string, pr int) (github.Pull, error) {
	return github.Pull{}, fmt.Errorf("fetch %d failed", pr)
}

// spyGH counts follow-up calls: a candidate-less PR must not trigger either.
type spyGH struct {
	reapGH
	pulls   int
	reverts int
}

func (g *spyGH) Pull(_ context.Context, _ string, pr int) (github.Pull, error) {
	g.pulls++
	return github.Pull{}, fmt.Errorf("unexpected pull %d", pr)
}

func (g *spyGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	g.reverts++
	return nil, nil
}

// listNoSHAGH mimics the real list endpoint: the list body carries no
// merge_commit_sha, so it must be resolved through Pull.
type listNoSHAGH struct{ reapGH }

func (listNoSHAGH) MergedPulls(_ context.Context, repo, branch string, _ time.Time) ([]github.Pull, error) {
	p := mergedPull
	p.MergeCommitSHA = ""
	return []github.Pull{p}, nil
}

func (listNoSHAGH) Pull(_ context.Context, _ string, pr int) (github.Pull, error) {
	return mergedPull, nil
}

// stillNoSHAGH resolves nothing: Pull cannot produce the SHA either.
type stillNoSHAGH struct{ listNoSHAGH }

func (stillNoSHAGH) Pull(_ context.Context, _ string, pr int) (github.Pull, error) {
	p := mergedPull
	p.MergeCommitSHA = ""
	return p, nil
}

// scanGH records the window handed to MergedPulls.
type scanGH struct {
	reapGH
	since time.Time
}

func (g *scanGH) MergedPulls(ctx context.Context, repo, branch string, since time.Time) ([]github.Pull, error) {
	g.since = since
	return g.reapGH.MergedPulls(ctx, repo, branch, since)
}

func TestReapPaneStatusNeverCloses(t *testing.T) {
	for _, st := range []string{"working", "blocked", "waiting", "", "idle", "done"} {
		outs := worktreeOuts("/repo")
		outs[key("herdr", "agent", "list")] = `{"result":{"agents":[` +
			`{"pane_id":"w1:p1","name":"wk-7","agent_status":"` + st + `","cwd":"/repo.wt"},` +
			`{"pane_id":"w1:p2","agent_status":"working","cwd":"/repo"}]}}`
		r := &scriptRunner{outs: outs}
		s := svc(t, &reapGH{}, r)
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatalf("status %q: %v", st, err)
		}
		if r.called("pane\x00close") {
			t.Fatalf("status %q pane closed", st)
		}
		var saw bool
		for _, row := range reapLogRows(t, s, "example/project") {
			saw = saw || row["reason"] == "pane-in-use"
		}
		if !saw {
			t.Fatalf("status %q: no pane-in-use row", st)
		}
	}
}

func TestReapRescansPanesBeforeRemoval(t *testing.T) {
	// The pane list at discovery names no pane in the worktree; a worker
	// spawned into it before the removal re-check must stop the removal.
	// The receipt-seeded candidate names the worktree, as a legacy close-pane
	// row still does.
	noPane := `{"result":{"panes":[{"pane_id":"w1:p2","cwd":"/repo"}]}}`
	fresh := `{"result":{"panes":[{"pane_id":"w1:p2","cwd":"/repo"},{"pane_id":"w1:p3","cwd":"/repo.wt"}]}}`
	outs := worktreeOuts("/repo")
	outs[key("herdr", "agent", "list")] = `{"result":{"agents":[{"pane_id":"w1:p2","agent_status":"working","cwd":"/repo"}]}}`
	outs[key("herdr", "pane", "list")] = noPane
	r := &scriptRunner{outs: outs, queue: map[string][]string{
		key("herdr", "pane", "list"): {noPane, fresh},
	}}
	s := svc(t, &reapGH{}, r)
	seedWorktreeReceipt(t, s, "example/project", "/repo.wt")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("worktree removed under a fresh pane")
	}
	var inUse, newClose bool
	for _, row := range reapLogRows(t, s, "example/project") {
		// The seeded legacy close-pane row names the candidate worktree; no
		// new close-pane row may follow it.
		newClose = newClose || row["action"] == "close-pane" && row["result"] != "closed"
		inUse = inUse || row["action"] == "remove-worktree" && row["reason"] == "pane-in-use"
	}
	if newClose {
		t.Fatal("new close-pane row written")
	}
	if !inUse {
		t.Fatal("expected fresh-pane removal skip")
	}
}

func TestReapSkipsTerminalReceiptPRs(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &failPullGH{}, r)
	// PR 55's only named worktree already reached a terminal remove outcome:
	// the pass must not refetch it from GitHub. The remove row is seeded in
	// the old shape — no Worktree field, only Cwd — to prove the rowKey
	// fallback still recognizes it as terminal.
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "close-pane", Repo: "example/project", PR: 55, Host: "local", Worktree: "/done.wt", Result: "closed"},
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 55, Host: "local", Cwd: "/done.wt", Result: "removed"},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
}

func TestReapReceiptRefetchBackoff(t *testing.T) {
	for name, age := range map[string]time.Duration{"recent": time.Hour, "stale": 25 * time.Hour} {
		r := &scriptRunner{outs: worktreeOuts("/repo")}
		s := svc(t, &failPullGH{}, r)
		// A receipt-named PR is refetched at most once a day, measured from
		// its newest row; a persistent skip must not poll forever.
		seedReapRows(t, s, "example/project",
			reapReceipt{Action: "close-pane", Repo: "example/project", PR: 77, Host: "local", Worktree: "/stale.wt", Result: "closed", Time: time.Now().Add(-age).UTC()},
		)
		err := s.Reap(context.Background(), testRepo, 0, Options{})
		if name == "recent" && err != nil {
			t.Fatalf("recent row should defer the refetch: %v", err)
		}
		if name == "stale" {
			if err == nil || !strings.Contains(err.Error(), "77") {
				t.Fatalf("stale row should refetch and surface the error: %v", err)
			}
			// The attempt stamped a check-pr row: a same-day pass must not
			// refetch even though the outcome stayed identical.
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatalf("refetch should back off for a day: %v", err)
			}
		}
	}
}

func TestReapDryRunBoundsRefetch(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &failPullGH{}, r)
	// Even under --dry-run the check-pr stamp is written: it records a
	// GitHub call that really happened, so repeated dry-run polls stay
	// bounded like acting ones.
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "close-pane", Repo: "example/project", PR: 77, Host: "local", Worktree: "/stale.wt", Result: "closed", Time: time.Now().Add(-25 * time.Hour).UTC()},
	)
	err := s.Reap(context.Background(), testRepo, 0, Options{DryRun: true})
	if err == nil || !strings.Contains(err.Error(), "77") {
		t.Fatalf("stale row should refetch even in dry-run: %v", err)
	}
	var stamped bool
	for _, row := range reapLogRows(t, s, "example/project") {
		stamped = stamped || row["action"] == "check-pr" && row["pr"] == float64(77)
	}
	if !stamped {
		t.Fatal("dry-run refetch left no check-pr stamp")
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{DryRun: true}); err != nil {
		t.Fatalf("dry-run refetch should back off for a day: %v", err)
	}
}

func TestReapNoCandidateNoRows(t *testing.T) {
	// A merged PR naming no worktree on any host produces no rows and no
	// follow-up GitHub calls — the listing alone already said all there is.
	r := &scriptRunner{outs: map[string]string{
		key("herdr", "agent", "list"): `{"result":{"agents":[]}}`,
		key("herdr", "pane", "list"):  `{"result":{"panes":[]}}`,
	}}
	gh := &spyGH{}
	s := svc(t, gh, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if gh.pulls != 0 || gh.reverts != 0 {
		t.Fatalf("candidate-less PR triggered %d pulls, %d reverts", gh.pulls, gh.reverts)
	}
	if _, err := os.Stat(s.reapLog(testRepo.Name)); !os.IsNotExist(err) {
		t.Fatal("candidate-less PR wrote rows")
	}
}

func TestReapResolvesMergeCommit(t *testing.T) {
	r := &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s := svc(t, &listNoSHAGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove") {
		t.Fatal("merge_commit_sha was not resolved through Pull")
	}

	r = &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s = svc(t, &stillNoSHAGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("removal ran without a merge commit")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "merge-commit-unknown" {
			saw = true
			if row["host"] == "" {
				t.Fatalf("merge-commit-unknown row has no host: %v", row)
			}
		}
	}
	if !saw {
		t.Fatal("unresolvable merge commit not recorded")
	}
}

// pullCacheGH is the real GitHub client with only the revert watch faked: the
// merge-commit path under test is the client's own list-then-single read.
type pullCacheGH struct{ *github.Client }

func (pullCacheGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	return nil, nil
}

// A merged PR body cached without merge_commit_sha must not starve reap: the
// merge commit comes from GraphQL, whatever body the REST caches replay.
// PullValidators primes the pull cache without the state writer's resolver.
func TestReapMergeCommitPastStalePullCache(t *testing.T) {
	mergedAt := time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	mergeSHA := strings.Repeat("b", 40)
	var unconditional atomic.Int32
	var conditional atomic.Int32
	var graphql atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/graphql":
			graphql.Add(1)
			fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"mergedAt":%q,"mergeCommit":{"oid":%q}}}}}`, mergedAt, mergeSHA)
		case path == "/repos/example/project/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number": 12, "state": "closed", "merged": true, "merged_at": mergedAt, "updated_at": updatedAt,
				"title": "t", "head": map[string]any{"sha": headSHA, "ref": "feat/x"}, "base": map[string]any{"sha": "b1", "ref": "main"},
				"user": map[string]any{"login": "a"},
			}})
		case strings.HasSuffix(path, "/pulls/12/reviews"):
			fmt.Fprint(w, `[]`)
		case strings.HasSuffix(path, "/check-runs"):
			fmt.Fprint(w, `{"check_runs":[]}`)
		case strings.HasSuffix(path, "/statuses"):
			fmt.Fprint(w, `[]`)
		case path == "/repos/example/project/commits/"+headSHA:
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": headSHA, "commit": map[string]any{"message": "t"}})
		case path == "/repos/example/project/pulls/12":
			if inm := r.Header.Get("If-None-Match"); inm == `"e1"` {
				conditional.Add(1)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			unconditional.Add(1)
			w.Header().Set("ETag", `"e1"`)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 12, "state": "closed", "merged": true, "merged_at": mergedAt, "updated_at": updatedAt,
				"title": "t", "head": map[string]any{"sha": headSHA, "ref": "feat/x"}, "base": map[string]any{"sha": "b1", "ref": "main"},
				"user": map[string]any{"login": "a"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	gh := github.New("")
	gh.BaseURL = server.URL
	gh.UploadURL = server.URL
	gh.HTTP = server.Client()
	// Prime the pull cache with the merged body that carries no sha.
	if _, err := gh.PullValidators(context.Background(), "example/project", 12); err != nil {
		t.Fatal(err)
	}
	// Precondition: a plain read replays the merged body without the sha.
	if _, err := gh.PullValidators(context.Background(), "example/project", 12); err != nil || conditional.Load() == 0 {
		t.Fatalf("no ETag replay to exercise: conditional=%d err=%v", conditional.Load(), err)
	}

	r := &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s := svc(t, pullCacheGH{gh}, r)
	greenHead(t, s, testRepo, mergeSHA)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove") {
		t.Fatal("stale cached PR body starved the merge commit")
	}
	if unconditional.Load() < 2 {
		t.Fatalf("reap did not read the PR unconditionally: %d", unconditional.Load())
	}
	if graphql.Load() != 1 {
		t.Fatalf("merge commit queries=%d, want one", graphql.Load())
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "merge-commit-unknown" {
			t.Fatalf("merge commit stayed unknown: %v", row)
		}
	}
}

// The PR was open when its single-PR body was cached, so a replayed body has
// neither merged nor merged_at and the client would not consult GraphQL. The
// list already says the PR is merged, so reap reads past every cache; the
// merge commit then comes from GraphQL.
func TestReapMergeCommitPastOpenPullReplay(t *testing.T) {
	mergedAt := time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	mergeSHA := strings.Repeat("b", 40)
	var primed atomic.Bool
	var conditional, unconditional, graphql atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/graphql":
			graphql.Add(1)
			fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"mergedAt":%q,"mergeCommit":{"oid":%q}}}}}`, mergedAt, mergeSHA)
		case path == "/repos/example/project/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number": 12, "state": "closed", "merged": true, "merged_at": mergedAt, "updated_at": updatedAt,
				"title": "t", "head": map[string]any{"sha": headSHA, "ref": "feat/x"}, "base": map[string]any{"sha": "b1", "ref": "main"},
				"user": map[string]any{"login": "a"},
			}})
		case strings.HasSuffix(path, "/pulls/12/reviews"):
			fmt.Fprint(w, `[]`)
		case strings.HasSuffix(path, "/check-runs"):
			fmt.Fprint(w, `{"check_runs":[]}`)
		case strings.HasSuffix(path, "/statuses"):
			fmt.Fprint(w, `[]`)
		case path == "/repos/example/project/commits/"+headSHA:
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": headSHA, "commit": map[string]any{"message": "t"}})
		case path == "/repos/example/project/pulls/12":
			if r.Header.Get("If-None-Match") == `"open"` {
				conditional.Add(1)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			unconditional.Add(1)
			body := map[string]any{
				"number": 12, "state": "open", "merged": false, "updated_at": updatedAt,
				"title": "t", "head": map[string]any{"sha": headSHA, "ref": "feat/x"}, "base": map[string]any{"sha": "b1", "ref": "main"},
				"user": map[string]any{"login": "a"},
			}
			etag := `"open"`
			if primed.Load() {
				body = map[string]any{
					"number": 12, "state": "closed", "merged": true, "merged_at": mergedAt, "updated_at": updatedAt,
					"title": "t", "head": map[string]any{"sha": headSHA, "ref": "feat/x"}, "base": map[string]any{"sha": "b1", "ref": "main"},
					"user": map[string]any{"login": "a"},
				}
				etag = `"merged"`
			}
			w.Header().Set("ETag", etag)
			_ = json.NewEncoder(w).Encode(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	gh := github.New("")
	gh.BaseURL = server.URL
	gh.UploadURL = server.URL
	gh.HTTP = server.Client()
	// Cache the open body with its validator, then let GitHub merge the PR.
	if _, err := gh.PullValidators(context.Background(), "example/project", 12); err != nil {
		t.Fatal(err)
	}
	// Precondition: a plain read now replays the open body with its ETag.
	if _, err := gh.PullValidators(context.Background(), "example/project", 12); err != nil || conditional.Load() == 0 {
		t.Fatalf("no ETag replay to exercise: conditional=%d err=%v", conditional.Load(), err)
	}
	primed.Store(true)

	r := &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s := svc(t, pullCacheGH{gh}, r)
	greenHead(t, s, testRepo, mergeSHA)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove") {
		t.Fatal("open-PR replay starved the merge commit on the first pass")
	}
	if unconditional.Load() < 2 {
		t.Fatalf("reap did not read the PR unconditionally: %d", unconditional.Load())
	}
	if graphql.Load() != 1 {
		t.Fatalf("merge commit queries=%d, want one", graphql.Load())
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "merge-commit-unknown" {
			t.Fatalf("merge commit stayed unknown: %v", row)
		}
	}
}

func TestReapScanAfterBound(t *testing.T) {
	gh := &scanGH{}
	s := svc(t, gh, &scriptRunner{outs: worktreeOuts("/repo")})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(gh.since); d < 29*24*time.Hour || d > 31*24*time.Hour {
		t.Fatalf("scan window = %v, want ~720h", d)
	}
}

func TestReapFetchErrorPerPR(t *testing.T) {
	r := &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s := svc(t, &failPullGH{}, r)
	// PR 77 still has pending work (a legacy close row, no remove row): the
	// fetch is attempted, fails, and only that PR is lost this pass.
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "close-pane", Repo: "example/project", PR: 77, Host: "local", Worktree: "/stale.wt", Result: "closed"},
	)
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "77") {
		t.Fatalf("expected per-PR error, got %v", err)
	}
	var removed bool
	for _, row := range reapLogRows(t, s, "example/project") {
		removed = removed || row["result"] == "removed" && row["pr"] == 12.0
	}
	if !removed {
		t.Fatal("PR 12 not processed after PR 77 fetch error")
	}
}

func TestReapRemovesWithRegenerableIgnored(t *testing.T) {
	outs := panegoneWorktreeOuts("/repo")
	// Only explicit directory forms approve wholly ignored dirs, and only
	// at the top level; node_modules/ and Library/ qualify, so the
	// worktree goes.
	outs[key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored", "-z")] = "!! .venv/\x00!! node_modules/\x00!! Library/\x00"
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove") {
		t.Fatal("regenerable ignored files blocked removal")
	}
}

func TestReapSkipsIgnoredFiles(t *testing.T) {
	outs := worktreeOuts("/repo")
	// .env and notes are not regenerable; .venv/ is approved by its
	// directory form so it stays safe and unnamed.
	outs[key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored", "-z")] = "!! .venv/\x00!! .env\x00!! notes.md\x00"
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("gitignored files deleted with the worktree")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		reason := fmt.Sprint(row["reason"])
		if strings.HasPrefix(reason, "ignored-files:") {
			saw = true
			if !regexp.MustCompile(`^ignored-files:2-[0-9a-f]{8}$`).MatchString(reason) {
				t.Fatalf("reason should be count+hash, got %q", reason)
			}
			if files := fmt.Sprint(row["files"]); !strings.Contains(files, ".env") || !strings.Contains(files, "notes.md") || strings.Contains(files, ".venv") {
				t.Fatalf("files should name only non-regenerable paths, got %q", files)
			}
		}
	}
	if !saw {
		t.Fatal("ignored-files skip not recorded")
	}
}

func TestReapRefusesUnapprovedDir(t *testing.T) {
	outs := worktreeOuts("/repo")
	// bin stays name-only in the defaults, so a wholly ignored bin/ refuses
	// on both sides even though it could hold build output — and any secret
	// inside it is never deleted.
	outs[key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored", "-z")] = "!! bin/\x00"
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("worktree holding bin/ deleted with the worktree")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		reason := fmt.Sprint(row["reason"])
		if strings.HasPrefix(reason, "ignored-files:") {
			saw = true
			if files := fmt.Sprint(row["files"]); !strings.Contains(files, "bin/") {
				t.Fatalf("files should name the refused bin/ dir, got %q", files)
			}
		}
	}
	if !saw {
		t.Fatal("ignored-files skip not recorded")
	}
}

func TestReapRepoScopedRegenerable(t *testing.T) {
	// .local/ is not a default; only a repo that lists it in its own
	// reap.regenerable removes a worktree holding a wholly ignored
	// .local/, and only there.
	outs := panegoneWorktreeOuts("/repo")
	outs[key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored", "-z")] = "!! .local/\x00"
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	scoped := testRepo
	scoped.Reap.Regenerable = []string{".local/"}
	if err := s.Reap(context.Background(), scoped, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove") {
		t.Fatal("repo-scoped .local/ blocked removal")
	}
	r2 := &scriptRunner{outs: outs}
	s2 := svc(t, &reapGH{}, r2)
	if err := s2.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r2.called("worktree\x00remove") {
		t.Fatal("default list removed a .local/ worktree")
	}
	var saw bool
	for _, row := range reapLogRows(t, s2, "example/project") {
		if strings.HasPrefix(fmt.Sprint(row["reason"]), "ignored-files:") {
			saw = true
			if files := fmt.Sprint(row["files"]); !strings.Contains(files, ".local/") {
				t.Fatalf("files should name .local/, got %q", files)
			}
		}
	}
	if !saw {
		t.Fatal("ignored-files skip not recorded")
	}
}

func TestReapActGate(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo"), fn: statAnswer("yes\n")}
	c := repoConfig(t.TempDir())
	c.Reap.Act = false
	var buf strings.Builder
	s := &Service{Config: c, GH: &reapGH{}, Runner: r, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: &buf}
	greenHead(t, s, testRepo, mergedPull.MergeCommitSHA)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("pane\x00close") || r.called("worktree\x00remove") || r.called("branch\x00-d") || r.called("herdr pane read") {
		t.Fatal("reap acted without reap.act")
	}
	if strings.Contains(buf.String(), "would-close") || strings.Contains(buf.String(), "would-remove") {
		t.Fatalf("reap.act off should not plan removals for a worktree in use: %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"reason":"pane-in-use"`) {
		t.Fatalf("reap.act off should stay observational: %s", buf.String())
	}
}

// oldRecoveryGH answers WatchRevert with a recovery PR merged long before the
// PR's own merge: outside the seven-day window, so it is not a revert.
type oldRecoveryGH struct{ reapGH }

func (oldRecoveryGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	return &github.Pull{Number: 99, State: "closed", Merged: true, MergedAt: rfc(time.Now().Add(-30 * 24 * time.Hour))}, nil
}

// A recovery PR merged outside the seven-day window does not excuse the watch
// verdict: the merged-PR path still waits for main to be green.
func TestReapOldRecoveryStillWaitsForWatchVerdict(t *testing.T) {
	r := &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s := svc(t, &oldRecoveryGH{}, r)
	seedWorktreeReceipt(t, s, "example/project", "/repo.wt")
	if err := os.Remove(filepath.Join(s.state("example/project"), "last-green")); err != nil {
		t.Fatal(err)
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("removed without a watch verdict")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "watch-pending" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("watch-pending not recorded")
	}

	// With the verdict the same checkout is removed.
	r = &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
	s = svc(t, &oldRecoveryGH{}, r)
	seedWorktreeReceipt(t, s, "example/project", "/repo.wt")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("worktree\x00remove") {
		t.Fatal("healthy verdict did not remove")
	}
}
