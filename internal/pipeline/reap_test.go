package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

type scriptRunner struct {
	outs  map[string]string
	queue map[string][]string // consumed in order before outs, for re-reads
	errs  map[string]error
	calls []string
}

func (s *scriptRunner) Run(_ context.Context, name string, args []string, _ string, _ string) ([]byte, error) {
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
	return nil, fmt.Errorf("unexpected command: %s", strings.ReplaceAll(k, "\x00", " "))
}

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
	Head: github.Ref{Ref: "wk/x", SHA: headSHA}, Base: github.Ref{Ref: "main"},
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
		Reap:     config.Reap{Enabled: true, Act: true, WorktreeAfter: "168h", Protect: []string{"*-integrator"}},
		Repos:    []config.Repo{{Name: "example/project", Branch: "main"}},
	}
}

func svc(t *testing.T, gh GitHub, run *scriptRunner) *Service {
	t.Helper()
	return &Service{Config: repoConfig(t.TempDir()), GH: gh, Runner: run, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: io.Discard}
}

func paneJSON(cwd2 string) (agents, panes string) {
	agents = `{"result":{"agents":[` +
		`{"pane_id":"w1:p1","name":"wk-7","agent_status":"idle","cwd":"/repo.wt"},` +
		`{"pane_id":"w1:p2","agent_status":"working","cwd":"` + cwd2 + `"}]}}`
	panes = `{"result":{"panes":[` +
		`{"pane_id":"w1:p1","cwd":"/repo.wt"},` +
		`{"pane_id":"w1:p2","cwd":"` + cwd2 + `"}]}}`
	return
}

func worktreeOuts(cwd2 string) map[string]string {
	agents, panes := paneJSON(cwd2)
	return map[string]string{
		key("herdr", "agent", "list"):          agents,
		key("herdr", "pane", "list"):           panes,
		key("herdr", "pane", "close", "w1:p1"): "",
		key("git", "-C", "/repo.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.wt\n/repo/.git\n/repo.wt\n",
		key("git", "-C", "/repo.wt", "branch", "--show-current"):                                               "wk/x\n",
		key("git", "-C", "/repo.wt", "rev-parse", "HEAD"):                                                      headSHA + "\n",
		key("git", "-C", "/repo.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"):    "/repo/.git\n/repo/.git\n/repo\n",
		key("git", "-C", "/repo", "branch", "--show-current"):                                                  "main\n",
		key("git", "-C", "/repo", "remote", "get-url", "origin"):                                               "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                           "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.wt\nHEAD aaaa\nbranch refs/heads/wk/x\n",
		key("git", "-C", "/repo.wt", "status", "--porcelain"):                                                  "",
		key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored"):                                     "",
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "wk/x", "origin/main"):                        "",
		key("git", "-C", "/repo", "worktree", "remove", "/repo.wt"):                                            "",
		key("git", "-C", "/repo", "branch", "-d", "wk/x"):                                                      "",
		key("sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", "/jobs/wk-7"):                             "yes\n",
		key("sh", "-c", `test -f "$1" && echo yes || echo no`, "sh", "/jobs/wk-7/report.md"):                   "yes\n",
	}
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
	b, _ := json.Marshal(reapReceipt{Action: "close-pane", Repo: repo, PR: 12, Host: "local", Worktree: worktree, Branch: "wk/x", Result: "closed"})
	if err := os.MkdirAll(filepath.Dir(s.reapLog(repo)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.reapLog(repo), append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestReapClosesPaneAndRemovesWorktree(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called("herdr\x00pane\x00close\x00w1:p1") {
		t.Fatal("pane never closed")
	}
	if !r.called("git\x00-C\x00/repo\x00worktree\x00remove\x00/repo.wt") {
		t.Fatal("worktree never removed")
	}
	if !r.called("git\x00-C\x00/repo\x00branch\x00-d\x00wk/x") {
		t.Fatal("branch never deleted")
	}
	for _, c := range r.calls {
		if strings.Contains(c, "--force") || strings.Contains(c, "\x00-D") {
			t.Fatal("forced mutation", c)
		}
	}
	var closed, removed, deleted bool
	for _, row := range reapLogRows(t, s, "example/project") {
		closed = closed || row["result"] == "closed" && row["pane"] == "w1:p1" && row["saved"] == "/jobs/wk-7/report.md"
		removed = removed || row["result"] == "removed"
		deleted = deleted || row["result"] == "deleted"
	}
	if !closed || !removed || !deleted {
		t.Fatal("receipt log incomplete")
	}
}

func TestReapCapturesScreenBeforeClose(t *testing.T) {
	outs := worktreeOuts("/repo")
	outs[key("sh", "-c", `test -f "$1" && echo yes || echo no`, "sh", "/jobs/wk-7/report.md")] = "no\n"
	outs[key("sh", "-c", `herdr pane read "$2" > "$1/pane-screen.txt"`, "sh", "/jobs/wk-7", "w1:p1")] = ""
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var closed bool
	for _, row := range reapLogRows(t, s, "example/project") {
		closed = closed || row["result"] == "closed" && row["saved"] == "/jobs/wk-7/pane-screen.txt"
	}
	if !closed {
		t.Fatal("screen was not saved before close")
	}
}

func TestReapShadowAndDryRunMutateNothing(t *testing.T) {
	for name, o := range map[string]Options{"shadow": {Shadow: true}, "dry": {DryRun: true}} {
		r := &scriptRunner{outs: worktreeOuts("/repo")}
		dir := t.TempDir()
		c := repoConfig(dir)
		var buf strings.Builder
		s := &Service{Config: c, GH: &reapGH{}, Runner: r, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: &buf}
		if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if r.called("pane\x00close") || r.called("worktree\x00remove") || r.called("branch\x00-d") || r.called("pane\x00read") {
			t.Fatalf("%s mutated", name)
		}
		if !strings.Contains(buf.String(), "would-close") || !strings.Contains(buf.String(), "would-remove") || !strings.Contains(buf.String(), "would-delete") {
			t.Fatalf("%s output: %s", name, buf.String())
		}
		if _, err := os.Stat(s.reapLog(c.Repos[0].Name)); o.DryRun != (err != nil) {
			t.Fatalf("%s log presence wrong", name)
		}
	}
}

func TestReapGuards(t *testing.T) {
	// pane2 sits in the same worktree but is still working: it must be
	// skipped, which also leaves the worktree in use for cleanup.
	outs := worktreeOuts("/repo.wt")
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var sawWorking bool
	for _, row := range reapLogRows(t, s, "example/project") {
		sawWorking = sawWorking || row["result"] == "skipped" && row["reason"] == "status:working"
	}
	if !sawWorking {
		t.Fatal("working pane not guarded")
	}
	if r.called("pane\x00close\x00w1:p2") {
		t.Fatal("working pane closed")
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
	r := &scriptRunner{outs: worktreeOuts("/repo"), errs: map[string]error{
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "wk/x", "origin/main"):            errors.New("exit 1"),
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "wk/x", "main"):                   errors.New("exit 1"),
		key("git", "-C", "/repo", "rev-list", "--count", "--left-right", "wk/x...wk/x@{upstream}"): errors.New("no upstream"),
		key("git", "-C", "/repo", "rev-list", "--count", "--left-right", "wk/x...origin/wk/x"):     errors.New("unknown revision"),
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
		key("git", "-C", "/elsewhere", "branch", "--show-current"):                                               "wk/other\n",
		key("git", "-C", "/elsewhere", "rev-parse", "HEAD"):                                                      "bbbb2222\n",
		key("git", "-C", "/elsewhere", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo.gone", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.gone\n/repo/.git\n/repo.gone\n",
		key("git", "-C", "/repo.gone", "branch", "--show-current"):                                               "wk/x\n",
		key("git", "-C", "/repo.gone", "rev-parse", "HEAD"):                                                      headSHA + "\n",
		key("git", "-C", "/repo.gone", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"):      "/repo/.git\n/repo/.git\n/repo\n",
		key("git", "-C", "/repo", "branch", "--show-current"):                                                    "main\n",
		key("git", "-C", "/repo", "remote", "get-url", "origin"):                                                 "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                             "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.gone\nHEAD aaaa\nbranch refs/heads/wk/x\n",
		key("git", "-C", "/repo.gone", "status", "--porcelain"):                                                  "",
		key("git", "-C", "/repo.gone", "status", "--porcelain", "--ignored"):                                     "",
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", "wk/x", "origin/main"):                          "",
		key("git", "-C", "/repo", "worktree", "remove", "/repo.gone"):                                            "",
		key("git", "-C", "/repo", "branch", "-d", "wk/x"):                                                        "",
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

func TestReapClosesOnlyIdleDone(t *testing.T) {
	for _, st := range []string{"working", "blocked", "waiting", ""} {
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
		want := "status:" + st
		if st == "" {
			want = "status:unknown"
		}
		var saw bool
		for _, row := range reapLogRows(t, s, "example/project") {
			saw = saw || row["reason"] == want
		}
		if !saw {
			t.Fatalf("status %q: no %s row", st, want)
		}
	}
}

func TestReapReverifiesBeforeClose(t *testing.T) {
	agents, _ := paneJSON("/repo")
	// The discovery list is a stale snapshot: re-read right before close and
	// require the pane to be present and unchanged.
	for name, second := range map[string]string{
		"changed": strings.Replace(agents, `"idle"`, `"working"`, 1),
		"gone":    `{"result":{"agents":[]}}`,
	} {
		r := &scriptRunner{outs: worktreeOuts("/repo"), queue: map[string][]string{
			key("herdr", "agent", "list"): {agents, second},
		}}
		s := svc(t, &reapGH{}, r)
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(name, err)
		}
		if r.called("pane\x00close\x00w1:p1") {
			t.Fatal(name, "stale pane closed")
		}
		want := "pane-" + name
		var saw bool
		for _, row := range reapLogRows(t, s, "example/project") {
			saw = saw || row["reason"] == want
		}
		if !saw {
			t.Fatalf("%s: no %s row", name, want)
		}
	}
}

func TestReapSkipsPaneWithoutJobDir(t *testing.T) {
	outs := worktreeOuts("/repo")
	outs[key("sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", "/jobs/wk-7")] = "no\n"
	r := &scriptRunner{outs: outs}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("pane\x00close") || r.called("report.md") || r.called("pane\x00read") {
		t.Fatal("non-job pane touched")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		saw = saw || row["reason"] == "not-a-job"
	}
	if !saw {
		t.Fatal("not-a-job skip not recorded")
	}
}

func TestReapRescansPanesBeforeRemoval(t *testing.T) {
	_, panes := paneJSON("/repo")
	// A worker spawned into the worktree after discovery must stop the removal.
	fresh := `{"result":{"panes":[{"pane_id":"w1:p2","cwd":"/repo"},{"pane_id":"w1:p3","cwd":"/repo.wt"}]}}`
	r := &scriptRunner{outs: worktreeOuts("/repo"), queue: map[string][]string{
		key("herdr", "pane", "list"): {panes, fresh},
	}}
	s := svc(t, &reapGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("worktree removed under a fresh pane")
	}
	var closed, inUse bool
	for _, row := range reapLogRows(t, s, "example/project") {
		closed = closed || row["result"] == "closed"
		inUse = inUse || row["action"] == "remove-worktree" && row["reason"] == "pane-in-use"
	}
	if !closed || !inUse {
		t.Fatal("expected pane closed and fresh-pane removal skip")
	}
}

func TestReapSkipsTerminalReceiptPRs(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &failPullGH{}, r)
	// PR 55's only named worktree already reached a terminal remove outcome:
	// the pass must not refetch it from GitHub.
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "close-pane", Repo: "example/project", PR: 55, Host: "local", Worktree: "/done.wt", Result: "closed"},
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 55, Host: "local", Worktree: "/done.wt", Result: "removed"},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
}

func TestReapFetchErrorPerPR(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &failPullGH{}, r)
	// PR 77 still has pending work (closed pane, no remove row): the fetch is
	// attempted, fails, and only that PR is lost this pass.
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "close-pane", Repo: "example/project", PR: 77, Host: "local", Worktree: "/stale.wt", Result: "closed"},
	)
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "77") {
		t.Fatalf("expected per-PR error, got %v", err)
	}
	var closed bool
	for _, row := range reapLogRows(t, s, "example/project") {
		closed = closed || row["result"] == "closed" && row["pr"] == 12.0
	}
	if !closed {
		t.Fatal("PR 12 not processed after PR 77 fetch error")
	}
}

func TestReapSkipsIgnoredFiles(t *testing.T) {
	outs := worktreeOuts("/repo")
	outs[key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored")] = "!! .env\n!! notes.md\n"
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
		saw = saw || strings.HasPrefix(fmt.Sprint(row["reason"]), "ignored-files:")
	}
	if !saw {
		t.Fatal("ignored-files skip not recorded")
	}
}

func TestReapActGate(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	c := repoConfig(t.TempDir())
	c.Reap.Act = false
	var buf strings.Builder
	s := &Service{Config: c, GH: &reapGH{}, Runner: r, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: &buf}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("pane\x00close") || r.called("worktree\x00remove") || r.called("branch\x00-d") || r.called("pane\x00read") {
		t.Fatal("reap acted without reap.act")
	}
	if !strings.Contains(buf.String(), "would-close") || !strings.Contains(buf.String(), "would-remove") {
		t.Fatalf("reap.act off should stay observational: %s", buf.String())
	}
}
