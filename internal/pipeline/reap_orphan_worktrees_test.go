package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

type orphanTestOps struct {
	reap.Exec
	panes []reap.Pane
}

type orphanDiscoveryRunner struct {
	integrations.Exec
	testFileChecks int
}

func (r *orphanDiscoveryRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "test" && len(args) > 0 && args[0] == "-f" {
		r.testFileChecks++
	}
	return r.Exec.Run(ctx, command, args, input, dir)
}

func (o orphanTestOps) Panes(context.Context) ([]reap.Pane, error)    { return o.panes, nil }
func (o orphanTestOps) AllPanes(context.Context) ([]reap.Pane, error) { return o.panes, nil }

type orphanFixture struct {
	s    *Service
	hs   *reapHost
	main string
	path string
	root string
}

func newOrphanFixture(t *testing.T, branch string, pane bool) orphanFixture {
	t.Helper()
	root := t.TempDir()
	remote := filepath.Join(root, "origin.git")
	main := filepath.Join(root, "project")
	path := filepath.Join(root, "candidate")
	gitRun(t, "init", "--bare", remote)
	gitRun(t, "clone", remote, main)
	gitRunAt(t, main, "config", "user.email", "test@example.invalid")
	gitRunAt(t, main, "config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(main, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRunAt(t, main, "add", "README.md")
	gitRunAt(t, main, "commit", "-m", "base")
	gitRunAt(t, main, "branch", "-M", "main")
	gitRunAt(t, main, "push", "-u", "origin", "main")
	gitRunAt(t, main, "remote", "set-url", "origin", "https://github.com/example/project.git")
	args := []string{"worktree", "add", "--detach", path, "origin/main"}
	if branch != "" {
		args = []string{"worktree", "add", "-b", branch, path, "origin/main"}
	}
	gitRunAt(t, main, args...)
	host := reap.Host{Name: "local", WorkRoot: root}
	ex := reap.Exec{Runner: integrations.Exec{}, Host: host}
	ops := orphanTestOps{Exec: ex}
	if pane {
		ops.panes = []reap.Pane{{ID: "p1", Cwd: path}}
	}
	cfg := config.Config{
		StateDir: filepath.Join(root, "state"), Repos: []config.Repo{{Name: "example/project", Branch: "main"}},
		Hosts: []reap.Host{host}, Reap: config.Reap{Act: true, Regenerable: config.DefaultReapRegenerable, OrphanWorktrees: config.OrphanWorktrees{Enabled: true}},
	}
	s := &Service{Config: cfg, Runner: integrations.Exec{}, Out: &bytes.Buffer{}, Now: func() time.Time { return time.Now() }}
	hs := &reapHost{host: host, ops: ops, exec: ex, trees: map[string]*reap.Worktree{}}
	return orphanFixture{s: s, hs: hs, main: main, path: path, root: root}
}

func gitRun(t *testing.T, args ...string)               { t.Helper(); runGit(t, "", args...) }
func gitRunAt(t *testing.T, dir string, args ...string) { t.Helper(); runGit(t, dir, args...) }
func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func (f orphanFixture) age(t *testing.T, age time.Duration) {
	t.Helper()
	gitdir := strings.TrimSpace(string(mustRunGit(t, f.path, "rev-parse", "--absolute-git-dir")))
	stamp := time.Now().Add(-age)
	for _, path := range []string{filepath.Join(gitdir, "index"), filepath.Join(gitdir, "HEAD"), filepath.Join(gitdir, "logs", "HEAD"), f.path} {
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
}

func mustRunGit(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	b, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, b)
	}
	return b
}

func runOrphanFixture(f orphanFixture, act bool) ([]reapReceipt, string, []error) {
	var rows []reapReceipt
	var errs []error
	var output bytes.Buffer
	f.s.Out = &output
	errs = append(errs, f.s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{f.hs}, act, true, func(r reapReceipt) {
		rows = append(rows, r)
		if err := f.s.reapNote(r, false); err != nil {
			errs = append(errs, err)
		}
		if r.Result == "error" {
			errs = append(errs, fmt.Errorf("%s %s %s: %s", r.Host, r.Action, r.Worktree, r.Reason))
		}
	})...)
	return rows, output.String(), errs
}

func TestOrphanWorktreeKeepsWhenAnySafetyConditionFails(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, orphanFixture)
		want  string
	}{
		{"modified", func(t *testing.T, f orphanFixture) {
			if e := os.WriteFile(filepath.Join(f.path, "change"), []byte("x"), 0o600); e != nil {
				t.Fatal(e)
			}
		}, "modified-file"},
		{"unpushed", func(t *testing.T, f orphanFixture) {
			gitRunAt(t, f.path, "config", "user.email", "test@example.invalid")
			gitRunAt(t, f.path, "config", "user.name", "Test")
			if e := os.WriteFile(filepath.Join(f.path, "change"), []byte("x"), 0o600); e != nil {
				t.Fatal(e)
			}
			gitRunAt(t, f.path, "add", "change")
			gitRunAt(t, f.path, "commit", "-m", "local")
		}, "commits-not-proven-pushed"},
		{"young", func(t *testing.T, f orphanFixture) { f.age(t, time.Hour) }, "younger-than-min-age-or-age-unknown"},
		{"pane-in-use", func(t *testing.T, f orphanFixture) {
			f.age(t, 30*24*time.Hour)
			ops := f.hs.ops.(orphanTestOps)
			ops.panes = []reap.Pane{{ID: "p1", Cwd: f.path}}
			f.hs.ops = ops
		}, "pane-in-use"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newOrphanFixture(t, "", false)
			if tc.name != "young" && tc.name != "pane-in-use" {
				f.age(t, 30*24*time.Hour)
			}
			tc.setup(t, f)
			rows, _, errs := runOrphanFixture(f, false)
			if len(errs) != 0 {
				t.Fatalf("errors: %v", errs)
			}
			if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != tc.want {
				t.Fatalf("rows = %#v, want kept %q", rows, tc.want)
			}
		})
	}
}

func TestOrphanWorktreeDisabledDoesNoListing(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	f.s.Config.Reap.OrphanWorktrees.Enabled = false
	if err := f.s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{f.hs}, true, true, func(reapReceipt) {}); len(err) != 0 {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.s.Config.StateDir, testRepo.Name, "orphan-worktree-cursor.json")); !os.IsNotExist(err) {
		t.Fatalf("disabled stage touched cursor: %v", err)
	}
}

func TestOrphanWorktreeStatusPinsUntrackedMode(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	gitRunAt(t, f.path, "config", "status.showUntrackedFiles", "no")
	if err := os.WriteFile(filepath.Join(f.path, "notes"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if clean, reason := orphanWorktreeClean(context.Background(), f.hs.exec, f.path, nil); clean || reason != "modified-file" {
		t.Fatalf("clean=%v reason=%q", clean, reason)
	}
}

func TestOrphanWorktreeLocalUpstreamIsNotPushed(t *testing.T) {
	f := newOrphanFixture(t, "review/local", false)
	gitRunAt(t, f.main, "branch", "local-upstream", "HEAD")
	gitRunAt(t, f.path, "config", "branch.review/local.remote", ".")
	gitRunAt(t, f.path, "config", "branch.review/local.merge", "refs/heads/local-upstream")
	wt, err := f.hs.exec.Inspect(context.Background(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	if pushed, err := orphanWorktreePushed(context.Background(), f.hs.exec, f.path, wt.Head, wt.Branch, "nonexistent"); err != nil || pushed {
		t.Fatalf("pushed=%v err=%v", pushed, err)
	}
}

func TestOrphanWorktreeCursorWraps(t *testing.T) {
	visits := []orphanWorktreeVisit{{host: "a", entry: reap.Entry{Path: "/a/1"}}, {host: "a", entry: reap.Entry{Path: "/a/2"}}, {host: "b", entry: reap.Entry{Path: "/b/1"}}}
	got := resumeOrphanWorktreeVisits(visits, orphanWorktreeCursor{Host: "a", Path: "/a/2"})
	if len(got) != 3 || got[0].entry.Path != "/b/1" || got[1].entry.Path != "/a/1" || got[2].entry.Path != "/a/2" {
		t.Fatalf("visits=%+v", got)
	}
}

func TestOrphanWorktreePerHostCursorDoesNotStarveCandidate(t *testing.T) {
	// A2 must follow A1 on the next pass even though host B also has work.
	a := newOrphanFixture(t, "", false)
	b := newOrphanFixture(t, "", false)
	a.hs.host.Name, b.hs.host.Name = "a", "b"
	a.hs.host.WorkRoot, b.hs.host.WorkRoot = a.root, b.root
	a.s.Config.Hosts = []reap.Host{a.hs.host, b.hs.host}
	a.s.Config.Reap.OrphanWorktrees.PerHost = 1
	second := filepath.Join(a.root, "candidate-2")
	gitRunAt(t, a.main, "worktree", "add", "--detach", second, "origin/main")
	var seen []string
	for pass := 0; pass < 2; pass++ {
		if errs := a.s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{a.hs, b.hs}, false, true, func(r reapReceipt) { seen = append(seen, filepath.Base(r.Worktree)) }); len(errs) != 0 {
			t.Fatal(errs)
		}
	}
	if len(seen) < 3 || seen[0] != "candidate" || seen[1] != "candidate" || seen[2] != "candidate-2" {
		t.Fatalf("visited candidates across passes = %v; want A1 then A2 inspected on pass two", seen)
	}
}

func TestOrphanWorktreeDiscoveryDoesNotSpendBudgetOnMarkerChecks(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	runner := &orphanDiscoveryRunner{}
	f.hs.exec.Runner = runner
	var rows []reapReceipt
	if errs := f.s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{f.hs}, false, true, func(r reapReceipt) { rows = append(rows, r) }); len(errs) != 0 {
		t.Fatal(errs)
	}
	if runner.testFileChecks != 0 || len(rows) != 1 {
		t.Fatalf("discovery test -f checks=%d inspected rows=%d", runner.testFileChecks, len(rows))
	}
}

func TestOrphanWorktreeDryRunDoesNotWriteCursor(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	errs := f.s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{f.hs}, false, false, func(reapReceipt) {})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if _, err := os.Stat(f.s.orphanWorktreeCursorPath(testRepo.Name)); !os.IsNotExist(err) {
		t.Fatalf("dry run wrote cursor: %v", err)
	}
}

func TestReapOrphanWorktreeDryRunDoesNotWriteCursor(t *testing.T) {
	runner := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &reapGH{}, runner)
	s.Config.Reap.OrphanWorktrees.Enabled = true
	if err := s.Reap(context.Background(), testRepo, 0, Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.orphanWorktreeCursorPath(testRepo.Name)); !os.IsNotExist(err) {
		t.Fatalf("dry-run reap wrote cursor: %v", err)
	}
}

func TestOrphanWorktreeBudgetBeforeVisitDoesNotAdvanceCursor(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if errs := f.s.reapOrphanWorktrees(ctx, testRepo, []*reapHost{f.hs}, false, true, func(reapReceipt) {}); len(errs) == 0 {
		t.Fatal("expected budget stop")
	}
	if _, err := os.Stat(f.s.orphanWorktreeCursorPath(testRepo.Name)); !os.IsNotExist(err) {
		t.Fatalf("unvisited candidate advanced cursor: %v", err)
	}
}

func TestOrphanWorktreeKindIncludesWorkerWithoutPR(t *testing.T) {
	if got := orphanWorktreeKind(reap.Entry{Path: "/work/wk/job", Branch: "wk/job"}); got != "worker-owned" {
		t.Fatalf("kind=%q", got)
	}
}

func TestOrphanWorktreeLockedEntryIsKept(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	gitRunAt(t, f.main, "worktree", "lock", f.path)
	f.age(t, 30*24*time.Hour)
	rows, _, errs := runOrphanFixture(f, false)
	if len(errs) != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "locked" {
		t.Fatalf("rows=%+v errs=%v", rows, errs)
	}
}

type orphanFailRemoveRunner struct{ integrations.Exec }

func (r orphanFailRemoveRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" && len(args) >= 4 && args[len(args)-3] == "worktree" && args[len(args)-2] == "remove" {
		return nil, fmt.Errorf("simulated removal failure")
	}
	return r.Exec.Run(ctx, command, args, input, dir)
}

func TestOrphanWorktreeRemovalErrorIsReported(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	f.age(t, 30*24*time.Hour)
	f.hs.exec.Runner = orphanFailRemoveRunner{}
	rows, _, errs := runOrphanFixture(f, true)
	if len(errs) != 1 || len(rows) != 1 || rows[0].Result != "error" || !strings.Contains(rows[0].Reason, "simulated removal failure") {
		t.Fatalf("rows=%+v errs=%v", rows, errs)
	}
}

func TestOrphanWorktreeReviewerLabelPathIsOwned(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	if err := os.MkdirAll(filepath.Join(f.s.Config.StateDir, "example", "project", "reviewer-jobs"), 0o700); err != nil {
		t.Fatal(err)
	}
	label := "candidate"
	record := filepath.Join(f.s.Config.StateDir, "example", "project", "reviewer-jobs", "job.json")
	if err := os.WriteFile(record, []byte(`{"label":"`+label+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	owned, err := f.s.orphanReviewerPaths()
	if err != nil || !owned[filepath.Join(f.root, label+reviewerCheckoutSuffix)] {
		t.Fatalf("owned=%v err=%v", owned, err)
	}
}

func TestOrphanWorktreeShareKeepsQuarterForFollowingStage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, reapBudgetKey{}, reapBudget{deadline: time.Now().Add(4 * time.Second), passDeadline: time.Now().Add(4 * time.Second)})
	child, childCancel := reapStageShare(ctx, 1, 4)
	defer childCancel()
	parentDeadline, _ := ctx.Deadline()
	childDeadline, _ := child.Deadline()
	if childDeadline.After(parentDeadline) || time.Until(childDeadline) > 2*time.Second {
		t.Fatalf("child deadline %v parent %v", childDeadline, parentDeadline)
	}
}

func TestOrphanWorktreeShadowWritesRowsAndCountsWithoutRemoving(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	f.age(t, 30*24*time.Hour)
	rows, out, errs := runOrphanFixture(f, false)
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if len(rows) != 1 || rows[0].Result != "would-remove" || rows[0].Kind != "detached" {
		t.Fatalf("rows = %#v", rows)
	}
	if _, err := os.Stat(f.path); err != nil {
		t.Fatalf("shadow removed worktree: %v", err)
	}
	logged, err := os.ReadFile(f.s.reapLog(testRepo.Name))
	if err != nil || !bytes.Contains(logged, []byte(`"action":"orphan-worktree"`)) || !bytes.Contains(logged, []byte(`"result":"would-remove"`)) {
		t.Fatalf("shadow receipt not written: %s (%v)", logged, err)
	}
	if !strings.Contains(out, `"action":"orphan-worktree-count"`) || !strings.Contains(out, `"kind":"detached"`) || !strings.Contains(out, `"count":1`) {
		t.Fatalf("missing host/kind count: %s", out)
	}
}

func TestOrphanWorktreeSkipsWorkerAndReviewerOwned(t *testing.T) {
	f := newOrphanFixture(t, "wk/job-1", false)
	f.age(t, 30*24*time.Hour)
	rows, _, errs := runOrphanFixture(f, false)
	if len(errs) != 0 || len(rows) != 1 || rows[0].Kind != "worker-owned" || rows[0].Result != "kept" {
		t.Fatalf("worker-owned: rows=%#v errors=%v", rows, errs)
	}

	f = newOrphanFixture(t, "review/test", false)
	f.age(t, 30*24*time.Hour)
	if err := os.MkdirAll(f.s.state(testRepo.Name), 0o700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, Worktree: f.path})
	if err := os.WriteFile(f.s.reapLog(testRepo.Name), append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, _, errs = runOrphanFixture(f, false)
	if len(errs) != 0 || len(rows) != 0 {
		t.Fatalf("reviewer-owned: rows=%#v errors=%v", rows, errs)
	}
}

func TestOrphanWorktreeActRemovesQualifyingWorktree(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	f.age(t, 30*24*time.Hour)
	rows, _, errs := runOrphanFixture(f, true)
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if len(rows) != 1 || rows[0].Result != "removed" {
		t.Fatalf("rows = %#v", rows)
	}
	if _, err := os.Stat(f.path); !os.IsNotExist(err) {
		t.Fatalf("worktree still exists, stat err=%v", err)
	}
}

func TestOrphanWorktreesMissingCloneIsAbsence(t *testing.T) {
	root := t.TempDir()
	host := reap.Host{Name: "local", WorkRoot: root}
	ex := reap.Exec{Runner: integrations.Exec{}, Host: host}
	cfg := config.Config{
		StateDir: filepath.Join(root, "state"), Repos: []config.Repo{{Name: "example/project", Branch: "main"}},
		Hosts: []reap.Host{host}, Reap: config.Reap{Regenerable: config.DefaultReapRegenerable, OrphanWorktrees: config.OrphanWorktrees{Enabled: true}},
	}
	s := &Service{Config: cfg, Runner: integrations.Exec{}, Out: &bytes.Buffer{}}
	hs := &reapHost{host: host, ops: orphanTestOps{Exec: ex}, exec: ex, trees: map[string]*reap.Worktree{}}
	var rows []reapReceipt
	errs := s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{hs}, false, true, func(r reapReceipt) { rows = append(rows, r) })
	if len(errs) != 0 || len(rows) != 0 {
		t.Fatalf("missing clone: rows=%#v errors=%v", rows, errs)
	}
}

func TestOrphanWorktreeKeepsWhenProcessUsesCheckout(t *testing.T) {
	f := newOrphanFixture(t, "", false)
	f.age(t, 30*24*time.Hour)
	previous := mergedProcessEvidence
	mergedProcessEvidence = func(context.Context, *Service) (procEvidence, error) {
		return procEvidence{cwds: []string{f.path}}, nil
	}
	defer func() { mergedProcessEvidence = previous }()
	rows, _, errs := runOrphanFixture(f, true)
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "process-in-use" {
		t.Fatalf("rows = %#v, want process-in-use keep", rows)
	}
	if _, err := os.Stat(f.path); err != nil {
		t.Fatalf("worktree removed while process used it: %v", err)
	}
}
