package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
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
		Hosts: []reap.Host{host}, Reap: config.Reap{Act: true, Regenerable: config.DefaultReapRegenerable},
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
	errs = append(errs, f.s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{f.hs}, act, func(r reapReceipt) {
		rows = append(rows, r)
		if err := f.s.reapNote(r, false); err != nil {
			errs = append(errs, err)
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
	if len(errs) != 0 || len(rows) != 0 {
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
		Hosts: []reap.Host{host}, Reap: config.Reap{Regenerable: config.DefaultReapRegenerable},
	}
	s := &Service{Config: cfg, Runner: integrations.Exec{}, Out: &bytes.Buffer{}}
	hs := &reapHost{host: host, ops: orphanTestOps{Exec: ex}, exec: ex, trees: map[string]*reap.Worktree{}}
	var rows []reapReceipt
	errs := s.reapOrphanWorktrees(context.Background(), testRepo, []*reapHost{hs}, false, func(r reapReceipt) { rows = append(rows, r) })
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
