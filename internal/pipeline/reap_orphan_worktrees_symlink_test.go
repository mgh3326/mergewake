package pipeline

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A configured work root that is a symlink (macOS: /var -> /private/var) must
// still match the physical paths git prints for its direct children.
func TestOrphanWorktreeSymlinkedWorkRootQualifiesRealChildren(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(base, "real")
	link := filepath.Join(base, "link")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{real, outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	remote := filepath.Join(real, "origin.git")
	main := filepath.Join(real, "project")
	candidate := filepath.Join(real, "candidate")
	sibling := filepath.Join(outside, "sibling")
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
	gitRunAt(t, main, "worktree", "add", "--detach", candidate, "origin/main")
	gitRunAt(t, main, "worktree", "add", "--detach", sibling, "origin/main")

	host := reap.Host{Name: "local", WorkRoot: link}
	ex := reap.Exec{Runner: integrations.Exec{}, Host: host}
	cfg := config.Config{
		StateDir: filepath.Join(base, "state"), Repos: []config.Repo{{Name: "example/project", Branch: "main"}},
		Hosts: []reap.Host{host}, Reap: config.Reap{Regenerable: config.DefaultReapRegenerable, OrphanWorktrees: config.OrphanWorktrees{Enabled: true}},
	}
	s := &Service{Config: cfg, Runner: integrations.Exec{}, Out: &bytes.Buffer{}, Now: time.Now}
	hs := &reapHost{host: host, ops: orphanTestOps{Exec: ex}, exec: ex, trees: map[string]*reap.Worktree{}}
	f := orphanFixture{s: s, hs: hs, main: main, path: candidate, root: link}
	f.age(t, 30*24*time.Hour)

	rows, _, errs := runOrphanFixture(f, false)
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if len(rows) != 1 || rows[0].Worktree != candidate || rows[0].Result != "would-remove" {
		t.Fatalf("rows = %+v, want one would-remove row for %s only", rows, candidate)
	}
}
