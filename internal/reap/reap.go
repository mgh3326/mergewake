// Package reap discovers herdr agent panes inside git worktrees and performs
// the local/ssh command boundary for merged-PR session and worktree cleanup.
// A pane is never identified by its title or name alone: only an inspected
// linked worktree whose checked-out branch equals the PR head branch, in a
// clone whose origin remote is the configured repository, qualifies.
package reap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/mgh3326/mergewake/internal/policy"
)

// Runner is satisfied by integrations.Exec without this package importing it.
type Runner interface {
	Run(context.Context, string, []string, string, string) ([]byte, error)
}

// Host is one configured session host. An empty or "local" SSH value means the
// commands run directly on this machine; any other value is an ssh(1)
// destination such as user@host. Jobs is the base directory holding one job
// directory per named pane; it must be absolute on ssh hosts.
type Host struct {
	Name string `yaml:"name" json:"name"`
	SSH  string `yaml:"ssh" json:"ssh"`
	Jobs string `yaml:"jobs" json:"jobs"`
}

func (h Host) Local() bool { return h.SSH == "" || h.SSH == "local" }

// JobDir resolves the job directory for a named pane. Empty jobs config or an
// unsafe name yields an empty result.
func (h Host) JobDir(name string) string {
	if h.Jobs == "" || !ValidJobName(name) {
		return ""
	}
	return filepath.Join(h.Jobs, name)
}

// ValidJobName keeps a pane name inside a single safe path segment.
func ValidJobName(name string) bool {
	if name == "" || name[0] == '-' || name[0] == '.' {
		return false
	}
	return !strings.ContainsAny(name, "/\\") && !strings.Contains(name, "..") &&
		!strings.ContainsAny(name, " \t\r\n\x00")
}

// Pane is one row of `herdr agent list`. cwd is the pane's working directory
// and agent_status is the reported lifecycle state (working, idle, done).
type Pane struct {
	ID     string `json:"pane_id"`
	Name   string `json:"name"`
	Status string `json:"agent_status"`
	Agent  string `json:"agent"`
	Cwd    string `json:"cwd"`
}

// Worktree describes a pane cwd resolved to its linked worktree.
type Worktree struct {
	Path   string // worktree toplevel
	Branch string // checked-out branch; empty on a detached HEAD
	Repo   string // owner/name of the origin remote; empty when unknown
	Common string // absolute git common dir, <main>/.git
}

// Exec runs herdr/git commands for one host, locally or over ssh.
type Exec struct {
	Runner Runner
	Host   Host
}

// Join quotes argv for the remote shell that ssh invokes.
func Join(args []string) string {
	var b strings.Builder
	for i, a := range args {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteByte('\'')
		b.WriteString(strings.ReplaceAll(a, "'", `'\''`))
		b.WriteByte('\'')
	}
	return b.String()
}

// Run executes argv locally, or through `ssh` with batch mode, no tty and no
// stdin so a remote prompt can never block the daemon.
func (e Exec) Run(ctx context.Context, args ...string) ([]byte, error) {
	if e.Host.Local() {
		return e.Runner.Run(ctx, args[0], args[1:], "", "")
	}
	return e.Runner.Run(ctx, "ssh", []string{"-o", "BatchMode=yes", "-T", "-n", e.Host.SSH, Join(args)}, "", "")
}

func (e Exec) Close(ctx context.Context, pane string) error {
	_, err := e.Run(ctx, "herdr", "pane", "close", pane)
	return err
}

// Panes decodes the `herdr agent list` JSON envelope.
func (e Exec) Panes(ctx context.Context) ([]Pane, error) {
	b, err := e.Run(ctx, "herdr", "agent", "list")
	if err != nil {
		return nil, err
	}
	var v struct {
		Result struct {
			Agents []Pane `json:"agents"`
		} `json:"result"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("herdr agent list: %w", err)
	}
	if v.Result.Agents == nil {
		return nil, errors.New("herdr agent list: missing agents")
	}
	return v.Result.Agents, nil
}

// AllPanes decodes `herdr pane list`, which also reports plain shell and job
// panes without an agent. Only pane_id and cwd are needed for usage checks.
func (e Exec) AllPanes(ctx context.Context) ([]Pane, error) {
	b, err := e.Run(ctx, "herdr", "pane", "list")
	if err != nil {
		return nil, err
	}
	var v struct {
		Result struct {
			Panes []Pane `json:"panes"`
		} `json:"result"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("herdr pane list: %w", err)
	}
	if v.Result.Panes == nil {
		return nil, errors.New("herdr pane list: missing panes")
	}
	return v.Result.Panes, nil
}

// Exists reports whether path is a regular file, distinguishing a missing file
// from an execution failure.
func (e Exec) Exists(ctx context.Context, path string) (bool, error) {
	b, err := e.Run(ctx, "sh", "-c", `test -f "$1" && echo yes || echo no`, "sh", path)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "yes", nil
}

// Capture returns the report/log path that proves the pane's output survives
// the close: an existing <dir>/report.md, or a freshly saved
// <dir>/pane-screen.txt holding the pane's last screen.
func (e Exec) Capture(ctx context.Context, pane, dir string) (string, error) {
	report := filepath.Join(dir, "report.md")
	ok, err := e.Exists(ctx, report)
	if err != nil {
		return "", err
	}
	if ok {
		return report, nil
	}
	screen := filepath.Join(dir, "pane-screen.txt")
	_, err = e.Run(ctx, "sh", "-c", `mkdir -p -- "$1" && herdr pane read "$2" > "$1/pane-screen.txt"`, "sh", dir, pane)
	if err != nil {
		return "", err
	}
	return screen, nil
}

// Pushed reports whether every commit on branch is reachable from the default
// branch (remote-tracking first, then local) or fully pushed to an upstream
// remote ref. Unknown evidence is not pushed: callers skip and report.
func (e Exec) Pushed(ctx context.Context, main, branch, base string) bool {
	for _, ref := range []string{"origin/" + base, base} {
		if _, err := e.Run(ctx, "git", "-C", main, "merge-base", "--is-ancestor", branch, ref); err == nil {
			return true
		}
	}
	for _, ref := range []string{branch + "@{upstream}", "origin/" + branch} {
		out, err := e.Run(ctx, "git", "-C", main, "rev-list", "--count", "--left-right", branch+"..."+ref)
		if err == nil && strings.TrimSpace(strings.SplitN(string(out), "\t", 2)[0]) == "0" {
			return true
		}
	}
	return false
}

// Inspect resolves a pane cwd to a linked git worktree. It returns (nil, nil)
// when the path is not a worktree — an ordinary directory, a bare repo, a
// submodule, or a clone's canonical checkout.
func (e Exec) Inspect(ctx context.Context, cwd string) (*Worktree, error) {
	b, err := e.Run(ctx, "git", "-C", cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")
	if err != nil {
		return nil, nil
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		return nil, fmt.Errorf("unexpected rev-parse output for %s", cwd)
	}
	gitDir, common, top := lines[0], lines[1], lines[2]
	if !filepath.IsAbs(common) {
		common = filepath.Join(top, common)
	}
	// A linked worktree keeps its git dir under <main>/.git/worktrees; the
	// canonical checkout's git dir is the common dir itself.
	if !strings.Contains(filepath.ToSlash(gitDir), "/.git/worktrees/") {
		return nil, nil
	}
	w := &Worktree{Path: top, Common: common}
	if b, err = e.Run(ctx, "git", "-C", cwd, "branch", "--show-current"); err == nil {
		w.Branch = strings.TrimSpace(string(b))
	}
	if b, err = e.Run(ctx, "git", "-C", cwd, "remote", "get-url", "origin"); err == nil {
		w.Repo = RepoFromRemote(string(b))
	}
	return w, nil
}

// Entry is one `git worktree list --porcelain` record.
type Entry struct {
	Path     string
	Branch   string // short branch name; empty when detached or bare
	Detached bool
	Bare     bool
	Locked   bool
}

// Entries parses `git worktree list --porcelain`.
func Entries(b []byte) ([]Entry, error) {
	var out []Entry
	var cur *Entry
	flush := func() {
		if cur != nil {
			out = append(out, *cur)
			cur = nil
		}
	}
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			flush()
			cur = &Entry{Path: strings.TrimPrefix(line, "worktree ")}
		case cur == nil:
			if strings.TrimSpace(line) != "" {
				return nil, errors.New("malformed worktree list")
			}
		case strings.HasPrefix(line, "branch "):
			cur.Branch = strings.TrimPrefix(strings.TrimPrefix(line, "branch "), "refs/heads/")
		case line == "detached":
			cur.Detached = true
		case line == "bare":
			cur.Bare = true
		case strings.HasPrefix(line, "locked"):
			cur.Locked = true
		}
	}
	flush()
	return out, nil
}

func (e Exec) WorktreeList(ctx context.Context, path string) ([]Entry, error) {
	b, err := e.Run(ctx, "git", "-C", path, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	return Entries(b)
}

// Clean reports whether the worktree has no modified or untracked files.
func (e Exec) Clean(ctx context.Context, path string) (bool, error) {
	b, err := e.Run(ctx, "git", "-C", path, "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "", nil
}

func (e Exec) RemoveWorktree(ctx context.Context, main, path string) error {
	_, err := e.Run(ctx, "git", "-C", main, "worktree", "remove", path)
	return err
}

func (e Exec) DeleteBranch(ctx context.Context, main, branch string) error {
	// -d, never -D: git still refuses a branch that is not merged into its
	// upstream or HEAD, so an unpushed or unmerged branch is retained.
	_, err := e.Run(ctx, "git", "-C", main, "branch", "-d", branch)
	return err
}

// RepoFromRemote normalizes an origin remote URL to owner/name. It returns ""
// for non-GitHub-style remotes: local paths, file:// URLs, or layouts that are
// not exactly two path segments.
func RepoFromRemote(raw string) string {
	u := strings.TrimSpace(raw)
	var path string
	if i := strings.Index(u, "://"); i >= 0 {
		v, err := url.Parse(u)
		if err != nil || v.Host == "" {
			return ""
		}
		path = v.Path
	} else if i := strings.Index(u, ":"); i >= 0 && !strings.Contains(u[:i], "/") {
		// scp-style remote: [user@]host:owner/name.git
		path = u[i+1:]
	} else {
		return ""
	}
	path = strings.TrimSuffix(strings.Trim(path, "/"), ".git")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	return parts[0] + "/" + parts[1]
}

// Protected reports whether a pane name matches the configured protect globs
// (slash-aware policy globs; the documented defaults are single-segment).
func Protected(name string, globs []string) bool {
	for _, g := range globs {
		if ok, _ := policy.Match(g, name); ok {
			return true
		}
	}
	return false
}
