// Package reap discovers herdr agent panes inside git worktrees and performs
// the local, ssh or panewire-hub command boundary for merged-PR session and
// worktree cleanup.
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
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/policy"
)

// Runner is satisfied by integrations.Exec without this package importing it.
type Runner interface {
	Run(context.Context, string, []string, string, string) ([]byte, error)
}

type mutationParentContextKey struct{}

type mutationParent struct {
	ctx     context.Context
	timeout time.Duration
}

const mutationTimeout = time.Minute

// WithMutationParent gives guarded mutations a bounded context independent of
// the repository budget, while retaining daemon shutdown cancellation.
func WithMutationParent(ctx, parent context.Context, timeout ...time.Duration) context.Context {
	limit := mutationTimeout
	if len(timeout) > 0 && timeout[0] > 0 {
		limit = timeout[0]
	}
	return context.WithValue(ctx, mutationParentContextKey{}, mutationParent{ctx: parent, timeout: limit})
}

func mutationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if parent, ok := ctx.Value(mutationParentContextKey{}).(mutationParent); ok && parent.ctx != nil {
		return context.WithTimeout(parent.ctx, parent.timeout)
	}
	return ctx, func() {}
}

// MutationContext returns a bounded context for one guarded mutation. Its
// deadline is independent of the repository budget, but daemon cancellation
// still propagates.
func MutationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return mutationContext(ctx)
}

// Host is one configured session host. An empty or "local" SSH value with no
// Machine means the commands run directly on this machine; otherwise the host
// is remote, reached by ssh(1) to SSH (a destination such as user@host) or,
// under the panewire transport, through the hub as panewire machine Machine
// (default Name). Jobs is the base directory holding one job directory per
// named pane; it must be absolute on ssh hosts and is unused through the hub,
// where the node owns its jobs directory.
type Host struct {
	Name    string `yaml:"name" json:"name"`
	SSH     string `yaml:"ssh" json:"ssh"`
	Machine string `yaml:"machine" json:"machine,omitempty"`
	Jobs    string `yaml:"jobs" json:"jobs"`
	// WorkRoot is the node's work root: the directory panewire prepares a
	// spec-placed job's checkout in, as <work_root>/<label>-checkout. Empty
	// means the sweep cannot build that path on this host.
	WorkRoot string `yaml:"work_root" json:"work_root,omitempty"`
}

func (h Host) Local() bool { return (h.SSH == "" || h.SSH == "local") && h.Machine == "" }

// MachineID is the panewire machine a remote host is reached as.
func (h Host) MachineID() string {
	if h.Machine != "" {
		return h.Machine
	}
	return h.Name
}

// Ops is one host's pane surface: discovery, worktree inspection, output
// capture and the guarded close. Exec implements it locally or over ssh,
// HubHost through the panewire hub.
type Ops interface {
	Panes(context.Context) ([]Pane, error)
	AllPanes(context.Context) ([]Pane, error)
	Inspect(context.Context, string) (*Worktree, error)
	Capture(context.Context, Pane, string, *time.Time, bool) (Capture, error)
	Composer(context.Context, Pane) (string, error)
	CloseExact(context.Context, Pane) (string, error)
	JobDone(context.Context, string, string) error
}

// ErrUnsupported is an operation the host's transport cannot perform yet.
var ErrUnsupported = errors.New("not supported on this host's transport")

// Capture and close outcomes. The skip values are receipt reasons verbatim.
const (
	CaptureNotAJob     = "not-a-job"
	CapturePaneGone    = "pane-gone"
	CapturePaneChanged = "pane-changed"
	CloseClosed        = "closed"
)

// Capture is where a pane's output was (or would be) saved before a close,
// or the reason the pane must be skipped instead.
type Capture struct {
	Saved string
	Skip  string
}

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
// Focused is herdr's own flag for the pane the operator has in front of them.
type Pane struct {
	ID      string `json:"pane_id"`
	Name    string `json:"name"`
	Status  string `json:"agent_status"`
	Agent   string `json:"agent"`
	Cwd     string `json:"cwd"`
	Focused bool   `json:"focused"`
}

// Worktree describes a pane cwd resolved to its linked worktree.
type Worktree struct {
	Path   string // worktree toplevel
	Branch string // checked-out branch; empty on a detached HEAD
	Head   string // checked-out commit; branches can be reused, heads cannot
	Repo   string // owner/name of the origin remote; empty when unknown
	Common string // absolute git common dir, <main>/.git
}

// WorktreeRemoveResult is the node's worktree.remove verdict.
type WorktreeRemoveResult struct {
	Outcome     string
	Files       []string
	RetryAfterS int
}

// ListedWorktree is one row of the node's worktree.list: a worktree of one
// common git dir, with the node's own read of its status.
type ListedWorktree struct {
	Path     string
	Branch   string
	Head     string
	Detached bool
	Status   WorktreeStatus
}

// WorktreeStatus is the node's status read of a listed worktree. Dirty includes
// tracked changes and untracked entries; Error is set when the status could not
// be read (a missing worktree stays in the list with an error).
type WorktreeStatus struct {
	Dirty     bool
	Untracked int
	Ignored   int
	Error     string
}

// Clean reports whether the node read the status and found nothing to lose.
func (s WorktreeStatus) Clean() bool { return s.Error == "" && !s.Dirty && s.Untracked == 0 }

// WorktreePage is one page of worktree.list; NextCursor is empty on the last.
type WorktreePage struct {
	Worktrees  []ListedWorktree
	NextCursor string
	Total      int
}

// WorktreeLister reads one page of a common git dir's worktrees.
type WorktreeLister interface {
	WorktreeList(ctx context.Context, common, cursor string) (WorktreePage, error)
}

// maxListPages bounds a listing so a node that keeps answering a cursor cannot
// hold the sweep: 100 rows a page is far beyond any host's worktree count.
const maxListPages = 200

// ListWorktrees reads every page of common. A cursor that does not advance, or
// a listing that never ends, is an error, never a short list.
func ListWorktrees(ctx context.Context, l WorktreeLister, common string) ([]ListedWorktree, error) {
	var all []ListedWorktree
	cursor := ""
	for page := 0; page < maxListPages; page++ {
		p, err := l.WorktreeList(ctx, common, cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, p.Worktrees...)
		if p.NextCursor == "" {
			return all, nil
		}
		if p.NextCursor == cursor {
			return nil, errors.New("worktree.list cursor did not advance")
		}
		cursor = p.NextCursor
	}
	return nil, fmt.Errorf("worktree.list of %s did not end after %d pages", common, maxListPages)
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

// CloseExact re-reads the agent list immediately before closing and only
// proceeds when the pane is still there, unchanged: the discovery list is a
// stale snapshot, and a pane that went back to work since must survive.
func (e Exec) CloseExact(ctx context.Context, pane Pane) (string, error) {
	fresh, err := e.Panes(ctx)
	if err != nil {
		return "", err
	}
	var cur *Pane
	for i := range fresh {
		if fresh[i].ID == pane.ID {
			cur = &fresh[i]
		}
	}
	switch {
	case cur == nil:
		return CapturePaneGone, nil
	case cur.Status != pane.Status || cur.Cwd != pane.Cwd || cur.Name != pane.Name:
		return CapturePaneChanged, nil
	}
	if err := e.Close(ctx, pane.ID); err != nil {
		return "", err
	}
	return CloseClosed, nil
}

// Capture saves a pane's output into the job's directory before it can
// close. Only a job whose dir already exists counts as a fleet job: an
// operator pane merely cd'd into a merged worktree must never be closed.
// report.md counts only when it is newer than the merge — a stale file could
// belong to a reused job name — otherwise the last screen is saved, or only
// named when write is false.
func (e Exec) Capture(ctx context.Context, pane Pane, job string, mergedAt *time.Time, write bool) (Capture, error) {
	dir := e.Host.JobDir(job)
	isJob, err := e.DirExists(ctx, dir)
	if err != nil {
		return Capture{}, err
	}
	if !isJob {
		return Capture{Skip: CaptureNotAJob}, nil
	}
	report := filepath.Join(dir, "report.md")
	exists, err := e.Exists(ctx, report)
	fresh := false
	if err == nil && exists && mergedAt != nil {
		fresh, err = e.NewerThan(ctx, report, mergedAt.Unix())
	}
	switch {
	case err != nil:
		return Capture{}, err
	case fresh:
		return Capture{Saved: report}, nil
	case !write:
		return Capture{Saved: filepath.Join(dir, "pane-screen.txt")}, nil
	}
	saved, err := e.Screen(ctx, pane.ID, dir)
	if err != nil {
		return Capture{}, err
	}
	return Capture{Saved: saved}, nil
}

// Composer outcomes: whether a pane's input line holds text nobody sent.
const (
	ComposerEmpty   = "empty"
	ComposerText    = "composer-text"
	ComposerUnknown = "composer-unknown"
)

// Composer reads the pane's visible screen and classifies its input line.
func (e Exec) Composer(ctx context.Context, pane Pane) (string, error) {
	b, err := e.Run(ctx, "herdr", "pane", "read", pane.ID, "--source", "visible")
	if err != nil {
		return "", err
	}
	return ComposerState(pane.Agent, string(b)), nil
}

// ComposerState classifies the input line of a claude or codex screen. Claude
// draws its composer between the last two horizontal divider lines behind a
// ❯ prompt; codex prefixes its input with ›, followed by a blank line before
// its footer. Anything else, or a screen
// without that shape, is unknown — and unknown never reads as empty. A dim
// placeholder reads as text: that keeps a pane, never closes one.
func ComposerState(agent, screen string) string {
	lines := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	switch strings.ToLower(agent) {
	case "claude":
		var dividers []int
		for i, line := range lines {
			if strings.Contains(line, "─") && strings.Trim(line, " \t─") == "" {
				dividers = append(dividers, i)
			}
		}
		if len(dividers) < 2 {
			return ComposerUnknown
		}
		// Only the first line carries the prompt glyph; every other
		// character in the box is input.
		for i, line := range lines[dividers[len(dividers)-2]+1 : dividers[len(dividers)-1]] {
			line = strings.TrimSpace(line)
			if i == 0 {
				line = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "❯"), ">"))
			}
			if line != "" {
				return ComposerText
			}
		}
		return ComposerEmpty
	case "codex":
		// The input starts on the last › line and runs, wrapped or
		// multi-line, until the first blank line above the footer.
		for i := len(lines) - 1; i >= 0; i-- {
			rest, ok := strings.CutPrefix(strings.TrimSpace(lines[i]), "›")
			if !ok {
				continue
			}
			if strings.TrimSpace(rest) != "" {
				return ComposerText
			}
			for _, next := range lines[i+1:] {
				if strings.TrimSpace(next) == "" {
					break
				}
				return ComposerText
			}
			return ComposerEmpty
		}
	}
	return ComposerUnknown
}

// JobDone marks a job finished the way its lead would: `panewire done` on the
// job's host, with the report or screen reap just saved. panewire records the
// completed state in hk and treats a repeat with the same report as done.
func (e Exec) JobDone(ctx context.Context, job, report string) error {
	if !ValidJobName(job) {
		return errors.New("job name invalid")
	}
	args := []string{"panewire", "done", job}
	if report != "" {
		args = append(args, "--report", report)
	}
	_, err := e.Run(ctx, args...)
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

// DirExists reports whether path is an existing directory. The job dir is
// proof the fleet spawned the pane, so it must predate the close: Screen
// never creates it.
func (e Exec) DirExists(ctx context.Context, path string) (bool, error) {
	b, err := e.Run(ctx, "sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", path)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "yes", nil
}

// terminalJobEvents are the events that end a job on its node.
var terminalJobEvents = map[string]bool{"job.revoked": true, "job.reaped": true, "job.completed": true, "job.lost": true}

// IsTerminalJobEvent reports whether kind is an event that ends a job.
func IsTerminalJobEvent(kind string) bool { return terminalJobEvents[kind] }

// TerminalEvent names the terminal event of a job's directory on this host:
// the newest events/NNNNN-<kind>.json whose kind ends the job, "" when the
// directory holds none. It only reads; an unreadable directory is an error.
func (e Exec) TerminalEvent(ctx context.Context, job string) (string, error) {
	dir := e.Host.JobDir(job)
	if dir == "" {
		return "", errors.New("job dir unavailable")
	}
	b, err := e.Run(ctx, "sh", "-c", `ls -1 "$1/events"`, "sh", dir)
	if err != nil {
		return "", err
	}
	return newestTerminalEvent(string(b)), nil
}

// newestTerminalEvent picks the kind of the newest terminal event in an events
// directory listing: one NNNNN-<kind>.json name per line, the sequence padded to
// five digits, so the names order by sequence as strings.
func newestTerminalEvent(listing string) string {
	var best, kind string
	for _, name := range strings.Fields(listing) {
		seq, rest, ok := strings.Cut(name, "-")
		if !ok || len(seq) != 5 || strings.Trim(seq, "0123456789") != "" {
			continue
		}
		k, ok := strings.CutSuffix(rest, ".json")
		if !ok || !terminalJobEvents[k] || seq <= best {
			continue
		}
		best, kind = seq, k
	}
	return kind
}

// NewerThan reports whether path's mtime is later than epoch seconds. GNU
// stat is tried first so a BSD -f flag can never be parsed as GNU's
// --file-system mode; either failure is reported, not guessed.
func (e Exec) NewerThan(ctx context.Context, path string, epoch int64) (bool, error) {
	b, err := e.Run(ctx, "sh", "-c", `m=$(stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null) && test "$m" -gt "$2" 2>/dev/null && echo yes || echo no`, "sh", path, strconv.FormatInt(epoch, 10))
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "yes", nil
}

// Screen saves the pane's last screen to <dir>/pane-screen.txt and returns
// the path. dir must already exist; callers check DirExists first and skip
// missing dirs as not-a-job.
func (e Exec) Screen(ctx context.Context, pane, dir string) (string, error) {
	screen := filepath.Join(dir, "pane-screen.txt")
	_, err := e.Run(ctx, "sh", "-c", `herdr pane read "$2" > "$1/pane-screen.txt"`, "sh", dir, pane)
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
// only when the path is proven not to be one: git ran and refused it (exit
// 128, "not a git repository" or a missing directory), or it resolved to an
// ordinary directory, a bare repo, a submodule, or a clone's canonical
// checkout. Any other failure — the command could not run, ssh exit 255, a
// timeout, a cancelled context — is an error: an unknown state is never gone.
func (e Exec) Inspect(ctx context.Context, cwd string) (*Worktree, error) {
	b, err := e.Run(ctx, "git", "-C", cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")
	if err != nil {
		if ctx.Err() == nil && gitRefused(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("inspect %s: %w", cwd, err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		return nil, fmt.Errorf("unexpected rev-parse output for %s", cwd)
	}
	gitDir, common, top := lines[0], lines[1], lines[2]
	if !filepath.IsAbs(common) {
		// git prints a relative common dir against the directory it ran in.
		common = filepath.Join(cwd, common)
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
	if b, err = e.Run(ctx, "git", "-C", cwd, "rev-parse", "HEAD"); err == nil {
		w.Head = strings.TrimSpace(string(b))
	}
	if b, err = e.Run(ctx, "git", "-C", cwd, "remote", "get-url", "origin"); err == nil {
		w.Repo = RepoFromRemote(string(b))
	}
	return w, nil
}

// gitRefused reports whether err is git itself exiting 128, its status for a
// fatal error such as "not a git repository" or a -C directory that does not
// exist. ssh passes a remote command's status through and reserves 255 for its
// own failures; a process killed by a signal has no status (-1). The status is
// used rather than git's stderr text, which is translated on localized hosts.
func gitRefused(err error) bool {
	var ex interface{ ExitCode() int }
	return errors.As(err, &ex) && ex.ExitCode() == 128
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

func (e Exec) WorktreeEntries(ctx context.Context, path string) ([]Entry, error) {
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

// TrackedClean reports whether the worktree has no modified tracked file.
// Untracked entries are judged separately, because a disposable one is
// allowed to go with the worktree.
func (e Exec) TrackedClean(ctx context.Context, path string) (bool, error) {
	b, err := e.Run(ctx, "git", "-C", path, "status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == "", nil
}

// Ignored lists gitignored files inside the worktree. `git status --porcelain`
// hides them, but `git worktree remove` deletes them anyway — callers refuse
// the removal rather than silently lose an .env or notes file. `-z` keeps
// names unquoted and NUL-separated, the same listing the node checks, so both
// sides see the same entries. A wholly ignored directory arrives as one
// collapsed `dir/` entry.
func (e Exec) Ignored(ctx context.Context, path string) ([]string, error) {
	b, err := e.Run(ctx, "git", "-C", path, "status", "--porcelain", "--ignored", "-z")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, f := range strings.Split(string(b), "\x00") {
		if strings.HasPrefix(f, "!! ") {
			out = append(out, f[3:])
		}
	}
	return out, nil
}

func (e Exec) RemoveWorktree(ctx context.Context, main, path string) error {
	mutationCtx, cancel := mutationContext(ctx)
	defer cancel()
	_, err := e.Run(mutationCtx, "git", "-C", main, "worktree", "remove", path)
	return err
}

func (e Exec) DeleteBranch(ctx context.Context, main, branch string) error {
	// -d, never -D: git still refuses a branch that is not merged into its
	// upstream or HEAD, so an unpushed or unmerged branch is retained.
	mutationCtx, cancel := mutationContext(ctx)
	defer cancel()
	_, err := e.Run(mutationCtx, "git", "-C", main, "branch", "-d", branch)
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

// Regenerable reports whether an ignored path inside a worktree matches the
// regenerable allowlist, using the node's rule (panewire
// `panewire@d2501b1 node_worktree.go nodeRegenerable`, documented under
// `worktree.remove` in docs/node-commands.md), so the precheck and the node
// refuse the same worktrees. The node reads each glob without its trailing
// `/` for the slash test: a glob with no other `/` (`name/`, `logs/`,
// `node_modules`) matches the entry's last segment at any depth, while a glob
// with an inner `/` (`a/b/`, `dotnet/*/bin/`) matches the whole path. A
// leading `/` is refused at load (ValidRegenerableGlob), so it never reaches
// this rule. A trailing-slash glob approves only a `dir/` entry, and a
// `dir/` entry needs a trailing-slash glob, so a slash-free `name/` approves
// a wholly ignored directory at any depth (`a/b/__pycache__/`) but never an
// ignored file of that name. A slash-free glob matches only the last segment
// of an ignored file, never a `dir/` entry: matching an ancestor would let a
// tracked bin/ or Logs/ dir smuggle an ignored file like bin/local.env past
// the refusal. Globs use path.Match, as the node does, so `*` never crosses a
// `/` and `**` is not a recursive wildcard.
func Regenerable(globs []string, entry string) bool {
	dirEntry := strings.HasSuffix(entry, "/")
	p := strings.Trim(entry, "/")
	base := p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		base = p[i+1:]
	}
	for _, raw := range globs {
		g := strings.Trim(raw, "/")
		if g == "" || dirEntry != strings.HasSuffix(raw, "/") {
			continue
		}
		match := base
		if strings.Contains(strings.TrimSuffix(raw, "/"), "/") {
			match = p
		}
		if ok, _ := path.Match(g, match); ok {
			return true
		}
	}
	return false
}

// globClass strips bracket character classes for the wildcard-only check.
var globClass = regexp.MustCompile(`\[[^\]]*\]`)

// ValidRegenerableGlob reports whether the node accepts a regenerable glob,
// mirroring its validNodeGlob rule: the pattern must be non-empty, relative,
// at most 128 bytes with no control bytes, must not be wildcard-only (after
// stripping character classes, trimming `*?/` leaves nothing), and may end
// in `/` as an explicit directory approval. Anything it refuses
// fails every `worktree.remove`, so the config refuses it at load.
func ValidRegenerableGlob(g string) error {
	if g == "" || strings.HasPrefix(g, "/") {
		return fmt.Errorf("invalid path glob %q", g)
	}
	if len(g) > 128 {
		return fmt.Errorf("invalid path glob %q", g)
	}
	for i := range len(g) {
		if g[i] < 0x20 || g[i] == 0x7f {
			return fmt.Errorf("invalid path glob %q", g)
		}
	}
	if strings.Trim(globClass.ReplaceAllString(g, ""), "*?/") == "" {
		return fmt.Errorf("invalid path glob %q", g)
	}
	if _, err := policy.Match(strings.TrimSuffix(g, "/"), "probe"); err != nil {
		return err
	}
	return nil
}
