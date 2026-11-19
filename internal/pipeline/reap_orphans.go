package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// errProcessScanUnavailable is returned when this host has no process
// working-directory scan (no /proc and no lsof). The sweep then removes nothing:
// it cannot prove that no process holds a checkout.
var errProcessScanUnavailable = errors.New("no process working-directory scan on this host")

// processScan reads this machine's processes; a variable so a test can stand in
// for the platform scan.
var processScan = scanProcesses

// procEvidence is what one scan read of this machine's processes: the working
// directories it could read, and every same-user process whose working
// directory it could not.
type procEvidence struct {
	cwds    []string
	unknown []procUnknown
}

// procUnknown is a same-user process that denies its working directory, or
// whose working directory the scan could not read. fds are the paths it has
// open, when those could be read; exe and comm are what the process let the
// scan read of its executable and its name. commTrusted marks a comm read
// from the kernel's process name under /proc, the name evidence the
// allow-list has always used; the lsof path's comm is the ps argv0 basename,
// which the process sets, so it is audit detail only and never satisfies the
// allow-list.
type procUnknown struct {
	pid         int
	comm        string
	exe         string
	fds         []string
	fdsKnown    bool
	commTrusted bool
}

// heldPaths resolves one scan into the paths a process may hold — every readable
// working directory, plus the open files of a process that denies its cwd — and
// the processes it cannot rule out, whose checkout is unknown.
func (ev procEvidence) heldPaths() (held []string, unresolved []procUnknown) {
	held = append(held, ev.cwds...)
	for _, p := range ev.unknown {
		if p.fdsKnown {
			held = append(held, p.fds...)
			continue
		}
		if procNoWorkDir(filepath.Base(p.exe)) || (p.exe == "" && p.commTrusted && procNoWorkDir(p.comm)) {
			continue
		}
		unresolved = append(unresolved, p)
	}
	return held, unresolved
}

// procNoWorkDir names the programs that never enter a work directory, so a
// same-user process whose working directory and open files both deny this user
// cannot hold a checkout: an sshd session, and systemd's PAM helper. It decides
// only when nothing stronger does: a process whose open files could be read is
// judged by those alone.
func procNoWorkDir(name string) bool {
	switch name {
	case "sshd", "sshd-session", "sd-pam", "(sd-pam)":
		return true
	}
	return false
}

// scanProcesses reads every process's working directory: from /proc where it
// exists (Linux), otherwise from lsof and ps (macOS). A process that exited,
// or belongs to another user and cannot be read under /proc, holds no checkout
// of this user's. A same-user process that denies its working directory is
// recorded with what it still allows; lsof lists only what it can read, so the
// ps pid list makes a missing cwd unknown instead of invisible. Any scan
// failure is errProcessScanUnavailable.
func scanProcesses(ctx context.Context, run integrations.Runner) (procEvidence, error) {
	if _, e := os.Stat("/proc/self/cwd"); e == nil {
		return procScanEvidence()
	}
	return scanProcessesLsof(ctx, run)
}

// scanProcessesLsof reads the working directories lsof can name, then lists
// this user's effective-uid processes with ps so a pid with no cwd record
// fails closed.
func scanProcessesLsof(ctx context.Context, run integrations.Runner) (procEvidence, error) {
	b, e := run.Run(ctx, "lsof", []string{"-a", "-d", "cwd", "-F", "pn"}, "", "")
	if e != nil {
		return procEvidence{}, fmt.Errorf("%w: lsof: %v", errProcessScanUnavailable, e)
	}
	uid := strconv.Itoa(os.Getuid())
	p, e := run.Run(ctx, "ps", []string{"-A", "-o", "pid=,uid=,state=,command="}, "", "")
	if e != nil {
		return procEvidence{}, fmt.Errorf("%w: ps: %v", errProcessScanUnavailable, e)
	}
	procs, e := parsePsProcesses(p, uid)
	if e != nil {
		return procEvidence{}, fmt.Errorf("%w: ps: %v", errProcessScanUnavailable, e)
	}
	scan := parseLsofCwdPids(b)
	ev := procEvidence{cwds: scan.cwds}
	for _, proc := range procs {
		if !scan.pids[proc.pid] {
			ev.unknown = append(ev.unknown, procUnknown{pid: proc.pid, comm: proc.comm})
		}
	}
	return ev, nil
}

// procScanEvidence reads the working directories under /proc.
func procScanEvidence() (procEvidence, error) {
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return procEvidence{}, fmt.Errorf("%w: %v", errProcessScanUnavailable, e)
	}
	var ev procEvidence
	for _, ent := range entries {
		pid, e := strconv.Atoi(ent.Name())
		if e != nil {
			continue
		}
		dir := filepath.Join("/proc", ent.Name())
		cwd, e := os.Readlink(filepath.Join(dir, "cwd"))
		switch {
		case e == nil:
			ev.cwds = append(ev.cwds, cwd)
		case errors.Is(e, fs.ErrNotExist):
		case !sameUserProcess(dir):
			// Another user's process: it cannot be running in this user's checkout.
		default:
			// A same-user process that denies its cwd may be in a checkout. A
			// non-dumpable process denies cwd, exe, fd and environ together, yet
			// keeps comm, stat and cmdline readable: read what it still allows.
			ev.unknown = append(ev.unknown, unknownProcess(dir, pid))
		}
	}
	return ev, nil
}

// unknownProcess reads what a same-user process that denies its working
// directory still lets this user read.
func unknownProcess(dir string, pid int) procUnknown {
	p := procUnknown{pid: pid, comm: procCommName(dir), commTrusted: true}
	if exe, e := os.Readlink(filepath.Join(dir, "exe")); e == nil {
		p.exe = exe
	}
	ents, e := os.ReadDir(filepath.Join(dir, "fd"))
	if e != nil {
		return p
	}
	p.fdsKnown = true
	for _, ent := range ents {
		target, e := os.Readlink(filepath.Join(dir, "fd", ent.Name()))
		switch {
		case e == nil:
			p.fds = append(p.fds, target)
		case errors.Is(e, fs.ErrNotExist):
			// Closed while scanning.
		default:
			// One hidden target proves nothing about the descriptors beside it.
			p.fds, p.fdsKnown = nil, false
			return p
		}
	}
	return p
}

// procCommName reads a process's name from /proc/<pid>/comm, or from the
// parenthesised name in /proc/<pid>/stat when that file does not read. It is
// empty when neither does.
func procCommName(dir string) string {
	if b, e := os.ReadFile(filepath.Join(dir, "comm")); e == nil {
		return strings.TrimSpace(string(b))
	}
	b, e := os.ReadFile(filepath.Join(dir, "stat"))
	if e != nil {
		return ""
	}
	op, cl := strings.IndexByte(string(b), '('), strings.LastIndexByte(string(b), ')')
	if op < 0 || cl <= op {
		return ""
	}
	return string(b[op+1 : cl])
}

// sameUserProcess reports whether the /proc entry at dir belongs to this user.
// It is true when the owner cannot be read, so an unknown process fails closed.
func sameUserProcess(dir string) bool {
	fi, e := os.Stat(dir)
	if e != nil {
		return true
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return !ok || int(st.Uid) == os.Getuid()
}

// lsofCwdScan is the readable cwd set from `lsof -F pn`: the paths, and the
// pids whose cwd lsof named.
type lsofCwdScan struct {
	cwds []string
	pids map[int]bool
}

// psProcess is one same-user process from `ps -o pid=,uid=,state=,command=`.
type psProcess struct {
	pid  int
	comm string
}

// parseLsofCwdPids reads the process and cwd name fields of `lsof -F pn`
// output. Other fields, including the file-descriptor field, are ignored. Only
// an absolute path counts as a readable cwd: Darwin lsof can emit an error
// string such as `cwd|rtd info error: ...` for a cwd it could not resolve.
func parseLsofCwdPids(out []byte) lsofCwdScan {
	scan := lsofCwdScan{pids: map[int]bool{}}
	pid := 0
	for _, line := range strings.Split(string(out), "\n") {
		switch {
		case strings.HasPrefix(line, "p"):
			pid, _ = strconv.Atoi(line[1:])
		case strings.HasPrefix(line, "n") && pid > 0 && len(line) > 1:
			if cwd := line[1:]; strings.HasPrefix(cwd, "/") {
				scan.cwds = append(scan.cwds, cwd)
				scan.pids[pid] = true
			}
		}
	}
	return scan
}

// parsePsProcesses reads `ps -A -o pid=,uid=,state=,command=` output and keeps
// the processes whose effective uid is uid. The command column supplies comm
// as the argv0 basename; ucomm is not used because ps does not escape spaces
// in it. A zombie has no cwd and cannot hold a checkout, so state Z is skipped.
// ps lists its own scanner process, which started after lsof ran; excluding
// that exact command keeps it from reading as a missing cwd. Any other
// malformed line makes the listing unreliable, so the scan fails closed.
func parsePsProcesses(out []byte, uid string) ([]psProcess, error) {
	const selfArgs = "-A -o pid=,uid=,state=,command="
	var procs []psProcess
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, e := strconv.Atoi(fields[0])
		if e != nil {
			return nil, fmt.Errorf("malformed pid %q", fields[0])
		}
		if len(fields) < 2 {
			return nil, fmt.Errorf("malformed process line %q", line)
		}
		if fields[1] != uid {
			continue
		}
		if len(fields) < 3 {
			return nil, fmt.Errorf("malformed process line %q", line)
		}
		if strings.HasPrefix(fields[2], "Z") {
			continue
		}
		if len(fields) > 6 && filepath.Base(fields[3]) == "ps" && strings.Join(fields[4:], " ") == selfArgs {
			continue
		}
		var comm string
		if len(fields) > 3 {
			comm = filepath.Base(fields[3])
		}
		procs = append(procs, psProcess{pid: pid, comm: comm})
	}
	if len(procs) == 0 {
		return nil, errors.New("no same-user processes")
	}
	return procs, nil
}

// anyWithin reports whether any path is dir or lies under it.
func anyWithin(paths []string, dir string) bool {
	dir = filepath.Clean(dir)
	for _, p := range paths {
		p = filepath.Clean(p)
		if p == dir || strings.HasPrefix(p, dir+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// The orphan sweep covers review checkouts the reviewer sweep cannot reach. The
// reviewer sweep learns a label from a pane, a receipt or a job record; a
// directory under review.panewire.checkouts that no record of any repo names
// has none of those, so it stays on disk with no reap row at all. A record is
// missing after a spawn that died before its record was written, or a restart
// mid-spawn.
//
// A directory is removed only when every guard passes: it is older than the
// reviewer stuck bound (review.panewire.stuck_after); no reviewer-job record
// names it; it is a detached linked worktree of this repo; it is clean and has
// no ignored file outside the regenerable list; no pane on this host runs in it;
// and no process has its working directory inside it. Removal is
// `git worktree remove` from the owning repo, never forced. A directory that is
// not a git worktree is kept and reported once. Shadow mode only plans.

const (
	orphanAction      = "reviewer-cleanup"
	orphanRemoved     = "removed"
	orphanPlanned     = "would-remove"
	orphanKept        = "kept"
	orphanSkipped     = "skipped"
	orphanReason      = "orphan-checkout"
	orphanNotWorktree = "not-a-git-worktree"
	// orphanScanUnavailable is the reason token of a row saying the pass could
	// not finish its process scan, so nothing was removed.
	orphanScanUnavailable = "process-scan-unavailable"
)

// recordedCheckouts names every checkout a reviewer-job record of any repo
// holds: its checkout field, and the path its label gives under dir, which is
// where a spawn places a local checkout whether or not the record names it. A
// spec-placed reviewer's record names no checkout (its worktree lives on the
// node), and its label still names that path, so a directory left there is not
// swept as an orphan. A record that cannot be read may name any directory, so
// the caller stops the sweep on its error.
func (s *Service) recordedCheckouts(dir string) (map[string]bool, error) {
	paths, e := recordFiles(s.Config.StateDir)
	if e != nil {
		return nil, e
	}
	named := map[string]bool{}
	for _, path := range paths {
		var rj reviewerJob
		if _, e := fsutil.ReadJSON(path, &rj); e != nil {
			return nil, fmt.Errorf("reviewer job record %s: %w", path, e)
		}
		if rj.Checkout != "" {
			named[filepath.Clean(rj.Checkout)] = true
		}
		if rj.Label != "" {
			named[filepath.Join(dir, rj.Label)] = true
		}
	}
	return named, nil
}

// recordFiles lists the reviewer-job record files under stateDir, at
// <name>/reviewer-jobs and <owner>/<repo>/reviewer-jobs. filepath.Glob cannot
// tell an unreadable directory from an empty one, so each is read here: only a
// missing directory is empty, and any other read failure is returned.
func recordFiles(stateDir string) ([]string, error) {
	var out []string
	add := func(dir string) error {
		ents, e := os.ReadDir(dir)
		if errors.Is(e, fs.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		for _, ent := range ents {
			if !ent.IsDir() && strings.HasSuffix(ent.Name(), ".json") {
				out = append(out, filepath.Join(dir, ent.Name()))
			}
		}
		return nil
	}
	top, e := os.ReadDir(stateDir)
	if errors.Is(e, fs.ErrNotExist) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	for _, t := range top {
		if !t.IsDir() {
			continue
		}
		base := filepath.Join(stateDir, t.Name())
		if e := add(filepath.Join(base, "reviewer-jobs")); e != nil {
			return nil, e
		}
		sub, e := os.ReadDir(base)
		if e != nil {
			return nil, e
		}
		for _, u := range sub {
			if u.IsDir() {
				if e := add(filepath.Join(base, u.Name(), "reviewer-jobs")); e != nil {
					return nil, e
				}
			}
		}
	}
	return out, nil
}

// orphanLocalHost is the configured local host, or this machine's own panes
// when none is configured, so the in-use check always reads real panes.
func (s *Service) orphanLocalHost(ctx context.Context, hosts []*reapHost) *reapHost {
	for _, hs := range hosts {
		if hs.host.Local() {
			return hs
		}
	}
	ex := reap.Exec{Runner: reapRunner(ctx, s.Runner), Host: reap.Host{Name: "local"}}
	hs := &reapHost{host: ex.Host, exec: ex, ops: ex, trees: map[string]*reap.Worktree{}}
	if e := hs.refreshPanes(ctx); e != nil {
		hs.usageUnknown = true
	}
	return hs
}

// reapOrphanCheckouts sweeps review.panewire.checkouts for directories no
// reviewer-job record names. It runs on each whole-repo reap pass; a directory
// is removed by the pass of the repo that owns its worktree. Rows go through
// note, which writes the reap log and the reviewer audit record.
func (s *Service) reapOrphanCheckouts(ctx context.Context, r config.Repo, receipts []reapReceipt, hosts []*reapHost, acting bool, note func(reapReceipt)) []error {
	dir := config.Expand(s.Config.Review.Panewire.Checkouts)
	if dir == "" {
		return nil
	}
	entries, e := os.ReadDir(dir)
	if errors.Is(e, fs.ErrNotExist) {
		return nil
	}
	if e != nil {
		return []error{fmt.Errorf("orphan checkouts: %w", e)}
	}
	stuckAfter, e := reviewerStuckAfter(s.Config)
	if e != nil {
		return []error{fmt.Errorf("orphan checkouts: %w", e)}
	}
	named, e := s.recordedCheckouts(dir)
	if e != nil {
		return []error{fmt.Errorf("orphan checkouts: %w", e)}
	}
	hs := s.orphanLocalHost(ctx, hosts)
	if hs.failed || hs.usageUnknown {
		return []error{errors.New("orphan checkouts: local pane scan failed; no checkout removed")}
	}
	held, unresolved, e := s.orphanHeldPaths(ctx)
	if e != nil && !errors.Is(e, errProcessScanUnavailable) {
		return []error{fmt.Errorf("orphan checkouts: %w", e)}
	}
	// Fail closed without a pass error: this host cannot prove that no process
	// holds a checkout, so nothing is removed. The day's row keeps the blocked
	// sweep visible, so a host that can never finish the scan is not silently
	// idle (hk 4792). One row a day, not one a pass: the condition persists.
	noted := false
	noteScan := func(e error, unresolved []procUnknown) {
		if !noted && s.orphanScanRowDue(r, receipts) {
			note(orphanRow(r, hs, "", orphanSkipped, orphanScanUnavailable+": "+orphanScanDetail(e, unresolved)))
		}
		noted = true
	}
	if e != nil || len(unresolved) > 0 {
		noteScan(e, unresolved)
		return nil
	}
	now := s.now()
	var errs []error
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		path := filepath.Join(dir, ent.Name())
		if named[path] {
			continue
		}
		info, e := os.Lstat(path)
		if e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: %w", path, e))
			continue
		}
		if now.Sub(info.ModTime()) < stuckAfter {
			continue
		}
		tree, e := hs.exec.Inspect(ctx, path)
		if e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: %w", path, e))
			continue
		}
		if tree == nil {
			// One repo's pass reports a non-worktree: the directory belongs to no
			// repo, so every repo's log would otherwise carry the same row.
			if s.reportsOrphanDirRows(r) {
				note(orphanRow(r, hs, path, orphanKept, orphanNotWorktree))
			}
			continue
		}
		// Another repo's worktree, a branch checkout, or a directory nested in a
		// worktree is not this pass's to remove.
		if filepath.Clean(tree.Path) != path || !strings.EqualFold(tree.Repo, r.Name) || tree.Branch != "" {
			continue
		}
		// Inspect reads the branch without failing on error, so a failed read
		// leaves Branch empty. Detached needs positive evidence: HEAD resolves to
		// itself, not to a branch ref.
		head, e := hs.exec.Run(ctx, "git", "-C", path, "rev-parse", "--symbolic-full-name", "HEAD")
		if e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: head: %w", path, e))
			continue
		}
		if strings.TrimSpace(string(head)) != "HEAD" {
			continue
		}
		clean, e := hs.exec.Clean(ctx, path)
		if e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: status: %w", path, e))
			continue
		}
		if !clean {
			continue
		}
		ignored, e := hs.exec.Ignored(ctx, path)
		if e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: ignored: %w", path, e))
			continue
		}
		unsafe := orphanHasUnsafeIgnored(s.Config.ReapRegenerable(r), ignored)
		if unsafe {
			continue
		}
		if e := hs.refreshPanes(ctx); e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: pane list: %w", path, e))
			continue
		}
		if hs.paneInUse(path) {
			continue
		}
		if anyWithin(held, path) {
			continue
		}
		if !acting {
			note(orphanRow(r, hs, path, orphanPlanned, orphanReason))
			continue
		}
		// The scan above is a cheap pre-filter: a process can start in the
		// checkout during this pass's slow per-candidate checks. Scan again right
		// before the removal, and keep the checkout if that scan proves nothing.
		fresh, freshUnresolved, e := s.orphanHeldPaths(ctx)
		if e != nil || len(freshUnresolved) > 0 {
			noteScan(e, freshUnresolved)
			continue
		}
		if anyWithin(fresh, path) {
			continue
		}
		if e := hs.exec.RemoveWorktree(ctx, filepath.Dir(tree.Common), path); e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: worktree remove: %w", path, e))
			continue
		}
		note(orphanRow(r, hs, path, orphanRemoved, orphanReason))
	}
	return errs
}

// orphanHeldPaths runs the process scan and resolves it: the paths a process
// may hold, and the same-user processes it cannot rule out.
func (s *Service) orphanHeldPaths(ctx context.Context) (held []string, unresolved []procUnknown, err error) {
	ev, e := processScan(ctx, s.Runner)
	if e != nil {
		return nil, nil, e
	}
	held, unresolved = ev.heldPaths()
	return held, unresolved, nil
}

// orphanScanDetail names the process that blocked the sweep, or the scan
// failure itself, so the row carries the pid and the process name.
func orphanScanDetail(e error, unresolved []procUnknown) string {
	if len(unresolved) == 0 {
		if e != nil {
			return e.Error()
		}
		return "unknown process"
	}
	p := unresolved[0]
	detail := fmt.Sprintf("pid %d", p.pid)
	if p.comm != "" {
		detail += " (" + p.comm + ")"
	}
	if n := len(unresolved) - 1; n > 0 {
		detail += fmt.Sprintf(" and %d more", n)
	}
	return detail
}

// dailyScanRow reports whether v is the day's process-scan-unavailable row. It
// is never compared against the stream's previous row: the day gate above
// already makes it one a day, and its reason can stay identical for days while
// the same process keeps blocking the sweep.
func dailyScanRow(v reapReceipt) bool {
	return v.Action == orphanAction && strings.HasPrefix(v.Reason, orphanScanUnavailable)
}

// orphanScanRowDue reports whether this pass writes the day's
// process-scan-unavailable row: only the pass that writes rows about the
// checkouts directory itself, and only once per UTC day, so a host that can
// never finish the scan says so once a day instead of on every pass.
func (s *Service) orphanScanRowDue(r config.Repo, receipts []reapReceipt) bool {
	if !s.reportsOrphanDirRows(r) {
		return false
	}
	today := s.now().UTC().Format(time.DateOnly)
	for _, v := range receipts {
		if v.Action == orphanAction && strings.HasPrefix(v.Reason, orphanScanUnavailable) && v.Time.UTC().Format(time.DateOnly) == today {
			return false
		}
	}
	return true
}

// reportsOrphanDirRows reports whether r's pass writes the rows about the
// checkouts directory itself — a non-worktree, or a process scan that cannot
// finish: the alphabetically first configured repo, so one log holds each.
func (s *Service) reportsOrphanDirRows(r config.Repo) bool {
	first := r.Name
	for _, c := range s.Config.Repos {
		if c.Name < first {
			first = c.Name
		}
	}
	return r.Name == first
}

// orphanRow starts a reviewer-cleanup row for a checkout no PR names.
func orphanRow(r config.Repo, hs *reapHost, path, result, reason string) reapReceipt {
	return reapReceipt{Action: orphanAction, Repo: r.Name, Host: hs.host.Name, Cwd: path, Worktree: path, Result: result, Reason: reason}
}

// orphanHasUnsafeIgnored reports whether an ignored file outside the
// regenerable list would be lost with the worktree.
func orphanHasUnsafeIgnored(globs []string, ignored []string) bool {
	for _, f := range ignored {
		if !reap.Regenerable(globs, f) {
			return true
		}
	}
	return false
}
