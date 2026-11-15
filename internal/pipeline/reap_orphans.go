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

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// errProcessScanUnavailable is returned when this host has no process
// working-directory scan (no /proc and no lsof). The sweep then removes nothing:
// it cannot prove that no process holds a checkout.
var errProcessScanUnavailable = errors.New("no process working-directory scan on this host")

// processCwds lists the working directories of this machine's processes; a
// variable so a test can stand in for the platform scan.
var processCwds = listProcessCwds

// listProcessCwds reads every process's working directory: from /proc where it
// exists (Linux), otherwise from `lsof -a -d cwd -F n` (macOS). A process that
// exited, or belongs to another user and cannot be read under /proc, holds no
// checkout of this user's. Any scan failure is errProcessScanUnavailable.
func listProcessCwds(ctx context.Context, run integrations.Runner) ([]string, error) {
	if _, e := os.Stat("/proc/self/cwd"); e == nil {
		return procCwds()
	}
	b, e := run.Run(ctx, "lsof", []string{"-a", "-d", "cwd", "-F", "n"}, "", "")
	if e != nil {
		return nil, fmt.Errorf("%w: lsof: %v", errProcessScanUnavailable, e)
	}
	return parseLsofCwds(b), nil
}

// procCwds reads the working directories under /proc.
func procCwds() ([]string, error) {
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return nil, fmt.Errorf("%w: %v", errProcessScanUnavailable, e)
	}
	var out []string
	for _, ent := range entries {
		if _, e := strconv.Atoi(ent.Name()); e != nil {
			continue
		}
		cwd, e := os.Readlink(filepath.Join("/proc", ent.Name(), "cwd"))
		switch {
		case e == nil:
			out = append(out, cwd)
		case errors.Is(e, fs.ErrNotExist), errors.Is(e, fs.ErrPermission):
		default:
			return nil, fmt.Errorf("%w: %v", errProcessScanUnavailable, e)
		}
	}
	return out, nil
}

// parseLsofCwds reads the name fields (n) of `lsof -F n` output: one path per
// cwd record.
func parseLsofCwds(out []byte) []string {
	var cwds []string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n") && len(line) > 1 {
			cwds = append(cwds, line[1:])
		}
	}
	return cwds
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
	orphanReason      = "orphan-checkout"
	orphanNotWorktree = "not-a-git-worktree"
)

// recordedCheckouts names every checkout a reviewer-job record of any repo
// holds: its checkout field, and the path its label gives under dir, which is
// where a spawn places one when the field is empty. A record that cannot be
// read may name any directory, so the caller stops the sweep on its error.
func (s *Service) recordedCheckouts(dir string) (map[string]bool, error) {
	var paths []string
	for _, pattern := range []string{
		filepath.Join(s.Config.StateDir, "*", "reviewer-jobs", "*.json"),
		filepath.Join(s.Config.StateDir, "*", "*", "reviewer-jobs", "*.json"),
	} {
		m, e := filepath.Glob(pattern)
		if e != nil {
			return nil, e
		}
		paths = append(paths, m...)
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

// orphanLocalHost is the configured local host, or this machine's own panes
// when none is configured, so the in-use check always reads real panes.
func (s *Service) orphanLocalHost(ctx context.Context, hosts []*reapHost) *reapHost {
	for _, hs := range hosts {
		if hs.host.Local() {
			return hs
		}
	}
	ex := reap.Exec{Runner: s.Runner, Host: reap.Host{Name: "local"}}
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
func (s *Service) reapOrphanCheckouts(ctx context.Context, r config.Repo, hosts []*reapHost, acting bool, note func(reapReceipt)) []error {
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
	cwds, e := processCwds(ctx, s.Runner)
	if errors.Is(e, errProcessScanUnavailable) {
		// Fail closed without a pass error: this host cannot prove a process
		// holds no checkout, so nothing is removed or receipted.
		return nil
	}
	if e != nil {
		return []error{fmt.Errorf("orphan checkouts: %w", e)}
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
			if s.reportsOrphanNonWorktrees(r) {
				note(orphanRow(r, hs, path, orphanKept, orphanNotWorktree))
			}
			continue
		}
		// Another repo's worktree, a branch checkout, or a directory nested in a
		// worktree is not this pass's to remove.
		if filepath.Clean(tree.Path) != path || !strings.EqualFold(tree.Repo, r.Name) || tree.Branch != "" {
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
		if orphanHasUnsafeIgnored(s.Config.Reap.Regenerable, ignored) {
			continue
		}
		if e := hs.refreshPanes(ctx); e != nil {
			errs = append(errs, fmt.Errorf("orphan checkout %s: pane list: %w", path, e))
			continue
		}
		if hs.paneInUse(path) {
			continue
		}
		if anyWithin(cwds, path) {
			continue
		}
		if !acting {
			note(orphanRow(r, hs, path, orphanPlanned, orphanReason))
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

// reportsOrphanNonWorktrees reports whether r's pass writes the non-worktree
// rows: the alphabetically first configured repo, so one log holds each.
func (s *Service) reportsOrphanNonWorktrees(r config.Repo) bool {
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
