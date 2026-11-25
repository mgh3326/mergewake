package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/reap"
)

const orphanWorktreeAction = "orphan-worktree"

// reapOrphanWorktrees considers registered one-level worktrees below each
// configured work root. Unknown evidence is always a kept row. The default
// configuration is shadow-only; acting additionally requires reap.act and
// the CLI's acting mode.
func (s *Service) reapOrphanWorktrees(ctx context.Context, r config.Repo, hosts []*reapHost, acting bool, note func(reapReceipt)) []error {
	age, err := s.Config.Reap.OrphanWorktrees.MinAgeOrDefault()
	if err != nil {
		return []error{fmt.Errorf("reap.orphan_worktrees.min_age: %w", err)}
	}
	limit := s.Config.Reap.OrphanWorktrees.PerHostOrDefault()
	owned, err := s.orphanReviewerPaths()
	if err != nil {
		return []error{fmt.Errorf("orphan worktrees reviewer records: %w", err)}
	}
	counts := map[string]int{}
	var errs []error
	for _, hs := range hosts {
		if hs.host.WorkRoot == "" || hs.failed {
			continue
		}
		for _, kind := range []string{"exp", "detached", "branch", "wk-no-pr", "other"} {
			counts[hs.host.Name+"\x00"+kind] = 0
		}
		main := filepath.Join(hs.host.WorkRoot, repoSegment(r.Name))
		var entries []reap.Entry
		if hs.viaHub {
			lister, ok := hs.ops.(reap.WorktreeLister)
			if !ok {
				continue
			}
			rows, e := reap.ListWorktrees(ctx, lister, filepath.Join(main, ".git"))
			if e != nil {
				if reap.InvalidArgs(e) {
					continue // this host has no clone of this repo
				}
				errs = append(errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s orphan worktrees list: %w", hs.host.Name, e)))
				continue
			}
			for _, row := range rows {
				entries = append(entries, reap.Entry{Path: row.Path, Branch: row.Branch, Detached: row.Detached})
			}
		} else {
			if hs.host.Local() {
				if _, e := os.Stat(main); os.IsNotExist(e) {
					continue // a host need not clone every configured repo
				} else if e != nil {
					errs = append(errs, fmt.Errorf("%s orphan worktrees root: %w", hs.host.Name, e))
					continue
				}
			} else if _, e := hs.exec.Run(ctx, "test", "-d", main); e != nil {
				if commandExitCode(e) == 1 {
					continue
				}
				errs = append(errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s orphan worktrees root: %w", hs.host.Name, e)))
				continue
			}
			entries, err = hs.exec.WorktreeEntries(ctx, main)
			if err != nil {
				if commandExitCode(err) == 128 {
					continue // existing path is not a git clone
				}
				errs = append(errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s orphan worktrees list: %w", hs.host.Name, err)))
				continue
			}
		}
		candidates := make([]reap.Entry, 0, len(entries))
		for _, entry := range entries {
			path := filepath.Clean(entry.Path)
			if path == filepath.Clean(main) || filepath.Dir(path) != filepath.Clean(hs.host.WorkRoot) || filepath.Base(path) == "" || filepath.Base(path) == "herdr-inbox" {
				continue
			}
			// The canonical clone and arbitrary directories never qualify: require
			// the worktree marker and a registration in this clone's worktree list.
			if !hs.viaHub {
				if _, e := hs.exec.Run(ctx, "test", "-f", filepath.Join(path, ".git")); e != nil {
					continue
				}
			}
			if strings.HasPrefix(entry.Branch, workerBranchPrefix) || owned[path] {
				continue // the worker/reviewer reaper owns this path
			}
			candidates = append(candidates, entry)
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Path < candidates[j].Path })
		for i, entry := range candidates {
			if i >= limit {
				break
			}
			kind := orphanWorktreeKind(entry)
			rec := reapReceipt{Action: orphanWorktreeAction, Repo: r.Name, Host: hs.host.Name, Worktree: entry.Path, Branch: entry.Branch, Kind: kind}
			keep := func(reason string) {
				rec.Result, rec.Reason = "kept", reason
				note(rec)
				counts[hs.host.Name+"\x00"+kind]++
			}
			if hs.viaHub {
				// Current node worktree.list exposes status summaries, but not the
				// required no-lock status paths, filesystem mtimes, or fetch-free
				// upstream refs. Keep until that evidence is available at the node.
				keep("host-evidence-unavailable")
				continue
			}
			wt, e := hs.exec.Inspect(ctx, entry.Path)
			if e != nil || wt == nil || filepath.Clean(wt.Path) != filepath.Clean(entry.Path) || !strings.EqualFold(wt.Repo, r.Name) {
				keep("worktree-identity-unknown")
				continue
			}
			if ok, reason := orphanWorktreeClean(ctx, hs.exec, entry.Path, s.Config.ReapRegenerable(r)); !ok {
				keep(reason)
				continue
			}
			pushed, e := orphanWorktreePushed(ctx, hs.exec, entry.Path, wt.Head, wt.Branch, r.Branch)
			if e != nil || !pushed {
				keep("commits-not-proven-pushed")
				continue
			}
			old, e := orphanWorktreeOld(ctx, hs.exec, entry.Path, age, s.now())
			if e != nil || !old {
				keep("younger-than-min-age-or-age-unknown")
				continue
			}
			if e := hs.refreshPanes(ctx); e != nil {
				errs = append(errs, fmt.Errorf("%s orphan worktrees pane scan: %w", hs.host.Name, e))
				keep("pane-scan-failed")
				continue
			}
			if hs.paneInUse(entry.Path) {
				keep("pane-in-use")
				continue
			}
			if !hs.host.Local() {
				keep("process-scan-unavailable")
				continue
			}
			if reason := s.processInUse(ctx, entry.Path); reason != "" {
				keep(reason)
				continue
			}
			if !acting {
				rec.Result, rec.Reason = "would-remove", ""
				note(rec)
				counts[hs.host.Name+"\x00"+kind]++
				continue
			}
			fresh, e := hs.exec.Inspect(ctx, entry.Path)
			if e != nil || fresh == nil || filepath.Clean(fresh.Path) != filepath.Clean(wt.Path) || fresh.Head != wt.Head || fresh.Branch != wt.Branch || !strings.EqualFold(fresh.Repo, r.Name) {
				keep("worktree-changed")
				continue
			}
			if ok, reason := orphanWorktreeClean(ctx, hs.exec, entry.Path, s.Config.ReapRegenerable(r)); !ok {
				keep(reason)
				continue
			}
			if pushed, e := orphanWorktreePushed(ctx, hs.exec, entry.Path, fresh.Head, fresh.Branch, r.Branch); e != nil || !pushed {
				keep("commits-not-proven-pushed")
				continue
			}
			if old, e := orphanWorktreeOld(ctx, hs.exec, entry.Path, age, s.now()); e != nil || !old {
				keep("younger-than-min-age-or-age-unknown")
				continue
			}
			// Pane evidence is refreshed immediately before the non-forcing
			// removal, after the other guards have been repeated.
			if e := hs.refreshPanes(ctx); e != nil {
				errs = append(errs, fmt.Errorf("%s orphan worktrees pane recheck: %w", hs.host.Name, e))
				keep("pane-scan-failed")
				continue
			}
			if hs.paneInUse(entry.Path) {
				keep("pane-in-use")
				continue
			}
			if reason := s.processInUse(ctx, entry.Path); reason != "" {
				keep(reason)
				continue
			}
			if e := hs.exec.RemoveWorktree(ctx, main, entry.Path); e != nil {
				keep("worktree-remove-refused")
				continue
			}
			rec.Result = "removed"
			note(rec)
			counts[hs.host.Name+"\x00"+kind]++
		}
	}
	// The regular reap CLI prints one machine-readable summary row per host and
	// kind so a shadow pass can be routed for policy review.
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		host, kind, _ := strings.Cut(key, "\x00")
		if e := s.Print(map[string]any{"action": "orphan-worktree-count", "repo": r.Name, "host": host, "kind": kind, "count": counts[key]}); e != nil {
			errs = append(errs, e)
		}
	}
	return errs
}

func orphanWorktreeKind(entry reap.Entry) string {
	if entry.Detached {
		return "detached"
	}
	branch := entry.Branch
	base := filepath.Base(entry.Path)
	if strings.HasPrefix(branch, "exp/") || orphanExperimentName(base) {
		return "exp"
	}
	if strings.HasPrefix(branch, workerBranchPrefix) {
		return "wk-no-pr"
	}
	if branch != "" {
		return "branch"
	}
	return "other"
}

func orphanExperimentName(name string) bool {
	_, suffix, ok := strings.Cut(name, ".exp")
	if !ok || len(suffix) < 6 || suffix[4] != '-' {
		return false
	}
	for _, r := range suffix[:4] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *Service) orphanReviewerPaths() (map[string]bool, error) {
	named := map[string]bool{}
	dir := config.Expand(s.Config.Review.Panewire.Checkouts)
	if dir != "" {
		var err error
		named, err = s.recordedCheckouts(dir)
		if err != nil {
			return nil, err
		}
	}
	for _, r := range s.Config.Repos {
		rows, e := s.reapReceipts(r.Name)
		if e != nil {
			return nil, e
		}
		for _, row := range rows {
			if row.Action == "reviewer-cleanup" || row.Action == "reviewer-reap" {
				if row.Worktree != "" {
					named[filepath.Clean(row.Worktree)] = true
				}
			}
		}
	}
	return named, nil
}

func orphanWorktreeClean(ctx context.Context, ex reap.Exec, path string, globs []string) (bool, string) {
	b, err := ex.Run(ctx, "git", "--no-optional-locks", "-C", path, "status", "--porcelain", "--ignored", "-z")
	if err != nil {
		return false, "status-unreadable"
	}
	for _, entry := range strings.Split(string(b), "\x00") {
		if entry == "" {
			continue
		}
		if !strings.HasPrefix(entry, "!! ") {
			return false, "modified-file"
		}
		if !reap.Regenerable(globs, strings.TrimPrefix(entry, "!! ")) {
			return false, "ignored-file-not-regenerable"
		}
	}
	return true, ""
}

func orphanWorktreePushed(ctx context.Context, ex reap.Exec, path, head, branch, base string) (bool, error) {
	for _, ref := range []string{"origin/" + base} {
		if _, err := ex.Run(ctx, "git", "--no-optional-locks", "-C", path, "merge-base", "--is-ancestor", head, ref); err == nil {
			return true, nil
		}
	}
	if branch == "" {
		return false, nil
	}
	ref := branch + "@{upstream}"
	got, err := ex.Run(ctx, "git", "--no-optional-locks", "-C", path, "rev-parse", "--verify", "--quiet", ref)
	if err != nil {
		return false, nil
	}
	return strings.TrimSpace(string(got)) == head, nil
}

func orphanWorktreeOld(ctx context.Context, ex reap.Exec, path string, age time.Duration, now time.Time) (bool, error) {
	gitdir, err := ex.Run(ctx, "git", "--no-optional-locks", "-C", path, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return false, err
	}
	gd := strings.TrimSpace(string(gitdir))
	paths := []string{filepath.Join(gd, "index"), filepath.Join(gd, "HEAD"), filepath.Join(gd, "logs", "HEAD"), path}
	newest := time.Time{}
	for _, p := range paths {
		out, e := ex.Run(ctx, "stat", "-c", "%Y", p)
		if e != nil {
			out, e = ex.Run(ctx, "stat", "-f", "%m", p)
		}
		if e != nil {
			return false, e
		}
		var sec int64
		if _, e := fmt.Sscan(strings.TrimSpace(string(out)), &sec); e != nil {
			return false, e
		}
		mt := time.Unix(sec, 0)
		if mt.After(newest) {
			newest = mt
		}
	}
	return now.Sub(newest) > age, nil
}
