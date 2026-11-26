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
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/reap"
)

const orphanWorktreeAction = "orphan-worktree"

type orphanWorktreeCursor struct {
	Hosts    map[string]string `json:"hosts,omitempty"`
	NextHost string            `json:"next_host,omitempty"`
	// Host and Path read cursors written by the first version.
	Host string `json:"host,omitempty"`
	Path string `json:"path,omitempty"`
}
type orphanWorktreeVisit struct {
	host  string
	hs    *reapHost
	entry reap.Entry
}

func (s *Service) orphanWorktreeCursorPath(repo string) string {
	return filepath.Join(s.state(repo), "orphan-worktree-cursor.json")
}
func (s *Service) orphanWorktreeCursor(repo string) (orphanWorktreeCursor, error) {
	var c orphanWorktreeCursor
	_, e := fsutil.ReadJSON(s.orphanWorktreeCursorPath(repo), &c)
	return c, e
}
func (s *Service) saveOrphanWorktreeCursor(repo string, c orphanWorktreeCursor) error {
	defer lockPath(s.orphanWorktreeCursorPath(repo))()
	return fsutil.WriteJSON(s.orphanWorktreeCursorPath(repo), c)
}

func resumeOrphanWorktreeVisits(visits []orphanWorktreeVisit, from orphanWorktreeCursor) []orphanWorktreeVisit {
	if from.Path == "" || len(visits) == 0 {
		return visits
	}
	start := sort.Search(len(visits), func(i int) bool {
		return visits[i].host > from.Host || (visits[i].host == from.Host && visits[i].entry.Path > from.Path)
	})
	out := append([]orphanWorktreeVisit{}, visits[start:]...)
	return append(out, visits[:start]...)
}

// reapOrphanWorktrees considers registered one-level worktrees below each
// configured work root. Unknown evidence is always a kept row. The default
// configuration is shadow-only; acting additionally requires reap.act and
// the CLI's acting mode.
func (s *Service) reapOrphanWorktrees(ctx context.Context, r config.Repo, hosts []*reapHost, acting, durable bool, note func(reapReceipt)) []error {
	if !s.Config.Reap.OrphanWorktrees.Enabled {
		return nil
	}
	age, err := s.Config.Reap.OrphanWorktrees.MinAgeOrDefault()
	if err != nil {
		return []error{fmt.Errorf("reap.orphan_worktrees.min_age: %w", err)}
	}
	limit := s.Config.Reap.OrphanWorktrees.PerHostOrDefault()
	from, cursorErr := s.orphanWorktreeCursor(r.Name)
	if cursorErr != nil {
		return []error{fmt.Errorf("orphan worktrees resume cursor: %w", cursorErr)}
	}
	owned, err := s.orphanReviewerPaths()
	if err != nil {
		return []error{fmt.Errorf("orphan worktrees reviewer records: %w", err)}
	}
	counts := map[string]int{}
	var errs []error
	orderedHosts := append([]*reapHost(nil), hosts...)
	sort.SliceStable(orderedHosts, func(i, j int) bool { return orderedHosts[i].host.Name < orderedHosts[j].host.Name })
	if from.NextHost != "" && len(orderedHosts) > 1 {
		start := sort.Search(len(orderedHosts), func(i int) bool { return orderedHosts[i].host.Name >= from.NextHost })
		if start == len(orderedHosts) {
			start = 0
		}
		orderedHosts = append(append([]*reapHost(nil), orderedHosts[start:]...), orderedHosts[:start]...)
	}
	visited := map[string]int{}
	lastVisited := map[string]string{}
	nextHost := from.NextHost
	var budgetStopped bool
	for hostIndex, hs := range orderedHosts {
		if err := reapBudgetCheck(ctx, "orphan worktree discovery"); err != nil {
			errs = append(errs, err)
			budgetStopped = true
			break
		}
		if err := budgetCut(ctx, ctx.Err(), "orphan worktree discovery"); err != nil {
			errs = append(errs, err)
			budgetStopped = true
			break
		}
		if hs.host.WorkRoot == "" || hs.failed {
			continue
		}
		// Move discovery start before the potentially slow host listing. If
		// this host consumes the stage share, the next pass starts at its peer.
		nextHost = orderedHosts[(hostIndex+1)%len(orderedHosts)].host.Name
		for _, kind := range []string{"exp", "detached", "branch", "worker-owned", "other"} {
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
			// registration in this clone's worktree list. Inspect validates identity
			// before any removal; a separate test -f per entry made discovery
			// unbounded and could consume the entire stage budget.
			if owned[path] {
				continue // reviewer receipts own this path
			}
			candidates = append(candidates, entry)
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Path < candidates[j].Path })
		var visits []orphanWorktreeVisit
		for _, entry := range candidates {
			visits = append(visits, orphanWorktreeVisit{hs.host.Name, hs, entry})
		}
		// Resume this host independently, then inspect its candidates before
		// spending the remaining budget discovering another host.
		cursor := orphanWorktreeCursor{Host: hs.host.Name, Path: from.Hosts[hs.host.Name]}
		if from.Hosts == nil && from.Host == hs.host.Name {
			cursor.Path = from.Path
		}
		visits = resumeOrphanWorktreeVisits(visits, cursor)
		for _, visit := range visits {
			hs, entry := visit.hs, visit.entry
			main := filepath.Join(hs.host.WorkRoot, repoSegment(r.Name))
			if visited[visit.host] >= limit {
				continue
			}
			if err := reapBudgetCheck(ctx, "orphan worktrees"); err != nil {
				errs = append(errs, err)
				budgetStopped = true
				break
			}
			if err := budgetCut(ctx, ctx.Err(), "orphan worktree candidate"); err != nil {
				errs = append(errs, err)
				budgetStopped = true
				break
			}
			visited[visit.host]++
			lastVisited[visit.host] = entry.Path
			kind := orphanWorktreeKind(entry)
			rec := reapReceipt{Action: orphanWorktreeAction, Repo: r.Name, Host: hs.host.Name, Worktree: entry.Path, Branch: entry.Branch, Kind: kind}
			keep := func(reason string) {
				rec.Result, rec.Reason = "kept", reason
				note(rec)
				counts[hs.host.Name+"\x00"+kind]++
			}
			if entry.Locked {
				keep("locked")
				continue
			}
			if strings.HasPrefix(entry.Branch, workerBranchPrefix) {
				keep("worker-owned")
				continue
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
				rec.Result, rec.Reason = "error", e.Error()
				note(rec)
				counts[hs.host.Name+"\x00"+kind]++
				continue
			}
			rec.Result = "removed"
			note(rec)
			counts[hs.host.Name+"\x00"+kind]++
		}
		if budgetStopped {
			break
		}
	}
	if durable && (len(lastVisited) != 0 || nextHost != from.NextHost || (!budgetStopped && len(lastVisited) == 0)) {
		if from.Hosts == nil {
			from.Hosts = map[string]string{}
		}
		for host, path := range lastVisited {
			from.Hosts[host] = path
		}
		if !budgetStopped && len(lastVisited) == 0 {
			from.Hosts = nil
		}
		from.NextHost = nextHost
		from.Host, from.Path = "", ""
		if e := s.saveOrphanWorktreeCursor(r.Name, from); e != nil {
			errs = append(errs, e)
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
		return "worker-owned"
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
	files, e := recordFiles(s.Config.StateDir)
	if e != nil {
		return nil, e
	}
	for _, file := range files {
		var job reviewerJob
		if _, e := fsutil.ReadJSON(file, &job); e != nil {
			return nil, fmt.Errorf("reviewer job record %s: %w", file, e)
		}
		if job.Checkout != "" {
			named[filepath.Clean(job.Checkout)] = true
		}
		if job.Label != "" {
			for _, host := range s.Config.Hosts {
				if path := reviewerWorktreePath(&reapHost{host: host}, reviewerLabel{name: job.Label}); path != "" {
					named[filepath.Clean(path)] = true
				}
			}
		}
	}
	dir := config.Expand(s.Config.Review.Panewire.Checkouts)
	if dir != "" {
		recorded, err := s.recordedCheckouts(dir)
		if err != nil {
			return nil, err
		}
		for path := range recorded {
			named[path] = true
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
					path := row.Worktree
					if !filepath.IsAbs(path) {
						for _, host := range s.Config.Hosts {
							if host.Name == row.Host && host.WorkRoot != "" {
								path = filepath.Join(host.WorkRoot, path)
								break
							}
						}
					}
					named[filepath.Clean(path)] = true
				}
			}
		}
	}
	return named, nil
}

func orphanWorktreeClean(ctx context.Context, ex reap.Exec, path string, globs []string) (bool, string) {
	b, err := ex.Run(ctx, "git", "--no-optional-locks", "-C", path, "-c", "status.showUntrackedFiles=normal", "status", "--porcelain", "--ignored", "--untracked-files=normal", "-z")
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
	full, err := ex.Run(ctx, "git", "--no-optional-locks", "-C", path, "rev-parse", "--symbolic-full-name", "--verify", "--quiet", ref)
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(full)), "refs/remotes/") {
		return false, nil
	}
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
