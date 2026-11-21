package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
)

// Per-host worker-worktree made/removed balance (hk 5168, order a329 part 2).
//
// Made counts worker worktrees the sweeps took custody of: the first reap row
// (remove-worktree or legacy-sweep) naming the host's worktree. No spawn audit
// rows exist in decisions.jsonl (only reviewer-spawn, for reviewers), and the
// hk JobRecord projection carries no timestamps and has no list endpoint, so
// neither source the brief names can supply replayable per-day made counts
// from a state copy. First sighting in reap.jsonl exists today, is
// timestamped, and is exactly what the kept list below already reads.
// Done counts reap remove-worktree rows with result removed or already-gone,
// plus rows proving the path gone (reason worktree-gone, already-gone or
// gone). The stall rule has the reviewer-checkout shape (balanceStalled) with
// its own thresholds, and the notice and ack path is hk 5109's liveness one.

const workerWorktreesDailyCommand = "worker-worktrees-daily"

// WorkerKeptWorktree is one worktree that stayed past the removal rules.
type WorkerKeptWorktree struct {
	Worktree string `json:"worktree"`
	Reason   string `json:"reason"`
	PR       int    `json:"pr,omitempty"`
	Repo     string `json:"repo,omitempty"`
}

// WorkerWorktreeHost is one host's daily worker-worktree counts plus the
// worktrees that stayed, with each reason.
type WorkerWorktreeHost struct {
	Host    string               `json:"host"`
	Made    int                  `json:"made"`
	Removed int                  `json:"removed"`
	Open    int                  `json:"open"`
	Stalled bool                 `json:"stalled"`
	Kept    []WorkerKeptWorktree `json:"kept,omitempty"`
}

// workerWorktreeTerminal reports whether a reap row proves the worktree gone:
// removed by the sweep, or a gone proof (worktree-gone, already-gone, gone).
// A legacy guard refusal (dirty, ignored-files, detached, ...) is terminal
// for the legacy sweep's own retries but the worktree stays, so it is kept
// with its reason, never done.
func workerWorktreeTerminal(result, reason string) bool {
	switch result {
	case "removed", "already-gone":
		return true
	}
	switch reason {
	case "worktree-gone", "already-gone", "gone":
		return true
	}
	return false
}

// isWorkerWorktree reports whether a reap row's path is a worker worktree.
// Reviewer checkouts (mwr-*) and consult checkouts (consult-*) are out of
// scope (hk 5167 owns removal, hk 5091 owns consults); what remains in reap
// rows is worker worktrees, named <repo>.<job> under the host's work root.
func isWorkerWorktree(path string) bool {
	if path == "" {
		return false
	}
	base := path
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if strings.HasPrefix(base, "mwr-") {
		return false
	}
	if strings.HasPrefix(base, "consult-") || strings.Contains(base, ".consult-") {
		return false
	}
	return true
}

type workerWorktreeRow struct {
	time     time.Time
	host     string
	worktree string
	pr       int
	repo     string
	result   string
	reason   string
}

// workerWorktreeScan streams every repo's reap log and calls fn for each
// remove-worktree or legacy-sweep row naming a worker worktree. Shadow and
// dry-run plans (would-*) are not verdicts and never count.
func (s *Service) workerWorktreeScan(repos []config.Repo, fn func(workerWorktreeRow)) error {
	for _, r := range repos {
		f, err := os.Open(filepath.Join(s.state(r.Name), "reap.jsonl"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		jl := fsutil.NewJSONL(f.Name(), 0, f)
		for line, ok := jl.Next(); ok; line, ok = jl.Next() {
			var row reapReceipt
			if json.Unmarshal(line, &row) == nil &&
				(row.Action == "remove-worktree" || row.Action == legacyAction) {
				if strings.HasPrefix(row.Result, "would-") {
					continue
				}
				wt := row.Worktree
				if wt == "" {
					wt = row.Cwd
				}
				if row.Host == "" {
					// Hostless rows (notably the hostless
					// merge-commit-unknown skips) carry no worktree
					// identity to tally; they are unattributed, never
					// stalling, and must not be dropped here.
					fn(workerWorktreeRow{
						time:   row.Time.UTC(),
						pr:     row.PR,
						repo:   r.Name,
						result: row.Result,
						reason: row.Reason,
					})
					continue
				}
				if !isWorkerWorktree(wt) {
					continue
				}
				fn(workerWorktreeRow{
					time:     row.Time.UTC(),
					host:     row.Host,
					worktree: wt,
					pr:       row.PR,
					repo:     r.Name,
					result:   row.Result,
					reason:   row.Reason,
				})
			}
		}
		_ = f.Close()
		if err := jl.Err(); err != nil {
			return err
		}
	}
	return nil
}

// workerWorktreesForDay tallies per-host made and removed worker worktrees
// for day, the trailing-window open balance, and the worktrees staying past
// the removal rules with each reason. Rows without a host or worktree
// (notably the hostless merge-commit-unknown skips) are unattributed: counted
// separately, never stalling.
func (s *Service) workerWorktreesForDay(repos []config.Repo, day string) ([]WorkerWorktreeHost, int, error) {
	window := s.Config.Liveness.WorkerWorktreesWindowDaysOrDefault()
	start, err := time.Parse(reviewerSummaryDay, day)
	if err != nil {
		return nil, 0, err
	}
	end := start.Add(24 * time.Hour)
	windowStart := start.AddDate(0, 0, -(window - 1))

	type key struct{ host, worktree string }
	seen := map[key]time.Time{}
	var order []key
	latest := map[key]workerWorktreeRow{}
	removedOn := map[key]bool{}
	unattributed := 0
	err = s.workerWorktreeScan(repos, func(w workerWorktreeRow) {
		if w.host == "" {
			if w.time.Format(reviewerSummaryDay) == day {
				unattributed++
			}
			return
		}
		k := key{w.host, w.worktree}
		if first, ok := seen[k]; !ok || w.time.Before(first) {
			if !ok {
				order = append(order, k)
			}
			seen[k] = w.time
		}
		if w.time.Before(end) {
			if cur, ok := latest[k]; !ok || !w.time.Before(cur.time) {
				latest[k] = w
			}
		}
		if w.time.Format(reviewerSummaryDay) == day && workerWorktreeTerminal(w.result, w.reason) {
			removedOn[k] = true
		}
	})
	minMade := s.Config.Liveness.WorkerWorktreesMinMadeOrDefault()
	maxRemovedPct := s.Config.Liveness.WorkerWorktreesMaxRemovedPctOrDefault()
	byHost := map[string]*WorkerWorktreeHost{}
	host := func(name string) *WorkerWorktreeHost {
		h, ok := byHost[name]
		if !ok {
			h = &WorkerWorktreeHost{Host: name}
			byHost[name] = h
		}
		return h
	}
	for _, k := range order {
		first := seen[k]
		if removedOn[k] {
			// A removal on the evaluated day counts even when custody
			// began before the trailing window; otherwise old worktrees
			// removed today vanish from Done and stall falsely.
			host(k.host).Removed++
		}
		if first.Before(windowStart) || !first.Before(end) {
			continue
		}
		h := host(k.host)
		if first.Format(reviewerSummaryDay) == day {
			h.Made++
		}
		// Open over the trailing window: custody minus removals.
		if last, ok := latest[k]; ok && !workerWorktreeTerminal(last.result, last.reason) {
			h.Open++
			h.Kept = append(h.Kept, WorkerKeptWorktree{
				Worktree: k.worktree,
				Reason:   last.reason,
				PR:       last.pr,
				Repo:     last.repo,
			})
		}
	}
	hosts := make([]WorkerWorktreeHost, 0, len(byHost))
	for _, h := range byHost {
		h.Stalled = balanceStalled(h.Made, h.Removed, minMade, maxRemovedPct)
		if h.Kept != nil {
			sort.Slice(h.Kept, func(i, j int) bool { return h.Kept[i].Worktree < h.Kept[j].Worktree })
		}
		hosts = append(hosts, *h)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Host < hosts[j].Host })
	return hosts, unattributed, nil
}

// workerWorktreesRowsWritten returns the hosts the target repo already holds
// a worker-worktrees-daily row for on day.
func (s *Service) workerWorktreesRowsWritten(repo, day string) (map[string]bool, error) {
	written := map[string]bool{}
	jsonlPath := filepath.Join(s.state(repo), "decisions.jsonl")
	f, err := os.Open(jsonlPath)
	if errors.Is(err, os.ErrNotExist) {
		return written, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	jl := fsutil.NewJSONL(jsonlPath, 0, f)
	for line, ok := jl.Next(); ok; line, ok = jl.Next() {
		var row struct {
			Command string          `json:"command"`
			Value   json.RawMessage `json:"value"`
		}
		if json.Unmarshal(line, &row) == nil && row.Command == workerWorktreesDailyCommand {
			var v struct {
				Day  string `json:"day"`
				Host string `json:"host"`
			}
			if json.Unmarshal(row.Value, &v) == nil && v.Day == day {
				written[v.Host] = true
			}
		}
	}
	return written, nil
}

// auditWorkerWorktreesDay writes one worker-worktrees-daily row per host for
// day into the target repo: the host's made, removed and trailing open
// balance plus the worktrees that stayed, each with its reason.
func (s *Service) auditWorkerWorktreesDay(target config.Repo, day string, hosts []WorkerWorktreeHost, dry bool) error {
	written, err := s.workerWorktreesRowsWritten(target.Name, day)
	if err != nil {
		return err
	}
	window := s.Config.Liveness.WorkerWorktreesWindowDaysOrDefault()
	for _, h := range hosts {
		if written[h.Host] {
			continue
		}
		kept := make([]map[string]any, 0, len(h.Kept))
		for _, k := range h.Kept {
			kept = append(kept, map[string]any{
				"worktree": k.Worktree,
				"reason":   k.Reason,
				"pr":       k.PR,
				"repo":     k.Repo,
			})
		}
		if err := s.audit(target.Name, workerWorktreesDailyCommand, map[string]any{
			"day":     day,
			"host":    h.Host,
			"made":    h.Made,
			"removed": h.Removed,
			"open":    h.Open,
			"window":  window,
			"kept":    kept,
		}, dry); err != nil {
			return err
		}
	}
	return nil
}

// workerWorktreesReason names the stalled hosts for the mechanism result and
// its stall notice.
func workerWorktreesReason(hosts []WorkerWorktreeHost) string {
	var stalled []string
	for _, h := range hosts {
		if h.Stalled {
			stalled = append(stalled, fmt.Sprintf("%s made %d removed %d", h.Host, h.Made, h.Removed))
		}
	}
	if len(stalled) == 0 {
		return ""
	}
	return "stalled hosts: " + strings.Join(stalled, "; ")
}
