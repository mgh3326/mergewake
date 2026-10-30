package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/archive"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reapReceipt is one durable row of <state>/<repo>/reap.jsonl. Rows are
// written in acting and shadow modes; a "would-*" row still names the worktree
// so a later pass can find it. Dry-run prints rows without writing the file.
type reapReceipt struct {
	Time     time.Time `json:"time"`
	Action   string    `json:"action"` // close-pane, remove-worktree, delete-branch, archive-job
	Repo     string    `json:"repo"`
	PR       int       `json:"pr"`
	Host     string    `json:"host"`
	Pane     string    `json:"pane,omitempty"`
	Cwd      string    `json:"cwd,omitempty"`
	Worktree string    `json:"worktree,omitempty"`
	Branch   string    `json:"branch,omitempty"`
	Saved    string    `json:"saved,omitempty"` // report/log path captured before a pane close
	Result   string    `json:"result"`          // closed, removed, deleted, would-*, skipped, error
	Reason   string    `json:"reason,omitempty"`
	Files    []string  `json:"files,omitempty"` // ignored paths named by a refusing removal skip
	Job      string    `json:"job,omitempty"`   // job dir name of a closed pane or archived job

	Archive *archive.Result `json:"archive,omitempty"` // Drive dir, hk ids and excluded files of an archive
}

func (s *Service) reapLog(repo string) string { return filepath.Join(s.state(repo), "reap.jsonl") }

func (s *Service) reapReceipts(repo string) ([]reapReceipt, error) {
	b, err := os.ReadFile(s.reapLog(repo))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []reapReceipt
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var v reapReceipt
		if e := json.Unmarshal([]byte(line), &v); e != nil {
			return nil, fmt.Errorf("reap receipts corrupt: %w", e)
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Service) reapNote(v reapReceipt, dry bool) error {
	v.Time = s.now().UTC()
	if e := s.Print(v); e != nil {
		return e
	}
	if dry {
		return nil
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return fsutil.AppendLine(s.reapLog(v.Repo), b)
}

// reapHost caches one host's agent list, full pane list and per-cwd worktree
// inspection for a single pass; closed records panes this pass closed or would
// close, and usageUnknown blocks removals when the full pane scan failed.
type reapHost struct {
	host         reap.Host
	exec         reap.Exec
	panes        []reap.Pane
	all          []reap.Pane
	trees        map[string]*reap.Worktree
	closed       map[string]bool
	failed       bool
	usageUnknown bool
}

// refreshPanes re-reads the full pane list so an in-use check runs on current
// state, not the discovery snapshot, and inspects any newly seen cwds.
func (hs *reapHost) refreshPanes(ctx context.Context) error {
	all, err := hs.exec.AllPanes(ctx)
	if err != nil {
		return err
	}
	hs.all = all
	for _, p := range all {
		if p.Cwd != "" {
			if _, ok := hs.trees[p.Cwd]; !ok {
				w, _ := hs.exec.Inspect(ctx, p.Cwd)
				hs.trees[p.Cwd] = w
			}
		}
	}
	return nil
}

// reclose re-reads the agent list immediately before a real close and only
// proceeds when the pane is still there, unchanged: the discovery list is a
// stale snapshot, and a pane that went back to work since must survive.
func (s *Service) reclose(ctx context.Context, hs *reapHost, pane reap.Pane) (string, string) {
	fresh, err := hs.exec.Panes(ctx)
	if err != nil {
		return "error", err.Error()
	}
	var cur *reap.Pane
	for i := range fresh {
		if fresh[i].ID == pane.ID {
			cur = &fresh[i]
		}
	}
	switch {
	case cur == nil:
		return "skipped", "pane-gone"
	case cur.Status != pane.Status || cur.Cwd != pane.Cwd || cur.Name != pane.Name:
		return "skipped", "pane-changed"
	}
	if err := hs.exec.Close(ctx, pane.ID); err != nil {
		return "error", err.Error()
	}
	return "closed", ""
}

// filesSig is a compact count-plus-hash identity for a set of paths, so a
// receipt reason stays small and the row dedupe still fires only when the
// refusing set actually changed.
func filesSig(files []string) string {
	sorted := append([]string(nil), files...)
	sort.Strings(sorted)
	h := sha256.Sum256([]byte(strings.Join(sorted, "\x00")))
	return fmt.Sprintf("%d-%x", len(sorted), h[:4])
}

func (hs *reapHost) paneInUse(path string) bool {
	clean := filepath.Clean(path)
	for _, pane := range hs.all {
		if pane.Cwd == "" || hs.closed[pane.ID] {
			continue
		}
		cwd := filepath.Clean(pane.Cwd)
		if cwd == clean || strings.HasPrefix(cwd, clean+string(filepath.Separator)) {
			return true
		}
		if w := hs.trees[pane.Cwd]; w != nil && filepath.Clean(w.Path) == clean {
			return true
		}
	}
	return false
}

// Reap closes agent panes sitting in worktrees of merged-PR head branches, then
// removes the worktree and local branch once the merge is older than
// reap.worktree_after and no watch recovery PR landed. Anything dirty, unknown
// or ambiguous is skipped and reported, never forced.
func (s *Service) Reap(ctx context.Context, r config.Repo, prNum int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "reap-error", map[string]any{"error": retErr.Error()}, o.DryRun))
		}
	}()
	if !s.Config.Reap.Enabled {
		return errors.New("reap requires reap.enabled in host config")
	}
	if prNum < 0 {
		return errors.New("--pr must be positive")
	}
	after, e := config.Duration(s.Config.Reap.WorktreeAfter)
	if e != nil {
		return e
	}
	scan, e := config.Duration(s.Config.Reap.ScanAfter)
	if e != nil {
		return e
	}
	if len(s.Config.Hosts) == 0 {
		return errors.New("reap requires configured hosts")
	}
	now := s.now()
	// reap.act is the role's own acting gate, separate from daemon.act:
	// without it every mode stays observational no matter what --apply says.
	acting := o.Acting() && s.Config.Reap.Act
	var errs []error

	receipts, e := s.reapReceipts(r.Name)
	if e != nil {
		return e
	}
	// rowKey identifies one receipt stream (pane closes key by pane, worktree
	// work keys by path); lastRow remembers the newest logged result per key
	// so a repeat pass only writes rows whose outcome changed: a shadow run
	// logging the same would-* every poll, or a removed worktree logging
	// worktree-gone forever, would otherwise grow reap.jsonl without bound.
	rowKey := func(v reapReceipt) string {
		path := v.Pane
		if v.Action == "archive-job" {
			path = v.Job
		} else if v.Action != "close-pane" {
			// Rows predating the Worktree field still name their path in Cwd.
			switch {
			case v.Worktree != "":
				path = v.Worktree
			case v.Cwd != "":
				path = v.Cwd
			}
		}
		return fmt.Sprintf("%s|%d|%s|%s", v.Action, v.PR, v.Host, path)
	}
	rowSig := func(v reapReceipt) string { return v.Result + "|" + v.Reason }
	lastRow := map[string]string{}
	for _, rec := range receipts {
		lastRow[rowKey(rec)] = rowSig(rec)
	}
	// prDone reports whether every worktree a PR's receipts ever named already
	// reached a terminal remove outcome. A closed pane alone is not done —
	// its worktree cleanup may still be pending — so only an all-terminal set
	// spares the GitHub refetch on every poll.
	prPaths := map[int]map[string]bool{}
	for _, rec := range receipts {
		if rec.PR == 0 {
			continue
		}
		path := rec.Worktree
		if path == "" {
			path = rec.Cwd
		}
		if path == "" {
			continue
		}
		if prPaths[rec.PR] == nil {
			prPaths[rec.PR] = map[string]bool{}
		}
		prPaths[rec.PR][rec.Host+"\x00"+path] = true
	}
	prDone := func(pr int) bool {
		paths := prPaths[pr]
		if len(paths) == 0 {
			return false
		}
		for hp := range paths {
			host, path, _ := strings.Cut(hp, "\x00")
			sig := lastRow[rowKey(reapReceipt{Action: "remove-worktree", PR: pr, Host: host, Worktree: path})]
			if sig != "removed|" && sig != "worktree-gone|" {
				return false
			}
		}
		return true
	}
	// A receipt-named PR beyond the listing horizon is refetched at most once
	// per day, measured from its newest row: each attempt appends a check-pr
	// row, so a persistent skip like reverted, unpushed or refused ignored
	// files stops costing a GitHub call every poll.
	lastPRCheck := map[int]time.Time{}
	for _, rec := range receipts {
		// Archive rows need no GitHub call and must not delay a refetch.
		if rec.Action != "archive-job" && rec.Time.After(lastPRCheck[rec.PR]) {
			lastPRCheck[rec.PR] = rec.Time
		}
	}
	const refetchAfter = 24 * time.Hour

	pulls := []github.Pull{}
	if prNum > 0 {
		p, e := s.GH.Pull(ctx, r.Name, prNum)
		if e != nil {
			return e
		}
		pulls = append(pulls, p)
	} else {
		batch, e := s.GH.MergedPulls(ctx, r.Name, r.Branch, now.Add(-scan))
		if e != nil {
			return e
		}
		known := map[int]bool{}
		for _, p := range batch {
			known[p.Number] = true
			pulls = append(pulls, p)
		}
		// Receipt-named PRs can be older than the listing horizon. A fetch
		// failure only loses that PR this pass; it never aborts the rest.
		for _, rec := range receipts {
			if rec.PR == 0 || known[rec.PR] || prDone(rec.PR) || now.Sub(lastPRCheck[rec.PR]) < refetchAfter {
				continue
			}
			known[rec.PR] = true
			// Stamp the attempt before the call: identical outcomes are
			// deduped and never write a row, so without this a persistent
			// skip would refetch every pass once the newest row aged out.
			// The stamp persists even under --dry-run — it is bookkeeping for
			// the GitHub call that just happened, so repeated dry-run polls
			// stay bounded too.
			if e := s.reapNote(reapReceipt{Action: "check-pr", Repo: r.Name, PR: rec.PR, Result: "checked"}, false); e != nil {
				errs = append(errs, e)
			}
			p, e := s.GH.Pull(ctx, r.Name, rec.PR)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d: %w", rec.PR, e))
				continue
			}
			pulls = append(pulls, p)
		}
	}
	sort.Slice(pulls, func(i, j int) bool { return pulls[i].Number < pulls[j].Number })

	hosts := []*reapHost{}
	byName := map[string]*reapHost{}
	for _, h := range s.Config.Hosts {
		hs := &reapHost{host: h, exec: reap.Exec{Runner: s.Runner, Host: h}, trees: map[string]*reap.Worktree{}, closed: map[string]bool{}}
		hosts = append(hosts, hs)
		byName[h.Name] = hs
		panes, e := hs.exec.Panes(ctx)
		if e != nil {
			hs.failed = true
			errs = append(errs, fmt.Errorf("%s: %w", h.Name, e))
			continue
		}
		hs.panes = panes
		// The full pane list also covers non-agent shells and jobs: without it
		// the usage check is unknown and removals must skip.
		all, e := hs.exec.AllPanes(ctx)
		if e != nil {
			hs.usageUnknown = true
			errs = append(errs, fmt.Errorf("%s: %w", h.Name, e))
		}
		hs.all = all
		for _, list := range [][]reap.Pane{panes, all} {
			for _, p := range list {
				if p.Cwd != "" {
					if _, ok := hs.trees[p.Cwd]; !ok {
						w, _ := hs.exec.Inspect(ctx, p.Cwd)
						hs.trees[p.Cwd] = w
					}
				}
			}
		}
	}

	// Errors are counted once, from errs: every "error" row also lands there.
	scanned, closed, planned, removed, deleted, skipped, archived := 0, 0, 0, 0, 0, 0, 0
	note := func(v reapReceipt) {
		switch v.Result {
		case "closed":
			closed++
		case "would-close", "would-remove", "would-delete", "would-archive":
			planned++
		case "archived":
			archived++
		case "removed":
			removed++
		case "deleted":
			deleted++
		case "skipped":
			skipped++
		}
		// Every row prints; only changed outcomes append to the receipt file.
		// A failed archive always appends: its time spaces the next retry.
		// So does a completed one, which carries its own Drive and hk record.
		repeat := lastRow[rowKey(v)] == rowSig(v) && !(v.Action == "archive-job" && (v.Result == "error" || v.Result == "archived"))
		lastRow[rowKey(v)] = rowSig(v)
		if e := s.reapNote(v, o.DryRun || repeat); e != nil {
			errs = append(errs, e)
		}
	}
	matches := func(w *reap.Worktree, p github.Pull) bool {
		return w != nil && w.Branch != "" && w.Branch == p.Head.Ref &&
			p.Head.SHA != "" && w.Head == p.Head.SHA && strings.EqualFold(w.Repo, r.Name)
	}

	for _, p := range pulls {
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		if !(p.Merged || p.MergedAt != nil) || p.Base.Ref != r.Branch {
			if prNum > 0 {
				return errors.Join(append(errs, fmt.Errorf("PR #%d is not a merged PR against %s", p.Number, r.Branch))...)
			}
			continue
		}
		scanned++
		var mergedAt *time.Time
		if p.MergedAt != nil {
			mergedAt = parsedTime(*p.MergedAt)
		}
		for _, hs := range hosts {
			if hs.failed {
				continue
			}
			for _, pane := range hs.panes {
				w := hs.trees[pane.Cwd]
				if !matches(w, p) {
					continue
				}
				rec := reapReceipt{Action: "close-pane", Repo: r.Name, PR: p.Number, Host: hs.host.Name, Pane: pane.ID, Cwd: pane.Cwd, Worktree: w.Path, Branch: w.Branch}
				switch {
				case pane.ID == "":
					rec.Result, rec.Reason = "skipped", "pane-id-missing"
				case pane.Status != "idle" && pane.Status != "done":
					// An allowlist, not a denylist: blocked, waiting, unknown
					// or empty states are live enough to keep.
					st := pane.Status
					if st == "" {
						st = "unknown"
					}
					rec.Result, rec.Reason = "skipped", "status:"+st
				case pane.Name == "":
					rec.Result, rec.Reason = "skipped", "unnamed"
				case reap.Protected(pane.Name, s.Config.Reap.Protect):
					rec.Result, rec.Reason = "skipped", "protected:"+pane.Name
				case hs.host.Jobs == "":
					rec.Result, rec.Reason = "skipped", "jobs-dir-unset"
				case !reap.ValidJobName(pane.Name):
					rec.Result, rec.Reason = "skipped", "job-name-invalid"
				default:
					// Only a pane whose job dir already exists counts as a
					// fleet job: an operator pane merely cd'd into a merged
					// worktree must never be closed.
					dir := hs.host.JobDir(pane.Name)
					isJob, e := hs.exec.DirExists(ctx, dir)
					switch {
					case e != nil:
						rec.Result, rec.Reason = "error", e.Error()
						errs = append(errs, fmt.Errorf("%s pane %s: %w", hs.host.Name, pane.ID, e))
					case !isJob:
						rec.Result, rec.Reason = "skipped", "not-a-job"
					default:
						rec.Job = pane.Name
						// The pane's output must be saved before the session
						// can go. report.md counts only when it is newer than
						// the merge — a stale file could belong to a reused
						// job name — otherwise the last screen is saved.
						report := filepath.Join(dir, "report.md")
						exists, e := hs.exec.Exists(ctx, report)
						fresh := false
						if e == nil && exists && mergedAt != nil {
							fresh, e = hs.exec.NewerThan(ctx, report, mergedAt.Unix())
						}
						switch {
						case e != nil:
							rec.Result, rec.Reason = "error", e.Error()
							errs = append(errs, fmt.Errorf("%s pane %s: %w", hs.host.Name, pane.ID, e))
						case fresh:
							rec.Saved = report
						case !acting:
							rec.Saved = filepath.Join(dir, "pane-screen.txt")
						default:
							if rec.Saved, e = hs.exec.Screen(ctx, pane.ID, dir); e != nil {
								rec.Result, rec.Reason = "error", e.Error()
								errs = append(errs, fmt.Errorf("%s pane %s: %w", hs.host.Name, pane.ID, e))
							}
						}
					}
					if rec.Result == "" {
						if !acting {
							rec.Result = "would-close"
							hs.closed[pane.ID] = true
						} else {
							rec.Result, rec.Reason = s.reclose(ctx, hs, pane)
							if rec.Result == "closed" {
								hs.closed[pane.ID] = true
							} else if rec.Result == "error" {
								errs = append(errs, fmt.Errorf("%s pane %s: %s", hs.host.Name, pane.ID, rec.Reason))
							}
						}
					}
				}
				note(rec)
			}
		}

		if mergedAt == nil || now.Sub(*mergedAt) < after {
			continue
		}

		type cand struct {
			hs   *reapHost
			path string
		}
		var cands []cand
		seen := map[string]bool{}
		add := func(hs *reapHost, path string) {
			if hs == nil || hs.failed || path == "" {
				return
			}
			k := hs.host.Name + "\x00" + path
			if !seen[k] {
				seen[k] = true
				cands = append(cands, cand{hs, path})
			}
		}
		for _, hs := range hosts {
			for _, pane := range hs.panes {
				if w := hs.trees[pane.Cwd]; matches(w, p) {
					add(hs, w.Path)
				}
			}
		}
		for _, rec := range receipts {
			if rec.PR != p.Number {
				continue
			}
			path := rec.Worktree
			if path == "" {
				path = rec.Cwd
			}
			// A worktree already removed or proven gone is terminal: re-adding
			// it as a candidate would only log the same skip every pass. Live
			// pane matches are never suppressed.
			sig := lastRow[rowKey(reapReceipt{Action: "remove-worktree", PR: p.Number, Host: rec.Host, Worktree: path})]
			if sig == "removed|" || sig == "worktree-gone|" {
				continue
			}
			add(byName[rec.Host], path)
		}
		// A PR with no discovered worktree on any host leaves no rows and
		// costs no GitHub calls beyond the listing: the merge-commit and
		// revert checks only run when there is real work to gate.
		if len(cands) == 0 {
			continue
		}
		if p.MergeCommitSHA == "" {
			// The pulls list endpoint omits merge_commit_sha; resolve it for
			// the few PRs that actually name work.
			fp, e := s.GH.Pull(ctx, r.Name, p.Number)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d merge commit: %w", p.Number, e))
				continue
			}
			p.MergeCommitSHA = fp.MergeCommitSHA
		}
		if p.MergeCommitSHA == "" {
			note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: "merge-commit-unknown"})
			continue
		}
		watcher, ok := s.GH.(interface {
			WatchRevert(context.Context, string, int, string) (*github.Pull, error)
		})
		if !ok {
			note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: "revert-check-unavailable"})
			continue
		}
		recovery, e := watcher.WatchRevert(ctx, r.Name, p.Number, p.MergeCommitSHA)
		if e != nil {
			errs = append(errs, fmt.Errorf("PR %d revert check: %w", p.Number, e))
			continue
		}
		if recovery != nil {
			reason := ""
			if recovery.MergedAt != nil {
				// Same convention as the watch role and outcomes ledger: a
				// recovery PR merged within seven days reverted this PR.
				if at := parsedTime(*recovery.MergedAt); at != nil && !at.Before(*mergedAt) && !at.After(mergedAt.Add(7*24*time.Hour)) {
					reason = "reverted"
				}
			} else if recovery.State == "open" {
				reason = "recovery-open"
			}
			if reason != "" {
				note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: reason})
				continue
			}
		}

		for _, c := range cands {
			rec := reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Cwd: c.path, Worktree: c.path, Branch: p.Head.Ref}
			w, e := c.hs.exec.Inspect(ctx, c.path)
			switch {
			case e != nil:
				rec.Result, rec.Reason = "skipped", "worktree-inspect-error"
				errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, c.path, e))
			case w == nil:
				rec.Result, rec.Reason = "skipped", "worktree-gone"
			case w.Branch != p.Head.Ref || w.Head != p.Head.SHA || !strings.EqualFold(w.Repo, r.Name):
				rec.Result, rec.Reason = "skipped", "worktree-changed"
			}
			if rec.Result != "" {
				note(rec)
				continue
			}
			rec.Worktree = w.Path
			main := filepath.Dir(w.Common)
			entries, e := c.hs.exec.WorktreeList(ctx, main)
			if e != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, w.Path, e))
				continue
			}
			var entry *reap.Entry
			for i := range entries {
				if entries[i].Path == w.Path {
					entry = &entries[i]
				}
			}
			remove := false
			switch {
			case c.hs.usageUnknown:
				rec.Result, rec.Reason = "skipped", "pane-scan-failed"
			case entry == nil:
				rec.Result, rec.Reason = "skipped", "unregistered"
			case entry.Locked:
				rec.Result, rec.Reason = "skipped", "locked"
			case entry.Branch == "":
				rec.Result, rec.Reason = "skipped", "detached"
			default:
				clean, e := c.hs.exec.Clean(ctx, w.Path)
				var ignored []string
				if e == nil {
					ignored, e = c.hs.exec.Ignored(ctx, w.Path)
				}
				var unsafe []string
				for _, f := range ignored {
					if !reap.Regenerable(s.Config.Reap.Regenerable, f) {
						unsafe = append(unsafe, f)
					}
				}
				switch {
				case e != nil:
					rec.Result, rec.Reason = "skipped", "status-check-failed"
					errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, w.Path, e))
				case !clean:
					rec.Result, rec.Reason = "skipped", "dirty"
				case len(unsafe) > 0:
					// `status --porcelain` hides gitignored files but
					// `worktree remove` deletes them: regenerable output is
					// expendable, anything else refuses and is named in files.
					rec.Files = unsafe
					rec.Result, rec.Reason = "skipped", "ignored-files:"+filesSig(unsafe)
				case c.hs.paneInUse(w.Path):
					rec.Result, rec.Reason = "skipped", "pane-in-use"
				case !c.hs.exec.Pushed(ctx, main, entry.Branch, r.Branch):
					rec.Result, rec.Reason = "skipped", "unmerged-or-unpushed"
				case !acting:
					rec.Result = "would-remove"
					remove = true
				default:
					// The pane list is a stale snapshot by now; a worker
					// spawned into the worktree after discovery must stop
					// the removal. Re-scan first, then re-check.
					switch e := c.hs.refreshPanes(ctx); {
					case e != nil:
						rec.Result, rec.Reason = "skipped", "pane-scan-failed"
						errs = append(errs, fmt.Errorf("%s pane list: %w", c.hs.host.Name, e))
					case c.hs.paneInUse(w.Path):
						rec.Result, rec.Reason = "skipped", "pane-in-use"
					default:
						if e := c.hs.exec.RemoveWorktree(ctx, main, w.Path); e != nil {
							rec.Result, rec.Reason = "error", e.Error()
							errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, w.Path, e))
						} else {
							rec.Result = "removed"
							remove = true
						}
					}
				}
			}
			note(rec)
			if !remove {
				continue
			}
			// -d only: git itself refuses a branch that never merged anywhere.
			brec := reapReceipt{Action: "delete-branch", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Worktree: w.Path, Branch: entry.Branch}
			if acting {
				if e := c.hs.exec.DeleteBranch(ctx, main, entry.Branch); e != nil {
					brec.Result, brec.Reason = "skipped", "refused: "+e.Error()
				} else {
					brec.Result = "deleted"
				}
			} else {
				brec.Result = "would-delete"
			}
			note(brec)
		}
	}
	if s.Config.Reap.Archive.Enabled {
		errs = append(errs, s.reapArchive(ctx, r, prNum, receipts, byName, acting, note)...)
	}
	result := struct {
		Action   string `json:"action"`
		Repo     string `json:"repo"`
		Shadow   bool   `json:"shadow"`
		Scanned  int    `json:"prs_scanned"`
		Closed   int    `json:"closed"`
		Planned  int    `json:"planned"`
		Removed  int    `json:"removed"`
		Deleted  int    `json:"branches_deleted"`
		Archived int    `json:"jobs_archived"`
		Skipped  int    `json:"skipped"`
		ErrCount int    `json:"errors"`
	}{"reap", r.Name, !acting, scanned, closed, planned, removed, deleted, archived, skipped, len(errs)}
	return errors.Join(append(errs, s.audit(r.Name, "reap", result, o.DryRun))...)
}
