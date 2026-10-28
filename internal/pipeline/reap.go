package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reapReceipt is one durable row of <state>/<repo>/reap.jsonl. Rows are
// written in acting and shadow modes; a "would-*" row still names the worktree
// so a later pass can find it. Dry-run prints rows without writing the file.
type reapReceipt struct {
	Time     time.Time `json:"time"`
	Action   string    `json:"action"` // close-pane, remove-worktree, delete-branch
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
	v.Time = time.Now().UTC()
	if e := s.Print(v); e != nil {
		return e
	}
	if dry {
		return nil
	}
	dir := s.state(v.Repo)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(s.reapLog(v.Repo), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = f.Write(append(b, '\n'))
	if e == nil {
		e = f.Sync()
	}
	return e
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
	if len(s.Config.Hosts) == 0 {
		return errors.New("reap requires configured hosts")
	}
	now := time.Now()
	acting := !o.DryRun && !o.Shadow
	var errs []error

	receipts, e := s.reapReceipts(r.Name)
	if e != nil {
		return e
	}
	pulls := []github.Pull{}
	if prNum > 0 {
		p, e := s.GH.Pull(ctx, r.Name, prNum)
		if e != nil {
			return e
		}
		pulls = append(pulls, p)
	} else {
		batch, e := s.GH.MergedPulls(ctx, r.Name, r.Branch, now.Add(-2*after))
		if e != nil {
			return e
		}
		known := map[int]bool{}
		for _, p := range batch {
			known[p.Number] = true
			pulls = append(pulls, p)
		}
		// Receipt-named PRs can be older than the listing horizon.
		for _, rec := range receipts {
			if rec.PR == 0 || known[rec.PR] {
				continue
			}
			known[rec.PR] = true
			p, e := s.GH.Pull(ctx, r.Name, rec.PR)
			if e != nil {
				return e
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

	scanned, closed, planned, removed, deleted, skipped, failed := 0, 0, 0, 0, 0, 0, 0
	// lastRow remembers the newest logged result per receipt identity so a
	// repeat pass only writes rows whose outcome changed: a shadow run logging
	// the same would-* every poll, or a removed worktree logging worktree-gone
	// forever, would otherwise grow reap.jsonl without bound.
	rowKey := func(v reapReceipt) string {
		path := v.Pane
		if v.Action != "close-pane" && v.Worktree != "" {
			path = v.Worktree
		}
		return fmt.Sprintf("%s|%d|%s|%s", v.Action, v.PR, v.Host, path)
	}
	rowSig := func(v reapReceipt) string { return v.Result + "|" + v.Reason }
	lastRow := map[string]string{}
	for _, rec := range receipts {
		lastRow[rowKey(rec)] = rowSig(rec)
	}
	note := func(v reapReceipt) {
		switch v.Result {
		case "closed":
			closed++
		case "would-close", "would-remove", "would-delete":
			planned++
		case "removed":
			removed++
		case "deleted":
			deleted++
		case "skipped":
			skipped++
		case "error":
			failed++
		}
		// Every row prints; only changed outcomes append to the receipt file.
		repeat := lastRow[rowKey(v)] == rowSig(v)
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
				case pane.Status == "working":
					rec.Result, rec.Reason = "skipped", "working"
				case pane.Name == "":
					rec.Result, rec.Reason = "skipped", "unnamed"
				case reap.Protected(pane.Name, s.Config.Reap.Protect):
					rec.Result, rec.Reason = "skipped", "protected:"+pane.Name
				case hs.host.Jobs == "":
					rec.Result, rec.Reason = "skipped", "jobs-dir-unset"
				case !reap.ValidJobName(pane.Name):
					rec.Result, rec.Reason = "skipped", "job-name-invalid"
				default:
					// The pane's report or last screen must be saved before the
					// session can go. report.md wins; otherwise save the screen.
					dir := hs.host.JobDir(pane.Name)
					report := filepath.Join(dir, "report.md")
					exists, e := hs.exec.Exists(ctx, report)
					switch {
					case e != nil:
						rec.Result, rec.Reason = "error", e.Error()
						errs = append(errs, fmt.Errorf("%s pane %s: %w", hs.host.Name, pane.ID, e))
					case exists:
						rec.Saved = report
					case !acting:
						rec.Saved = filepath.Join(dir, "pane-screen.txt")
					default:
						if rec.Saved, e = hs.exec.Capture(ctx, pane.ID, dir); e != nil {
							rec.Result, rec.Reason = "error", e.Error()
							errs = append(errs, fmt.Errorf("%s pane %s: %w", hs.host.Name, pane.ID, e))
						}
					}
					if rec.Result == "" {
						if !acting {
							rec.Result = "would-close"
							hs.closed[pane.ID] = true
						} else if e := hs.exec.Close(ctx, pane.ID); e != nil {
							rec.Result, rec.Reason = "error", e.Error()
							errs = append(errs, fmt.Errorf("%s pane %s: %w", hs.host.Name, pane.ID, e))
						} else {
							rec.Result = "closed"
							hs.closed[pane.ID] = true
						}
					}
				}
				note(rec)
			}
		}

		var mergedAt *time.Time
		if p.MergedAt != nil {
			mergedAt = parsedTime(*p.MergedAt)
		}
		if mergedAt == nil || now.Sub(*mergedAt) < after {
			continue
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
		for _, c := range cands {
			rec := reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Cwd: c.path, Branch: p.Head.Ref}
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
				switch {
				case e != nil:
					errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, w.Path, e))
				case !clean:
					rec.Result, rec.Reason = "skipped", "dirty"
				case c.hs.paneInUse(w.Path):
					rec.Result, rec.Reason = "skipped", "pane-in-use"
				case !c.hs.exec.Pushed(ctx, main, entry.Branch, r.Branch):
					rec.Result, rec.Reason = "skipped", "unmerged-or-unpushed"
				case !acting:
					rec.Result = "would-remove"
					remove = true
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
	result := struct {
		Action   string `json:"action"`
		Repo     string `json:"repo"`
		Shadow   bool   `json:"shadow"`
		Scanned  int    `json:"prs_scanned"`
		Closed   int    `json:"closed"`
		Planned  int    `json:"planned"`
		Removed  int    `json:"removed"`
		Deleted  int    `json:"branches_deleted"`
		Skipped  int    `json:"skipped"`
		ErrCount int    `json:"errors"`
	}{"reap", r.Name, !acting, scanned, closed, planned, removed, deleted, skipped, failed + len(errs)}
	return errors.Join(append(errs, s.audit(r.Name, "reap", result, o.DryRun))...)
}
