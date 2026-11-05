package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The legacy sweep is a one-shot, conservative pass over worktrees the daemon's
// reap never sees again: reap.scan_after bounds which merged PRs it looks at,
// so a worktree whose merge is older than that stays forever.
//
// The node has no worktree listing, so candidates come from the binding reap's
// job path already uses: every closed PR of the repo (all time) → its
// Mergewake-Task → the bound job → the job record's host and cwd. Anything
// outside that binding (a PR with no trailer, a task with no job, a job with no
// record) is not reachable; the sweep counts those per repo and removes
// nothing. Removal goes only through the node's guarded worktree.remove: the
// node decides cleanliness, ignored files and identity at the moment of
// removal, so a dry run never probes it and a dirty worktree is answered by the
// node's own refusal.

const (
	legacyAction = "legacy-sweep"
	// legacyMinAge is how long a merge must have stood before its worktree is
	// removed. It is deliberately not a config key: a longer wait than
	// reap.worktree_after is this command's whole point.
	legacyMinAge = 14 * 24 * time.Hour
	// legacyGuardNote is the would-remove reason of a plan: every rule the
	// sweep can check without touching the worktree passed.
	legacyGuardNote = "node guard decides cleanliness"
)

func legacyRow(v reapReceipt) bool { return v.Action == legacyAction }

// legacyClosedLister and legacyOpenLister are the GitHub reads the sweep needs
// beyond the Service's GitHub interface; the real client has both.
type legacyClosedLister interface {
	AllClosedPulls(context.Context, string) ([]github.Pull, error)
}

type legacyRevertWatcher interface {
	WatchRevert(context.Context, string, int, string) (*github.Pull, error)
}

// legacyCand is one worktree path on one host that some PR's bound job worked
// in.
type legacyCand struct {
	host string // configured host name
	cwd  string
	job  string
	task string
}

// legacyReach counts what the binding could not reach, per repo.
type legacyReach struct {
	ClosedPRs    int            `json:"closed_prs"`
	Reached      int            `json:"reached_prs"`
	Unreachable  map[string]int `json:"unreachable"`
	BoundJobsBy  map[string]int `json:"bound_jobs_by_host"`
	Worktrees    int            `json:"worktrees"`
	boundJobSeen map[string]bool
}

func (g *legacyReach) miss(reason string) { g.Unreachable[reason]++ }

// legacyTally counts a host's rows by result and reason.
type legacyTally map[string]map[string]int

func (t legacyTally) add(result, reason string) {
	if t[result] == nil {
		t[result] = map[string]int{}
	}
	t[result][reason]++
}

// legacyVerdict maps the node's worktree.remove answer to a receipt result. A
// refusal that says "try again" (busy, a pane, an old git) or "already gone" is
// kept, so a rerun re-evaluates it; every other refusal is final: a rerun
// never forces it.
func legacyVerdict(v reap.WorktreeRemoveResult) (result, reason string) {
	switch v.Outcome {
	case "removed":
		return "removed", ""
	case "worktree-gone":
		return "kept", "gone"
	case "busy", "pane-in-use", "pane-scan-failed", "git_too_old":
		return "kept", v.Outcome
	case "ignored-files":
		return "refused", "ignored-files:" + filesSig(v.Files)
	case "worktree-changed", "outside-work-root", "unregistered", "locked", "detached", "status-check-failed", "dirty", "unmerged-or-unpushed", "git-failed":
		return "refused", v.Outcome
	default:
		return "refused", "cleanup-unknown:" + v.Outcome
	}
}

// ReapLegacy runs the one-time sweep for repo r over one configured host
// (host non-empty) or all of them, one host at a time. Without --apply, or
// without reap.act, it plans: rows say would-remove and nothing is removed.
// Plans and results both land in <state>/<repo>/reap.jsonl as legacy-sweep
// rows. The daemon's Reap ignores those rows.
func (s *Service) ReapLegacy(ctx context.Context, r config.Repo, host string, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "reap-legacy-error", map[string]any{"error": retErr.Error()}, o.DryRun))
		}
	}()
	if !s.Config.Reap.Enabled {
		return errors.New("reap requires reap.enabled in host config")
	}
	if len(s.Config.Hosts) == 0 {
		return errors.New("reap requires configured hosts")
	}
	jobs, ok := s.Sink.(reapJobs)
	if !ok {
		return errors.New("legacy sweep needs the hk jobs reader to bind PRs to worktrees")
	}
	closedLister, ok := s.GH.(legacyClosedLister)
	if !ok {
		return errors.New("closed PR listing unavailable")
	}
	openLister, ok := s.GH.(interface {
		OpenPulls(context.Context, string, string) ([]github.Pull, error)
	})
	if !ok {
		return errors.New("open PR listing unavailable")
	}
	watcher, ok := s.GH.(legacyRevertWatcher)
	if !ok {
		return errors.New("revert check unavailable")
	}
	var wanted []reap.Host
	for _, h := range s.Config.Hosts {
		if host == "" || h.Name == host || h.MachineID() == host {
			wanted = append(wanted, h)
		}
	}
	if len(wanted) == 0 {
		return fmt.Errorf("--host %q is not a configured host", host)
	}
	acting := o.Acting() && s.Config.Reap.Act

	receipts, e := s.reapReceipts(r.Name)
	if e != nil {
		return e
	}
	// lastRow is the newest result per worktree, so a rerun skips what is
	// already removed or refused and writes only rows whose outcome changed.
	lastRow := map[string]string{}
	for _, rec := range receipts {
		if legacyRow(rec) {
			lastRow[legacyKey(rec.Host, rec.Worktree)] = rec.Result + "|" + rec.Reason
		}
	}

	closed, e := closedLister.AllClosedPulls(ctx, r.Name)
	if e != nil {
		return fmt.Errorf("list closed PRs: %w", e)
	}
	open, e := openLister.OpenPulls(ctx, r.Name, "")
	if e != nil {
		return fmt.Errorf("list open PRs: %w", e)
	}
	sort.Slice(closed, func(i, j int) bool { return closed[i].Number < closed[j].Number })
	// newest closed PR of this repo per head branch, whether or not it is bound
	// to a job: a newer closed-unmerged PR on a reused branch must keep the
	// worktree of an older merged one.
	newest := map[string]*github.Pull{}
	for i := range closed {
		if q := &closed[i]; sameRepoHead(*q, r.Name) {
			newest[q.Head.Ref] = q
		}
	}

	var errs []error
	reach := legacyReach{ClosedPRs: len(closed), Unreachable: map[string]int{}, BoundJobsBy: map[string]int{}, boundJobSeen: map[string]bool{}}
	byHost := map[string]map[string]*legacyCand{} // host name → cwd → candidate
	records := map[string]*integrations.JobRecord{}
	for _, p := range closed {
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		b, e := s.reapBind(ctx, r, p)
		switch {
		case e != nil:
			reach.miss("binding-error")
			errs = append(errs, fmt.Errorf("PR %d job binding: %s", p.Number, integrations.Detail(e)))
			continue
		case b.Task == "":
			reach.miss("no-task-trailer")
			continue
		case b.Job == "":
			reach.miss("no-bound-job")
			continue
		}
		rec, seen := records[b.Job]
		if !seen {
			job, e := jobs.Job(ctx, b.Job)
			switch {
			case errors.Is(e, integrations.ErrJobNotFound):
			case e != nil:
				errs = append(errs, fmt.Errorf("job %s: %s", b.Job, integrations.Detail(e)))
				continue // not cached: a read error is not "no record"
			default:
				rec = &job
			}
			records[b.Job] = rec
		}
		if rec == nil {
			reach.miss("no-job-record")
			continue
		}
		if rec.Cwd == "" {
			reach.miss("job-without-cwd")
			continue
		}
		var hostName string
		for _, h := range s.Config.Hosts {
			if rec.Host != "" && (h.Name == rec.Host || h.MachineID() == rec.Host) {
				hostName = h.Name
			}
		}
		if hostName == "" {
			reach.miss("job-host-unconfigured")
			continue
		}
		reach.Reached++
		if !reach.boundJobSeen[hostName+"\x00"+b.Job] {
			reach.boundJobSeen[hostName+"\x00"+b.Job] = true
			reach.BoundJobsBy[hostName]++
		}
		if byHost[hostName] == nil {
			byHost[hostName] = map[string]*legacyCand{}
		}
		c := byHost[hostName][rec.Cwd]
		if c == nil {
			c = &legacyCand{host: hostName, cwd: rec.Cwd, job: b.Job, task: b.Task}
			byHost[hostName][rec.Cwd] = c
			reach.Worktrees++
		}
	}
	if e := s.Print(map[string]any{"action": legacyAction + "-reach", "repo": r.Name, "reach": reach}); e != nil {
		errs = append(errs, e)
	}

	hub, e := s.reapHub()
	if e != nil {
		return errors.Join(append(errs, e)...)
	}
	sweep := &legacySweep{s: s, r: r, o: o, acting: acting, lastRow: lastRow, newest: newest, open: openBranches(open, r.Name), openLister: openLister, watcher: watcher, ancestry: map[string]bool{}}
	for _, h := range wanted {
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		tally := legacyTally{}
		cands := byHost[h.Name]
		var hs *reapHost
		if len(cands) > 0 {
			var hostErrs []error
			hs, hostErrs = s.reapScanHost(ctx, hub, h)
			errs = append(errs, hostErrs...)
		}
		paths := make([]string, 0, len(cands))
		for p := range cands {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		for _, p := range paths {
			if e = ctx.Err(); e != nil {
				return errors.Join(append(errs, e)...)
			}
			errs = append(errs, sweep.candidate(ctx, hs, cands[p], tally)...)
		}
		summary := map[string]any{"action": legacyAction + "-summary", "repo": r.Name, "host": h.Name, "apply": acting,
			"worktrees": len(cands), "bound_jobs": reach.BoundJobsBy[h.Name], "results": tally}
		if e := s.Print(summary); e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(append(errs, s.audit(r.Name, "reap-legacy", map[string]any{"host": host, "apply": acting, "reach": reach}, o.DryRun))...)
}

// sameRepoHead reports whether PR p's head branch lives in repo itself. A fork's
// branch of the same name, or a fork that was deleted (no head repository),
// is not the repository's branch.
func sameRepoHead(p github.Pull, repo string) bool {
	return strings.EqualFold(p.Head.Repo.FullName, repo)
}

// openBranches is the set of the repo's own head branches that have an open PR.
func openBranches(open []github.Pull, repo string) map[string]bool {
	out := map[string]bool{}
	for _, p := range open {
		if sameRepoHead(p, repo) {
			out[p.Head.Ref] = true
		}
	}
	return out
}

func legacyKey(host, path string) string { return host + "\x00" + path }

// legacySweep carries one repo's pass state across its hosts.
type legacySweep struct {
	s       *Service
	r       config.Repo
	o       Options
	acting  bool
	lastRow map[string]string
	newest  map[string]*github.Pull // head branch → its newest closed PR
	open    map[string]bool         // head branches that had an open PR when the sweep began
	watcher legacyRevertWatcher
	// openLister re-reads open PRs right before a removal.
	openLister interface {
		OpenPulls(context.Context, string, string) ([]github.Pull, error)
	}
	ancestry map[string]bool
}

// candidate evaluates and records one worktree. It returns the errors that
// should fail the run; every evaluation, even a kept one, leaves a row.
func (w *legacySweep) candidate(ctx context.Context, hs *reapHost, c *legacyCand, tally legacyTally) []error {
	// A removed worktree is finished and a refused one is never retried on its
	// own: neither is inspected again. This is the cheap check by the job's cwd;
	// evaluate repeats it by the inspected worktree root, which is what rows
	// are keyed by when the cwd sits inside the worktree.
	if prior := w.prior(c.host, c.cwd); prior != "" {
		tally.add("already-"+prior, "")
		return nil
	}
	rec := reapReceipt{Action: legacyAction, Repo: w.r.Name, Host: c.host, Cwd: c.cwd, Worktree: c.cwd, Job: c.job, Task: c.task}
	var errs []error
	switch skip := w.evaluate(ctx, hs, c, &rec, &errs); skip {
	case "":
	case "reviewer-checkout":
		tally.add("out-of-scope", skip)
		return errs
	default: // already-removed, already-refused
		tally.add(skip, "")
		return errs
	}
	tally.add(rec.Result, strings.SplitN(rec.Reason, ":", 2)[0])
	if e := w.note(rec); e != nil {
		errs = append(errs, e)
	}
	return errs
}

// prior is "removed" or "refused" when the worktree's newest row is final.
func (w *legacySweep) prior(host, path string) string {
	switch r, _, _ := strings.Cut(w.lastRow[legacyKey(host, path)], "|"); r {
	case "removed", "refused":
		return r
	}
	return ""
}

// note prints a row and appends it to reap.jsonl unless it repeats the
// worktree's newest outcome. Plans are written too: they are the list the
// operator reviews before the apply.
func (w *legacySweep) note(v reapReceipt) error {
	v.Time = w.s.now().UTC()
	key, sig := legacyKey(v.Host, v.Worktree), v.Result+"|"+v.Reason
	repeat := w.lastRow[key] == sig
	w.lastRow[key] = sig
	// The receipt is the durable record, so it is written before the row is
	// printed: a failing stdout must not lose a removal.
	var werr error
	if !repeat {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		werr = appendLine(w.s.reapLog(v.Repo), b)
	}
	return errors.Join(werr, w.s.Print(v))
}

// evaluate fills rec with the verdict for one worktree, in the order the
// cheap rules come before GitHub calls and the node is asked last. It returns
// a non-empty skip for a worktree that gets no row: "reviewer-checkout" is out
// of scope, "already-removed" and "already-refused" are final rows of an
// earlier run found under the inspected worktree root.
func (w *legacySweep) evaluate(ctx context.Context, hs *reapHost, c *legacyCand, rec *reapReceipt, errs *[]error) (skip string) {
	keep := func(reason string) { rec.Result, rec.Reason = "kept", reason }
	if hs == nil || hs.failed {
		keep("host-unreadable")
		return ""
	}
	wt, e := hs.ops.Inspect(ctx, c.cwd)
	switch {
	case e != nil:
		rec.Result, rec.Reason = "error", "worktree-inspect-error"
		*errs = append(*errs, fmt.Errorf("%s %s: %w", c.host, c.cwd, e))
		return ""
	case wt == nil:
		keep("gone")
		return ""
	}
	// Reviewer checkouts (mwr-*) belong to the reviewer sweep, attached or not.
	if strings.HasPrefix(filepath.Base(wt.Path), "mwr-") {
		return "reviewer-checkout"
	}
	rec.Worktree, rec.Branch, rec.Head = wt.Path, wt.Branch, wt.Head
	// Rows are keyed by the worktree root the node reported, which differs from
	// the job's cwd when the job worked in a subdirectory.
	if prior := w.prior(c.host, wt.Path); prior != "" {
		return "already-" + prior
	}
	switch {
	case wt.Repo == "":
		keep("repo-unknown")
		return ""
	case !strings.EqualFold(wt.Repo, w.r.Name):
		keep("other-repo")
		return ""
	case wt.Branch == "":
		keep("detached")
		return ""
	case wt.Head == "":
		keep("head-unknown")
		return ""
	}
	// The PR of this branch is the newest closed one on it, bound to a job or
	// not; a fork's branch of the same name is not this repository's.
	p := w.newest[wt.Branch]
	if p == nil {
		keep("no-pr")
		return ""
	}
	rec.PR = p.Number
	var mergedAt *time.Time
	if p.MergedAt != nil {
		mergedAt = parsedTime(*p.MergedAt)
	}
	switch {
	case w.open[wt.Branch]:
		keep("open-pr")
	case !(p.Merged || p.MergedAt != nil):
		keep("closed-unmerged")
	case p.Base.Ref != w.r.Branch:
		keep("base-mismatch")
	case mergedAt == nil || p.Head.SHA == "":
		keep("unclassified")
	case w.s.now().Sub(*mergedAt) < legacyMinAge:
		keep("too-young")
	default:
		w.merged(ctx, hs, wt, *p, *mergedAt, rec, errs)
	}
	return ""
}

// merged runs the rules that apply to a merged PR old enough to sweep.
func (w *legacySweep) merged(ctx context.Context, hs *reapHost, wt *reap.Worktree, p github.Pull, mergedAt time.Time, rec *reapReceipt, errs *[]error) {
	keep := func(reason string) { rec.Result, rec.Reason = "kept", reason }
	// Every commit in the worktree must already be in the merged PR or in the
	// base: the PR head itself, or an ancestor of it or of the base.
	if wt.Head != p.Head.SHA {
		ok := false
		for _, newer := range []string{p.Head.SHA, w.r.Branch} {
			k := wt.Head + "\x00" + newer
			a, seen := w.ancestry[k]
			if !seen {
				var e error
				a, e = w.s.GH.Ancestor(ctx, w.r.Name, wt.Head, newer)
				switch {
				case github.NotFound(e):
					a = false
				case e != nil:
					*errs = append(*errs, fmt.Errorf("PR %d head ancestry: %w", p.Number, e))
					keep("head-check-failed")
					return
				}
				w.ancestry[k] = a
			}
			if a {
				ok = true
				break
			}
		}
		if !ok {
			keep("head-moved")
			return
		}
	}
	switch {
	case hs.usageUnknown:
		keep("pane-scan-failed")
		return
	case hs.paneInUse(wt.Path):
		keep("pane-in-use")
		return
	}
	if p.MergeCommitSHA == "" {
		fp, e := w.s.GH.Pull(ctx, w.r.Name, p.Number)
		if e != nil {
			*errs = append(*errs, fmt.Errorf("PR %d merge commit: %w", p.Number, e))
			keep("merge-commit-unknown")
			return
		}
		p.MergeCommitSHA = fp.MergeCommitSHA
	}
	if p.MergeCommitSHA == "" {
		keep("merge-commit-unknown")
		return
	}
	recovery, e := w.watcher.WatchRevert(ctx, w.r.Name, p.Number, p.MergeCommitSHA)
	switch {
	case e != nil:
		*errs = append(*errs, fmt.Errorf("PR %d revert check: %w", p.Number, e))
		keep("revert-check-failed")
		return
	case recovery != nil && recovery.State == "open":
		keep("recovery-open")
		return
	case recovery != nil && recovery.MergedAt != nil:
		if at := parsedTime(*recovery.MergedAt); at != nil && !at.Before(mergedAt) && !at.After(mergedAt.Add(7*24*time.Hour)) {
			keep("reverted")
			return
		}
	}
	// The one removal path is the node's guarded worktree.remove.
	remover, ok := hs.ops.(interface {
		WorktreeRemove(context.Context, string, string, string, []string, bool) (reap.WorktreeRemoveResult, error)
	})
	if !ok {
		keep("no-guarded-remove")
		return
	}
	if !w.acting {
		rec.Result, rec.Reason = "would-remove", legacyGuardNote
		return
	}
	// The pane list is a stale snapshot by now: re-read it, then re-check.
	if e := hs.refreshPanes(ctx); e != nil {
		*errs = append(*errs, fmt.Errorf("%s pane list: %w", hs.host.Name, e))
		keep("pane-scan-failed")
		return
	}
	if hs.paneInUse(wt.Path) {
		keep("pane-in-use")
		return
	}
	// A PR may have opened on the branch since the sweep began.
	open, e := w.openLister.OpenPulls(ctx, w.r.Name, "")
	if e != nil {
		*errs = append(*errs, fmt.Errorf("%s open PRs: %w", w.r.Name, e))
		keep("open-pr-check-failed")
		return
	}
	if openBranches(open, w.r.Name)[wt.Branch] {
		keep("open-pr")
		return
	}
	// The PR was chosen from the branch seen at the first inspection; the
	// removal request carries only the head, so confirm the checkout is still
	// that branch at that head.
	cur, e := hs.ops.Inspect(ctx, wt.Path)
	switch {
	case e != nil:
		rec.Result, rec.Reason = "error", "worktree-inspect-error"
		*errs = append(*errs, fmt.Errorf("%s %s: %w", hs.host.Name, wt.Path, e))
		return
	case cur == nil:
		keep("gone")
		return
	case cur.Path != wt.Path || cur.Branch != wt.Branch || cur.Head != wt.Head:
		keep("worktree-changed")
		return
	}
	result, e := remover.WorktreeRemove(ctx, wt.Path, wt.Head, w.r.Branch, w.s.Config.Reap.Regenerable, false)
	if e != nil {
		rec.Result, rec.Reason = "error", e.Error()
		*errs = append(*errs, fmt.Errorf("%s %s: %w", hs.host.Name, wt.Path, e))
		return
	}
	rec.Files = result.Files
	rec.Result, rec.Reason = legacyVerdict(result)
}
