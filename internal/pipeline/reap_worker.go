package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The worker-worktree pass removes a worker's checkout once its work is on
// GitHub. Discovery is the host's own `git worktree list`: any worktree on a
// wk/<job> branch is a candidate, whether or not a pane, an earlier receipt or
// a job record ever named it. A merged PR's worktree goes when the watch
// verdict for its merge commit is healthy, or when a revert's own follow-up is
// green; an unmerged one goes when its PR is closed (or absent) and the hk job
// is terminal. Every removal goes through the host's guarded removal, and
// every refusal leaves one row naming its reason.

// workerBranchPrefix marks a worker's own worktree branch. The job name after
// the slash binds the checkout to its hk job record.
const workerBranchPrefix = "wk/"

// workerSource is the receipt Source of a row this pass wrote.
const workerSource = "worktree-list"

// workerRows counts one pass's work.
type workerRows struct {
	Candidates int
	Removed    int
	Kept       map[string]int
}

// reapWorkerWorktrees is one repo's worker-worktree pass over every host.
func (s *Service) reapWorkerWorktrees(ctx context.Context, r config.Repo, hosts []*reapHost, receipts []reapReceipt, records *passJobRecords, h *reapHistory, acting bool, note func(reapReceipt)) []error {
	after, e := config.OptionalDuration(s.Config.Reap.WorktreeAfter)
	if e != nil {
		return []error{e}
	}
	w := &workerPass{s: s, r: r, records: records, acting: acting, now: s.now(), after: after, note: note,
		healthyCache: map[string]bool{}, ancestry: map[string]bool{}, last: workerHistory(receipts)}
	// Discovery comes first and is the only host read this pass costs when no
	// worker worktree exists: the branch inventories below page every closed
	// PR, so a quiet repo must never pay for them.
	order := workerHostOrder(s.Config, hosts)
	var errs []error
	cands := map[*reapHost][]workerCand{}
	total := 0
	for _, hs := range order {
		if e := ctx.Err(); e != nil {
			errs = append(errs, e)
			break
		}
		if hs.failed {
			continue
		}
		found, listErrs := w.discover(ctx, hs)
		errs = append(errs, listErrs...)
		cands[hs] = found
		total += len(found)
	}
	if total == 0 {
		return errs
	}
	closedLister, okClosed := s.GH.(legacyClosedLister)
	openLister, okOpen := s.GH.(interface {
		OpenPulls(context.Context, string, string) ([]github.Pull, error)
	})
	// Without the branch inventories the pass cannot decide any candidate, so
	// every candidate keeps its row rather than reading as unmerged.
	if !okClosed || !okOpen {
		return errs
	}
	closed, e := closedLister.AllClosedPulls(ctx, r.Name)
	if e != nil {
		return append(errs, fmt.Errorf("worker worktrees: closed PR listing: %w", e))
	}
	open, e := openLister.OpenPulls(ctx, r.Name, "")
	if e != nil {
		return append(errs, fmt.Errorf("worker worktrees: open PR listing: %w", e))
	}
	// The newest closed PR of this repository per head branch decides, whether
	// or not it merged: a newer closed-unmerged PR on a reused branch keeps the
	// worktree of an older merged one.
	newest := map[string]*github.Pull{}
	for i := range closed {
		p := &closed[i]
		if !sameRepoHead(*p, r.Name) {
			continue
		}
		if prev := newest[p.Head.Ref]; prev == nil || prev.Number < p.Number {
			newest[p.Head.Ref] = p
		}
	}
	openSet := openBranches(open, r.Name)
	for _, hs := range order {
		if e := ctx.Err(); e != nil {
			errs = append(errs, e)
			break
		}
		drain := s.Config.Reap.WorktreeDrain.DrainPerHost(config.DefaultWorktreeDrainPerHost)
		rows := &workerRows{Kept: map[string]int{}}
		rows.Candidates = len(cands[hs])
		for _, c := range cands[hs] {
			if e := ctx.Err(); e != nil {
				errs = append(errs, e)
				break
			}
			if drain <= 0 {
				// The cap only bounds removals; the rest wait for the next
				// pass, so a host's backlog drains in bounded steps.
				continue
			}
			rec := w.evaluate(ctx, hs, c, newest, openSet, &errs)
			if rec == nil {
				continue
			}
			note(*rec)
			if rec.Result == "removed" || rec.Result == "would-remove" {
				rows.Removed++
				drain--
			} else {
				rows.Kept[rec.Result+"|"+strings.SplitN(rec.Reason, ":", 2)[0]]++
			}
		}
		if e := s.Print(map[string]any{"action": "worker-worktrees", "repo": r.Name, "host": hs.host.Name,
			"apply": acting, "candidates": rows.Candidates, "removed": rows.Removed, "kept": rows.Kept}); e != nil {
			errs = append(errs, e)
		}
	}
	return errs
}

// workerHostOrder visits the hosts in reap.worktree_drain.host_order first,
// then in config order, the way the reviewer sweep visits its queue.
func workerHostOrder(c config.Config, hosts []*reapHost) []*reapHost {
	ordered := make([]*reapHost, 0, len(hosts))
	seen := map[string]bool{}
	for _, name := range c.Reap.WorktreeDrain.HostOrder {
		for _, hs := range hosts {
			if hs.host.Name == name && !seen[name] {
				ordered = append(ordered, hs)
				seen[name] = true
			}
		}
	}
	for _, hs := range hosts {
		if !seen[hs.host.Name] {
			ordered = append(ordered, hs)
		}
	}
	return ordered
}

// workerPass is one repo's pass state: the ancestry and watch-verdict answers
// it reads at most once each.
type workerPass struct {
	s       *Service
	r       config.Repo
	records *passJobRecords
	acting  bool
	note    func(reapReceipt)
	// last is each path's newest worker-pass row, so a finished checkout is
	// not inspected again while a replacement at the same path is.
	last         map[string]workerLast
	procOnce     sync.Once
	procEv       procEvidence
	procErr      error
	now          time.Time
	after        time.Duration
	healthyCache map[string]bool
	ancestry     map[string]bool
}

// workerLast is one path's newest worker-pass row.
type workerLast struct {
	sig    string
	branch string
	head   string
}

// workerHistory replays the worker-pass rows per host and path.
func workerHistory(receipts []reapReceipt) map[string]workerLast {
	out := map[string]workerLast{}
	for _, rec := range receipts {
		if rec.Action != "remove-worktree" {
			continue
		}
		path := rec.Worktree
		if path == "" {
			path = rec.Cwd
		}
		if path == "" {
			continue
		}
		out[rec.Host+"\x00"+path] = workerLast{sig: rec.Result + "|" + rec.Reason, branch: rec.Branch, head: rec.Head}
	}
	return out
}

// workerCand is one listed worktree on a worker branch.
type workerCand struct {
	path   string
	branch string
	head   string
	job    string
	common string // the clone the worktree was listed under
}

// discover lists every clone's registered worktrees on the host and keeps the
// worker branches. The clones are the common git dirs reap already knows from
// panes and job records, plus the configured clone itself
// (<work_root>/<repo>, and the local checkout on a local host), so a worktree
// no pane, receipt or job record names is still found.
func (w *workerPass) discover(ctx context.Context, hs *reapHost) ([]workerCand, []error) {
	lister, ok := hs.ops.(reap.WorktreeLister)
	if !ok {
		return nil, nil
	}
	var errs []error
	seenCommon := map[string]bool{}
	var clones []string
	add := func(path string) {
		if path == "" {
			return
		}
		p := filepath.Clean(path)
		if seenCommon[p] {
			return
		}
		seenCommon[p] = true
		clones = append(clones, p)
	}
	for _, t := range hs.trees {
		if t != nil && t.Common != "" && strings.EqualFold(t.Repo, w.r.Name) {
			add(filepath.Dir(t.Common))
		}
	}
	if hs.host.WorkRoot != "" {
		add(filepath.Join(hs.host.WorkRoot, repoSegment(w.r.Name)))
	}
	if hs.host.Local() && config.Expand(w.r.Root) != "" {
		add(config.Expand(w.r.Root))
	}
	sort.Strings(clones)
	seenPath := map[string]bool{}
	var out []workerCand
	for _, clone := range clones {
		rows, e := reap.ListWorktrees(ctx, lister, clone)
		if e != nil {
			// A repository not cloned on this host answers invalid args: that
			// is an absence, not a failure. Anything else is reported once.
			if reap.InvalidArgs(e) || isNotARepo(e) {
				continue
			}
			errs = append(errs, fmt.Errorf("%s worktree.list %s: %w", hs.host.Name, clone, e))
			continue
		}
		main := filepath.Clean(clone)
		for _, row := range rows {
			if row.Detached || !strings.HasPrefix(row.Branch, workerBranchPrefix) {
				continue
			}
			if filepath.Clean(row.Path) == main {
				continue
			}
			if strings.HasPrefix(filepath.Base(row.Path), "mwr-") || seenPath[row.Path] {
				continue
			}
			seenPath[row.Path] = true
			out = append(out, workerCand{path: row.Path, branch: row.Branch, head: row.Head,
				job: strings.TrimPrefix(row.Branch, workerBranchPrefix), common: clone})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, errs
}

// isNotARepo reports an Exec worktree list on a path git refuses (exit 128):
// the clone is not on this host.
func isNotARepo(err error) bool {
	var ex interface{ ExitCode() int }
	return errors.As(err, &ex) && ex.ExitCode() == 128
}

// repoSegment is the clone directory name panewire prepares under a host's
// work root: the last segment of owner/name.
func repoSegment(repo string) string {
	if i := strings.LastIndex(repo, "/"); i >= 0 {
		return repo[i+1:]
	}
	return repo
}

// evaluate decides one candidate and returns its row, or nil when an earlier
// terminal row already covers this checkout.
func (w *workerPass) evaluate(ctx context.Context, hs *reapHost, c workerCand, newest map[string]*github.Pull, open map[string]bool, errs *[]error) *reapReceipt {
	rec := reapReceipt{Action: "remove-worktree", Repo: w.r.Name, Host: hs.host.Name, Cwd: c.path,
		Worktree: c.path, Branch: c.branch, Head: c.head, Job: c.job, Source: workerSource}
	var p *github.Pull
	if q := newest[c.branch]; q != nil {
		rec.PR = q.Number
		p = q
	}
	// A checkout this pass already finished is not inspected again: the row
	// would only repeat. A replacement at the same path — another branch, or
	// the same branch at another head — is a new identity and is evaluated.
	if prev, ok := w.last[hs.host.Name+"\x00"+c.path]; ok && terminalWorktreeSig(prev.sig) &&
		prev.head != "" && prev.head == c.head && prev.branch == c.branch {
		return nil
	}
	keep := func(reason string) *reapReceipt {
		rec.Result, rec.Reason = "skipped", reason
		return &rec
	}
	// A pane or process in the checkout outranks every rule below: the row
	// says why it stays so hk 5168 can list it.
	if reason := w.inUse(ctx, hs, c.path); reason != "" {
		return keep(reason)
	}
	switch {
	case open[c.branch]:
		return keep("open-pr")
	case p != nil && (p.Merged || p.MergedAt != nil):
		if reason := w.merged(ctx, hs, c, p, errs); reason != "" {
			return keep(reason)
		}
	case p != nil:
		// A closed-unmerged PR or none at all: the job has to be over.
		if reason := w.unmerged(ctx, hs, c, p); reason != "" {
			return keep(reason)
		}
	default:
		if reason := w.unmerged(ctx, hs, c, nil); reason != "" {
			return keep(reason)
		}
	}
	return w.remove(ctx, hs, c, &rec, errs)
}

// procs scans this machine's processes once per pass.
func (w *workerPass) procs(ctx context.Context) (procEvidence, error) {
	w.procOnce.Do(func() {
		w.procEv, w.procErr = processScan(ctx, w.s.Runner)
	})
	return w.procEv, w.procErr
}

// inUse names the live pane or process holding path, or "" when none does.
// Processes are scanned only on this machine: a remote host's pane list
// already includes its plain shells, and no node command reads its processes.
func (w *workerPass) inUse(ctx context.Context, hs *reapHost, path string) string {
	if hs.usageUnknown {
		return "pane-scan-failed"
	}
	if hs.paneInUse(path) {
		return "pane-in-use"
	}
	if !hs.host.Local() {
		return ""
	}
	ev, e := w.procs(ctx)
	if e != nil {
		// No scan means no proof that no process holds the checkout.
		return "process-scan-unavailable"
	}
	held, unresolved := ev.heldPaths()
	for _, held := range held {
		if under(path, held) || under(held, path) {
			return "process-in-use"
		}
	}
	if len(unresolved) > 0 {
		return "process-unknown"
	}
	return ""
}

// under reports whether p sits inside root.
func under(root, p string) bool {
	root, p = filepath.Clean(root), filepath.Clean(p)
	return p == root || strings.HasPrefix(p, root+string(filepath.Separator))
}

// merged is the merged-PR rule: the merge is followed by a watch verdict that
// is healthy, or by a revert whose own follow-up is green. It returns a keep
// reason, or "" when the checkout may go.
func (w *workerPass) merged(ctx context.Context, hs *reapHost, c workerCand, p *github.Pull, errs *[]error) string {
	var mergedAt *time.Time
	if p.MergedAt != nil {
		mergedAt = parsedTime(*p.MergedAt)
	}
	if p.Base.Ref != w.r.Branch {
		return "base-mismatch"
	}
	if mergedAt == nil {
		return "merged-at-unknown"
	}
	// reap.worktree_after is an optional extra delay a host may set; zero is
	// the default: the verdict, not the clock, decides.
	if w.after > 0 && w.now.Sub(*mergedAt) < w.after {
		return "too-young"
	}
	// A head the merged PR (and the base) does not contain is local work
	// GitHub never got; it must never be removed, and the reason says so.
	if p.Head.SHA != "" && c.head != p.Head.SHA {
		if !w.ancestor(ctx, c.head, p.Head.SHA, errs) && !w.ancestor(ctx, c.head, w.r.Branch, errs) {
			return "unpushed"
		}
	}
	mergeSHA := p.MergeCommitSHA
	if mergeSHA == "" {
		fp, e := w.s.GH.Pull(github.Uncached(ctx), w.r.Name, p.Number)
		switch {
		case e != nil:
			*errs = append(*errs, fmt.Errorf("PR %d merge commit: %w", p.Number, e))
			return "merge-commit-unknown"
		case fp.MergeCommitSHA == "":
			return "merge-commit-unknown"
		}
		mergeSHA = fp.MergeCommitSHA
	}
	watcher, ok := w.s.GH.(interface {
		WatchRevert(context.Context, string, int, string) (*github.Pull, error)
	})
	if !ok {
		return "revert-check-unavailable"
	}
	recovery, e := watcher.WatchRevert(ctx, w.r.Name, p.Number, mergeSHA)
	if e != nil {
		*errs = append(*errs, fmt.Errorf("PR %d revert check: %w", p.Number, e))
		return "revert-check-failed"
	}
	if recovery != nil {
		if recovery.MergedAt != nil {
			at := parsedTime(*recovery.MergedAt)
			if at != nil && !at.Before(*mergedAt) && !at.After(mergedAt.Add(7*24*time.Hour)) {
				// Reverted: the worktree goes once the revert's own follow-up
				// is healthy. The head branch is never deleted, so the work
				// stays fetchable from GitHub.
				if recovery.MergeCommitSHA != "" && w.healthy(ctx, recovery.MergeCommitSHA, errs) {
					return ""
				}
				return "reverted"
			}
		}
		if recovery.State == "open" {
			return "recovery-open"
		}
	}
	if w.healthy(ctx, mergeSHA, errs) {
		return ""
	}
	return "watch-pending"
}

// unmerged is the closed-unmerged and no-PR rule: the hk job that worked the
// checkout must be terminal (completed, abandoned or lost).
func (w *workerPass) unmerged(ctx context.Context, hs *reapHost, c workerCand, p *github.Pull) string {
	job := c.job
	if p != nil {
		if b, e := w.s.reapBind(ctx, w.r, *p); e == nil && b.Job != "" {
			job = b.Job
		}
	}
	if job == "" || !reap.ValidJobName(job) {
		return "job-unknown"
	}
	if w.records == nil {
		return "job-unreadable"
	}
	rec, e := w.records.job(ctx, job)
	switch {
	case errors.Is(e, integrations.ErrJobNotFound):
		return "job-unknown"
	case e != nil:
		return "job-unreadable"
	}
	if !jobOnHost(rec, hs.host, c.branch) {
		return "job-elsewhere"
	}
	if !terminalJobState(rec.State) {
		if rec.State == "" {
			return "job-active:unknown"
		}
		return "job-active:" + rec.State
	}
	return ""
}

// jobOnHost reports whether the job record names this host and this checkout's
// branch, so a job reusing the name elsewhere never releases this worktree.
func jobOnHost(rec integrations.JobRecord, h reap.Host, branch string) bool {
	if rec.Host == "" || (rec.Host != h.Name && rec.Host != h.MachineID()) {
		return false
	}
	if rec.Worktree != nil && rec.Worktree.Branch != nil && *rec.Worktree.Branch != "" {
		return *rec.Worktree.Branch == branch
	}
	return true
}

// terminalJobState names the job states a finished job ends in.
func terminalJobState(state string) bool {
	switch state {
	case "completed", "abandoned", "escalated", "lost":
		return true
	}
	return false
}

// remove is the one removal path: inspect the checkout, re-check pane usage,
// then ask the transport for its guarded removal with the disposable list. A
// refusal is a row with its reason.
func (w *workerPass) remove(ctx context.Context, hs *reapHost, c workerCand, rec *reapReceipt, errs *[]error) *reapReceipt {
	keep := func(reason string) *reapReceipt {
		rec.Result, rec.Reason = "skipped", reason
		return rec
	}
	remover, ok := hs.ops.(interface {
		WorktreeRemoveDisposable(context.Context, string, string, string, []string, []string, bool) (reap.WorktreeRemoveResult, error)
	})
	if !ok {
		return keep("no-guarded-remove")
	}
	if !w.acting {
		rec.Result, rec.Reason = "would-remove", ""
		return rec
	}
	// The pane list is a stale snapshot by now; a worker spawned into the
	// checkout after discovery must stop the removal.
	if e := hs.refreshPanes(ctx); e != nil {
		*errs = append(*errs, fmt.Errorf("%s pane list: %w", hs.host.Name, e))
		return keep("pane-scan-failed")
	}
	if reason := w.inUse(ctx, hs, c.path); reason != "" {
		return keep(reason)
	}
	result, e := remover.WorktreeRemoveDisposable(ctx, c.path, c.head, w.r.Branch,
		w.s.Config.Reap.Regenerable, w.s.Config.Reap.Disposable, false)
	if e != nil {
		*errs = append(*errs, fmt.Errorf("%s %s: %w", hs.host.Name, c.path, e))
		return keep("guard-error")
	}
	rec.Files = result.Files
	switch result.Outcome {
	case "removed":
		rec.Result = "removed"
	case "dirty":
		rec.Result, rec.Reason = "skipped", "dirty"
	case "unmerged-or-unpushed", "unpushed":
		rec.Result, rec.Reason = "skipped", "unpushed"
	case "ignored-files":
		rec.Result, rec.Reason = "skipped", "ignored-files:"+filesSig(result.Files)
	default:
		rec.Result, rec.Reason = worktreeRemoveVerdict(result)
	}
	if rec.Result == "removed" && !hs.viaHub {
		// The hub node deletes the branch itself; a local or ssh host still
		// needs the same -d, which git refuses on a branch it cannot prove
		// merged. The remote head branch is never touched on either path, and
		// a refused local delete is a row, not a pass error: a squash-merged
		// branch is refused on every removal.
		if deleter, ok := hs.ops.(interface {
			DeleteBranch(context.Context, string, string) error
		}); ok {
			brec := reapReceipt{Action: "delete-branch", Repo: w.r.Name, PR: rec.PR, Host: hs.host.Name,
				Worktree: c.path, Branch: c.branch, Source: workerSource}
			if err := deleter.DeleteBranch(ctx, c.common, c.branch); err != nil {
				brec.Result, brec.Reason = "skipped", "refused: "+err.Error()
			} else {
				brec.Result = "deleted"
			}
			if w.note != nil {
				w.note(brec)
			}
		}
	}
	return rec
}

// healthy reports whether a watch verdict on main covers sha, cached for the
// pass.
func (w *workerPass) healthy(ctx context.Context, sha string, errs *[]error) bool {
	if v, ok := w.healthyCache[sha]; ok {
		return v
	}
	ok, e := w.s.watchCovers(ctx, w.r, sha)
	if e != nil {
		*errs = append(*errs, fmt.Errorf("watch verdict: %w", e))
	}
	w.healthyCache[sha] = ok
	return ok
}

// watchCovers reports whether a watch verdict on main covers sha: the last
// green main head is sha itself or a descendant of it. A repository with no
// verdict yet reads false, so its worktrees wait instead of leaving at the
// merge.
func (s *Service) watchCovers(ctx context.Context, r config.Repo, sha string) (bool, error) {
	if sha == "" {
		return false, nil
	}
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "last-green"))
	if e != nil {
		if os.IsNotExist(e) {
			return false, nil
		}
		return false, e
	}
	green := strings.TrimSpace(string(b))
	if green == "" {
		return false, nil
	}
	if green == sha {
		return true, nil
	}
	ok, e := s.GH.Ancestor(ctx, r.Name, sha, green)
	if github.NotFound(e) {
		return false, nil
	}
	return ok, e
}

// ancestor reports whether older is an ancestor of newer in this repository,
// reading GitHub at most once per pair per pass. An unknown commit is not an
// ancestor; a failed read is reported and treated the same way.
func (w *workerPass) ancestor(ctx context.Context, older, newer string, errs *[]error) bool {
	if older == "" || newer == "" {
		return false
	}
	if older == newer {
		return true
	}
	k := older + "\x00" + newer
	if v, ok := w.ancestry[k]; ok {
		return v
	}
	ok, e := w.s.GH.Ancestor(ctx, w.r.Name, older, newer)
	switch {
	case github.NotFound(e):
		ok = false
	case e != nil:
		*errs = append(*errs, fmt.Errorf("ancestry %s..%s: %w", older, newer, e))
		ok = false
	}
	w.ancestry[k] = ok
	return ok
}
