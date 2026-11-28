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
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The worker-worktree pass removes a worker's checkout once its work is on
// GitHub. It is the only code that removes a wk/ checkout: the older
// merged-author cleanup hands these back to it. Discovery and removal both go
// through the hub's node commands, the same transport reap uses: a host with no
// node mapping or no guarded removal keeps its checkouts and records why.
// A merged PR's worktree goes when the watch verdict for its merge commit is
// healthy, or when a revert's own follow-up is green; an unmerged one goes when
// its PR is closed (or absent) and the hk job on its branch is terminal.

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
// durable reports that this pass writes its rows: only then is the stage's
// resume cursor read and saved (worker_cursor.go).
func (s *Service) reapWorkerWorktrees(ctx context.Context, r config.Repo, hosts []*reapHost, records *passJobRecords, acting, durable bool, note func(reapReceipt)) []error {
	after, e := config.OptionalDuration(s.Config.Reap.WorktreeAfter)
	if e != nil {
		return []error{e}
	}
	w := &workerPass{s: s, r: r, records: records, acting: acting, now: s.now(), after: after,
		healthyCache: map[string]bool{}, ancestry: map[string]bool{}, resumable: durable}
	var errs []error
	var from workerCursor
	if durable {
		c, e := s.workerStageCursor(r.Name)
		if e != nil {
			errs = append(errs, fmt.Errorf("worker worktrees: resume cursor: %w", e))
			w.unreadable = cursorDecodeError(e)
			w.cursorReadFailed = !w.unreadable
		} else {
			from = c
		}
	}
	// Discovery comes first and is the only host read this pass costs when no
	// worker worktree exists: the branch inventories below page every closed
	// PR, so a quiet repo must never pay for them.
	order := workerHostOrder(s.Config, hosts)
	cands := map[*reapHost][]workerCand{}
	total := 0
	// discovered is false when the budget or the context ended discovery before
	// every host: the stage is not finished then and keeps its cursor.
	discovered := true
	for _, hs := range order {
		if e := reapBudgetCheck(ctx, "worker worktree discovery and cleanup"); e != nil {
			errs = append(errs, e)
			discovered = false
			break
		}
		if e := budgetCut(ctx, ctx.Err(), "worker worktree discovery"); e != nil {
			errs = append(errs, e)
			discovered = false
			break
		}
		if hs.failed {
			// A skipped host was not discovered: the stage is not finished,
			// so its resume cursor survives until a pass reads the host.
			discovered = false
			continue
		}
		if _, ok := hs.ops.(reap.WorktreeLister); !ok || !hs.viaHub {
			// No node mapping means no registered-worktree view and no
			// guarded removal. A wk/ checkout some pane or job record already
			// named stays and gets one row; there is no ssh or local fallback.
			for _, t := range hs.trees {
				if t == nil || !strings.HasPrefix(t.Branch, workerBranchPrefix) || !strings.EqualFold(t.Repo, r.Name) {
					continue
				}
				note(reapReceipt{Action: "remove-worktree", Repo: r.Name, Host: hs.host.Name,
					Worktree: t.Path, Cwd: t.Path, Branch: t.Branch, Result: "skipped", Reason: "hub-required", Source: workerSource})
			}
			continue
		}
		found, listErrs := w.discover(ctx, hs, note)
		errs = append(errs, listErrs...)
		cands[hs] = found
		total += len(found)
	}
	if total == 0 {
		if discovered || w.unreadable {
			if e := w.finished(from); e != nil {
				errs = append(errs, e)
			}
		}
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
		return append(errs, fmt.Errorf("worker worktrees: closed PR listing: %w", budgetCut(ctx, e, "closed PR listing")))
	}
	open, e := openLister.OpenPulls(ctx, r.Name, "")
	if e != nil {
		return append(errs, fmt.Errorf("worker worktrees: open PR listing: %w", budgetCut(ctx, e, "open PR listing")))
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
	visits := make([]workerVisit, 0, total)
	for _, hs := range order {
		for _, c := range cands[hs] {
			visits = append(visits, workerVisit{hs: hs, cand: c})
		}
	}
	// A full stage resumes after the candidate its last budget stopped before
	// and wraps round, so a candidate behind a run of kept ones is reached on a
	// later pass (worker_cursor.go).
	visits = resumeWorkerVisits(visits, order, from)
	drain := map[*reapHost]int{}
	rows := map[*reapHost]*workerRows{}
	for _, hs := range order {
		drain[hs] = s.Config.Reap.WorktreeDrain.DrainPerHost(config.DefaultWorktreeDrainPerHost)
		rows[hs] = &workerRows{Candidates: len(cands[hs]), Kept: map[string]int{}}
	}
	cut := -1
	for i, v := range visits {
		if e := reapBudgetCheck(ctx, "worker worktree candidates"); e != nil {
			errs = append(errs, e)
			cut = i
			break
		}
		if e := budgetCut(ctx, ctx.Err(), "worker worktree candidates"); e != nil {
			errs = append(errs, e)
			cut = i
			break
		}
		if drain[v.hs] <= 0 {
			// The cap only bounds removals; the rest wait for the next
			// pass, so a host's backlog drains in bounded steps.
			continue
		}
		rec := w.evaluate(ctx, v.hs, v.cand, newest, openSet, &errs)
		if rec == nil {
			continue
		}
		note(*rec)
		if rec.Result == "removed" || rec.Result == "would-remove" {
			rows[v.hs].Removed++
			drain[v.hs]--
		} else {
			rows[v.hs].Kept[rec.Result+"|"+strings.SplitN(rec.Reason, ":", 2)[0]]++
		}
	}
	// A stage the budget cut keeps its cursor at the candidate it stopped
	// before; a stage that visited the whole order clears it. An unreadable
	// cursor holds no position to keep, so it is cleared even when a skipped
	// host left discovery incomplete.
	switch {
	case cut >= 0:
		if e := w.stopAt(visits[cut]); e != nil {
			errs = append(errs, e)
		}
	case discovered || w.unreadable:
		if e := w.finished(from); e != nil {
			errs = append(errs, e)
		}
	}
	for _, hs := range order {
		if e := s.Print(map[string]any{"action": "worker-worktrees", "repo": r.Name, "host": hs.host.Name,
			"apply": acting, "candidates": rows[hs].Candidates, "removed": rows[hs].Removed, "kept": rows[hs].Kept}); e != nil {
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
	s            *Service
	r            config.Repo
	records      *passJobRecords
	acting       bool
	now          time.Time
	after        time.Duration
	healthyCache map[string]bool
	ancestry     map[string]bool
	// resumable marks a durable stage: only it reads and records the resume
	// cursor (worker_cursor.go).
	resumable bool
	// unreadable marks a corrupt resume cursor the stage can safely reset.
	unreadable bool
	// cursorReadFailed prevents a later budget stop from overwriting a cursor
	// that could not be read due to an I/O error.
	cursorReadFailed bool
}

func cursorDecodeError(err error) bool {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntaxErr) || errors.As(err, &typeErr)
}

// workerClone is one registered clone to list: its main checkout and its git
// common dir, which is what the node's worktree.list takes.
type workerClone struct {
	main   string
	common string
	// trusted marks a clone reap already knows: a common dir of an inspected
	// worktree whose origin is this repository. Any other clone is a path
	// guess and must prove its origin before its worktrees count.
	trusted bool
}

// workerCand is one listed worktree on a worker branch.
type workerCand struct {
	path   string
	branch string
	head   string
	job    string
	common string // the clone's git common dir
}

// discover lists every clone's registered worktrees on the hub host and keeps
// the worker branches. Clones are the common dirs reap already knows from
// panes, plus the configured clone `<work_root>/<repo>`; a worktree no pane,
// receipt or job record names is still found.
func (w *workerPass) discover(ctx context.Context, hs *reapHost, note func(reapReceipt)) ([]workerCand, []error) {
	lister, ok := hs.ops.(reap.WorktreeLister)
	if !ok {
		return nil, nil
	}
	var errs []error
	seen := map[string]bool{}
	var clones []workerClone
	add := func(c workerClone) {
		c.main, c.common = filepath.Clean(c.main), filepath.Clean(c.common)
		if c.main == "" || c.common == "" || seen[c.common] {
			return
		}
		seen[c.common] = true
		clones = append(clones, c)
	}
	for _, t := range hs.trees {
		if t != nil && t.Common != "" && strings.EqualFold(t.Repo, w.r.Name) {
			add(workerClone{main: filepath.Dir(t.Common), common: t.Common, trusted: true})
		}
	}
	if hs.host.WorkRoot != "" {
		main := filepath.Join(hs.host.WorkRoot, repoSegment(w.r.Name))
		// A path guess is trusted only after its worktrees' origin says it is
		// this repository: two configured repositories can share the last
		// segment, and another owner's clone must never be judged against this
		// repository's PRs and jobs.
		add(workerClone{main: main, common: filepath.Join(main, ".git")})
	}
	sort.Slice(clones, func(i, j int) bool { return clones[i].common < clones[j].common })
	seenPath := map[string]bool{}
	var out []workerCand
	for _, clone := range clones {
		rows, e := reap.ListWorktrees(ctx, lister, clone.common)
		if e != nil {
			// A repository not cloned on this host answers invalid args: that
			// is an absence, not a failure. Anything else is reported once.
			if reap.InvalidArgs(e) {
				continue
			}
			errs = append(errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s worktree.list %s: %w", hs.host.Name, clone.common, e)))
			continue
		}
		var listed []workerCand
		for _, row := range rows {
			if row.Detached || !strings.HasPrefix(row.Branch, workerBranchPrefix) {
				continue
			}
			if filepath.Clean(row.Path) == clone.main {
				continue
			}
			if strings.HasPrefix(filepath.Base(row.Path), "mwr-") || seenPath[row.Path] {
				continue
			}
			listed = append(listed, workerCand{path: row.Path, branch: row.Branch, head: row.Head,
				job: strings.TrimPrefix(row.Branch, workerBranchPrefix), common: clone.common})
		}
		if len(listed) == 0 {
			continue
		}
		if !clone.trusted {
			// Every worktree of a clone shares its origin. Inspect registered
			// candidates in turn: a stale registration answers nothing and is
			// inconclusive, a proven origin and common dir admit the clone,
			// and a verified mismatch refuses it. None proven keeps it out.
			sort.Slice(listed, func(i, j int) bool { return listed[i].path < listed[j].path })
			admitted, proven, errored := false, false, false
			stale := 0
			for _, c := range listed {
				wt, e := hs.ops.Inspect(ctx, c.path)
				switch {
				case e != nil:
					errored = true
					errs = append(errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s %s repository identity: %w", hs.host.Name, c.path, e)))
				case wt == nil:
					// A registered worktree whose directory is gone: stale,
					// inconclusive.
					stale++
				case !strings.EqualFold(wt.Repo, w.r.Name) || filepath.Clean(wt.Common) != clone.common:
					// A proven different repository or clone: refuse the
					// whole path guess.
					proven = true
				default:
					proven, admitted = true, true
				}
				if proven {
					break
				}
			}
			if !admitted {
				switch {
				case proven:
					// A verified mismatch refuses the clone silently.
				case !errored && stale == len(listed):
					// Only stale registrations: nothing proves or refutes the
					// clone this pass. Skipped, not a pass error.
					if e := w.noteInconclusiveClone(hs, clone, note); e != nil {
						errs = append(errs, e)
					}
				default:
					errs = append(errs, fmt.Errorf("%s clone %s: repository identity not established", hs.host.Name, clone.main))
				}
				continue
			}
		}
		for _, c := range listed {
			seenPath[c.path] = true
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, errs
}

// inconclusiveCloneReason names the skipped row of a clone whose every
// registered worktree is stale, so no pass can prove its identity yet.
const inconclusiveCloneReason = "repository-identity-inconclusive"

// inconclusiveCloneAction is the action of that skipped row. It is not
// remove-worktree: the clone is never removed, so the worker-worktree and
// author-sweep tallies must not count it as made.
const inconclusiveCloneAction = "clone-identity"

// inconclusiveCloneRow reports whether v is a clone's once-a-day skipped row.
// It is never compared against the stream's previous row: the same clone keeps
// the same key and signature on every day, and the day gate in
// noteInconclusiveClone already limits it to one a day.
func inconclusiveCloneRow(v reapReceipt) bool {
	return v.Action == inconclusiveCloneAction && v.Reason == inconclusiveCloneReason
}

// noteInconclusiveClone records one skipped row for an untrusted clone whose
// registrations are all stale, at most once per clone per UTC day, so the skip
// stays visible without repeating on every pass.
func (w *workerPass) noteInconclusiveClone(hs *reapHost, clone workerClone, note func(reapReceipt)) error {
	day := w.now.UTC().Format(reviewerSummaryDay)
	receipts, e := w.s.reapReceipts(w.r.Name)
	if e != nil {
		return fmt.Errorf("%s clone %s: receipts: %w", hs.host.Name, clone.main, e)
	}
	for _, v := range receipts {
		if inconclusiveCloneRow(v) && v.Host == hs.host.Name && v.Worktree == clone.main &&
			v.Time.UTC().Format(reviewerSummaryDay) == day {
			return nil
		}
	}
	note(reapReceipt{Action: inconclusiveCloneAction, Repo: w.r.Name, Host: hs.host.Name, Worktree: clone.main,
		Result: "skipped", Reason: inconclusiveCloneReason, Source: workerSource})
	return nil
}

// repoSegment is the clone directory name panewire prepares under a host's
// work root: the last segment of owner/name.
func repoSegment(repo string) string {
	if i := strings.LastIndex(repo, "/"); i >= 0 {
		return repo[i+1:]
	}
	return repo
}

// evaluate decides one candidate and returns its row.
func (w *workerPass) evaluate(ctx context.Context, hs *reapHost, c workerCand, newest map[string]*github.Pull, open map[string]bool, errs *[]error) *reapReceipt {
	rec := reapReceipt{Action: "remove-worktree", Repo: w.r.Name, Host: hs.host.Name, Cwd: c.path,
		Worktree: c.path, Branch: c.branch, Head: c.head, Job: c.job, Source: workerSource}
	var p *github.Pull
	if q := newest[c.branch]; q != nil {
		rec.PR = q.Number
		p = q
	}
	keep := func(reason string) *reapReceipt {
		rec.Result, rec.Reason = "skipped", reason
		return &rec
	}
	if reason := paneUse(hs, c.path); reason != "" {
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

// paneUse names the pane evidence keeping path: the node's pane list includes
// plain shells, so it is the usage evidence the guard observes.
func paneUse(hs *reapHost, path string) string {
	if hs.usageUnknown {
		return "pane-scan-failed"
	}
	if hs.paneInUse(path) {
		return "pane-in-use"
	}
	return ""
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
			*errs = append(*errs, fmt.Errorf("PR %d merge commit: %w", p.Number, budgetCut(ctx, e, "merge commit read")))
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
		*errs = append(*errs, fmt.Errorf("PR %d revert check: %w", p.Number, budgetCut(ctx, e, "revert check")))
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

// unmerged is the closed-unmerged and no-PR rule: the hk job encoded by
// wk/<job> on this host must be terminal. A PR binding another job refuses the
// checkout instead of standing in for the branch's own job.
func (w *workerPass) unmerged(ctx context.Context, hs *reapHost, c workerCand, p *github.Pull) string {
	job := c.job
	if job == "" || !reap.ValidJobName(job) {
		return "job-unknown"
	}
	if p != nil {
		b, e := w.s.reapBind(ctx, w.r, *p)
		switch {
		case e != nil:
			return "job-binding-error"
		case b.Job != "" && b.Job != job:
			return "job-conflict"
		}
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

// remove is the one removal path: re-read the pane list, revalidate the whole
// candidate identity through the node, then ask the node's guarded removal
// with the disposable list. A refusal is a row with its reason; nothing is
// deleted here, so a refused removal leaves the checkout, draft included.
func (w *workerPass) remove(ctx context.Context, hs *reapHost, c workerCand, rec *reapReceipt, errs *[]error) *reapReceipt {
	keep := func(reason string) *reapReceipt {
		rec.Result, rec.Reason = "skipped", reason
		return rec
	}
	remover, ok := hs.ops.(interface {
		WorktreeRemoveDisposable(context.Context, string, string, string, []string, []string, bool) (reap.WorktreeRemoveResult, error)
	})
	if !ok || !hs.viaHub {
		return keep("hub-guard-unavailable")
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
	if reason := paneUse(hs, c.path); reason != "" {
		return keep(reason)
	}
	// Bind the policy to the checkout the node is about to act on: the same
	// origin, common dir, path, branch and head that discovery proved.
	wt, e := hs.ops.Inspect(ctx, c.path)
	switch {
	case e != nil:
		*errs = append(*errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s %s: %w", hs.host.Name, c.path, e)))
		return keep("worktree-inspect-error")
	case wt == nil:
		return keep("worktree-gone")
	case wt.Path != c.path || wt.Branch != c.branch || wt.Head != c.head ||
		filepath.Clean(wt.Common) != c.common || !strings.EqualFold(wt.Repo, w.r.Name):
		return keep("worktree-changed")
	}
	result, e := remover.WorktreeRemoveDisposable(ctx, c.path, c.head, w.r.Branch,
		w.s.Config.ReapRegenerable(w.r), w.s.Config.Reap.Disposable, false)
	if e != nil {
		*errs = append(*errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s %s: %w", hs.host.Name, c.path, e)))
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
		*errs = append(*errs, fmt.Errorf("watch verdict: %w", budgetCut(ctx, e, "watch verdict")))
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
		*errs = append(*errs, fmt.Errorf("ancestry %s..%s: %w", older, newer, budgetCut(ctx, e, "ancestry read")))
		ok = false
	}
	w.ancestry[k] = ok
	return ok
}

// under reports whether p sits inside root.
func under(root, p string) bool {
	root, p = filepath.Clean(root), filepath.Clean(p)
	return p == root || strings.HasPrefix(p, root+string(filepath.Separator))
}

// mergedProcessEvidence is the process scan the older merged-author path
// consults before its Exec removal. It is a variable so a test can stand in for
// the platform scan, which sees the test runner's own processes.
var mergedProcessEvidence = func(ctx context.Context, s *Service) (procEvidence, error) {
	return processScan(ctx, s.Runner)
}

// processInUse names the process evidence keeping the older merged-author path
// from removing path on this machine, or "". A scan that cannot read every
// process fails closed: no proof means the checkout stays.
func (s *Service) processInUse(ctx context.Context, path string) string {
	ev, e := mergedProcessEvidence(ctx, s)
	if e != nil {
		return "process-scan-unavailable"
	}
	held, unresolved := ev.heldPaths()
	for _, p := range held {
		// Fail closed across spellings: a held path may name a symlinked root
		// (macOS /var) while path is the physical path git printed.
		if under(path, p) || under(path, physicalPath(p)) {
			return "process-in-use"
		}
	}
	if len(unresolved) > 0 {
		return "process-unknown"
	}
	return ""
}

// physicalPath returns p with symlinks expanded, or p unchanged when it cannot
// be resolved (for example, the directory is gone).
func physicalPath(p string) string {
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return resolved
	}
	return p
}
