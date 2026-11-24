package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The legacy sweep is a conservative pass over worktrees the daemon's reap
// never sees again: reap.scan_after bounds which merged PRs it looks at, so a
// worktree whose merge is older than that stays forever. The daemon runs it on
// reap.legacy_every per repo (ReapLegacyScheduled); `mergewake reap --legacy`
// runs one pass by hand.
//
// Candidates come from two sources, merged by worktree path. The node's
// worktree.list names every worktree of a common git dir: the common dirs are
// those of the worktrees reap already inspects on the host (pane cwds and job
// cwds) whose origin is the configured repo. The binding reap's job path
// already uses is the second source: every closed PR of the repo (all time) →
// its Mergewake-Task → the bound job → the job record's host and cwd. Anything
// the binding cannot reach (a PR with no trailer, a task with no job, a job
// with no record) is counted per repo. Removal goes only through the node's
// guarded worktree.remove: the node decides cleanliness, ignored files and
// identity at the moment of removal. The listed status only adds two cheap
// pre-checks, so a dry run never probes the guard.

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

// legacyTaskReader is the hk task read the sweep needs for a candidate whose
// PR did not merge: its state and close time decide dropped-task removal.
type legacyTaskReader interface {
	Task(context.Context, string) (client.Task, error)
}

// legacyCand is one worktree path on one host: one that some PR's bound job
// worked in, one the node listed, or both.
type legacyCand struct {
	host   string // configured host name
	cwd    string
	job    string
	task   string
	listed *reap.ListedWorktree // the node's row, nil when only a job reached it
	main   bool                 // the listed row is its common's main checkout
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

// count is the number of rows with one result.
func (t legacyTally) count(result string) int {
	n := 0
	for _, c := range t[result] {
		n += c
	}
	return n
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

// ReapLegacy runs one sweep for repo r over one configured host (host
// non-empty) or all of them, one host at a time. Without --apply, or
// without reap.act, it plans: rows say would-remove and nothing is removed.
// Plans and results both land in <state>/<repo>/reap.jsonl as legacy-sweep
// rows. The daemon's Reap ignores those rows; the schedule that calls this
// from the daemon lives in ReapLegacyScheduled.
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
	// lastRow is the newest row per worktree, so a rerun skips what is
	// already removed or refused — but a terminal row suppresses only the
	// checkout it names: a replacement at the same path is evaluated again.
	lastRow := map[string]legacyLast{}
	for _, rec := range receipts {
		if legacyRow(rec) {
			lastRow[legacyKey(rec.Host, filepath.Clean(rec.Worktree))] = legacyLast{sig: rec.Result + "|" + rec.Reason, result: rec.Result, branch: rec.Branch, head: rec.Head}
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
	bindings := map[int]reapBinding{}
	jobTasks := map[string]string{}
	for _, p := range closed {
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		b, e := s.reapBind(ctx, r, p)
		if e == nil {
			bindings[p.Number] = b
		}
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
		jobTasks[b.Job] = b.Task
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
	taskReader, _ := s.Sink.(legacyTaskReader)
	sweep := &legacySweep{s: s, r: r, o: o, acting: acting, lastRow: lastRow, newest: newest, open: openBranches(open, r.Name), openLister: openLister, watcher: watcher, ancestry: map[string]bool{},
		bindings: bindings, jobRecs: records, jobTasks: jobTasks, tasks: taskReader, taskReads: map[string]legacyTaskRead{}}
	for _, h := range wanted {
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		tally := legacyTally{}
		hs, hostErrs := s.reapScanHost(ctx, hub, h)
		errs = append(errs, hostErrs...)
		items, found := sweep.enumerate(ctx, hs, byHost[h.Name], &errs)
		paths := make([]string, 0, len(items))
		for p := range items {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		for _, p := range paths {
			if e = ctx.Err(); e != nil {
				return errors.Join(append(errs, e)...)
			}
			errs = append(errs, sweep.candidate(ctx, hs, items[p], tally)...)
		}
		found.Candidates = len(items) - tally.count("out-of-scope")
		summary := map[string]any{"action": legacyAction + "-summary", "repo": r.Name, "host": h.Name, "apply": acting,
			"worktrees": len(byHost[h.Name]), "bound_jobs": reach.BoundJobsBy[h.Name], "results": tally,
			"listing": found.Listing, "commons": found.Commons, "commons_not_found": found.NotFound, "listed": found.Listed,
			"candidates": found.Candidates, "would_remove": tally.count("would-remove"), "kept_by_reason": tally["kept"]}
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

// receiptPath is the one normalization every legacy-sweep receipt key goes
// through: receipts are keyed by legacyKey(host, receiptPath(...)). The path
// is the worktree root when it is known — an inspected wt.Path or a listed
// row. gone reports a path inspection found missing, reachable only through a
// job cwd that may sit inside the worktree: it resolves to the nearest
// ancestor whose newest receipt is terminal — the finished worktree root
// earlier sweeps saw. A nearer row that is not terminal means the path sits
// under a live worktree's subtree, so the gone row keys the path itself
// rather than overwriting that worktree's signature. A known root never
// resolves to an ancestor: a fresh checkout must not inherit the rows of a
// worktree above it.
func (w *legacySweep) receiptPath(host, path string, gone bool) string {
	p := filepath.Clean(path)
	if gone {
		for q := filepath.Dir(p); ; {
			if l, ok := w.lastRow[legacyKey(host, q)]; ok {
				if l.result == "removed" || l.result == "refused" {
					return q
				}
				break
			}
			parent := filepath.Dir(q)
			if parent == q {
				break
			}
			q = parent
		}
	}
	return p
}

// checkoutField reports whether a receipt's recorded branch or head matches
// the candidate's. A receipt that never recorded the field is a wildcard —
// it cannot tell checkouts apart — but the candidate's empty field is not:
// a detached or headless checkout is a different checkout and must be
// evaluated, not suppressed by a row that named one.
func checkoutField(recorded, have string) bool {
	return recorded == "" || recorded == have
}

// legacyLast is the newest legacy-sweep row of one worktree path: the
// result|reason signature note compares repeats against, and the checkout
// identity (branch and head) a terminal row is bound to.
type legacyLast struct {
	sig    string
	result string
	branch string
	head   string
}

// legacySweep carries one repo's pass state across its hosts.
type legacySweep struct {
	s       *Service
	r       config.Repo
	o       Options
	acting  bool
	lastRow map[string]legacyLast
	newest  map[string]*github.Pull // head branch → its newest closed PR
	open    map[string]bool         // head branches that had an open PR when the sweep began
	watcher legacyRevertWatcher
	// openLister re-reads open PRs right before a removal.
	openLister interface {
		OpenPulls(context.Context, string, string) ([]github.Pull, error)
	}
	ancestry map[string]bool
	// bindings, jobRecs and jobTasks carry what the closed-PR binding pass
	// learned, so an unmerged branch can still find its hk task: the PR's
	// trailer first, then a job record that names the worktree.
	bindings  map[int]reapBinding
	jobRecs   map[string]*integrations.JobRecord
	jobTasks  map[string]string
	tasks     legacyTaskReader
	taskReads map[string]legacyTaskRead
}

// legacyTaskRead caches one hk task read for the sweep.
type legacyTaskRead struct {
	task client.Task
	err  error
}

// legacyFound is what one host's enumeration reached.
type legacyFound struct {
	Listing    string // "hub" when worktree.list was asked, "unavailable" otherwise
	Commons    int    // common git dirs of the configured repo found on the host
	NotFound   int    // commons the node did not accept for worktree.list
	Listed     int    // rows worktree.list returned, main checkouts included
	Candidates int    // worktrees evaluated, out-of-scope ones excluded
}

// enumerate merges the host's two candidate sources by worktree path: the
// job-bound cwds, and every worktree worktree.list names under the common git
// dirs of the configured repo that reap sees on the host. A job cwd inside a
// worktree is keyed by the worktree root the node reports.
func (w *legacySweep) enumerate(ctx context.Context, hs *reapHost, bound map[string]*legacyCand, errs *[]error) (map[string]*legacyCand, legacyFound) {
	found := legacyFound{Listing: "unavailable"}
	items := map[string]*legacyCand{}
	if hs == nil || hs.failed {
		for cwd, c := range bound {
			items[cwd] = c
		}
		return items, found
	}
	commons := map[string]bool{}
	note := func(t *reap.Worktree) {
		if t != nil && t.Common != "" && strings.EqualFold(t.Repo, w.r.Name) {
			commons[t.Common] = true
		}
	}
	for _, t := range hs.trees {
		note(t)
	}
	cwds := make([]string, 0, len(bound))
	for cwd := range bound {
		cwds = append(cwds, cwd)
	}
	sort.Strings(cwds)
	for _, cwd := range cwds {
		c := bound[cwd]
		t, e := hs.ops.Inspect(ctx, cwd)
		if e != nil || t == nil {
			items[cwd] = c // evaluate reports it
			continue
		}
		note(t)
		if prev := items[t.Path]; prev == nil {
			c.cwd = t.Path
			items[t.Path] = c
		}
	}
	found.Commons = len(commons)
	lister, ok := hs.ops.(reap.WorktreeLister)
	if !ok {
		return items, found
	}
	found.Listing = "hub"
	sorted := make([]string, 0, len(commons))
	for c := range commons {
		sorted = append(sorted, c)
	}
	sort.Strings(sorted)
	for _, common := range sorted {
		rows, e := reap.ListWorktrees(ctx, lister, common)
		switch {
		case reap.InvalidArgs(e):
			found.NotFound++
			continue
		case e != nil:
			*errs = append(*errs, hostUnreachableFailure(hs.host.Name, fmt.Errorf("%s worktree.list %s: %w", hs.host.Name, common, e)))
			continue
		}
		main := filepath.Dir(common)
		for i := range rows {
			row := rows[i]
			found.Listed++
			c := items[row.Path]
			if c == nil {
				c = &legacyCand{host: hs.host.Name, cwd: row.Path}
				items[row.Path] = c
			}
			c.listed = &row
			c.main = row.Path == main
		}
	}
	return items, found
}

// candidate evaluates and records one worktree. It returns the errors that
// should fail the run; every evaluation, even a kept one, leaves a row.
func (w *legacySweep) candidate(ctx context.Context, hs *reapHost, c *legacyCand, tally legacyTally) []error {
	// A removed worktree is finished and a refused one is never retried on its
	// own: neither is inspected again — but only for the checkout the terminal
	// row names. The listed row carries branch and head, so a replacement at
	// the same path falls through to evaluate, which inspects and repeats the
	// check by the worktree root, which is what rows are keyed by when the cwd
	// sits inside the worktree. Without a listed row the identity is unknown
	// and evaluate decides after inspecting.
	if c.listed != nil {
		if prior := w.prior(c.host, c.cwd, c.listed.Branch, c.listed.Head); prior != "" {
			tally.add("already-"+prior, "")
			return nil
		}
	}
	// The main checkout and reviewer checkouts are never this sweep's, and a
	// worker worktree (wk/<job>) belongs to the worker-worktree pass: its
	// rules, not this sweep's 14-day floor, decide when it goes.
	switch {
	case c.main:
		tally.add("out-of-scope", "main-checkout")
		return nil
	case c.listed != nil && strings.HasPrefix(filepath.Base(c.cwd), "mwr-"):
		tally.add("out-of-scope", "reviewer-checkout")
		return nil
	case c.listed != nil && strings.HasPrefix(c.listed.Branch, workerBranchPrefix):
		tally.add("out-of-scope", "worker-worktree")
		return nil
	}
	rec := reapReceipt{Action: legacyAction, Repo: w.r.Name, Host: c.host, Cwd: c.cwd, Worktree: c.cwd, Job: c.job, Task: c.task}
	var errs []error
	switch skip := w.evaluate(ctx, hs, c, &rec, &errs); skip {
	case "":
	case "reviewer-checkout", "worker-worktree":
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

// terminal is "removed" or "refused" when the path's newest row is final for
// the path itself, whatever checkout it named. It is the gone case's
// suppression: a path an earlier sweep finished cannot be listed any more, and
// inspection of it has no branch or head to bind a row to — but a bound job's
// cwd can still reach it, so a remove that was recorded once must not be
// re-recorded as kept/gone on every sweep. path is already a receipt key:
// evaluate resolves a gone job cwd to its worktree root before calling.
func (w *legacySweep) terminal(host, path string) string {
	switch l := w.lastRow[legacyKey(host, w.receiptPath(host, path, false))]; l.result {
	case "removed", "refused":
		return l.result
	}
	return ""
}

// prior is "removed" or "refused" when the worktree's newest row is final for
// this checkout: a terminal row suppresses only the branch and head it
// recorded, so a different checkout at the same path is evaluated again. A
// row that never recorded a branch or head (rows written before they were
// recorded) is a wildcard on that field, so old rows do not all re-evaluate
// once; a candidate with an empty field is a different checkout, not a
// wildcard.
func (w *legacySweep) prior(host, path, branch, head string) string {
	l, ok := w.lastRow[legacyKey(host, w.receiptPath(host, path, false))]
	if !ok || (l.result != "removed" && l.result != "refused") {
		return ""
	}
	if !checkoutField(l.branch, branch) || !checkoutField(l.head, head) {
		return ""
	}
	return l.result
}

// note prints a row and appends it to reap.jsonl unless it repeats the
// worktree's newest outcome for the same checkout, with the same wildcard
// rule as prior: a row that never recorded the field cannot tell checkouts
// apart. A different checkout at the same path — another branch,
// or the same branch at another head — is a new
// removal and is always written, so the journal's newest row names the
// checkout that was actually removed. Plans are written too: they are the list
// the operator reviews before the apply.
func (w *legacySweep) note(v reapReceipt) error {
	v.Time = w.s.now().UTC()
	key, sig := legacyKey(v.Host, w.receiptPath(v.Host, v.Worktree, false)), v.Result+"|"+v.Reason
	prev := w.lastRow[key]
	repeat := prev.sig == sig && checkoutField(prev.branch, v.Branch) && checkoutField(prev.head, v.Head)
	w.lastRow[key] = legacyLast{sig: sig, result: v.Result, branch: v.Branch, head: v.Head}
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
// earlier run found under the inspected worktree root, or — when inspection
// reports a path gone, so there is no identity to compare — at the path itself.
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
		*errs = append(*errs, hostUnreachableFailure(c.host, fmt.Errorf("%s %s: %w", c.host, c.cwd, e)))
		return ""
	case wt == nil:
		// Inspection has no identity to bind a row to. The candidate's path
		// may be a job cwd inside the gone worktree, so the receipt key is the
		// nearest ancestor whose newest row is terminal — the worktree root
		// earlier sweeps saw. A path an earlier sweep finished, reached again
		// only through a stale job cwd, is quiet: it is tallied, not
		// re-recorded, on every sweep. A listed path was already matched
		// against its listed identity by candidate. A gone row names the
		// path, not a checkout, so it carries no branch or head. A listed
		// path is a known worktree root and keys itself; only an unlisted
		// job cwd may resolve to an ancestor's receipt.
		rec.Worktree = w.receiptPath(c.host, c.cwd, c.listed == nil)
		if c.listed == nil {
			if prior := w.terminal(c.host, rec.Worktree); prior != "" {
				return "already-" + prior
			}
		}
		rec.Branch, rec.Head = "", ""
		keep("gone")
		return ""
	}
	// Reviewer checkouts (mwr-*) belong to the reviewer sweep, attached or not.
	if strings.HasPrefix(filepath.Base(wt.Path), "mwr-") {
		return "reviewer-checkout"
	}
	if strings.HasPrefix(wt.Branch, workerBranchPrefix) {
		return "worker-worktree"
	}
	rec.Worktree, rec.Branch, rec.Head = wt.Path, wt.Branch, wt.Head
	// Rows are keyed by the worktree root the node reported, which differs from
	// the job's cwd when the job worked in a subdirectory.
	if prior := w.prior(c.host, wt.Path, wt.Branch, wt.Head); prior != "" {
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
	if p != nil {
		rec.PR = p.Number
	}
	switch {
	case w.open[wt.Branch]:
		keep("open-pr")
	case p == nil:
		w.unmerged(ctx, hs, c, wt, nil, rec, errs)
	default:
		var mergedAt *time.Time
		if p.MergedAt != nil {
			mergedAt = parsedTime(*p.MergedAt)
		}
		switch {
		case !(p.Merged || p.MergedAt != nil):
			w.unmerged(ctx, hs, c, wt, p, rec, errs)
		case p.Base.Ref != w.r.Branch:
			keep("base-mismatch")
		case mergedAt == nil || p.Head.SHA == "":
			keep("unclassified")
		case w.s.now().Sub(*mergedAt) < legacyMinAge:
			keep("too-young")
		default:
			w.merged(ctx, hs, c.listed, wt, *p, *mergedAt, rec, errs)
		}
	}
	return ""
}

// merged runs the rules that apply to a merged PR old enough to sweep.
func (w *legacySweep) merged(ctx context.Context, hs *reapHost, l *reap.ListedWorktree, wt *reap.Worktree, p github.Pull, mergedAt time.Time, rec *reapReceipt, errs *[]error) {
	keep := func(reason string) { rec.Result, rec.Reason = "kept", reason }
	// The node's listed status is a cheap pre-check before any GitHub call; the
	// guarded removal still decides at the moment of removal.
	if l != nil {
		switch {
		case l.Status.Error != "":
			keep("status-error")
			return
		case l.Status.Dirty || l.Status.Untracked > 0:
			keep("dirty")
			return
		}
	}
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
		fp, e := w.s.GH.Pull(github.Uncached(ctx), w.r.Name, p.Number)
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
	w.removeGuarded(ctx, hs, wt, rec, errs)
}

// removeGuarded is the one removal path both the merged and the dropped-task
// sweeps share: the node's guarded worktree.remove, behind a pane re-scan, an
// open-PR re-read and a worktree re-inspection.
func (w *legacySweep) removeGuarded(ctx context.Context, hs *reapHost, wt *reap.Worktree, rec *reapReceipt, errs *[]error) {
	keep := func(reason string) { rec.Result, rec.Reason = "kept", reason }
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
		// The checkout vanished between inspections: like the inspect-time
		// gone, the row names the path and carries no checkout identity.
		rec.Branch, rec.Head = "", ""
		keep("gone")
		return
	case cur.Path != wt.Path || cur.Branch != wt.Branch || cur.Head != wt.Head:
		keep("worktree-changed")
		return
	}
	result, e := remover.WorktreeRemove(ctx, wt.Path, wt.Head, w.r.Branch, w.s.Config.ReapRegenerable(w.r), false)
	if e != nil {
		rec.Result, rec.Reason = "error", e.Error()
		*errs = append(*errs, fmt.Errorf("%s %s: %w", hs.host.Name, wt.Path, e))
		return
	}
	rec.Files = result.Files
	rec.Result, rec.Reason = legacyVerdict(result)
}

// unmerged runs the dropped-task path for a worktree whose branch has no PR
// or whose newest closed PR did not merge. p is nil when the branch has no
// closed PR at all. Removal goes through the same guarded path as a merged
// PR's, and only when hk positively names the worktree's task as dropped or
// duplicate and closed at least legacyMinAge ago.
func (w *legacySweep) unmerged(ctx context.Context, hs *reapHost, c *legacyCand, wt *reap.Worktree, p *github.Pull, rec *reapReceipt, errs *[]error) {
	keep := func(reason string) { rec.Result, rec.Reason = "kept", reason }
	// The node's listed status is a cheap pre-check before any hk read; the
	// guarded removal still decides at the moment of removal.
	if l := c.listed; l != nil {
		switch {
		case l.Status.Error != "":
			keep("status-error")
			return
		case l.Status.Dirty || l.Status.Untracked > 0:
			keep("local-only")
			return
		}
	}
	id := w.taskFor(hs.host, c, wt, p)
	if id == "" {
		keep("no-task")
		return
	}
	t, e := w.task(ctx, id)
	switch {
	case e == nil:
	case errors.Is(e, integrations.ErrTaskNotFound):
		keep("no-task")
		return
	default:
		*errs = append(*errs, fmt.Errorf("task %s: %s", id, integrations.Detail(e)))
		keep("task-read-error")
		return
	}
	rec.Task = id
	if t.State != "dropped" && t.State != "duplicate" {
		state := t.State
		if state == "" {
			state = "unknown"
		}
		keep("task-" + state)
		return
	}
	// hk exposes no dedicated close time on the public task; the last update
	// is the close for a task that ended dropped or duplicate.
	if t.UpdatedAt.IsZero() {
		keep("task-close-unknown")
		return
	}
	if w.s.now().Sub(t.UpdatedAt) < legacyMinAge {
		keep("task-too-young")
		return
	}
	// Every commit must already be on the remote: the closed PR's head, the
	// base, or the remote branch of the same name.
	refs := []string{}
	if p != nil && p.Head.SHA != "" {
		refs = append(refs, p.Head.SHA)
	}
	for _, ref := range []string{w.r.Branch, wt.Branch} {
		seen := false
		for _, have := range refs {
			seen = seen || have == ref
		}
		if !seen {
			refs = append(refs, ref)
		}
	}
	if ok, failed := w.headOnRefs(ctx, wt.Head, refs, errs); failed {
		keep("head-check-failed")
		return
	} else if !ok {
		keep("local-only")
		return
	}
	switch {
	case hs.usageUnknown:
		keep("pane-scan-failed")
		return
	case hs.paneInUse(wt.Path):
		keep("pane-in-use")
		return
	}
	w.removeGuarded(ctx, hs, wt, rec, errs)
}

// taskFor names the hk task a worktree belongs to on host. Every positive
// identification counts — the closed PR's Mergewake-Task trailer, a bound
// job's trailer task, and each job record whose worktree or cwd is this path
// on this host (its own task_id or the task that bound it this pass). It
// returns "" unless all of them name exactly one task: no identification, or
// identifications that disagree, keep the worktree.
func (w *legacySweep) taskFor(host reap.Host, c *legacyCand, wt *reap.Worktree, p *github.Pull) string {
	var ids []string
	seen := map[string]bool{}
	addID := func(id string) {
		if id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	addJob := func(job string) {
		rec := w.jobRecs[job]
		if rec == nil || !jobRecordNames(rec, wt, host) {
			return
		}
		if rec.TaskID != nil && *rec.TaskID > 0 {
			addID(strconv.FormatInt(*rec.TaskID, 10))
		}
		addID(w.jobTasks[job])
	}
	var boundJob string
	if p != nil {
		b, ok := w.bindings[p.Number]
		if !ok {
			// The newest closed PR's binding failed this pass, so its task is
			// unknown: no job record may stand in for it.
			return ""
		}
		addID(b.Task)
		boundJob = b.Job
		addJob(b.Job)
	}
	if c.job != "" {
		addJob(c.job)
	}
	for _, job := range sortedJobIDs(w.jobRecs) {
		if job != c.job && job != boundJob {
			addJob(job)
		}
	}
	if len(ids) == 1 {
		return ids[0]
	}
	return ""
}

// jobRecordNames reports whether the job record's cwd or recorded worktree is
// this worktree on this host: the record must name the host the job ran on —
// absolute paths and branch-head pairs collide across hosts. A record that
// carries a recorded worktree must agree with it on branch and repo before
// its cwd may name the path: worktree paths get reused, and a record from a
// checkout that has since moved to another branch or repo is not this
// worktree's. The recorded head is spawn-time and legitimately advances as
// the job commits, so it confirms a match but never vetoes one.
func jobRecordNames(rec *integrations.JobRecord, wt *reap.Worktree, host reap.Host) bool {
	if rec.Host == "" || (rec.Host != host.Name && rec.Host != host.MachineID()) {
		return false
	}
	cwdMatch := func() bool {
		if rec.Cwd == "" {
			return false
		}
		cwd, path := filepath.Clean(rec.Cwd), filepath.Clean(wt.Path)
		return cwd == path || strings.HasPrefix(cwd, path+string(filepath.Separator))
	}
	if jw := rec.Worktree; jw != nil {
		if jw.Branch == nil || *jw.Branch == "" || *jw.Branch != wt.Branch {
			return false
		}
		origin := reap.RepoFromRemote(jw.Origin)
		if origin == "" || !strings.EqualFold(origin, wt.Repo) {
			return false
		}
		return cwdMatch() || (jw.Head != "" && jw.Head == wt.Head)
	}
	return cwdMatch()
}

func sortedJobIDs(recs map[string]*integrations.JobRecord) []string {
	ids := make([]string, 0, len(recs))
	for id := range recs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// headOnRefs reports whether head is one of refs or an ancestor of one,
// through GitHub's compare. A read error is appended to errs and reported as
// failed; a ref GitHub does not know counts as not an ancestor.
func (w *legacySweep) headOnRefs(ctx context.Context, head string, refs []string, errs *[]error) (ok, failed bool) {
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		if head == ref {
			return true, false
		}
		k := head + "\x00" + ref
		a, seen := w.ancestry[k]
		if !seen {
			var e error
			a, e = w.s.GH.Ancestor(ctx, w.r.Name, head, ref)
			switch {
			case github.NotFound(e):
				a = false
			case e != nil:
				*errs = append(*errs, fmt.Errorf("head ancestry %s...%s: %w", head, ref, e))
				return false, true
			}
			w.ancestry[k] = a
		}
		if a {
			return true, false
		}
	}
	return false, false
}

// task reads one hk task, cached for the sweep so a worktree's task is read
// at most once per pass.
func (w *legacySweep) task(ctx context.Context, id string) (client.Task, error) {
	if r, ok := w.taskReads[id]; ok {
		return r.task, r.err
	}
	var t client.Task
	var err error
	if w.tasks == nil {
		err = errors.New("hk task reader unavailable")
	} else {
		t, err = w.tasks.Task(ctx, id)
	}
	w.taskReads[id] = legacyTaskRead{task: t, err: err}
	return t, err
}
