package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A merged PR carrying Mergewake-Task is reaped through its job, not its
// worktree: the same binding the findings router uses (Mergewake-Task → hk
// task → bound job → the job's recorded host and label) names the lead pane
// wherever it was launched. The pane closes only when no open PR in any
// configured repo still carries the task, the pane is idle or done and
// nobody owns it, its composer is empty and its job dir exists; the screen
// is saved first and the job is then marked done with `panewire done`. A PR
// without the trailer, or whose task has no bound job, keeps the worktree
// match.

// reapJobs is the hk surface the job path reads.
type reapJobs interface {
	TaskJob(context.Context, string) (string, string, error)
	Job(context.Context, string) (integrations.JobRecord, error)
}

// reapBinding is a merged PR's Mergewake-Task and the job hk bound to it;
// Job is empty when the PR has no trailer or its task no job.
type reapBinding struct{ Task, Job string }

// passJobRecords is one pass's read of hk job records, shared by the pane path
// and the worktree candidate source so a job is read at most once a pass. A
// read error is kept for the pass, so neither reader retries it, but it is
// never remembered across passes and never reads as a missing record.
type passJobRecords struct {
	jobs reapJobs
	recs map[string]integrations.JobRecord
	errs map[string]error
}

func newPassJobRecords(jobs reapJobs) *passJobRecords {
	return &passJobRecords{jobs: jobs, recs: map[string]integrations.JobRecord{}, errs: map[string]error{}}
}

// job reads one record, from the pass's cache when either reader already did.
func (c *passJobRecords) job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if rec, ok := c.recs[id]; ok {
		return rec, nil
	}
	if e, ok := c.errs[id]; ok {
		return integrations.JobRecord{}, e
	}
	rec, e := c.jobs.Job(ctx, id)
	if e != nil {
		c.errs[id] = e
		return integrations.JobRecord{}, e
	}
	c.recs[id] = rec
	return rec, nil
}

// jobCand is one worktree a merged PR's job record named: a configured host and
// the directory of an attempt that worked the PR.
type jobCand struct{ host, path string }

// reapJobReach counts what the job-record source reached in one pass and what
// it could not, the way the legacy sweep counts its binding.
type reapJobReach struct {
	PRs       int            `json:"prs"`
	Worktrees int            `json:"worktrees"`
	Misses    map[string]int `json:"misses"`
}

func newReapJobReach() reapJobReach { return reapJobReach{Misses: map[string]int{}} }

func (r *reapJobReach) miss(reason string) { r.Misses[reason]++ }

// reapJobCandidates returns the worktrees PR p's task names through hk: the
// task's bound job (already resolved by reapBind) and the record of that job,
// whose host and cwd panewire wrote at spawn. It is reap's third discovery
// source beside a live pane and an earlier receipt: neither sees a worktree
// whose pane closed before reap looked at it, or whose close row recorded no
// cwd. The record's host is mapped to a configured host by name or machine id,
// the rule the pane path and the legacy sweep already use. Misses are counted
// in reach; a record read error is returned and is never counted as a missing
// job, and it yields no candidate. p is a PR reap already listed and both reads
// are the ones the pane path already made, so this costs no GitHub call.
func (s *Service) reapJobCandidates(ctx context.Context, p github.Pull, b reapBinding, bindErr error, records *passJobRecords, reach *reapJobReach) ([]jobCand, error) {
	reach.PRs++
	switch {
	case bindErr != nil:
		// The caller reports it; a failed binding is not "no job".
		reach.miss("binding-error")
		return nil, nil
	case b.Task == "":
		reach.miss("no-task-trailer")
		return nil, nil
	case b.Job == "":
		reach.miss("no-bound-job")
		return nil, nil
	}
	rec, e := records.job(ctx, b.Job)
	switch {
	case errors.Is(e, integrations.ErrJobNotFound):
		// A job may still write its record; that is a miss, not a failure.
		reach.miss("no-job-record")
		return nil, nil
	case e != nil:
		reach.miss("job-read-error")
		return nil, fmt.Errorf("PR %d job record %s: %s", p.Number, b.Job, integrations.Detail(e))
	}
	if rec.Cwd == "" {
		reach.miss("job-without-cwd")
		return nil, nil
	}
	var host string
	for _, h := range s.Config.Hosts {
		if rec.Host != "" && (h.Name == rec.Host || h.MachineID() == rec.Host) {
			host = h.Name
		}
	}
	if host == "" {
		reach.miss("job-host-unconfigured")
		return nil, nil
	}
	reach.Worktrees++
	return []jobCand{{host: host, path: rec.Cwd}}, nil
}

var reapBindMu sync.Mutex

// reapBind resolves a merged PR to its task and bound job once per process: a
// merged PR's trailer and binding do not change, so neither the head commit
// nor hk is read again for it on later passes. A read error is not cached
// and retries next pass, and neither is a task with no job yet or one read
// without an hk jobs reader.
func (s *Service) reapBind(ctx context.Context, r config.Repo, p github.Pull) (reapBinding, error) {
	k := fmt.Sprintf("%s#%d", r.Name, p.Number)
	reapBindMu.Lock()
	b, ok := s.reapBinds[k]
	reapBindMu.Unlock()
	if ok {
		return b, nil
	}
	task, e := s.prTask(ctx, r, p)
	if e != nil {
		return b, e
	}
	if task != "" {
		jobs, ok := s.Sink.(reapJobs)
		if !ok {
			return reapBinding{Task: task}, nil
		}
		job, _, e := jobs.TaskJob(ctx, task)
		switch {
		case e != nil:
			return b, e
		case job == "":
			// A job may still be bound to the task later: ask again.
			return reapBinding{Task: task}, nil
		}
		b = reapBinding{Task: task, Job: job}
	}
	reapBindMu.Lock()
	if s.reapBinds == nil {
		s.reapBinds = map[string]reapBinding{}
	}
	s.reapBinds[k] = b
	reapBindMu.Unlock()
	return b, nil
}

// reapJobPane closes the pane of PR p's bound job when the whole job is
// finished, then marks the job done. closed names jobs an earlier pass closed
// and reap has not marked done, so a job whose pane is already gone still
// gets its done; everClosed names jobs any pass closed, so a pane gone for that
// reason is not reported missing. match reports whether a worktree is PR p's own: a lead
// sitting in it records the worktree, which later cleanup reads.
func (s *Service) reapJobPane(ctx context.Context, r config.Repo, p github.Pull, b reapBinding, hosts []*reapHost, byName map[string]*reapHost, match func(*reap.Worktree) bool, mergedAt *time.Time, acting bool, records *passJobRecords, closed map[string]reapReceipt, everClosed map[string]bool, note func(reapReceipt)) {
	rec := reapReceipt{Action: "close-pane", Repo: r.Name, PR: p.Number, Task: b.Task, Job: b.Job}
	if records == nil {
		// Only a sink holding an hk reader binds a job, so Reap always passes
		// one; without it there is nothing to read.
		return
	}
	job, e := records.job(ctx, b.Job)
	switch {
	case errors.Is(e, integrations.ErrJobNotFound):
		rec.Result, rec.Reason = "skipped", "job-record-missing"
		note(rec)
		return
	case e != nil:
		rec.Result, rec.Reason = "error", integrations.Detail(e)
		note(rec)
		return
	}
	hs, pane, n, ok, scanned := jobPane(hosts, job, b.Job)
	switch {
	case !ok:
		rec.Result, rec.Reason = "skipped", "job-host-unconfigured"
		note(rec)
		return
	case n == 0:
		switch prior, ok := closed[b.Job]; {
		case ok && !terminalJob(job.State):
			s.reapJobDone(ctx, prior, byName[prior.Host], job, acting, note)
		case scanned != nil && !ok && !everClosed[b.Job]:
			// The host's panes were read and none wears the label, and no pass
			// closed it: say why nothing follows, once (the row dedupes).
			rec.Host = scanned.host.Name
			rec.Result, rec.Reason = "skipped", "job-pane-missing"
			note(rec)
		}
		return
	case n > 1:
		rec.Result, rec.Reason = "skipped", "job-pane-ambiguous"
		note(rec)
		return
	}
	rec.Host, rec.Pane = hs.host.Name, pane.ID
	if w := hs.trees[pane.Cwd]; match(w) {
		rec.Cwd, rec.Worktree, rec.Branch = pane.Cwd, w.Path, w.Branch
	}
	switch {
	case pane.ID == "":
		rec.Result, rec.Reason = "skipped", "pane-id-missing"
	case pane.Status != "idle" && pane.Status != "done":
		st := pane.Status
		if st == "" {
			st = "unknown"
		}
		rec.Result, rec.Reason = "skipped", "status:"+st
	case reap.Protected(pane.Name, s.Config.Reap.Protect):
		rec.Result, rec.Reason = "skipped", "protected:"+pane.Name
	case hs.host.Jobs == "" && !hs.viaHub:
		rec.Result, rec.Reason = "skipped", "jobs-dir-unset"
	case !reap.ValidJobName(b.Job):
		rec.Result, rec.Reason = "skipped", "job-name-invalid"
	case job.PaneID == "":
		// Without the recorded pane any pane wearing the label could match.
		rec.Result, rec.Reason = "skipped", "pane-id-unrecorded"
	case pane.Focused || job.PaneID != pane.ID:
		// The operator has the pane in front of them, or the label now
		// belongs to a pane the job record does not name.
		rec.Result, rec.Reason = "skipped", "operator-owned"
	default:
		s.reapPaneClose(ctx, &rec, hs, pane, b.Job, b.Task, mergedAt, acting)
	}
	note(rec)
	if rec.Result == "closed" || rec.Result == "would-close" {
		s.reapJobDone(ctx, rec, hs, job, acting, note)
	}
}

// reapPaneClose runs the whole-task, composer and capture checks in that
// order and closes the pane when they all pass. task may be empty (a PR
// without the trailer); the pane's output is saved into job's directory, and
// only a job whose dir already exists is a fleet job (reap.Exec.Capture has
// the rules). Shadow and dry runs only name the screen path.
func (s *Service) reapPaneClose(ctx context.Context, rec *reapReceipt, hs *reapHost, pane reap.Pane, job, task string, mergedAt *time.Time, acting bool) {
	if task != "" {
		open, e := s.jobOpenPRs(ctx, task)
		if e != nil {
			rec.Result, rec.Reason = "error", "open PRs: "+integrations.Detail(e)
			return
		}
		if len(open) > 0 {
			rec.Result, rec.Reason, rec.Open = "skipped", "job_has_open_pr", open
			return
		}
	}
	switch c, e := hs.ops.Composer(ctx, pane); {
	case e != nil:
		rec.Result, rec.Reason = "skipped", reap.ComposerUnknown
		return
	case c != reap.ComposerEmpty:
		rec.Result, rec.Reason = "skipped", c
		return
	}
	saved, e := hs.ops.Capture(ctx, pane, job, mergedAt, acting)
	switch {
	case e != nil:
		rec.Result, rec.Reason = "error", e.Error()
		return
	case saved.Skip != "":
		rec.Result, rec.Reason = "skipped", saved.Skip
		return
	}
	rec.Job, rec.Saved = job, saved.Saved
	if !acting {
		rec.Result = "would-close"
		hs.closed[pane.ID] = true
		return
	}
	rec.Result, rec.Reason = s.reclose(ctx, hs, pane)
	if rec.Result == "closed" {
		hs.closed[pane.ID] = true
	}
}

// reapJobDone marks the job of a closed pane done, unless hk already holds it
// terminal (a lead that ran panewire done itself).
func (s *Service) reapJobDone(ctx context.Context, closed reapReceipt, hs *reapHost, job integrations.JobRecord, acting bool, note func(reapReceipt)) {
	v := reapReceipt{Action: "job-done", Repo: closed.Repo, PR: closed.PR, Host: closed.Host, Pane: closed.Pane, Task: closed.Task, Job: closed.Job, Saved: closed.Saved}
	switch {
	case terminalJob(job.State):
		v.Result, v.Reason = "skipped", "already-"+job.State
	case !acting:
		v.Result = "would-done"
	case hs == nil:
		v.Result, v.Reason = "error", "host "+closed.Host+" not configured"
	default:
		if e := hs.ops.JobDone(ctx, closed.Job, closed.Saved); e != nil {
			v.Result, v.Reason = "error", e.Error()
		} else {
			v.Result = "done"
		}
	}
	note(v)
}

// jobPane finds the job's pane by its label on the host panewire recorded: a
// configured host whose name or machine is the record's host. herdr pane ids
// are host-local, so no other host is searched; a record whose host is not
// configured resolves no host (ok false). n counts label matches: only
// exactly one is used. scanned is a host whose panes were read, set only when every
// host matching the record was readable, so it supports "no pane wears the label".
func jobPane(hosts []*reapHost, job integrations.JobRecord, id string) (at *reapHost, found reap.Pane, n int, ok bool, scanned *reapHost) {
	unread := false
	label := job.Label
	if label == "" {
		label = id
	}
	for _, hs := range hosts {
		if job.Host == "" || (hs.host.Name != job.Host && hs.host.MachineID() != job.Host) {
			continue
		}
		ok = true
		if hs.failed {
			unread = true
			continue
		}
		scanned = hs
		for _, pane := range hs.panes {
			if pane.Name == label {
				at, found = hs, pane
				n++
			}
		}
	}
	if unread {
		// A host that could not be read may hold the label: say nothing.
		scanned = nil
	}
	return at, found, n, ok, scanned
}

// jobOpenPRs lists the open PRs, against any base in every configured repo,
// that carry Mergewake-Task task. A repo mergewake does not manage is not
// seen.
func (s *Service) jobOpenPRs(ctx context.Context, task string) ([]string, error) {
	lister, ok := s.GH.(interface {
		OpenPulls(context.Context, string, string) ([]github.Pull, error)
	})
	if !ok {
		return nil, errors.New("open PR listing unavailable")
	}
	var open []string
	for _, repo := range s.Config.Repos {
		prs, e := lister.OpenPulls(ctx, repo.Name, "")
		if e != nil {
			return nil, fmt.Errorf("%s: %w", repo.Name, e)
		}
		for _, q := range prs {
			t, e := s.prTask(ctx, repo, q)
			if e != nil {
				return nil, fmt.Errorf("%s#%d: %w", repo.Name, q.Number, e)
			}
			if t == task {
				open = append(open, fmt.Sprintf("%s#%d", repo.Name, q.Number))
			}
		}
	}
	return open, nil
}
