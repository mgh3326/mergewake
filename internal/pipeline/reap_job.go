package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A merged PR carrying Mergewake-Task is reaped through its job, not only its
// worktree: the same binding the findings router uses (Mergewake-Task → hk
// task → bound job → the job's recorded host and cwd) names a worktree that
// outlives any pane, so cleanup still finds it after panewire's node has
// closed the pane. Mergewake never closes the pane itself and never marks the
// job done: the node owns the pane's close, and the job's own harness ends it.
// A PR without the trailer, or whose task has no bound job, keeps the worktree
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
	jobs  reapJobs
	recs  map[string]integrations.JobRecord
	errs  map[string]error
	cands map[string]jobResolved
}

func newPassJobRecords(jobs reapJobs) *passJobRecords {
	return &passJobRecords{jobs: jobs, recs: map[string]integrations.JobRecord{}, errs: map[string]error{}, cands: map[string]jobResolved{}}
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

// Job is job, so the pass's memo can stand in for the job source of any sweep
// that reads a record the pass has already read.
func (c *passJobRecords) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	return c.job(ctx, id)
}

// TaskJob is the job source's own task lookup, unchanged by the memo.
func (c *passJobRecords) TaskJob(ctx context.Context, task string) (string, string, error) {
	return c.jobs.TaskJob(ctx, task)
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

// jobResolved is one job's worktree as a pass resolved it: the configured host
// and cwd its record names, and the branch that worktree is on. miss names why
// there is no worktree; err is a read error, which is never a miss. It is
// resolved once per job per pass, and each merged PR of the task then takes or
// refuses it by branch.
type jobResolved struct {
	cand   jobCand
	branch string
	miss   string
	err    error
}

// reapJobCandidates returns the worktree PR p's task names through hk: the
// task's bound job (already resolved by reapBind) and the record of that job,
// whose host and cwd panewire wrote at spawn. It is reap's third discovery
// source beside a live pane and an earlier receipt: neither sees a worktree
// whose pane closed before reap looked at it, or whose close row recorded no
// cwd. The record's host is mapped to a configured host by name or machine id,
// the rule the pane path and the legacy sweep already use. The worktree is
// offered only to a PR whose head branch it is on, so a sibling PR of the same
// task costs no GitHub call and leaves no row. Misses are counted in reach; a
// record or worktree read error is returned and is never counted as a missing
// job, and it yields no candidate. p is a PR reap already listed; the record
// and worktree reads are local, so this costs no GitHub call.
func (s *Service) reapJobCandidates(ctx context.Context, p github.Pull, b reapBinding, bindErr error, records *passJobRecords, byName map[string]*reapHost, reach *reapJobReach, settled func(host, path string) bool) ([]jobCand, error) {
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
	res := s.resolveJobWorktree(ctx, b.Job, records, byName, settled)
	if res.err != nil {
		reach.miss(res.miss)
		return nil, fmt.Errorf("PR %d %w", p.Number, res.err)
	}
	if res.miss != "" {
		reach.miss(res.miss)
		return nil, nil
	}
	if res.branch != p.Head.Ref {
		// The worktree is on another PR's branch: not this PR's, and not a miss.
		return nil, nil
	}
	reach.Worktrees++
	return []jobCand{res.cand}, nil
}

// resolveJobWorktree resolves job's worktree once per pass; later PRs of the
// same job reuse the result. A settled miss is not cached: settled reads the
// current PR's own rows, so another PR of the job must still resolve it.
func (s *Service) resolveJobWorktree(ctx context.Context, job string, records *passJobRecords, byName map[string]*reapHost, settled func(host, path string) bool) jobResolved {
	if res, ok := records.cands[job]; ok {
		return res
	}
	res := s.jobWorktree(ctx, job, records, byName, settled)
	if res.miss != "job-worktree-settled" {
		records.cands[job] = res
	}
	return res
}

// jobWorktree reads job's record and the worktree it names: the host and cwd
// panewire wrote at spawn, and the branch that worktree is on. A worktree the
// host no longer has is a miss, not a candidate.
func (s *Service) jobWorktree(ctx context.Context, job string, records *passJobRecords, byName map[string]*reapHost, settled func(host, path string) bool) jobResolved {
	rec, e := records.job(ctx, job)
	switch {
	case errors.Is(e, integrations.ErrJobNotFound):
		// A job may still write its record; that is a miss, not a failure.
		return jobResolved{miss: "no-job-record"}
	case e != nil:
		return jobResolved{miss: "job-read-error", err: fmt.Errorf("job record %s: %s", job, integrations.Detail(e))}
	}
	if rec.Cwd == "" {
		return jobResolved{miss: "job-without-cwd"}
	}
	var host string
	for _, h := range s.Config.Hosts {
		if rec.Host != "" && (h.Name == rec.Host || h.MachineID() == rec.Host) {
			host = h.Name
		}
	}
	if host == "" {
		return jobResolved{miss: "job-host-unconfigured"}
	}
	// A worktree an earlier pass already removed or proved gone is terminal:
	// it is not inspected again (the receipt source skips it the same way).
	if settled != nil && settled(host, rec.Cwd) {
		return jobResolved{miss: "job-worktree-settled"}
	}
	hs := byName[host]
	if hs == nil || hs.failed {
		return jobResolved{miss: "job-host-unreadable"}
	}
	w, ok := hs.trees[rec.Cwd]
	if !ok {
		var e error
		if w, e = hs.ops.Inspect(ctx, rec.Cwd); e != nil {
			return jobResolved{miss: "job-worktree-error", err: fmt.Errorf("job %s worktree %s: %s", job, rec.Cwd, integrations.Detail(e))}
		}
	}
	if w == nil {
		return jobResolved{miss: "job-worktree-gone"}
	}
	return jobResolved{cand: jobCand{host: host, path: rec.Cwd}, branch: w.Branch}
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
