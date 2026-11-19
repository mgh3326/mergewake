package integrations

import (
	"context"
	"sync"

	"github.com/mgh3326/handoffkeep/client"
)

// taskMemo is one evaluation's memo of hk reads. Within the evaluation its
// context carries, a task is read once: Lane, TaskJob, TaskPriority and Task
// share the answer; a job record read through JobReadOnce is read once for the
// route lookups that ask for it. A failed read is not kept, and a write to a
// task drops its entry, so the memo never answers with a task the evaluation
// has changed. The memo ends with its context; nothing carries across
// evaluations.
type taskMemo struct {
	mu    sync.Mutex
	tasks map[int64]client.Task
	jobs  map[string]JobRecord
}

type taskMemoKey struct{}

// WithTaskMemo scopes a task- and job-read memo to ctx, one evaluation.
func WithTaskMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, taskMemoKey{}, &taskMemo{tasks: map[int64]client.Task{}, jobs: map[string]JobRecord{}})
}

// JobReadOnce reads job id through jobs for a route or liveness decision. Within
// one evaluation (see WithTaskMemo) a found record is read once and shared by
// the bound-job lookup, the author-pane route and the takeover check. A
// not-found answer and a failed read are not kept: a spawn can create the
// record later in the same evaluation, and a failure is not an answer.
// Polling reads (a waiter watching a job's state) must not use this: a kept
// record never changes under them; they call the reader directly.
func JobReadOnce(ctx context.Context, jobs JobReader, id string) (JobRecord, error) {
	memo := taskMemoOf(ctx)
	if rec, ok := memo.getJob(id); ok {
		return rec, nil
	}
	rec, e := jobs.Job(ctx, id)
	if e == nil {
		memo.putJob(id, rec)
	}
	return rec, e
}

func (m *taskMemo) getJob(id string) (JobRecord, bool) {
	if m == nil {
		return JobRecord{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	x, ok := m.jobs[id]
	return x, ok
}

func (m *taskMemo) putJob(id string, x JobRecord) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[id] = x
}

// taskMemoOf is nil outside an evaluation; every taskMemo method accepts nil.
func taskMemoOf(ctx context.Context) *taskMemo {
	m, _ := ctx.Value(taskMemoKey{}).(*taskMemo)
	return m
}

func (m *taskMemo) get(id int64) (client.Task, bool) {
	if m == nil {
		return client.Task{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	x, ok := m.tasks[id]
	return x, ok
}

func (m *taskMemo) put(id int64, x client.Task) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[id] = x
}

// forget drops the entries of the tasks a write touched.
func (m *taskMemo) forget(ids ...int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.tasks, id)
	}
}
