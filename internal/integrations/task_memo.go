package integrations

import (
	"context"
	"sync"

	"github.com/mgh3326/handoffkeep/client"
)

// taskMemo is one evaluation's memo of hk task reads. Within the evaluation
// its context carries, a task is read once: Lane, TaskJob, TaskPriority and
// Task share the answer. A failed read is not kept, and a write to a task
// drops its entry, so the memo never answers with a task the evaluation has
// changed. The memo ends with its context; nothing carries across evaluations.
type taskMemo struct {
	mu    sync.Mutex
	tasks map[int64]client.Task
}

type taskMemoKey struct{}

// WithTaskMemo scopes a task-read memo to ctx, one evaluation.
func WithTaskMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, taskMemoKey{}, &taskMemo{tasks: map[int64]client.Task{}})
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
