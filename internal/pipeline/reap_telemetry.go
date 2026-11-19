package pipeline

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

type reapTelemetryKey struct{}

// ReapTelemetry is the per-repository wait accounting emitted by the daemon.
type ReapTelemetry struct {
	mu    sync.Mutex
	times map[string]time.Duration
	calls map[string]int
}

func WithReapTelemetry(ctx context.Context) (context.Context, *ReapTelemetry) {
	t := &ReapTelemetry{times: map[string]time.Duration{}, calls: map[string]int{}}
	return context.WithValue(ctx, reapTelemetryKey{}, t), t
}

func telemetryFrom(ctx context.Context) *ReapTelemetry {
	t, _ := ctx.Value(reapTelemetryKey{}).(*ReapTelemetry)
	return t
}

func (t *ReapTelemetry) add(category string, elapsed time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.times[category] += elapsed
	t.calls[category]++
}

// Add records a non-command reap wait, such as a panewire hub host scan.
func (t *ReapTelemetry) Add(category string, elapsed time.Duration) { t.add(category, elapsed) }

func (t *ReapTelemetry) Snapshot() (map[string]int64, map[string]int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	times, calls := map[string]int64{}, map[string]int{}
	for k, v := range t.times {
		times[k] = v.Milliseconds()
	}
	for k, v := range t.calls {
		calls[k] = v
	}
	return times, calls
}

type reapTimedRunner struct {
	base      integrations.Runner
	telemetry *ReapTelemetry
}

func (r reapTimedRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	category := "hosts"
	if command == "git" {
		category = "git"
	}
	if filepath.Base(command) == "panewire" {
		category = "panewire"
	}
	started := time.Now()
	out, err := r.base.Run(ctx, command, args, input, dir)
	r.telemetry.add(category, time.Since(started))
	return out, err
}

func reapRunner(ctx context.Context, base integrations.Runner) integrations.Runner {
	if t := telemetryFrom(ctx); t != nil {
		return reapTimedRunner{base: base, telemetry: t}
	}
	return base
}

// timedHubOps includes the optional guarded worktree calls so host wait time
// stays attributed when cleanup reaches a node after discovery.
type timedHubOps struct {
	reap.Ops
	telemetry *ReapTelemetry
}

func (o timedHubOps) measure(start time.Time) { o.telemetry.add("hosts", time.Since(start)) }
func (o timedHubOps) Panes(ctx context.Context) ([]reap.Pane, error) {
	st := time.Now()
	v, e := o.Ops.Panes(ctx)
	o.measure(st)
	return v, e
}
func (o timedHubOps) AllPanes(ctx context.Context) ([]reap.Pane, error) {
	st := time.Now()
	v, e := o.Ops.AllPanes(ctx)
	o.measure(st)
	return v, e
}
func (o timedHubOps) Inspect(ctx context.Context, path string) (*reap.Worktree, error) {
	st := time.Now()
	v, e := o.Ops.Inspect(ctx, path)
	o.measure(st)
	return v, e
}
func (o timedHubOps) Capture(ctx context.Context, p reap.Pane, dir string, since *time.Time, append bool) (reap.Capture, error) {
	st := time.Now()
	v, e := o.Ops.Capture(ctx, p, dir, since, append)
	o.measure(st)
	return v, e
}
func (o timedHubOps) Composer(ctx context.Context, p reap.Pane) (string, error) {
	st := time.Now()
	v, e := o.Ops.Composer(ctx, p)
	o.measure(st)
	return v, e
}
func (o timedHubOps) CloseExact(ctx context.Context, p reap.Pane) (string, error) {
	st := time.Now()
	v, e := o.Ops.CloseExact(ctx, p)
	o.measure(st)
	return v, e
}
func (o timedHubOps) JobDone(ctx context.Context, job, lane string) error {
	st := time.Now()
	e := o.Ops.JobDone(ctx, job, lane)
	o.measure(st)
	return e
}
func (o timedHubOps) WorktreeList(ctx context.Context, common, cursor string) (reap.WorktreePage, error) {
	st := time.Now()
	v, e := o.Ops.(reap.WorktreeLister).WorktreeList(ctx, common, cursor)
	o.measure(st)
	return v, e
}
func (o timedHubOps) WorktreeRemove(ctx context.Context, cwd, head, base string, regenerable []string, detached bool) (reap.WorktreeRemoveResult, error) {
	st := time.Now()
	v, e := o.Ops.(interface {
		WorktreeRemove(context.Context, string, string, string, []string, bool) (reap.WorktreeRemoveResult, error)
	}).WorktreeRemove(ctx, cwd, head, base, regenerable, detached)
	o.measure(st)
	return v, e
}
func (o timedHubOps) WorktreeRemoveDisposable(ctx context.Context, cwd, head, base string, regenerable, disposable []string, detached bool) (reap.WorktreeRemoveResult, error) {
	st := time.Now()
	v, e := o.Ops.(interface {
		WorktreeRemoveDisposable(context.Context, string, string, string, []string, []string, bool) (reap.WorktreeRemoveResult, error)
	}).WorktreeRemoveDisposable(ctx, cwd, head, base, regenerable, disposable, detached)
	o.measure(st)
	return v, e
}

type reapTimedJobs struct {
	base      reapJobs
	telemetry *ReapTelemetry
}

func (j reapTimedJobs) TaskJob(ctx context.Context, task string) (string, string, error) {
	started := time.Now()
	job, state, err := j.base.TaskJob(ctx, task)
	j.telemetry.add("hk", time.Since(started))
	return job, state, err
}

func (j reapTimedJobs) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	started := time.Now()
	job, err := j.base.Job(ctx, id)
	j.telemetry.add("hk", time.Since(started))
	return job, err
}
