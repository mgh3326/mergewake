package pipeline

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
	"github.com/mgh3326/mergewake/internal/tracing"
)

type reapTelemetryKey struct{}

// ReapTelemetry is the per-repository wait accounting emitted by the daemon.
//
// Besides the repository totals it splits the wall time of each pass stage
// (Stage) by category: the host, git, panewire and hk calls, the GitHub
// requests and the waits. It only times calls; it never changes what a pass does.
type ReapTelemetry struct {
	mu    sync.Mutex
	times map[string]time.Duration
	calls map[string]int

	// now and github are the clocks the stage accounting reads. nil means
	// time.Now, and no GitHub counters.
	now    func() time.Time
	github func() (time.Duration, int)
	// open is the stage running now. A call or wait recorded while it is open
	// belongs to it. A stage ends when the next one starts or Finish is called.
	open   *reapStageRun
	order  []string
	stages map[string]*reapStageTotals
}

// callTotal is the time and the count of one kind of call or wait.
type callTotal struct {
	time  time.Duration
	calls int
}

// reapStageTotals is one stage's wall time, split by category. A category is a
// kind of call or wait ("hosts", "git", "panewire", "hk", "wait"); detail names
// the kind within it, such as "hosts/panes".
type reapStageTotals struct {
	wall       time.Duration
	github     callTotal
	categories map[string]*callTotal
	detail     map[string]*callTotal
}

type reapStageRun struct {
	name    string
	start   time.Time
	gh      time.Duration
	ghCalls int
	end     func()
}

func WithReapTelemetry(ctx context.Context) (context.Context, *ReapTelemetry) {
	t := &ReapTelemetry{times: map[string]time.Duration{}, calls: map[string]int{}, stages: map[string]*reapStageTotals{}}
	return context.WithValue(ctx, reapTelemetryKey{}, t), t
}

// SetGitHub reads the GitHub request time and count of the pass, so each stage
// reports the GitHub work it did.
func (t *ReapTelemetry) SetGitHub(total func() time.Duration, calls func() int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.github = func() (time.Duration, int) { return total(), calls() }
}

func telemetryFrom(ctx context.Context) *ReapTelemetry {
	t, _ := ctx.Value(reapTelemetryKey{}).(*ReapTelemetry)
	return t
}

func (t *ReapTelemetry) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

func (t *ReapTelemetry) githubNow() (time.Duration, int) {
	t.mu.Lock()
	read := t.github
	t.mu.Unlock()
	if read == nil {
		return 0, 0
	}
	return read()
}

// Stage starts a named stage. The stage open before it ends here, so the stages
// of one repository run back to back. end, when set, runs when the stage ends;
// the daemon uses it to finish a trace span.
func (t *ReapTelemetry) Stage(name string, end func()) {
	if t == nil {
		return
	}
	t.Finish()
	gh, ghCalls := t.githubNow()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.open = &reapStageRun{name: name, start: t.clock(), gh: gh, ghCalls: ghCalls, end: end}
	t.totalsLocked(name)
}

// Finish ends the open stage, if there is one. A pass that returns early leaves
// its last stage open, so the daemon calls this before it reads the stages.
func (t *ReapTelemetry) Finish() {
	if t == nil {
		return
	}
	t.mu.Lock()
	run := t.open
	t.open = nil
	t.mu.Unlock()
	if run == nil {
		return
	}
	gh, ghCalls := t.githubNow()
	t.mu.Lock()
	st := t.totalsLocked(run.name)
	st.wall += t.clock().Sub(run.start)
	st.github.time += gh - run.gh
	st.github.calls += ghCalls - run.ghCalls
	t.mu.Unlock()
	if run.end != nil {
		run.end()
	}
}

// totalsLocked returns the totals of the named stage. The caller holds t.mu.
func (t *ReapTelemetry) totalsLocked(name string) *reapStageTotals {
	st := t.stages[name]
	if st == nil {
		st = &reapStageTotals{categories: map[string]*callTotal{}, detail: map[string]*callTotal{}}
		t.stages[name] = st
		t.order = append(t.order, name)
	}
	return st
}

// record adds one call or wait to its category. Inside an open stage it also
// adds it to the stage. A kind of "" counts the category only.
func (t *ReapTelemetry) record(category, kind string, elapsed time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.times[category] += elapsed
	t.calls[category]++
	if t.open == nil {
		return
	}
	st := t.totalsLocked(t.open.name)
	addCall(st.categories, category, elapsed)
	if kind != "" {
		addCall(st.detail, category+"/"+kind, elapsed)
	}
}

func addCall(into map[string]*callTotal, key string, elapsed time.Duration) {
	c := into[key]
	if c == nil {
		c = &callTotal{}
		into[key] = c
	}
	c.time += elapsed
	c.calls++
}

func (t *ReapTelemetry) add(category string, elapsed time.Duration) {
	t.record(category, "", elapsed)
}

// Add records a non-command reap wait, such as a panewire hub host scan.
func (t *ReapTelemetry) Add(category string, elapsed time.Duration) { t.add(category, elapsed) }

// Wait records time a pass spent waiting on a shared resource, such as the
// host-scan lock another repository of the same pass holds.
func (t *ReapTelemetry) Wait(kind string, elapsed time.Duration) {
	t.record("wait", kind, elapsed)
}

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

// ReapPass adds up the stage accounting of every repository one reap pass
// visits, and of the pass's own sweeps such as the legacy sweep.
type ReapPass struct {
	mu     sync.Mutex
	order  []string
	stages map[string]*reapStageTotals
}

func NewReapPass() *ReapPass {
	return &ReapPass{stages: map[string]*reapStageTotals{}}
}

// Add finishes t's open stage and adds its stages to the pass.
func (p *ReapPass) Add(t *ReapTelemetry) {
	if p == nil || t == nil {
		return
	}
	t.Finish()
	t.mu.Lock()
	defer t.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, name := range t.order {
		src := t.stages[name]
		dst := p.totalsLocked(name)
		dst.wall += src.wall
		dst.github.time += src.github.time
		dst.github.calls += src.github.calls
		for k, c := range src.categories {
			addTotal(dst.categories, k, c)
		}
		for k, c := range src.detail {
			addTotal(dst.detail, k, c)
		}
	}
}

func (p *ReapPass) totalsLocked(name string) *reapStageTotals {
	st := p.stages[name]
	if st == nil {
		st = &reapStageTotals{categories: map[string]*callTotal{}, detail: map[string]*callTotal{}}
		p.stages[name] = st
		p.order = append(p.order, name)
	}
	return st
}

func addTotal(into map[string]*callTotal, key string, c *callTotal) {
	d := into[key]
	if d == nil {
		d = &callTotal{}
		into[key] = d
	}
	d.time += c.time
	d.calls += c.calls
}

// reapSummaryCategories is the fixed category set of every stage, so every
// pass row has the same fields. other is what no category accounts for.
var reapSummaryCategories = []string{"hosts", "git", "panewire", "hk", "github", "wait"}

// Summary is the pass summary row. wall is the pass's wall time and rateWait the
// GitHub throttle the next pass waits on. Each stage's categories, other
// included, add up to its wall time. unstaged_ms is the pass time no stage
// covers.
func (p *ReapPass) Summary(wall, rateWait time.Duration) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	stages := map[string]any{}
	var staged time.Duration
	for _, name := range p.order {
		st := p.stages[name]
		staged += st.wall
		cats := map[string]int64{}
		calls := map[string]int{}
		var sum time.Duration
		for _, k := range reapSummaryCategories {
			c := st.categories[k]
			if k == "github" {
				c = &callTotal{time: st.github.time, calls: st.github.calls}
			}
			if c == nil {
				c = &callTotal{}
			}
			cats[k] = c.time.Milliseconds()
			calls[k] = c.calls
			sum += c.time
		}
		cats["other"] = (st.wall - sum).Milliseconds()
		detail := map[string]int64{}
		for k, c := range st.detail {
			detail[k] = c.time.Milliseconds()
		}
		stages[name] = map[string]any{
			"wall_ms":     st.wall.Milliseconds(),
			"categories":  cats,
			"calls":       calls,
			"detail":      detail,
			"other_ratio": ratio(st.wall-sum, st.wall),
		}
	}
	return map[string]any{
		"wall_ms":      wall.Milliseconds(),
		"staged_ms":    staged.Milliseconds(),
		"unstaged_ms":  (wall - staged).Milliseconds(),
		"rate_wait_ms": rateWait.Milliseconds(),
		"stages":       stages,
	}
}

func ratio(part, whole time.Duration) float64 {
	if whole <= 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

// reapStage starts a stage of the pass under ctx. Its trace span is a child of
// the pass span in ctx; it ends with the stage.
func reapStage(ctx context.Context, name string) {
	t := telemetryFrom(ctx)
	if t == nil {
		return
	}
	_, span := tracing.Span(ctx, "mergewake.reap.stage")
	tracing.Tag(span, "stage", name)
	t.Stage(name, func() { tracing.Finish(span) })
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
	operation := filepath.Base(command)
	if len(args) > 0 {
		operation += " " + args[0]
	}
	r.telemetry.record(category, operation, time.Since(started))
	return out, budgetCut(ctx, err, operation)
}

func reapRunner(ctx context.Context, base integrations.Runner) integrations.Runner {
	return reapTimedRunner{base: base, telemetry: telemetryFrom(ctx)}
}

// timedHubOps includes the optional guarded worktree calls so host wait time
// stays attributed when cleanup reaches a node after discovery.
type timedHubOps struct {
	reap.Ops
	telemetry *ReapTelemetry
}

func (o timedHubOps) measure(kind string, start time.Time) {
	o.telemetry.record("hosts", kind, time.Since(start))
}
func (o timedHubOps) Panes(ctx context.Context) ([]reap.Pane, error) {
	st := time.Now()
	v, e := o.Ops.Panes(ctx)
	o.measure("panes", st)
	return v, budgetCut(ctx, e, "host panes")
}
func (o timedHubOps) AllPanes(ctx context.Context) ([]reap.Pane, error) {
	st := time.Now()
	v, e := o.Ops.AllPanes(ctx)
	o.measure("all-panes", st)
	return v, budgetCut(ctx, e, "host all-panes scan")
}
func (o timedHubOps) Inspect(ctx context.Context, path string) (*reap.Worktree, error) {
	st := time.Now()
	v, e := o.Ops.Inspect(ctx, path)
	o.measure("inspect", st)
	return v, budgetCut(ctx, e, "worktree inspect")
}
func (o timedHubOps) Capture(ctx context.Context, p reap.Pane, dir string, since *time.Time, append bool) (reap.Capture, error) {
	st := time.Now()
	v, e := o.Ops.Capture(ctx, p, dir, since, append)
	o.measure("capture", st)
	return v, budgetCut(ctx, e, "pane capture")
}
func (o timedHubOps) Composer(ctx context.Context, p reap.Pane) (string, error) {
	st := time.Now()
	v, e := o.Ops.Composer(ctx, p)
	o.measure("composer", st)
	return v, budgetCut(ctx, e, "pane composer scan")
}
func (o timedHubOps) CloseExact(ctx context.Context, p reap.Pane) (string, error) {
	st := time.Now()
	v, e := o.Ops.CloseExact(ctx, p)
	o.measure("pane-close", st)
	return v, budgetCut(ctx, e, "pane close")
}
func (o timedHubOps) JobDone(ctx context.Context, job, lane string) error {
	st := time.Now()
	e := o.Ops.JobDone(ctx, job, lane)
	o.measure("job-done", st)
	return budgetCut(ctx, e, "panewire job-done check")
}
func (o timedHubOps) WorktreeList(ctx context.Context, common, cursor string) (reap.WorktreePage, error) {
	st := time.Now()
	v, e := o.Ops.(reap.WorktreeLister).WorktreeList(ctx, common, cursor)
	o.measure("worktree-list", st)
	return v, budgetCut(ctx, e, "worktree list")
}
func (o timedHubOps) WorktreeRemove(ctx context.Context, cwd, head, base string, regenerable []string, detached bool) (reap.WorktreeRemoveResult, error) {
	st := time.Now()
	mutationCtx, cancel := reap.MutationContext(ctx)
	defer cancel()
	v, e := o.Ops.(interface {
		WorktreeRemove(context.Context, string, string, string, []string, bool) (reap.WorktreeRemoveResult, error)
	}).WorktreeRemove(mutationCtx, cwd, head, base, regenerable, detached)
	o.measure("worktree-remove", st)
	return v, budgetCut(ctx, e, "guarded worktree removal")
}
func (o timedHubOps) WorktreeRemoveDisposable(ctx context.Context, cwd, head, base string, regenerable, disposable []string, detached bool) (reap.WorktreeRemoveResult, error) {
	st := time.Now()
	mutationCtx, cancel := reap.MutationContext(ctx)
	defer cancel()
	v, e := o.Ops.(interface {
		WorktreeRemoveDisposable(context.Context, string, string, string, []string, []string, bool) (reap.WorktreeRemoveResult, error)
	}).WorktreeRemoveDisposable(mutationCtx, cwd, head, base, regenerable, disposable, detached)
	o.measure("worktree-remove", st)
	return v, budgetCut(ctx, e, "guarded worktree removal")
}

type reapTimedJobs struct {
	base      reapJobs
	telemetry *ReapTelemetry
}

func (j reapTimedJobs) TaskJob(ctx context.Context, task string) (string, string, error) {
	started := time.Now()
	job, state, err := j.base.TaskJob(ctx, task)
	j.telemetry.record("hk", "task-job", time.Since(started))
	return job, state, budgetCut(ctx, err, "hk task-job lookup")
}

func (j reapTimedJobs) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	started := time.Now()
	job, err := j.base.Job(ctx, id)
	j.telemetry.record("hk", "job", time.Since(started))
	return job, budgetCut(ctx, err, "hk job lookup")
}
