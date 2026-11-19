package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// notifySlow is the length of a notify run from which the run logs its steps.
const notifySlow = 10 * time.Second

// notifyTrace times the steps of one NotifyPR run: each outside call it makes
// (GitHub, hk, the author pane scan, panewire), by name. A step can run inside
// another (verdict holds lane and emit), so the steps overlap and each reports
// its own time.
type notifyTrace struct {
	mu    sync.Mutex
	steps []*NotifyStep
	index map[string]*NotifyStep
}

// NotifyStep is one named step of a notify run: how many times it ran, its
// time, and the GitHub and hk requests sent under it. Requests count attempts,
// so a retried request counts once per attempt.
type NotifyStep struct {
	Name           string `json:"name"`
	Calls          int    `json:"calls"`
	Ms             int64  `json:"ms"`
	GitHubRequests int    `json:"github_requests"`
	HKRequests     int    `json:"hk_requests"`
}

type notifyTraceKey struct{}

// withNotifyTrace scopes a step trace and an hk request counter to ctx, one
// NotifyPR run.
func withNotifyTrace(ctx context.Context) (context.Context, *notifyTrace) {
	t := &notifyTrace{index: map[string]*NotifyStep{}}
	ctx, _ = integrations.CountHK(ctx)
	return context.WithValue(ctx, notifyTraceKey{}, t), t
}

// notifyStart begins the named step of the notify run under ctx and returns
// the function that ends it. Outside a notify run it records nothing.
func notifyStart(ctx context.Context, name string) (end func()) {
	t, _ := ctx.Value(notifyTraceKey{}).(*notifyTrace)
	if t == nil {
		return func() {}
	}
	start := time.Now()
	gh, hk := github.RequestTotal(ctx), integrations.HKRequestTotal(ctx)
	return func() {
		t.add(name, time.Since(start), github.RequestTotal(ctx)-gh, integrations.HKRequestTotal(ctx)-hk)
	}
}

// notifyStep runs fn as the named step of the notify run under ctx.
func notifyStep(ctx context.Context, name string, fn func() error) error {
	end := notifyStart(ctx, name)
	defer end()
	return fn()
}

func (t *notifyTrace) add(name string, took time.Duration, gh, hk int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.index[name]
	if st == nil {
		st = &NotifyStep{Name: name}
		t.index[name] = st
		t.steps = append(t.steps, st)
	}
	st.Calls++
	st.Ms += took.Milliseconds()
	st.GitHubRequests += gh
	st.HKRequests += hk
}

// logIfSlow logs the steps of a run that took notifySlow or longer, so a slow
// notify names its cause. A nil trace or log records nothing.
func (t *notifyTrace) logIfSlow(log *slog.Logger, repo string, pr int, took time.Duration) {
	if t == nil || log == nil || took < notifySlow {
		return
	}
	t.mu.Lock()
	steps := make([]NotifyStep, 0, len(t.steps))
	for _, st := range t.steps {
		steps = append(steps, *st)
	}
	t.mu.Unlock()
	log.Info("notify slow", "repo", repo, "pr", pr, "notify_ms", took.Milliseconds(), "steps", steps)
}
