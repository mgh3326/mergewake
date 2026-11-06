package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// reviewerSink is a sink that reads hk job records, so the engine starts
// reviewers without waiting for them.
type reviewerSink struct {
	workerSink
	mu   sync.Mutex
	jobs map[string]integrations.JobRecord
}

func (s *reviewerSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	s.mu.Lock()
	rec, ok := s.jobs[id]
	s.mu.Unlock()
	if ok {
		return rec, nil
	}
	return s.workerSink.Job(ctx, id)
}

func (s *reviewerSink) set(id string, rec integrations.JobRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[id] = rec
}

// asyncRunner is panewire for reviewers that run on their own: a spawn records
// the job as running and returns, and one that is asked to wait blocks as a
// reviewer would until the test ends.
type asyncRunner struct {
	sink *reviewerSink
	done <-chan struct{}
	mu   sync.Mutex
	// labels are the reviewer labels spawned per PR; waited counts spawns
	// that asked to wait.
	labels map[int][]string
	waited int
}

func (r *asyncRunner) Run(ctx context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command != "panewire" || len(args) == 0 || args[0] != "spawn" {
		return nil, nil
	}
	brief, err := os.ReadFile(args[slices.Index(args, "-p")+1])
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(string(reviewedBrief.FindSubmatch(brief)[1]))
	label := args[slices.Index(args, "-l")+1]
	r.mu.Lock()
	r.labels[n] = append(r.labels[n], label)
	r.mu.Unlock()
	r.sink.set(label, integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p" + strconv.Itoa(n)})
	if slices.Contains(args, "--wait") {
		r.mu.Lock()
		r.waited++
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-r.done:
		}
		return nil, errors.New("wait ended")
	}
	return []byte("OK pane=p1 model=seat-x status=working landed=yes\n"), nil
}

func (r *asyncRunner) label(n int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.labels[n]) == 0 {
		return ""
	}
	return r.labels[n][0]
}

func (r *asyncRunner) spawns() (n, waited int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.labels {
		n += len(l)
	}
	return n, r.waited
}

// timers records what the engine asked the queue to fire, in place of a clock.
type timers struct {
	mu  sync.Mutex
	set []timerCall
}

type timerCall struct {
	after time.Duration
	fire  func()
	// stopped is set when the queue cancels the timer.
	stopped *atomic.Bool
}

func (t *timers) after(d time.Duration, f func()) func() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	stopped := &atomic.Bool{}
	t.set = append(t.set, timerCall{d, f, stopped})
	return func() bool { return !stopped.Swap(true) }
}

func (t *timers) calls() []timerCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.set)
}

type asyncEngine struct {
	e      *prEngine
	f      *parallelFixture
	runner *asyncRunner
	sink   *reviewerSink
	timers *timers
}

// asyncEngineFixture is an engine whose reviewers run for 25 minutes, over n PRs
// and a queue that runs `parallel` evaluations of the repo at once.
func asyncEngineFixture(t *testing.T, prs, parallel int) *asyncEngine {
	t.Helper()
	f := newParallelFixture(t, prs, parallel)
	ctx, cancel := context.WithCancel(context.Background())
	sink := &reviewerSink{jobs: map[string]integrations.JobRecord{}}
	runner := &asyncRunner{sink: sink, done: ctx.Done(), labels: map[int][]string{}}
	f.s.Sink, f.s.Runner = sink, runner
	f.s.Config.Review.Timeout = "25m"
	e := newPREngine(f.s, f.gh, pipeline.Options{}, []config.Repo{f.repo}, nil, nil, func(string, int) {})
	tm := &timers{}
	e.q.After = tm.after
	e.q.Start(ctx)
	t.Cleanup(func() {
		cancel()
		e.q.Wait()
	})
	return &asyncEngine{e, f, runner, sink, tm}
}

// idle waits until the queue holds nothing running or waiting.
func (a *asyncEngine) idle(t *testing.T, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st := a.e.q.Take(a.f.repo.Name); st.Running == 0 && st.Depth == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the queue never went idle while %s: %+v", what, a.e.q.Take(a.f.repo.Name))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (a *asyncEngine) finish(label string) {
	a.sink.set(label, integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", Result: json.RawMessage(`{"findings":[]}`)})
}

func (a *asyncEngine) notified(n int) bool {
	return slices.Contains(a.f.trace.snapshot(), "notify "+strconv.Itoa(n))
}

// (a), (b) A queue item that starts a reviewer ends while it runs; the
// reviewer-finished event is the next item for the PR, which collects the
// result, posts the pass and settles.
func TestAsyncEngineItemEndsWhileReviewerRunsAndFinishEventSettles(t *testing.T) {
	a := asyncEngineFixture(t, 1, 1)
	a.e.q.EnqueueNamed(daemon.SourceWebhook, a.f.repo.Name, 1, "")
	a.idle(t, "its reviewer runs")
	if n, waited := a.runner.spawns(); n != 1 || waited != 0 {
		t.Fatalf("spawns=%d waited=%d", n, waited)
	}
	if a.notified(1) {
		t.Fatal("the PR settled before its reviewer finished")
	}
	calls := a.timers.calls()
	if len(calls) != 1 || calls[0].after < 25*time.Minute || calls[0].after > 25*time.Minute+11*time.Second {
		t.Fatalf("timers %v, want one at review.timeout plus the margin", calls)
	}
	a.finish(a.runner.label(1))
	a.e.q.EnqueueNamed(daemon.SourceReviewer, a.f.repo.Name, 1, "")
	a.idle(t, "the finish event settles the PR")
	if n, _ := a.runner.spawns(); n != 1 || !a.notified(1) {
		t.Fatalf("spawns=%d trace=%v", n, a.f.trace.snapshot())
	}
	if !strings.Contains(a.f.out.String(), `"state":"collected"`) {
		t.Fatalf("output: %s", a.f.out)
	}
}

// The timer the engine asked for queues the PR as an ordinary item: with the
// reviewer inside its budget it ends pending again and asks for its timer.
func TestAsyncEngineTimerQueuesThePR(t *testing.T) {
	a := asyncEngineFixture(t, 1, 1)
	a.e.q.EnqueueNamed(daemon.SourceWebhook, a.f.repo.Name, 1, "")
	a.idle(t, "its reviewer runs")
	calls := a.timers.calls()
	calls[0].fire()
	a.idle(t, "the timer's item runs")
	if n, _ := a.runner.spawns(); n != 1 || len(a.timers.calls()) != 2 {
		t.Fatalf("spawns=%d timers=%d", n, len(a.timers.calls()))
	}
}

// (d) PR 1's reviewer runs 25 minutes while the queue, bounded to one
// evaluation at a time, reviews and settles PR 2's new head.
func TestAsyncEngineSlowReviewerDoesNotHoldTheNextPR(t *testing.T) {
	a := asyncEngineFixture(t, 2, 1)
	a.e.q.EnqueueNamed(daemon.SourceWebhook, a.f.repo.Name, 1, "")
	a.idle(t, "PR 1's reviewer runs")
	a.e.q.EnqueueNamed(daemon.SourceWebhook, a.f.repo.Name, 2, "")
	a.idle(t, "PR 2's reviewer runs")
	a.finish(a.runner.label(2))
	a.e.q.EnqueueNamed(daemon.SourceReviewer, a.f.repo.Name, 2, "")
	a.idle(t, "PR 2 settles")
	if !a.notified(2) || a.notified(1) {
		t.Fatalf("trace %v", a.f.trace.snapshot())
	}
	if n, waited := a.runner.spawns(); n != 2 || waited != 0 {
		t.Fatalf("spawns=%d waited=%d", n, waited)
	}
}

// (e) A sink that cannot read hk job records never reports a finished
// reviewer, so the engine keeps the pass's bounded wait.
func TestEngineWithoutJobRecordsKeepsTheBoundedWait(t *testing.T) {
	e, f := engineFixture(t, 1)
	if e.revisit != nil {
		t.Fatal("an engine over a sink with no job records starts reviewers without waiting")
	}
	close(f.runner.release)
	e.q.EnqueueNamed(daemon.SourceWebhook, f.repo.Name, 1, "")
	e.q.Wait()
	if !slices.Contains(f.trace.snapshot(), "merge 1") {
		t.Fatalf("trace %v", f.trace.snapshot())
	}
}
