package main

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
)

// evalSources records what put each evaluation on the queue.
type evalSources struct {
	mu  sync.Mutex
	got []daemon.Source
}

func (s *evalSources) count(src daemon.Source) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, got := range s.got {
		if got == src {
			n++
		}
	}
	return n
}

// watchEvaluations wraps the engine's queue run. It runs before anything is
// queued, so the queue sees it from the first item.
func watchEvaluations(a *asyncEngine) *evalSources {
	s := &evalSources{}
	inner := a.e.q.Run
	a.e.q.Run = func(ctx context.Context, w daemon.Work) {
		s.mu.Lock()
		s.got = append(s.got, w.Sources...)
		s.mu.Unlock()
		inner(ctx, w)
	}
	return s
}

// (a) A pass that collects its reviewer's result ends the reviewer's deadline
// timer: nothing evaluates the PR at the deadline.
func TestEngineCollectedPassCancelsItsTimer(t *testing.T) {
	a := asyncEngineFixture(t, 1, 1)
	seen := watchEvaluations(a)
	repo := a.f.repo.Name
	a.e.q.EnqueueNamed(daemon.SourceWebhook, repo, 1, "")
	a.idle(t, "its reviewer runs")
	calls := a.timers.calls()
	label := a.runner.label(1)
	if len(calls) != 1 || calls[0].stopped.Load() || !slices.Equal(a.e.q.TimerIDs(repo, 1), []string{label}) {
		t.Fatalf("timers %d, ids %v, want the reviewer's timer pending", len(calls), a.e.q.TimerIDs(repo, 1))
	}
	a.finish(label)
	a.e.q.EnqueueNamed(daemon.SourceReviewer, repo, 1, "")
	a.idle(t, "the finish event settles the PR")
	if !a.notified(1) {
		t.Fatalf("the PR did not settle: %v", a.f.trace.snapshot())
	}
	if ids := a.e.q.TimerIDs(repo, 1); len(ids) != 0 || !calls[0].stopped.Load() {
		t.Fatalf("timer ids %v stopped=%v after the pass was collected", ids, calls[0].stopped.Load())
	}
	// A callback already in flight when the timer was cancelled does nothing.
	calls[0].fire()
	a.idle(t, "the stale timer ran")
	if n := seen.count(daemon.SourceTimer); n != 0 {
		t.Fatalf("%d evaluations came from the cancelled timer", n)
	}
}

// (b) A reviewer replaced as lost leaves its old label's timer behind; the
// engine cancels it and keeps only the replacement's.
func TestEngineReplacedReviewerCancelsTheOldLabelsTimer(t *testing.T) {
	a := asyncEngineFixture(t, 1, 1)
	seen := watchEvaluations(a)
	repo := a.f.repo.Name
	a.e.q.EnqueueNamed(daemon.SourceWebhook, repo, 1, "")
	a.idle(t, "its reviewer runs")
	old := a.runner.label(1)
	first := a.timers.calls()[0]
	// The reviewer's record is gone and the review budget is spent.
	a.sink.mu.Lock()
	delete(a.sink.jobs, old)
	a.sink.mu.Unlock()
	now := time.Now().Add(time.Hour)
	a.f.s.Now = func() time.Time { return now }
	a.e.q.EnqueueNamed(daemon.SourceTimer, repo, 1, "")
	a.idle(t, "the lost reviewer is replaced")
	n, _ := a.runner.spawns()
	if n != 2 {
		t.Fatalf("spawns=%d, want the replacement spawned", n)
	}
	fresh := a.runner.labels[1][1]
	if ids := a.e.q.TimerIDs(repo, 1); !slices.Equal(ids, []string{fresh}) || !first.stopped.Load() {
		t.Fatalf("timer ids %v (old %s, new %s) stopped=%v", ids, old, fresh, first.stopped.Load())
	}
	first.fire()
	a.idle(t, "the old timer ran")
	if got := seen.count(daemon.SourceTimer); got != 1 {
		t.Fatalf("%d timer evaluations, want only the manual one", got)
	}
}

// (d) A reviewer that is still running keeps its timer through other
// evaluations of the PR, and the timer fires as before.
func TestEnginePendingReviewerTimerStillFires(t *testing.T) {
	a := asyncEngineFixture(t, 1, 1)
	seen := watchEvaluations(a)
	repo := a.f.repo.Name
	a.e.q.EnqueueNamed(daemon.SourceWebhook, repo, 1, "")
	a.idle(t, "its reviewer runs")
	label := a.runner.label(1)
	a.e.q.EnqueueNamed(daemon.SourceWebhook, repo, 1, "")
	a.idle(t, "the second evaluation ends pending")
	if ids := a.e.q.TimerIDs(repo, 1); !slices.Equal(ids, []string{label}) {
		t.Fatalf("timer ids %v, want the reviewer's", ids)
	}
	calls := a.timers.calls()
	if last := calls[len(calls)-1]; last.stopped.Load() {
		t.Fatal("the timer the pass armed again was cancelled")
	}
	calls[len(calls)-1].fire()
	a.idle(t, "the timer's evaluation runs")
	if got := seen.count(daemon.SourceTimer); got != 1 {
		t.Fatalf("%d timer evaluations, want 1", got)
	}
	if ids := a.e.q.TimerIDs(repo, 1); !slices.Equal(ids, []string{label}) {
		t.Fatalf("timer ids %v after the timer fired on a running reviewer", ids)
	}
}
