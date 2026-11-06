package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// engineFixture runs a prEngine over a parallelFixture's fake GitHub and
// blocking reviewers.
func engineFixture(t *testing.T, prs int) (*prEngine, *parallelFixture) {
	t.Helper()
	f := newParallelFixture(t, prs, 0)
	e := newPREngine(f.s, f.gh, pipeline.Options{}, []config.Repo{f.repo}, nil, nil, func(string, int) {})
	ctx, cancel := context.WithCancel(context.Background())
	e.q.Start(ctx)
	t.Cleanup(func() {
		cancel()
		e.q.Wait()
	})
	return e, f
}

// An event for PR 2 starts PR 2's evaluation, review included, while PR 1's
// review is blocked: the evaluations of different PRs never wait for each other.
func TestEventQueueEngineEvaluatesAnEventPRWhileAnotherReviewIsBlocked(t *testing.T) {
	e, f := engineFixture(t, 2)
	e.q.EnqueueNamed(daemon.SourceWebhook, f.repo.Name, 1, "")
	f.runner.waitFor(t, "PR 1's review", func(in, started, _ int) bool { return in == 1 && started == 1 })
	e.q.EnqueueNamed(daemon.SourceWebhook, f.repo.Name, 2, "")
	f.runner.waitFor(t, "PR 2's review beside PR 1's", func(in, started, returned int) bool { return in == 2 && started == 2 && returned == 0 })
	close(f.runner.release)
	e.q.Wait()
	if errs := e.takeErrors(f.repo.Name); len(errs) > 0 && !strings.Contains(errs[0].Error(), "land") {
		t.Fatal(errs)
	}
}

// (d) Two PRs of one repo that reach their land decision at once are settled
// one after the other: the fake notifier, called inside each settle, never
// sees two at a time.
func TestEventQueueLandsOfOneRepoRunOneAfterTheOther(t *testing.T) {
	e, f := engineFixture(t, 2)
	// A worker binding, so each READY settles through the notifier.
	f.s.Sink = workerSink{}
	close(f.runner.release)
	g := &settleGauge{trace: f.trace}
	f.s.Notifier = g
	e.q.Enqueue(daemon.SourceWebhook, daemon.PRKey{Repo: f.repo.Name, PR: 1}, daemon.PRKey{Repo: f.repo.Name, PR: 2})
	e.q.Wait()
	if g.entered != 2 {
		t.Fatalf("%d settles reached the notifier, want both PRs: %v", g.entered, f.trace.snapshot())
	}
	if g.peak != 1 {
		t.Fatalf("%d settles ran at once, want one after the other", g.peak)
	}
}

// settleGauge is a notifier that takes a while, to give a second settle the
// time to overlap the first if nothing serializes them.
type settleGauge struct {
	trace              *eventLog
	mu                 sync.Mutex
	now, peak, entered int
}

func (g *settleGauge) Notify(_ context.Context, ev integrations.Event) error {
	g.mu.Lock()
	g.now++
	g.entered++
	g.peak = max(g.peak, g.now)
	g.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	g.trace.add("notify")
	g.mu.Lock()
	g.now--
	g.mu.Unlock()
	return nil
}

// (g) The sweep puts the open PRs on the queue and returns: it does not wait
// for their reviews.
func TestEventQueueSweepQueuesInsteadOfEvaluatingInline(t *testing.T) {
	e, f := engineFixture(t, 3)
	returned := make(chan error, 1)
	go func() { returned <- e.sweep(context.Background(), f.repo) }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the sweep waited for the reviews")
	}
	f.runner.waitFor(t, "the queued PRs to review", func(in, started, returned int) bool { return in == 3 && returned == 0 })
	if st := e.q.Take(f.repo.Name); st.Running != 3 {
		t.Fatalf("queue %+v, want the three PRs running after the sweep returned", st)
	}
	close(f.runner.release)
	e.q.Wait()
}

// (g) A PR whose delivery was dropped is still evaluated by the sweep: nothing
// marks it dirty, its head moved, and the next pass evaluates it, and only it.
func TestEventQueueSweepRepairsADroppedDelivery(t *testing.T) {
	f := newSkipFixture(t, 1, 2)
	f.pass()
	f.pr(2).head = "dddddddddddddddddddddddddddddddddddddddd"
	res := f.pass()
	t.Logf("%s", res)
	if res.evaluated != 1 || res.skipped != 1 || !hasReason(res, 2, "head") {
		t.Fatalf("the sweep did not evaluate PR 2 alone: %s", res)
	}
}
