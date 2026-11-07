package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// pendingFixture is one PR whose reviewers run on their own: the engine starts
// them without waiting, as it does when it can read hk job records.
type pendingFixture struct {
	f      *parallelFixture
	e      *prEngine
	sink   *reviewerSink
	runner *asyncRunner
}

func newPendingFixture(t *testing.T) *pendingFixture {
	t.Helper()
	f := newParallelFixture(t, 1, 1)
	f.s.Config.Review.Timeout = "25m"
	// The skip rule needs ETags from every read, as GitHub answers them.
	proxy := httptest.NewServer(etagProxy{f.hub})
	t.Cleanup(proxy.Close)
	f.gh.BaseURL = proxy.URL

	ctx, cancel := context.WithCancel(context.Background())
	sink := &reviewerSink{jobs: map[string]integrations.JobRecord{}}
	runner := &asyncRunner{sink: sink, done: ctx.Done(), labels: map[int][]string{}}
	f.s.Sink, f.s.Runner = sink, runner

	logs := &syncBuffer{}
	skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}
	e := newPREngine(f.s, f.gh, pipeline.Options{}, []config.Repo{f.repo}, skips, skips.Log, nil)
	e.q.Start(ctx)
	t.Cleanup(func() {
		cancel()
		e.q.Wait()
	})
	return &pendingFixture{f, e, sink, runner}
}

// event queues one named evaluation of the PR and returns what it did.
func (p *pendingFixture) event(t *testing.T, src daemon.Source) (skipped, evaluated int) {
	t.Helper()
	p.e.mu.Lock()
	before := *p.e.counts(p.f.repo.Name)
	p.e.mu.Unlock()
	p.e.q.EnqueueNamed(src, p.f.repo.Name, 1, "")
	p.e.q.Wait()
	p.e.mu.Lock()
	after := *p.e.counts(p.f.repo.Name)
	p.e.mu.Unlock()
	return after.skipped - before.skipped, after.evaluated - before.evaluated
}

func (p *pendingFixture) settled() bool {
	return slices.Contains(p.f.trace.snapshot(), "merge 1")
}

// A pending async pass is unreviewed like a held or quota-stopped one: it
// produced no result, so the head must not be cached as settled. Otherwise the
// reviewer-finished wake is skipped as unchanged and the review never lands.
func TestPendingAsyncPassUnderStaleDoneMarkerIsNotSettled(t *testing.T) {
	p := newPendingFixture(t)

	// The first evaluation places a reviewer and ends pending: nothing is
	// cached and nothing settles.
	if skipped, evaluated := p.event(t, daemon.SourceWebhook); skipped != 0 || evaluated != 1 {
		t.Fatalf("first pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if n, _ := p.runner.spawns(); n != 1 {
		t.Fatalf("spawns=%d, want 1", n)
	}
	if _, ok := cachedEvaluation(p.e.skips, p.f.repo.Name, 1); ok {
		t.Fatal("a pending pass was cached as settled")
	}
	if p.settled() {
		t.Fatalf("a pending pass settled the PR: %v", p.f.trace.snapshot())
	}

	// A completion marker with no review receipt is stale, not evidence that a
	// review ran. The reviewer is still running, so the evaluation is pending
	// again and must not be cached either.
	p.f.hub.refs["mergewake/reviews/1/"+hubHead(1)+"-done"] = hubHead(1)
	if skipped, evaluated := p.event(t, daemon.SourceWebhook); skipped != 0 || evaluated != 1 {
		t.Fatalf("pending pass under a stale done marker: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if _, ok := cachedEvaluation(p.e.skips, p.f.repo.Name, 1); ok {
		t.Fatal("a pending pass under a stale done marker was cached as settled")
	}
	if n, _ := p.runner.spawns(); n != 1 {
		t.Fatalf("spawns=%d, want the running reviewer only", n)
	}

	// The reviewer finishes. Its event must evaluate the PR rather than skip it
	// as unchanged; the resumed pass collects and posts the review. The stale
	// marker made the settle that runs before a done marker a no-op, so the
	// next evaluation, which sees the posted pass, settles.
	label := p.runner.label(1)
	p.sink.set(label, integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", Result: json.RawMessage(`{"findings":[]}`)})
	if skipped, evaluated := p.event(t, daemon.SourceReviewer); skipped != 0 || evaluated != 1 {
		t.Fatalf("reviewer-finished wake: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if n, _ := p.runner.spawns(); n != 1 {
		t.Fatalf("spawns=%d, want no second reviewer", n)
	}
	if len(p.f.hub.reviews[1]) != 1 {
		t.Fatalf("the finished pass was not posted: %v", p.f.hub.reviews[1])
	}
	if skipped, evaluated := p.event(t, daemon.SourceSweep); skipped != 0 || evaluated != 1 {
		t.Fatalf("evaluation after the posted pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if !p.settled() {
		t.Fatalf("the finished reviewer did not settle the PR: %v", p.f.trace.snapshot())
	}
}
