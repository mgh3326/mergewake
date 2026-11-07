package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// quotaRunner places a reviewer and returns at once, as the async fixture's
// runner does; every other command succeeds with no output.
type quotaRunner struct {
	mu     sync.Mutex
	spawns int
}

func (r *quotaRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command != "panewire" || len(args) == 0 || args[0] != "spawn" {
		return nil, nil
	}
	r.mu.Lock()
	r.spawns++
	r.mu.Unlock()
	return []byte("OK pane=p1 host=node-a status=working landed=yes\n"), nil
}

func (r *quotaRunner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spawns
}

// quotaSink reads every reviewer's job record as the record it was given: the
// task's own job still answers from the worker sink.
type quotaSink struct {
	workerSink
	mu  sync.Mutex
	rec integrations.JobRecord
}

func (s *quotaSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if id == fixtureJob {
		return s.workerSink.Job(ctx, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec, nil
}

func (s *quotaSink) setRecord(rec integrations.JobRecord) {
	s.mu.Lock()
	s.rec = rec
	s.mu.Unlock()
}

// etagProxy is the fixture's GitHub behind ETags: every GET carries a tag
// derived from its body, and a matching If-None-Match is answered 304, so the
// skip rule can decide on the fixture as it does on GitHub.
type etagProxy struct{ http.Handler }

func (p etagProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := httptest.NewRecorder()
	p.Handler.ServeHTTP(rec, r)
	for k, vs := range rec.Header() {
		w.Header()[k] = vs
	}
	body := rec.Body.Bytes()
	if r.Method == http.MethodGet {
		sum := sha256.Sum256(body)
		tag := `"` + hex.EncodeToString(sum[:8]) + `"`
		w.Header().Set("ETag", tag)
		if r.Header.Get("If-None-Match") == tag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(body)
}

// (b) An evaluation whose only outcome was a quota stop is not cached as
// settled: the next evaluation runs inside the wait, and the retry at the
// pool's reset places a fresh reviewer.
func TestQuotaStoppedOnlyEvaluationIsNotSettled(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	proxy := httptest.NewServer(etagProxy{f.hub})
	t.Cleanup(proxy.Close)
	f.gh.BaseURL = proxy.URL
	runner := &quotaRunner{}
	sink := &quotaSink{rec: integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}}
	f.s.Runner, f.s.Sink = runner, sink

	now := time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC)
	f.s.Now = func() time.Time { return now }
	logs := &syncBuffer{}
	skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}

	// The first evaluation places a reviewer; its pass ends pending and is not
	// settled, so nothing is cached.
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("first pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if runner.count() != 1 {
		t.Fatalf("spawns=%d, want 1", runner.count())
	}

	// A completion marker appears with no receipt, and the placed reviewer's
	// record says panewire stopped it on an exhausted pool. This evaluation
	// ends on the quota stop and must not be cached as settled.
	reset := now.Add(10 * time.Minute)
	f.hub.refs["mergewake/reviews/1/"+hubHead(1)+"-done"] = hubHead(1)
	sink.setRecord(integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=seat reset_at=" + reset.Format(time.RFC3339), Host: "node-a", PaneID: "p1"})
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("quota pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if _, ok := cachedEvaluation(skips, f.repo.Name, 1); ok {
		t.Fatal("a quota-stop-only evaluation was cached as settled")
	}

	// Inside the wait the PR is still evaluated: it must not be skipped as
	// unchanged until the retry runs.
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("quota wait pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if _, ok := cachedEvaluation(skips, f.repo.Name, 1); ok {
		t.Fatal("a quota wait was cached as settled")
	}

	// At the reset the retry runs and places a fresh reviewer.
	now = reset.Add(time.Second)
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("after the reset: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if runner.count() != 2 {
		t.Fatalf("spawns=%d, want a retry once the pool resets", runner.count())
	}
}
