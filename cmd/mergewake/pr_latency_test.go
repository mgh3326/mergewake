package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// latencyClock is the one clock of a latency test. Every GitHub request, the
// panewire spawn and the verdict notice move it, so each phase of an
// evaluation takes a known time.
type latencyClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *latencyClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *latencyClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

const (
	latencyRequest = 100 * time.Millisecond
	latencySpawn   = 700 * time.Millisecond
	latencyNotify  = 300 * time.Millisecond
)

type tickRunner struct {
	next integrations.Runner
	clk  *latencyClock
}

func (r tickRunner) Run(ctx context.Context, command string, args []string, a, b string) ([]byte, error) {
	r.clk.advance(latencySpawn)
	return r.next.Run(ctx, command, args, a, b)
}

type tickNotifier struct {
	next integrations.Notifier
	clk  *latencyClock
}

func (n tickNotifier) Notify(ctx context.Context, e integrations.Event) error {
	n.clk.advance(latencyNotify)
	return n.next.Notify(ctx, e)
}

// timedEngine is an async engine on a fake clock whose log is captured.
type timedEngine struct {
	*asyncEngine
	clk *latencyClock
	log *syncBuffer
}

var latencyStart = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func timedEngineFixture(t *testing.T, changedFiles int) *timedEngine {
	t.Helper()
	a := asyncEngineFixture(t, 1, 1)
	a.f.hub.changedFiles = changedFiles
	clk := &latencyClock{t: latencyStart}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clk.advance(latencyRequest)
		a.f.hub.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	a.f.gh.BaseURL = srv.URL
	a.f.s.Runner = tickRunner{a.f.s.Runner, clk}
	a.f.s.Notifier = tickNotifier{a.f.s.Notifier, clk}
	buf := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(buf, nil))
	a.e.q.Now, a.e.q.Log, a.e.log, a.e.seen.Log = clk.now, log, log, log
	return &timedEngine{a, clk, buf}
}

// lines are the decoded log lines whose message is msg.
func (te *timedEngine) lines(msg string) []map[string]any {
	te.log.mu.Lock()
	defer te.log.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(te.log.b.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func ms(t *testing.T, line map[string]any, key string) int64 {
	t.Helper()
	v, ok := line[key].(float64)
	if !ok {
		t.Fatalf("log line has no %q: %v", key, line)
	}
	return int64(v)
}

// (a) One evaluation logs every phase, and the top-level phases add up to
// eval_ms within 5%. The nested spawn and gate times fit inside their phases.
func TestPREvaluatedLogsEveryPhaseAndTheyAddUp(t *testing.T) {
	te := timedEngineFixture(t, 0)
	te.e.q.EnqueueNamed(daemon.SourceWebhook, te.f.repo.Name, 1, "")
	te.idle(t, "its reviewer runs")
	evaluated := te.lines("pr evaluated")
	if len(evaluated) != 1 {
		t.Fatalf("%d pr evaluated lines, want 1: %s", len(evaluated), te.log.b.String())
	}
	line := evaluated[0]
	eval := ms(t, line, "eval_ms")
	read, observe, review := ms(t, line, "read_ms"), ms(t, line, "observe_ms"), ms(t, line, "review_ms")
	lockWait, settle := ms(t, line, "lock_wait_ms"), ms(t, line, "settle_ms")
	spawn, gate := ms(t, line, "spawn_ms"), ms(t, line, "gate_ms")
	sum := read + observe + review + lockWait + settle
	if eval == 0 || sum < eval*95/100 || sum > eval*105/100 {
		t.Fatalf("phases add up to %d ms (read %d, observe %d, review %d, lock_wait %d, settle %d), eval_ms is %d", sum, read, observe, review, lockWait, settle, eval)
	}
	if read == 0 || review == 0 || settle < latencyNotify.Milliseconds() {
		t.Fatalf("a phase took no time: read %d, review %d, settle %d", read, review, settle)
	}
	if spawn != latencySpawn.Milliseconds() || spawn > review {
		t.Fatalf("spawn_ms %d, want %d inside review_ms %d", spawn, latencySpawn.Milliseconds(), review)
	}
	if gate == 0 || gate > settle {
		t.Fatalf("gate_ms %d, want a time inside settle_ms %d", gate, settle)
	}
	if _, ok := line["settling"]; ok {
		t.Fatalf("settling is set for a PR whose evidence is settled: %v", line)
	}
}

// (b) An evaluation that held the review and the verdict back while GitHub's
// file list caught up logs why.
func TestPREvaluatedLogsTheSettlingReason(t *testing.T) {
	te := timedEngineFixture(t, 2)
	te.e.q.EnqueueNamed(daemon.SourceWebhook, te.f.repo.Name, 1, "")
	te.idle(t, "the review waits for the file list")
	evaluated := te.lines("pr evaluated")
	if len(evaluated) != 1 {
		t.Fatalf("%d pr evaluated lines, want 1", len(evaluated))
	}
	settling, _ := evaluated[0]["settling"].(string)
	if !strings.Contains(settling, "review_files_settling") || !strings.Contains(settling, "+files_settling") {
		t.Fatalf("settling %q, want the review's and the gate's reason", settling)
	}
	// (e) No reviewer was spawned, so there is no latency line.
	if n, _ := te.runner.spawns(); n != 0 || len(te.lines("review-start latency")) != 0 {
		t.Fatalf("spawns %d, latency lines %v", n, te.lines("review-start latency"))
	}
}

// (c) The latency line counts from the earliest time the head was seen: a
// delivery that named it before the first evaluation, not the evaluation.
func TestReviewStartLatencyCountsFromTheDelivery(t *testing.T) {
	te := timedEngineFixture(t, 0)
	head := hubHead(1)
	te.e.seen.Delivered(te.f.repo.Name, 1, head, te.clk.now())
	te.clk.advance(40 * time.Second)
	te.e.q.EnqueueNamed(daemon.SourceSweep, te.f.repo.Name, 1, "")
	te.idle(t, "its reviewer runs")
	got := te.lines("review-start latency")
	if len(got) != 1 {
		t.Fatalf("%d latency lines, want 1: %s", len(got), te.log.b.String())
	}
	line := got[0]
	if line["first_seen"] != latencyStart.Format(time.RFC3339Nano) || line["first_seen_by"] != "webhook" || line["head"] != head {
		t.Fatalf("line %v, want the delivery's time and head", line)
	}
	if v := ms(t, line, "seen_to_first_eval_ms"); v != 40000 {
		t.Fatalf("seen_to_first_eval_ms %d, want 40000", v)
	}
	if v := ms(t, line, "first_eval_to_eval_ms"); v != 0 {
		t.Fatalf("first_eval_to_eval_ms %d, want 0 for the first evaluation", v)
	}
	accepted := ms(t, line, "eval_to_accepted_ms")
	if accepted < latencySpawn.Milliseconds() {
		t.Fatalf("eval_to_accepted_ms %d, want at least the spawn's %d", accepted, latencySpawn.Milliseconds())
	}
	if total := ms(t, line, "total_ms"); total != 40000+accepted {
		t.Fatalf("total_ms %d, want %d", total, 40000+accepted)
	}
}

// With no delivery the head is first seen by its first evaluation, and a later
// evaluation that spawns reports the time between the two.
func TestReviewStartLatencyReportsTheEvaluationThatSpawned(t *testing.T) {
	te := timedEngineFixture(t, 2)
	te.e.q.EnqueueNamed(daemon.SourceSweep, te.f.repo.Name, 1, "")
	te.idle(t, "the review waits for the file list")
	te.clk.advance(2 * time.Minute)
	te.f.hub.mu.Lock()
	te.f.hub.changedFiles = 1
	te.f.hub.mu.Unlock()
	te.e.q.EnqueueNamed(daemon.SourceSweep, te.f.repo.Name, 1, "")
	te.idle(t, "its reviewer runs")
	got := te.lines("review-start latency")
	if len(got) != 1 {
		t.Fatalf("%d latency lines, want 1: %s", len(got), te.log.b.String())
	}
	line := got[0]
	if line["first_seen_by"] != "eval" || ms(t, line, "evals") != 2 {
		t.Fatalf("line %v, want a head first seen by its first evaluation, spawned by the second", line)
	}
	if v := ms(t, line, "first_eval_to_eval_ms"); v < 120000 {
		t.Fatalf("first_eval_to_eval_ms %d, want the two minutes between the evaluations", v)
	}
	if ms(t, line, "seen_to_first_eval_ms") != 0 {
		t.Fatalf("line %v", line)
	}
}

// (d) The first-seen time survives a restart: a new engine over the same state
// directory reads it back.
func TestReviewStartLatencyFirstSeenSurvivesARestart(t *testing.T) {
	te := timedEngineFixture(t, 0)
	te.e.seen.Delivered(te.f.repo.Name, 1, hubHead(1), te.clk.now())
	te.clk.advance(5 * time.Minute)
	// The restarted daemon opens the store anew.
	log := slog.New(slog.NewJSONHandler(te.log, nil))
	te.e.seen = daemon.OpenHeadSeen(te.f.s.Config.StateDir)
	te.e.seen.Log = log
	te.e.q.Seen = te.e.seen
	te.e.q.EnqueueNamed(daemon.SourceSweep, te.f.repo.Name, 1, "")
	te.idle(t, "its reviewer runs")
	got := te.lines("review-start latency")
	if len(got) != 1 {
		t.Fatalf("%d latency lines, want 1", len(got))
	}
	if got[0]["first_seen"] != latencyStart.Format(time.RFC3339Nano) || got[0]["first_seen_by"] != "webhook" {
		t.Fatalf("line %v, want the time before the restart", got[0])
	}
}

// (e) An evaluation that does not spawn logs no latency line: the timer's
// evaluation finds the reviewer still running.
func TestReviewStartLatencyIsNotLoggedWithoutASpawn(t *testing.T) {
	te := timedEngineFixture(t, 0)
	te.e.q.EnqueueNamed(daemon.SourceWebhook, te.f.repo.Name, 1, "")
	te.idle(t, "its reviewer runs")
	if n := len(te.lines("review-start latency")); n != 1 {
		t.Fatalf("%d latency lines after the spawn, want 1", n)
	}
	te.timers.calls()[0].fire()
	te.idle(t, "the timer's evaluation runs")
	if n := len(te.lines("pr evaluated")); n != 2 {
		t.Fatalf("%d pr evaluated lines, want 2", n)
	}
	if n, _ := te.runner.spawns(); n != 1 || len(te.lines("review-start latency")) != 1 {
		t.Fatalf("spawns %d, latency lines %d: a second evaluation that spawns nothing must log none", n, len(te.lines("review-start latency")))
	}
}
