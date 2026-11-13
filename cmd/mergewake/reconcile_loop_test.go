package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// pullReads counts the GET requests the fixture's GitHub receives for one PR.
type pullReads struct {
	mu sync.Mutex
	n  map[string]int
}

func (p *pullReads) count(suffix string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n[suffix]
}

// countPullReads puts a counting proxy in front of the fixture's GitHub hub.
func countPullReads(t *testing.T, f *parallelFixture) *pullReads {
	t.Helper()
	reads := &pullReads{n: map[string]int{}}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			reads.mu.Lock()
			reads.n[r.URL.Path[strings.LastIndex(r.URL.Path, "/pulls/")+1:]]++
			reads.mu.Unlock()
		}
		f.hub.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	f.gh.BaseURL = proxy.URL
	return reads
}

// closedRow records an outcome row for a PR the hub does not list as open.
func closedRow(t *testing.T, f *parallelFixture, pr int) {
	t.Helper()
	ledger := outcomes.Ledger{Dir: f.s.Config.StateDir}
	if err := ledger.Update(f.repo.Name, pr, func(v *outcomes.Outcome) { v.Decision = "blocked" }); err != nil {
		t.Fatal(err)
	}
}

// (d) The PR sweep keeps to open PRs: it never reads a closed PR for the
// outcome reconcile.
func TestSweepDoesNotReconcileClosedRows(t *testing.T) {
	e, f := engineFixture(t, 1)
	reads := countPullReads(t, f)
	closedRow(t, f, 900)
	close(f.runner.release)
	if err := e.sweep(context.Background(), f.repo); err != nil {
		t.Fatal(err)
	}
	e.q.Wait()
	if n := reads.count("pulls/900"); n != 0 {
		t.Fatalf("the sweep read the closed PR %d times", n)
	}
}

// noneOpen is an open set that is known and lists no PR.
func noneOpen(string) (map[int]bool, bool) { return map[int]bool{}, true }

// Before the first sweep the open set is unknown, and the reconcile reads no
// row: it could not tell a closed PR from an open one.
func TestReconcileLoopWaitsForTheFirstSweep(t *testing.T) {
	f := newParallelFixture(t, 1, 0)
	reads := countPullReads(t, f)
	closedRow(t, f, 900)
	unknown := func(string) (map[int]bool, bool) { return nil, false }
	if _, err := reconcileClosed(context.Background(), f.s, unknown, f.repo, pipeline.Options{Shadow: true}, nil); err != nil {
		t.Fatal(err)
	}
	if n := reads.count("pulls/900"); n != 0 {
		t.Fatalf("read the row %d times with no open set", n)
	}
}

// A PR the sweep listed as open is not reconciled as closed, and the engine
// reports no open set before its first sweep.
func TestReconcileLoopSkipsOpenPRs(t *testing.T) {
	e, f := engineFixture(t, 1)
	reads := countPullReads(t, f)
	closedRow(t, f, 1)
	closedRow(t, f, 900)
	if _, known := e.openPRs(f.repo.Name); known {
		t.Fatal("open set known before the first sweep")
	}
	if ran, err := reconcileClosed(context.Background(), f.s, e.openPRs, f.repo, pipeline.Options{Shadow: true}, nil); ran || err != nil {
		t.Fatalf("ran=%v err=%v before the first sweep", ran, err)
	}
	close(f.runner.release)
	if err := e.sweep(context.Background(), f.repo); err != nil {
		t.Fatal(err)
	}
	e.q.Wait()
	before := reads.count("pulls/1")
	if ran, err := reconcileClosed(context.Background(), f.s, e.openPRs, f.repo, pipeline.Options{Shadow: true}, nil); !ran || err != nil {
		t.Fatalf("ran=%v err=%v after the sweep", ran, err)
	}
	if reads.count("pulls/900") != 1 || reads.count("pulls/1") != before {
		t.Fatalf("reads: closed PR 900=%d, open PR 1 grew by %d", reads.count("pulls/900"), reads.count("pulls/1")-before)
	}
}

// The loop runs every interval, retries soon when it had no open set, and
// returns when its context ends.
func TestReconcileLoopRunsAndStopsWithContext(t *testing.T) {
	f := newParallelFixture(t, 1, 0)
	reads := countPullReads(t, f)
	closedRow(t, f, 900)
	f.s.Now = func() time.Time {
		return time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC).Add(time.Duration(reads.count("pulls/900")) * 2 * time.Hour)
	}
	old := reconcileRetryWait
	reconcileRetryWait = 10 * time.Millisecond
	t.Cleanup(func() { reconcileRetryWait = old })
	var mu sync.Mutex
	known := false
	openPRs := func(string) (map[int]bool, bool) { mu.Lock(); defer mu.Unlock(); return map[int]bool{}, known }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runReconcileLoop(ctx, f.s, openPRs, []config.Repo{f.repo}, time.Hour, pipeline.Options{Shadow: true}, nil)
	}()
	time.Sleep(100 * time.Millisecond)
	if n := reads.count("pulls/900"); n != 0 {
		t.Fatalf("read %d times before an open set existed", n)
	}
	mu.Lock()
	known = true
	mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for reads.count("pulls/900") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the loop never retried after the open set appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop kept running after its context ended")
	}
}

// (d) The reconcile loop reads a due closed row once per hour: a run inside
// the hour reads nothing, the first after it reads again.
func TestReconcileLoopReadsADueRowOncePerHour(t *testing.T) {
	f := newParallelFixture(t, 1, 0)
	reads := countPullReads(t, f)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	f.s.Now = func() time.Time { return now }
	closedRow(t, f, 900)
	run := func(at time.Time) {
		t.Helper()
		now = at
		if _, err := reconcileClosed(context.Background(), f.s, noneOpen, f.repo, pipeline.Options{Shadow: true}, nil); err != nil {
			t.Fatal(err)
		}
	}
	run(now)
	if n := reads.count("pulls/900"); n != 1 {
		t.Fatalf("first run read the due row %d times, want 1", n)
	}
	run(now.Add(30 * time.Minute))
	if n := reads.count("pulls/900"); n != 1 {
		t.Fatalf("a run inside the hour read the row again (%d reads)", n)
	}
	run(now.Add(61 * time.Minute))
	if n := reads.count("pulls/900"); n != 2 {
		t.Fatalf("a run after the hour read the row %d times in all, want 2", n)
	}
}

// The reconcile run has its own bound, and a run that hits it returns the
// deadline instead of holding the loop.
func TestReconcileLoopIsBounded(t *testing.T) {
	f := newParallelFixture(t, 1, 0)
	closedRow(t, f, 900)
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	f.gh.BaseURL = proxy.URL
	old := reconcileTimeout
	reconcileTimeout = 200 * time.Millisecond
	t.Cleanup(func() { reconcileTimeout = old })
	done := make(chan error, 1)
	go func() {
		_, err := reconcileClosed(context.Background(), f.s, noneOpen, f.repo, pipeline.Options{Shadow: true}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a run that hit its bound reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile run ignored its bound")
	}
}

// repsHang answers every reps command by waiting for its context, and passes
// anything else to the fixture's runner.
type repsHang struct {
	next    integrations.Runner
	started chan struct{}
}

func (h repsHang) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if len(args) > 0 && args[0] == "reps" {
		select {
		case h.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return h.next.Run(ctx, command, args, input, dir)
}

// The daemon runs the publisher while the pr role is on, and stops it with its
// context even when a publish is hanging: a stuck outcomes command never keeps
// the daemon from shutting down.
func TestDaemonStopsWithAPublishInFlight(t *testing.T) {
	m := newMultiRepo(t, []string{"o/one"}, 0, 1)
	started := make(chan struct{}, 1)
	m.s.Runner = repsHang{next: m.s.Runner, started: started}
	ledger := outcomes.Ledger{Dir: m.s.Config.StateDir}
	if err := ledger.Update("o/one", 5, func(v *outcomes.Outcome) {
		v.Final, v.Decision = true, "landed"
		v.Profile, v.Model, v.Effort, v.Grade, v.Tier = "example", "example-model", "high", "B", "T1"
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	exit := make(chan error, 1)
	go func() { exit <- runDaemonLoops(ctx, m.s, m.gh, "", pipeline.Options{}, io.Discard) }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the daemon never started publishing")
	}
	cancel()
	select {
	case err := <-exit:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the daemon did not stop with a publish in flight")
	}
	if state, _ := ledger.Read(); len(state.Published) != 0 {
		t.Fatalf("a cut-off publish was acknowledged: %v", state.Published)
	}
}
