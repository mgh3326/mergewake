package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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

func TestEvaluationNotifiesRecentExternallyMergedPROnce(t *testing.T) {
	f := newParallelFixture(t, 1, 0)
	f.repo.Lane = "design"
	f.hub.merged = true
	collector := &mergedEventCollector{}
	f.s.Notifier = collector
	e := newPREngine(f.s, f.gh, pipeline.Options{}, []config.Repo{f.repo}, nil, nil, nil)
	w := daemon.Work{Key: daemon.PRKey{Repo: f.repo.Name, PR: 1}}
	if _, res := e.evaluate(context.Background(), f.repo, w); !res.gone {
		t.Fatal("merged PR stayed in the open evaluation set")
	}
	if _, res := e.evaluate(context.Background(), f.repo, w); !res.gone {
		t.Fatal("repeat merged PR stayed in the open evaluation set")
	}
	if len(collector.events) != 1 {
		t.Fatalf("recent external merge notices=%+v trace=%v", collector.events, f.trace.snapshot())
	}
	wantID := "mergewake-merged-" + f.repo.Name + "-1-" + hubHead(1)
	if collector.events[0].ID != wantID || collector.events[0].Lane != "design" || collector.events[0].Type != "merged" {
		t.Fatalf("external MERGED event=%+v", collector.events[0])
	}
	rows, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, f.repo.Name, "decisions.jsonl"))
	if err != nil || !strings.Contains(string(rows), `"command":"pr-notify"`) || !strings.Contains(string(rows), `"head":"`+hubHead(1)+`"`) {
		t.Fatalf("external MERGED pr-notify row missing: %s err=%v", rows, err)
	}
}

type mergedEventCollector struct{ events []integrations.Event }

func (c *mergedEventCollector) Notify(_ context.Context, event integrations.Event) error {
	c.events = append(c.events, event)
	return nil
}

// (d) Two PRs of one repo that reach their land decision at once are settled
// one after the other: the merge request, made inside each settle, never has
// a second one in flight beside it.
func TestEventQueueLandsOfOneRepoRunOneAfterTheOther(t *testing.T) {
	e, f := engineFixture(t, 2)
	close(f.runner.release)
	g := &settleGauge{}
	f.hub.mergeProbe = g.probe
	e.q.Enqueue(daemon.SourceWebhook, daemon.PRKey{Repo: f.repo.Name, PR: 1}, daemon.PRKey{Repo: f.repo.Name, PR: 2})
	e.q.Wait()
	if g.entered != 2 {
		t.Fatalf("%d settles reached the merge, want both PRs: %v", g.entered, f.trace.snapshot())
	}
	if g.peak != 1 {
		t.Fatalf("%d settles ran at once, want one after the other", g.peak)
	}
}

// settleGauge counts merge requests in flight; each takes a while, to give a
// second settle the time to overlap the first if nothing serializes them.
type settleGauge struct {
	mu                 sync.Mutex
	now, peak, entered int
}

func (g *settleGauge) probe() func() {
	g.mu.Lock()
	g.now++
	g.entered++
	g.peak = max(g.peak, g.now)
	g.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	return func() {
		g.mu.Lock()
		g.now--
		g.mu.Unlock()
	}
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

func TestSweepKeepsWebhookLearnedHeadWhileListingOpenPulls(t *testing.T) {
	f := newParallelFixture(t, 1, 0)
	e := newPREngine(f.s, f.gh, pipeline.Options{}, []config.Repo{f.repo}, nil, nil, func(string, int) {})
	works := make(chan daemon.Work, 10)
	e.q.Run = func(_ context.Context, w daemon.Work) { works <- w }
	ctx, cancel := context.WithCancel(context.Background())
	e.q.Start(ctx)
	t.Cleanup(func() { cancel(); e.q.Wait() })
	entered, releaseList := make(chan struct{}), make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pulls") && r.URL.Query().Get("state") == "open" {
			close(entered)
			<-releaseList
		}
		f.hub.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	e.gh.BaseURL = proxy.URL
	returned := make(chan error, 1)
	go func() { returned <- e.sweep(context.Background(), f.repo) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not enter OpenPulls")
	}
	// This is the queue call made by the webhook handler for a delivery naming
	// PR 5 and its head while the list response is in flight.
	e.q.EnqueueNamed(daemon.SourceWebhook, f.repo.Name, 5, "eee")
	close(releaseList)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	if got := e.q.Head(f.repo.Name, 5); got != "eee" {
		t.Fatalf("sweep lost learned head: got %q, want eee", got)
	}
	e.q.Wait()
	initial := []daemon.Work{<-works, <-works}
	for _, want := range []int{1, 5} {
		found := false
		for _, w := range initial {
			found = found || w.Key.PR == want
		}
		if !found {
			t.Fatalf("initial sweep/webhook work %v missing PR %d", initial, want)
		}
	}
	e.q.EnqueueNamed(daemon.SourceCheck, f.repo.Name, 0, "eee")
	e.q.Wait()
	var check daemon.Work
	select {
	case check = <-works:
	case <-time.After(5 * time.Second):
		t.Fatal("commit-only check did not queue PR 5")
	}
	if check.Key.PR != 5 || !slices.Equal(check.Sources, []daemon.Source{daemon.SourceCheck}) {
		t.Fatalf("commit-only check work %+v, want only PR 5 from a check", check)
	}
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
