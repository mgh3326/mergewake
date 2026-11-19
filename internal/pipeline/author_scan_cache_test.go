package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// agentListReads counts the agent-list reads the runner saw on the local host.
func agentListReads(run *scriptRunner) int {
	n := 0
	for _, c := range run.calls {
		if strings.Contains(c, "herdr\x00agent\x00list") {
			n++
		}
	}
	return n
}

// unavailableListReads counts the agent-list reads sent to the unavailable host.
func unavailableListReads(run *scriptRunner) int {
	n := 0
	remote := reap.Join([]string{"herdr", "agent", "list"})
	for _, c := range run.calls {
		if strings.Contains(c, "unavailable") && strings.Contains(c, remote) {
			n++
		}
	}
	return n
}

// fixedClock points the service at a clock the test moves by hand.
func fixedClock(s *Service) *time.Time {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	return &now
}

// A burst of evaluations over one PR, each in its own evaluation context, reads
// the host's pane list once; the next evaluation after the cache window reads
// it again.
func TestAuthorScanBurstSharesOnePaneListPerHost(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	now := fixedClock(s)
	pr := s.GH.(*fakeGH).pull
	for i := 0; i < 5; i++ {
		rt, e := s.findingsRoute(WithAuthorScan(context.Background()), testRepoFor(s), pr)
		if e != nil {
			t.Fatal(e)
		}
		if rt.lane != "w-9-lane" {
			t.Fatalf("evaluation %d routed to %q, want w-9-lane", i, rt.lane)
		}
	}
	if n := agentListReads(run); n != 1 {
		t.Fatalf("five evaluations in one burst read the agent list %d times, want 1", n)
	}
	*now = now.Add(10 * time.Minute)
	if _, e := s.findingsRoute(WithAuthorScan(context.Background()), testRepoFor(s), pr); e != nil {
		t.Fatal(e)
	}
	if n := agentListReads(run); n != 2 {
		t.Fatalf("an evaluation after the cache window read the agent list %d times in total, want 2", n)
	}
}

// A host whose scan failed is not cached as healthy: the failure is kept for
// the window and every evaluation in it still sees the host failed, then the
// first scan after the window retries it.
func TestAuthorScanFailedHostStaysFailedForTTL(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	setPartialAuthorScan(s, run)
	now := fixedClock(s)
	for i := 0; i < 3; i++ {
		_, err := s.authorHosts(context.Background())
		if !hostScanOnly(err) || !strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("evaluation %d: authorHosts error %v, want the unavailable host's scan error", i, err)
		}
	}
	if n := unavailableListReads(run); n != 1 {
		t.Fatalf("failed host read %d times across three evaluations in one window, want 1", n)
	}
	*now = now.Add(10 * time.Minute)
	if _, err := s.authorHosts(context.Background()); !hostScanOnly(err) {
		t.Fatalf("after the window authorHosts error %v, want the unavailable host's scan error", err)
	}
	if n := unavailableListReads(run); n != 2 {
		t.Fatalf("failed host read %d times after the window, want 2 (one retry)", n)
	}
}

// A scan that fails under an evaluation already cancelled proves nothing about
// the host, so it is not kept: the next evaluation scans the host again.
func TestAuthorScanCancelledEvaluationIsNotKept(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	setPartialAuthorScan(s, run)
	fixedClock(s)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.authorHosts(WithAuthorScan(ctx)); err == nil {
		t.Fatal("a cancelled evaluation's failing host scan returned no error")
	}
	if n := unavailableListReads(run); n != 1 {
		t.Fatalf("cancelled evaluation read the failing host %d times, want 1", n)
	}
	// The next healthy evaluation scans the host again and keeps that failure
	// for the window, so a second healthy evaluation does not read it.
	for i := 0; i < 2; i++ {
		if _, err := s.authorHosts(context.Background()); err == nil {
			t.Fatal("the failing host scan returned no error on a healthy evaluation")
		}
	}
	if n := unavailableListReads(run); n != 2 {
		t.Fatalf("after a cancelled evaluation and two healthy ones the host was read %d times, want 2 (the cancelled scan was not kept; the healthy one was)", n)
	}
}

// jobReadCounter counts the job reads that reach the test sink.
type jobReadCounter struct {
	*jobSink
	reads map[string]int
}

func (c *jobReadCounter) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	c.reads[id]++
	return c.jobSink.Job(ctx, id)
}

// One evaluation that routes FINDINGS and READY for a PR whose task binds a job
// reads that job once: the route's bound-job lookup and the READY lane lookup
// share the read. The takeover candidate is read once as well.
func TestEvaluationReadsBoundJobOnce(t *testing.T) {
	s, sink, _ := takeoverSetup(t, paneB)
	counter := &jobReadCounter{jobSink: sink, reads: map[string]int{}}
	s.Sink = counter
	ctx := integrations.WithTaskMemo(WithAuthorScan(context.Background()))
	pr := s.GH.(*fakeGH).pull
	if _, e := s.findingsRoute(ctx, testRepoFor(s), pr); e != nil {
		t.Fatal(e)
	}
	if _, _, e := s.notifyLane(ctx, testRepoFor(s), pr); e != nil {
		t.Fatal(e)
	}
	if counter.reads["w-7"] != 1 || counter.reads["w-9"] != 1 {
		t.Fatalf("job reads in one evaluation: w-7 %d, w-9 %d; want 1 and 1", counter.reads["w-7"], counter.reads["w-9"])
	}
}
