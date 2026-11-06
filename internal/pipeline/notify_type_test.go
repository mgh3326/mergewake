package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// notifyTypeReady polls NotifyPR on a clock that crosses the reminder interval
// and returns the types of the READY and its first reminder on the worker lane.
func notifyTypeReady(t *testing.T) (first, reminder string) {
	t.Helper()
	s, _, n, r := workerNotifySetup(t)
	now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	for _, d := range []time.Duration{0, 31 * time.Minute} {
		now = now.Add(d)
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 2 {
		t.Fatalf("want READY and one reminder: %+v", n.events)
	}
	for _, e := range n.events {
		if e.Lane != "worker-lane" {
			t.Fatalf("lane %q, want worker-lane: %+v", e.Lane, e)
		}
	}
	return n.events[0].Type, n.events[1].Type
}

func TestNotifyTypeBlockedKeepsBlockedOnAuthorLane(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	sink.recs["w-7"] = integrations.JobRecord{Role: "worker", Label: "author-lane", OwnerLane: "operator-desk", State: "completed"}
	sink.lanes["2481"] = "author-lane"
	// A blocking finding and a review_missing carrying a block detail both wait
	// on the author and stay blocked.
	for i, summary := range []string{"blocking_findings:1", "review_missing (blocked: missing-task-trailer)"} {
		g.pull.Head.SHA = []string{shaA, shaB}[i]
		if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "blocked", summary); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 2 {
		t.Fatal(n.events)
	}
	for i, e := range n.events {
		if e.Type != "blocked" || e.Lane != "author-lane" {
			t.Fatalf("event %d: type %q lane %q: %+v", i, e.Type, e.Lane, e)
		}
	}
}

func TestNotifyTypeReadyToWorkerLaneIsInfo(t *testing.T) {
	first, reminder := notifyTypeReady(t)
	if first != "info" || reminder != "info" {
		t.Fatalf("READY %q reminder %q, want info/info", first, reminder)
	}
}

// A READY with no worker lane sends nothing, so no type reaches a desk lane.
func TestNotifyTypeReadyWithoutWorkerLaneIsSilent(t *testing.T) {
	s, _, n, r := notifySetup(t)
	now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	for _, d := range []time.Duration{0, 31 * time.Minute} {
		now = now.Add(d)
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 0 {
		t.Fatalf("READY reached a non-worker lane: %+v", n.events)
	}
}

func TestNotifyTypeBlockedUnknownAuthorToDeskIsDecision(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	delete(sink.jobs, "2481")
	if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "blocked", "check_failed:unknown"); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "operator-desk" || n.events[0].Type != "decision" {
		t.Fatal(n.events)
	}
}

// MERGED keeps its type on the author worker lane and is not sent at all
// without one.
func TestNotifyTypeMergedStaysMerged(t *testing.T) {
	s, g, n, r := workerNotifySetup(t)
	if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Type != "merged" || n.events[0].Lane != "worker-lane" {
		t.Fatalf("%+v", n.events)
	}

	s, g, n, r = notifySetup(t)
	if _, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil || sent || n.count != 0 {
		t.Fatalf("sent=%t events=%+v err=%v", sent, n.events, e)
	}
}
