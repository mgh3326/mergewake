package pipeline

import (
	"context"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

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
