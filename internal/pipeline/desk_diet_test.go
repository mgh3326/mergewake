package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestDeskDietLeadBindingAndBlockRouting(t *testing.T) {
	for _, role := range []string{"worker", "builder"} {
		t.Run(role, func(t *testing.T) {
			s, g, n, sink, r, _ := deliverSetup(t)
			sink.recs["w-7"] = integrations.JobRecord{Role: role, Label: "author-lane", OwnerLane: "operator-desk", State: "completed"}
			for _, reason := range []string{"blocking_findings:1", "triage_unanswered:1", "check_failed:unit"} {
				lane, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "blocked", reason)
				if e != nil || !sent || lane != "author-lane" {
					t.Fatalf("route %s: %s %t %v", reason, lane, sent, e)
				}
			}
			for _, ev := range n.events {
				if ev.Lane == "operator-desk" {
					t.Fatal("premature desk copy", ev)
				}
			}
			g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
			deliver(t, s, r)
			if n.events[len(n.events)-1].Lane != "author-lane" {
				t.Fatal(n.events)
			}
		})
	}
}

func TestDeskDietFindingsAgeDespiteAckAndAcrossRestart(t *testing.T) {
	for _, by := range []string{"time", "rounds"} {
		t.Run(by, func(t *testing.T) {
			s, g, n, _, r, now := deliverSetup(t)
			g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
			deliver(t, s, r)
			n.acked[n.events[0].ID] = true
			deliver(t, s, r)
			restarted := *s
			s = &restarted
			if by == "time" {
				*now = now.Add(2 * time.Hour)
			} else {
				g.pull.Head.SHA = shaB
				deliver(t, s, r)
				if n.count != 1 {
					t.Fatal("escalated before N rounds", n.events)
				}
				g.pull.Head.SHA = shaC
			}
			deliver(t, s, r)
			deliver(t, s, r)
			if n.count != 2 || n.events[1].Lane != "operator-desk" || n.events[1].Type != "decision" {
				t.Fatal(n.events)
			}
		})
	}
}

func TestDeskDietDisposedDoesNotAgeAndFleetReplyStopsDelivery(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "unconfigured", true: "configured"}[configured], func(t *testing.T) {
			s, g, n, _, r, now := deliverSetup(t)
			if configured {
				s.Config.Review.FleetAuthors = []string{"fleet-author"}
			}
			g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
			deliver(t, s, r)
			g.feedback.Threads[0].Comments = append(g.feedback.Threads[0].Comments, github.ReviewComment{Author: "fleet-author[bot]", AuthorType: "Bot", Body: "Disposition: dismiss covered by a focused test"})
			*now = now.Add(3 * time.Hour)
			deliver(t, s, r)
			if configured && n.count != 1 {
				t.Fatal("disposed finding escalated", n.events)
			}
			if !configured && n.count != 2 {
				t.Fatal("untrusted reply accepted", n.events)
			}
			ledger, e := s.loadDeliveries(r.Name)
			if e != nil || (configured && ledger[0].AckBy != "disposition") {
				t.Fatal(ledger, e)
			}
		})
	}
}

func TestDeskDietCheckFailureAgeClearsAndUnknownAuthor(t *testing.T) {
	s, g, n, sink, r, now := deliverSetup(t)
	for _, head := range []string{shaA, shaB, shaC} {
		g.pull.Head.SHA = head
		if e := s.ageCheckFailures(context.Background(), r, g.pull, []string{"check_failed:unit"}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 1 || n.events[0].Type != "decision" {
		t.Fatal(n.events)
	}
	if e := s.ageCheckFailures(context.Background(), r, g.pull, nil); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(time.Hour)
	if e := s.ageCheckFailures(context.Background(), r, g.pull, []string{"check_failed:unit"}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 {
		t.Fatal("new failure inherited old age", n.events)
	}
	delete(sink.jobs, "2481")
	_, _, _, e := s.laneEvent(context.Background(), r, g.pull, "blocked", "check_failed:unknown")
	if e != nil || n.count != 2 || n.events[1].Lane != "operator-desk" || n.events[1].Type != "decision" || !strings.Contains(n.events[1].Text, "unknown author") {
		t.Fatal(n.events, e)
	}
	if eventType("merged") != "merged" {
		t.Fatal("MERGED is untyped")
	}
}
