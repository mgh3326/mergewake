package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// A lost review is mergewake's own automation failing, so it goes to the
// tool lane when one is set, and to the desk when it is not. The event id
// is unchanged either way, so the hub dedupe still works.
func TestLostReviewGoesToToolLaneWhenSet(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	s.Config.Notifier.ToolLane = "tool-lane"
	lost := &reviewResultLost{label: "reviewer", cause: errors.New("result file missing")}
	if err := s.notifyReviewResultLost(context.Background(), r, g.pull, &passPlan{pass: 1, lens: "default"}, lost); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || n.events[0].Lane != "tool-lane" {
		t.Fatalf("lost-review notice = %+v, want one tool-lane event", n.events)
	}
	if !strings.HasPrefix(n.events[0].ID, "mergewake-review-result-lost-") {
		t.Fatalf("lost-review event id changed: %q", n.events[0].ID)
	}
}

func TestLostReviewFallsBackToDeskWithoutToolLane(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	lost := &reviewResultLost{label: "reviewer", cause: errors.New("result file missing")}
	if err := s.notifyReviewResultLost(context.Background(), r, g.pull, &passPlan{pass: 1, lens: "default"}, lost); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || n.events[0].Lane != "operator-desk" {
		t.Fatalf("lost-review notice = %+v, want one desk event", n.events)
	}
}

// An exhausted review retry notifies the tool lane and the PR's lane once
// each. Without a tool lane the PR-lane notice is the only copy, exactly as
// before, with the same event id.
func TestReviewRetryEscalationNotifiesToolAndPRLanes(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	s.Config.Notifier.ToolLane = "tool-lane"
	r.Lane = "owner-lane"
	sent, err := s.escalateReviewFailure(context.Background(), r, g.pull, 3, "boom")
	if err != nil || !sent {
		t.Fatalf("escalate = %t, %v", sent, err)
	}
	if n.count != 2 {
		t.Fatalf("escalation notices = %+v, want one tool and one owner event", n.events)
	}
	wantID := "mergewake-review-failed-example/project-7-" + g.pull.Head.SHA
	byLane := map[string]string{}
	for _, e := range n.events {
		byLane[e.Lane] = e.ID
		if e.Type != "automation-failed" || !strings.Contains(e.Text, "review failed 3 times") {
			t.Fatalf("escalation event = %+v", e)
		}
	}
	if byLane["owner-lane"] != wantID {
		t.Fatalf("owner event id = %q, want %q", byLane["owner-lane"], wantID)
	}
	if byLane["tool-lane"] != wantID+"-tool" {
		t.Fatalf("tool event id = %q, want %q", byLane["tool-lane"], wantID+"-tool")
	}
	// A repeat escalation keeps both ids, so the hub dedupes it per lane.
	n.count, n.events = 0, nil
	if sent, err := s.escalateReviewFailure(context.Background(), r, g.pull, 3, "boom"); err != nil || !sent {
		t.Fatalf("repeat escalate = %t, %v", sent, err)
	}
	if n.count != 2 || n.events[0].ID != wantID || n.events[1].ID != wantID+"-tool" {
		t.Fatalf("repeat escalation ids = %+v, want stable ids", n.events)
	}
}

// A tool-lane emit failure must not suppress the design-lane escalation: the
// owner notice is always attempted, and its delivery counts even when the
// tool copy fails.
func TestReviewRetryEscalationToolLaneFailureKeepsOwnerNotice(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	s.Config.Notifier.ToolLane = "tool-lane"
	r.Lane = "owner-lane"
	n.failLanes = map[string]error{"tool-lane": errors.New("lane not registered on this host")}
	sent, err := s.escalateReviewFailure(context.Background(), r, g.pull, 3, "boom")
	if err == nil || !sent {
		t.Fatalf("escalate = %t, %v; want the owner delivery with a joined tool error", sent, err)
	}
	if n.count != 1 || n.events[0].Lane != "owner-lane" || n.events[0].ID != "mergewake-review-failed-example/project-7-"+g.pull.Head.SHA {
		t.Fatalf("escalation notices = %+v, want the owner copy delivered", n.events)
	}
}

func TestReviewRetryEscalationWithoutToolLaneIsOneOwnerNotice(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	r.Lane = "owner-lane"
	sent, err := s.escalateReviewFailure(context.Background(), r, g.pull, 3, "boom")
	if err != nil || !sent {
		t.Fatalf("escalate = %t, %v", sent, err)
	}
	wantID := "mergewake-review-failed-example/project-7-" + g.pull.Head.SHA
	if n.count != 1 || n.events[0].Lane != "owner-lane" || n.events[0].ID != wantID {
		t.Fatalf("escalation notices = %+v, want one owner event %q", n.events, wantID)
	}
}

func TestReviewRetryEscalationCollapsesWhenToolLaneIsPRLane(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	s.Config.Notifier.ToolLane = "owner-lane"
	r.Lane = "owner-lane"
	if sent, err := s.escalateReviewFailure(context.Background(), r, g.pull, 3, "boom"); err != nil || !sent {
		t.Fatalf("escalate = %t, %v", sent, err)
	}
	if n.count != 1 || n.events[0].Lane != "owner-lane" {
		t.Fatalf("escalation notices = %+v, want one event on the shared lane", n.events)
	}
}

// A round hold goes to the lane the PR names: its Notify-Lane trailer, else
// its task's lane, else the repo's lane. The desk gets it only when no lane
// resolves. Event ids are unchanged.
func TestRoundHoldPrefersNotifyLaneTrailerOverTaskLane(t *testing.T) {
	s, g, _, sink, run, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	g.pull.Body = "Summary\n\nMergewake-Task: 2481\nMergewake-Notify-Lane: trailer-lane\n"
	rec := &emitRecorder{roundRuleRunner: run}
	s.Runner = rec
	wirePanewire(s, rec)
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	_ = sink
	if len(rec.holdArgs) != 1 {
		t.Fatalf("want one round-hold emit, got %d", len(rec.holdArgs))
	}
	if got := flagValue(rec.holdArgs[0], "--lane"); got != "trailer-lane" {
		t.Fatalf("round-hold --lane = %q, want trailer-lane", got)
	}
	if id := flagValue(rec.holdArgs[0], "--event-id"); id != "mergewake-round-hold-example/project-7-7" {
		t.Fatalf("round-hold --event-id = %q, want the stable hold id", id)
	}
}

func TestRoundHoldFallsBackToRepoLaneWithoutTaskLane(t *testing.T) {
	s, g, _, sink, run, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	sink.lanes["2481"] = ""
	r.Lane = "repo-lane"
	rec := &emitRecorder{roundRuleRunner: run}
	s.Runner = rec
	wirePanewire(s, rec)
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(rec.holdArgs) != 1 {
		t.Fatalf("want one round-hold emit, got %d", len(rec.holdArgs))
	}
	if got := flagValue(rec.holdArgs[0], "--lane"); got != "repo-lane" {
		t.Fatalf("round-hold --lane = %q, want repo-lane", got)
	}
}

func TestRoundHoldFallsBackToDeskWithoutAnyLane(t *testing.T) {
	s, g, _, sink, run, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	sink.lanes["2481"] = ""
	r.Lane = ""
	rec := &emitRecorder{roundRuleRunner: run}
	s.Runner = rec
	wirePanewire(s, rec)
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(rec.holdArgs) != 1 {
		t.Fatalf("want one round-hold emit, got %d", len(rec.holdArgs))
	}
	if got := flagValue(rec.holdArgs[0], "--lane"); got != "operator-desk" {
		t.Fatalf("round-hold --lane = %q, want operator-desk", got)
	}
}
