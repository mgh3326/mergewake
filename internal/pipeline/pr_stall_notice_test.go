package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

func TestPRIdleStallThresholdAndNewHead(t *testing.T) {
	s, g, n, r := notifySetup(t)
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	g.pull.CreatedAt = now.Add(-time.Hour).Format(time.RFC3339)
	if err := s.notifyPRIdle(context.Background(), r, g.pull); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("at threshold sent %d notices", n.count)
	}
	now = now.Add(time.Minute)
	if err := s.notifyPRIdle(context.Background(), r, g.pull); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "cause=idle") {
		t.Fatalf("events=%+v", n.events)
	}
	if err := s.notifyPRIdle(context.Background(), r, g.pull); err != nil || n.count != 1 {
		t.Fatalf("repeat events=%d err=%v", n.count, err)
	}
	g.pull.Head.SHA = shaB
	if err := s.notifyPRIdle(context.Background(), r, g.pull); err != nil {
		t.Fatal(err)
	}
	if n.count != 2 {
		t.Fatalf("new head did not re-arm idle notice: %d", n.count)
	}
}

func TestPRCheckRedStallThresholdClearAndNewHead(t *testing.T) {
	s, g, n, r := notifySetup(t)
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	g.checkNames = map[string]map[string]github.CI{shaA: {"CI": github.Red}, shaB: {"CI": github.Red}}
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30*time.Minute - time.Nanosecond)
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 {
		t.Fatalf("below threshold sent %d notices", n.count)
	}
	now = now.Add(time.Nanosecond)
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "cause=check_red:CI") {
		t.Fatalf("events=%+v", n.events)
	}
	g.checkNames[shaA]["CI"] = github.Green
	states, err := s.GH.CheckStates(context.Background(), r.Name, shaA, []string{"CI"})
	if err != nil || states["CI"] != github.Green {
		t.Fatalf("check clear state=%v err=%v", states, err)
	}
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	g.checkNames[shaA]["CI"] = github.Red
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("recurrence notified before its window: %d", n.count)
	}
	now = now.Add(30 * time.Minute)
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 2 {
		t.Fatalf("cleared cause did not re-arm: %d", n.count)
	}
	g.pull.Head.SHA = shaB
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 2 {
		t.Fatalf("new head notified before threshold: %d", n.count)
	}
	now = now.Add(30 * time.Minute)
	if err := s.notifyPRStalls(context.Background(), r, g.pull, policy.Policy{RequiredChecks: []string{"CI"}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 3 {
		t.Fatalf("new head did not re-arm: %d", n.count)
	}
}

func TestRedCheckClockRearmsAfterClearAndNewHead(t *testing.T) {
	now := time.Now()
	first := redCheckClock(prActivityClock{}, false, shaA, now)
	cleared := redCheckClock(prActivityClock{}, false, shaA, now.Add(time.Hour))
	recurred := redCheckClock(cleared, true, shaA, now.Add(2*time.Hour))
	if !recurred.Since.Equal(now.Add(time.Hour)) {
		t.Fatalf("recurrence since=%s", recurred.Since)
	}
	newHead := redCheckClock(first, true, shaB, now.Add(3*time.Hour))
	if !newHead.Since.Equal(now.Add(3 * time.Hour)) {
		t.Fatalf("new head since=%s", newHead.Since)
	}
}
