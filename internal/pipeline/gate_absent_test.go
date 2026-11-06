package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

var absentNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

// absentGate gates a PR whose required checks are vet and "PR gate", with the
// head committed headAge ago.
func absentGate(t *testing.T, vet, prGate github.CI, headAge time.Duration) (gateVerdict, *Service, *fakeGH) {
	t.Helper()
	s, g, _, _ := notifySetup(t)
	s.Now = func() time.Time { return absentNow }
	g.required = []string{"vet", "PR gate"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": vet, "PR gate": prGate}}
	g.committedAt = map[string]time.Time{shaA: absentNow.Add(-headAge)}
	return gateAt(t, s, g, shaB), s, g
}

// (a) A required check that never reported, with the rest green and the grace
// spent, blocks and is named.
func TestGateAbsentPastGraceBlocks(t *testing.T) {
	v, _, _ := absentGate(t, github.Green, github.Absent, absentGrace+time.Minute)
	if v.State != "blocked" || reasons(v) != "check_absent:PR gate" {
		t.Fatalf("verdict=%+v", v)
	}
	if got := v.Status.Checks; len(got) != 2 || got[1].Name != "PR gate" || got[1].State != "absent" {
		t.Fatalf("status checks=%+v", got)
	}
	// The same state still blocks when the sibling check is still running.
	v, _, _ = absentGate(t, github.Pending, github.Absent, 2*absentGrace)
	if v.State != "blocked" || reasons(v) != "check_absent:PR gate" {
		t.Fatalf("sibling running: %+v", v)
	}
}

// (b) Inside the grace, with another check still running, the missing check
// may just not have started: the verdict stays a quiet pending.
func TestGateAbsentWithinGraceStaysPending(t *testing.T) {
	for _, age := range []time.Duration{0, 5 * time.Minute, absentGrace - time.Second} {
		v, _, _ := absentGate(t, github.Pending, github.Absent, age)
		if v.State != "pending" || len(v.Reasons) != 0 {
			t.Fatalf("age %s: %+v", age, v)
		}
	}
	v, _, _ := absentGate(t, github.Pending, github.Absent, absentGrace)
	if v.State != "blocked" || reasons(v) != "check_absent:PR gate" {
		t.Fatalf("at the grace: %+v", v)
	}
}

// (c) Every other check completed and this one never reported: named at once,
// however young the head.
func TestGateAbsentOthersCompletedIsImmediate(t *testing.T) {
	for _, other := range []github.CI{github.Green, github.Skipped} {
		v, _, g := absentGate(t, other, github.Absent, time.Minute)
		if v.State != "blocked" || reasons(v) != "check_absent:PR gate" {
			t.Fatalf("other %s: %+v", other, v)
		}
		if g.commitCalls != 0 {
			t.Fatalf("read the head commit although the rule settled it")
		}
	}
	v, _, _ := absentGate(t, github.Red, github.Absent, time.Minute)
	if v.State != "blocked" || reasons(v) != "check_failed:vet;check_absent:PR gate" {
		t.Fatalf("red sibling: %+v", v)
	}
	// With no other required check there is nothing to say the workflow ran.
	s, g, _, _ := notifySetup(t)
	s.Now = func() time.Time { return absentNow }
	g.required = []string{"PR gate"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"PR gate": github.Absent}}
	g.committedAt = map[string]time.Time{shaA: absentNow.Add(-time.Minute)}
	if v := gateAt(t, s, g, shaB); v.State != "pending" || len(v.Reasons) != 0 {
		t.Fatalf("sole absent check inside the grace: %+v", v)
	}
}

// (d) A queued or running required check is pending as before, never absent,
// however old the head.
func TestGateQueuedRequiredCheckIsNotAbsent(t *testing.T) {
	v, _, _ := absentGate(t, github.Green, github.Pending, 3*absentGrace)
	if v.State != "pending" || len(v.Reasons) != 0 {
		t.Fatalf("verdict=%+v", v)
	}
	if v.Status.Checks[1].State != "pending" {
		t.Fatalf("status checks=%+v", v.Status.Checks)
	}
}

// (e) A conflicting PR gets no workflow, so its checks are not reported absent:
// the reason stays conflict alone.
func TestGateAbsentConflictingPRSaysConflictOnly(t *testing.T) {
	no := false
	for _, vet := range []github.CI{github.Green, github.Pending} {
		s, g, _, _ := notifySetup(t)
		s.Now = func() time.Time { return absentNow }
		g.required = []string{"vet", "PR gate"}
		g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
		g.checkNames = map[string]map[string]github.CI{shaA: {"vet": vet, "PR gate": github.Absent}}
		g.committedAt = map[string]time.Time{shaA: absentNow.Add(-2 * absentGrace)}
		if v := gateAt(t, s, g, shaB); v.State != "blocked" || reasons(v) != "conflict" {
			t.Fatalf("vet=%s: %+v", vet, v)
		}
	}
}

// (f) check_absent goes to the author: the blocked notice reaches the job lane
// and names the reason.
func TestNotifyAbsentRoutesToAuthor(t *testing.T) {
	for _, summary := range []string{"check_absent:PR gate", "threads_open:1; check_absent:PR gate"} {
		if !authorAction(summary) {
			t.Fatalf("%q is not author-actionable", summary)
		}
	}
	s, g, n, r := notifySetup(t)
	s.Now = func() time.Time { return absentNow }
	g.required = []string{"vet", "PR gate"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Green, "PR gate": github.Absent}}
	g.pull.Body = "Mergewake-Task: 2481"
	s.Sink = &jobSink{
		lanes: map[string]string{"2481": "operator-desk"},
		jobs:  map[string]string{"2481": "w-7"},
		recs:  map[string]integrations.JobRecord{"w-7": {State: "spawned", Role: "worker", Label: "w-7"}},
	}
	for i := 0; i < 2; i++ {
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 1 {
		t.Fatalf("%d events, want 1: %+v", n.count, n.events)
	}
	ev := n.events[0]
	if ev.Lane != "w-7" || !strings.Contains(ev.Text, "BLOCKED example/project#7 head=aaaaaaa | check_absent:PR gate | lane=w-7 via task 2481 job w-7") {
		t.Fatalf("%+v", ev)
	}
}

// A pending verdict says nothing: an absent check inside the grace notifies no one.
func TestNotifyAbsentWithinGraceIsSilent(t *testing.T) {
	s, g, n, r := notifySetup(t)
	s.Now = func() time.Time { return absentNow }
	g.required = []string{"vet", "PR gate"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Pending, "PR gate": github.Absent}}
	g.committedAt = map[string]time.Time{shaA: absentNow.Add(-time.Minute)}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatalf("notified: %+v", n.events)
	}
}

// (g) Skipped and cancelled checks keep their handling next to an absent one's
// rules: skipped holds nothing, a young cancellation waits, an old one blocks.
func TestGateAbsentLeavesSkippedAndCancelledAlone(t *testing.T) {
	v, _, _ := absentGate(t, github.Green, github.Skipped, 3*absentGrace)
	if v.State != "ready" || len(v.Reasons) != 0 || !slices.Equal(v.Review.ChecksSkipped, []string{"PR gate"}) {
		t.Fatalf("skipped: %+v", v)
	}
	s, g, _, _ := notifySetup(t)
	s.Now = func() time.Time { return absentNow }
	g.required = []string{"vet", "PR gate"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Green, "PR gate": github.Cancelled}}
	g.cancelledAt = map[string]time.Time{"PR gate": absentNow.Add(-5 * time.Minute)}
	g.committedAt = map[string]time.Time{shaA: absentNow.Add(-3 * absentGrace)}
	if v := gateAt(t, s, g, shaB); v.State != "pending" || reasons(v) != "check_cancelled:PR gate" {
		t.Fatalf("young cancellation: %+v", v)
	}
	g.cancelledAt["PR gate"] = absentNow.Add(-40 * time.Minute)
	if v := gateAt(t, s, g, shaB); v.State != "blocked" || reasons(v) != "check_cancelled:PR gate" {
		t.Fatalf("old cancellation: %+v", v)
	}
}
