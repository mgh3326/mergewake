package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// GitHub runs no pull_request workflow for a PR that conflicts with its base,
// so its required checks never appear. The gate cannot wait for them.
func conflictGate(t *testing.T, mergeable *bool, state string, checks map[string]github.CI) gateVerdict {
	t.Helper()
	s, g, _, _, _ := setup(t)
	g.enabled = true
	markReviewed(g)
	g.required = []string{"vet", "test"}
	g.ci = github.Pending // a required check that never ran is not green
	g.pull.Mergeable, g.pull.MergeableState = mergeable, state
	if checks != nil {
		g.checkNames = map[string]map[string]github.CI{shaA: checks}
	}
	return gateAt(t, s, g, shaB)
}

func TestGateConflictNoChecksBlocks(t *testing.T) {
	no := false
	for _, state := range []string{"dirty", "unknown"} {
		v := conflictGate(t, &no, state, nil)
		if v.State != "blocked" || reasons(v) != "conflict" {
			t.Fatalf("mergeable=false state=%s: %+v, want blocked conflict", state, v)
		}
	}
	v := conflictGate(t, nil, "dirty", nil)
	if v.State != "blocked" || reasons(v) != "conflict" {
		t.Fatalf("dirty alone: %+v, want blocked conflict", v)
	}
}

func TestGateConflictKeepsFailedCheckReasons(t *testing.T) {
	no := false
	v := conflictGate(t, &no, "dirty", map[string]github.CI{"vet": github.Red})
	if v.State != "blocked" || reasons(v) != "check_failed:vet;conflict" {
		t.Fatalf("%+v, want blocked check_failed:vet;conflict", v)
	}
}

func TestGateUnknownMergeableNoChecksStaysPending(t *testing.T) {
	yes := true
	for _, tt := range []struct {
		mergeable *bool
		state     string
	}{{nil, ""}, {nil, "unknown"}, {&yes, "unknown"}} {
		v := conflictGate(t, tt.mergeable, tt.state, nil)
		if v.State != "pending" || len(v.Reasons) != 0 {
			t.Fatalf("mergeable=%v state=%q: %+v, want pending without reasons", tt.mergeable, tt.state, v)
		}
	}
}

func TestGateMergeableQueuedCheckStaysPending(t *testing.T) {
	yes := true
	v := conflictGate(t, &yes, "clean", map[string]github.CI{"vet": github.Green, "test": github.Pending})
	if v.State != "pending" || len(v.Reasons) != 0 {
		t.Fatalf("%+v, want pending without reasons", v)
	}
}

// The blocked notice for a conflicting PR goes to the author's lane through
// findingsRoute, once per head.
func TestNotifyConflictNoChecksRoutesToAuthorOnce(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.required = []string{"vet", "test"}
	g.ci = github.Pending
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
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
	if ev.Lane != "w-7" || !strings.Contains(ev.Text, "BLOCKED example/project#7 head=aaaaaaa | conflict | lane=w-7 via task 2481 job w-7") {
		t.Fatalf("%+v", ev)
	}
}
