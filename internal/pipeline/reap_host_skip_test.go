package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/reap"
)

func TestReapUnreachableNodeIsASkippedHostNotAFailure(t *testing.T) {
	hub := newFakeReapHub()
	hub.agents = map[string]any{"machine_id": "node-a", "state": "disconnected", "agents": nil, "error": "node_unavailable"}
	s, _ := hubSvc(t, hub, &scriptRunner{})
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	skips, rest := SplitReapHostSkips(err)
	if len(skips) != 1 || skips[0] != (HostSkip{Host: "node", Reason: "node_unavailable"}) {
		t.Fatalf("skips=%v err=%v", skips, err)
	}
	if rest != nil {
		t.Fatalf("an unreachable host alone left a failure: %v", rest)
	}
}

func TestSplitReapHostSkipsKeepsRealErrors(t *testing.T) {
	skip := hostScanFailure("node-a", &reap.HostUnreachable{Machine: "node-a", Reason: "node_timeout"})
	real := errors.New("node-b /tree: removal refused")
	skips, rest := SplitReapHostSkips(errors.Join(skip, real))
	if len(skips) != 1 || skips[0].Host != "node-a" {
		t.Fatalf("skips=%v", skips)
	}
	if rest == nil || !strings.Contains(rest.Error(), "removal refused") || strings.Contains(rest.Error(), "node-a") {
		t.Fatalf("rest=%v", rest)
	}
}

func TestSplitReapHostSkipsUnderBudgetStop(t *testing.T) {
	stop := &ReapBudgetStop{Left: "x", Failures: hostScanFailure("node-a", &reap.HostUnreachable{Machine: "node-a", Reason: "unknown"})}
	skips, rest := SplitReapHostSkips(stop.Failures)
	if len(skips) != 1 || rest != nil {
		t.Fatalf("skips=%v rest=%v", skips, rest)
	}
}

func TestHostScanFailureOtherErrorsStayFailures(t *testing.T) {
	skips, rest := SplitReapHostSkips(hostScanFailure("node-a", errors.New("hub: machine node-a missing agents")))
	if len(skips) != 0 || rest == nil {
		t.Fatalf("skips=%v rest=%v", skips, rest)
	}
}
