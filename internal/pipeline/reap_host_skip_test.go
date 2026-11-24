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

func TestSplitReapRepoErrorKeepsOnlyRealFailures(t *testing.T) {
	skip := hostScanFailure("node-a", &reap.HostUnreachable{Machine: "node-a", Reason: "node_unavailable"})
	real := errors.New("node-b /tree: removal refused")

	budget, skips, kept := SplitReapRepoError(errors.Join(skip, real))
	if budget != nil || len(skips) != 1 || kept == nil {
		t.Fatalf("budget=%v skips=%v kept=%v", budget, skips, kept)
	}
	if strings.Contains(kept.Error(), "node-a") || !strings.Contains(kept.Error(), "removal refused") {
		t.Fatalf("skipped host kept in the repository error: %v", kept)
	}

	stop := &ReapBudgetStop{Left: "hosts", Failures: errors.Join(skip, real)}
	budget, skips, kept = SplitReapRepoError(stop)
	if budget == nil || budget.Left != "hosts" || len(skips) != 1 {
		t.Fatalf("budget=%v skips=%v", budget, skips)
	}
	if strings.Contains(kept.Error(), "node-a") || !strings.Contains(kept.Error(), "removal refused") {
		t.Fatalf("skipped host kept under a budget stop: %v", kept)
	}

	budget, skips, kept = SplitReapRepoError(errors.Join(skip))
	if budget != nil || len(skips) != 1 || kept != nil {
		t.Fatalf("skip alone: budget=%v skips=%v kept=%v", budget, skips, kept)
	}
}

type fakeTimeoutError struct {
	msg string
}

func (e *fakeTimeoutError) Error() string { return e.msg }
func (e *fakeTimeoutError) Timeout() bool { return true }

func TestHostScanFailureTimeoutIsSkippedHost(t *testing.T) {
	skip := hostScanFailure("node-timeout", &fakeTimeoutError{msg: "i/o timeout"})
	skips, rest := SplitReapHostSkips(skip)
	if len(skips) != 1 || skips[0] != (HostSkip{Host: "node-timeout", Reason: "timeout"}) {
		t.Fatalf("skips=%v", skips)
	}
	if rest != nil {
		t.Fatalf("timeout alone left a failure: %v", rest)
	}

	budget, repoSkips, kept := SplitReapRepoError(skip)
	if budget != nil || len(repoSkips) != 1 || repoSkips[0] != (HostSkip{Host: "node-timeout", Reason: "timeout"}) || kept != nil {
		t.Fatalf("SplitReapRepoError: budget=%v skips=%v kept=%v", budget, repoSkips, kept)
	}
}

func TestSplitReapRepoErrorBudgetStopNotASkip(t *testing.T) {
	stop := &ReapBudgetStop{Left: "hosts", Failures: ErrReapRepoBudget}
	budget, skips, kept := SplitReapRepoError(stop)
	if budget == nil || budget.Left != "hosts" {
		t.Fatalf("budget=%v", budget)
	}
	if len(skips) != 0 {
		t.Fatalf("budget cut produced skips: %v", skips)
	}
	if !errors.Is(kept, ErrReapRepoBudget) {
		t.Fatalf("kept error missing budget: %v", kept)
	}
}
