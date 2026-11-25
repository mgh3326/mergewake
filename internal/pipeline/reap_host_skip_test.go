package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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
	skip := hostScanFailure(context.Background(), "node-a", "host panes", &reap.HostUnreachable{Machine: "node-a", Reason: "node_timeout"})
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
	stop := &ReapBudgetStop{Left: "x", Failures: hostScanFailure(context.Background(), "node-a", "host panes", &reap.HostUnreachable{Machine: "node-a", Reason: "unknown"})}
	skips, rest := SplitReapHostSkips(stop.Failures)
	if len(skips) != 1 || rest != nil {
		t.Fatalf("skips=%v rest=%v", skips, rest)
	}
}

func TestHostScanFailureOtherErrorsStayFailures(t *testing.T) {
	skips, rest := SplitReapHostSkips(hostScanFailure(context.Background(), "node-a", "host panes", errors.New("hub: machine node-a missing agents")))
	if len(skips) != 0 || rest == nil {
		t.Fatalf("skips=%v rest=%v", skips, rest)
	}
}

func TestSplitReapRepoErrorKeepsOnlyRealFailures(t *testing.T) {
	skip := hostScanFailure(context.Background(), "node-a", "host panes", &reap.HostUnreachable{Machine: "node-a", Reason: "node_unavailable"})
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
	skip := hostScanFailure(context.Background(), "node-timeout", "host panes", &fakeTimeoutError{msg: "i/o timeout"})
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

func TestHostScanFailureLocalBudgetCutIsNotASkip(t *testing.T) {
	ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), ErrReapRepoBudget)
	defer cancel()
	state := &reapPassState{}
	ctx = context.WithValue(ctx, reapPassStateKey{}, state)
	cut := fmt.Errorf("herdr agent list timed out; process group terminated: %w", context.DeadlineExceeded)

	err := hostScanFailure(ctx, "local", "host panes", cut)
	skips, rest := SplitReapHostSkips(err)
	if len(skips) != 0 {
		t.Fatalf("a budget cut on the local host became a host skip: %v", skips)
	}
	if !errors.Is(rest, ErrReapRepoBudget) {
		t.Fatalf("budget cut lost its stop: %v", rest)
	}
	if left, inflight := state.snapshot(); left != "" || inflight != "host panes" {
		t.Fatalf("cut state left=%q in-flight=%q", left, inflight)
	}
}

func TestHostScanFailureTimeoutWithBudgetLeftStaysASkip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	state := &reapPassState{}
	ctx = context.WithValue(ctx, reapPassStateKey{}, state)

	err := hostScanFailure(ctx, "local", "host panes", fmt.Errorf("call: %w", context.DeadlineExceeded))
	skips, rest := SplitReapHostSkips(err)
	if len(skips) != 1 || skips[0] != (HostSkip{Host: "local", Reason: "timeout"}) || rest != nil {
		t.Fatalf("skips=%v rest=%v", skips, rest)
	}
	if left, inflight := state.snapshot(); left != "" || inflight != "" {
		t.Fatalf("a host timeout recorded a budget cut: left=%q in-flight=%q", left, inflight)
	}
}

func TestHostScanFailureHubBudgetCutUnchanged(t *testing.T) {
	ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), ErrReapRepoBudget)
	defer cancel()
	state := &reapPassState{}
	ctx = context.WithValue(ctx, reapPassStateKey{}, state)
	// A hub call already cut by the budget reports its own cut (timedHubOps).
	hubCut := fmt.Errorf("%w: host panes: %v", ErrReapRepoBudget, context.DeadlineExceeded)

	err := hostScanFailure(ctx, "node-a", "host panes", hubCut)
	skips, rest := SplitReapHostSkips(err)
	if len(skips) != 0 || !errors.Is(rest, ErrReapRepoBudget) {
		t.Fatalf("skips=%v rest=%v", skips, rest)
	}
	if left, inflight := state.snapshot(); left != "" || inflight != "" {
		t.Fatalf("hub budget cut was recorded twice: left=%q in-flight=%q", left, inflight)
	}
}
