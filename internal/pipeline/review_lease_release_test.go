package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// hookRunner runs before each panewire spawn, to stand in for another host.
type hookRunner struct {
	*panewireRunner
	before func()
}

func (h *hookRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && h.before != nil {
		h.before()
	}
	return h.panewireRunner.Run(ctx, command, args, input, dir)
}

func leaseHeld(g *fakeGH, pass int) bool {
	fakeGHMu.Lock()
	defer fakeGHMu.Unlock()
	return g.refs[claimRef(7, shaA, pass)]
}

func assertLeaseReleased(t *testing.T, s *Service, g *fakeGH, r config.Repo, runner *panewireRunner, reason string) {
	t.Helper()
	if leaseHeld(g, 1) {
		t.Fatal("the pass lease is still held after a spawn that placed no pane")
	}
	rows := auditRows(t, s, r, "review-lease-released")
	if len(rows) != 1 || rows[0]["pr"] != float64(7) || rows[0]["head"] != shaA || rows[0]["pass"] != float64(1) || rows[0]["reason"] != reason {
		t.Fatalf("release audit=%v", rows)
	}
	// The next evaluation starts the pass at once, not review-pending.
	runner.findings = `{"findings":[]}`
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("next review: %v", err)
	}
	if g.postCount != 1 || len(runner.spawns) < 2 {
		t.Fatalf("next review posts=%d spawns=%d", g.postCount, len(runner.spawns))
	}
}

// (a) A spawn refused because another reviewer job holds the reservation
// (exit 74) releases the lease; the next run starts the pass.
func TestLeaseReleaseOnReviewerHeld(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld}}
	s, g, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	assertLeaseReleased(t, s, g, r, runner, "reviewer-held")
}

// (b) A quota gate timeout (exit 124) before any pane exists releases the lease.
func TestLeaseReleaseOnQuotaGateTimeout(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireQuotaTimeout}}
	s, g, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	assertLeaseReleased(t, s, g, r, runner, "quota-gate-timeout")
}

// (c) A lease another host renewed while the spawn ran is never deleted.
func TestLeaseReleaseSkipsRenewedLease(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld}}
	s, g, r := panewireSetup(t, runner)
	s.Runner = &hookRunner{runner, func() {
		fakeGHMu.Lock()
		defer fakeGHMu.Unlock()
		g.leaseIDs = map[string]string{claimRef(7, shaA, 1): shaB}
	}}
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if !leaseHeld(g, 1) {
		t.Fatal("a lease renewed by another host was deleted")
	}
	if rows := auditRows(t, s, r, "review-lease-released"); len(rows) != 0 {
		t.Fatalf("release audit=%v", rows)
	}
}

// (d) A wait that timed out after the spawn was accepted may still land a
// pane: the lease stays.
func TestLeaseReleaseKeepsLeaseOnWaitTimeout(t *testing.T) {
	s, g, r, _, _ := interruptedReviewer(t)
	if !leaseHeld(g, 1) {
		t.Fatal("the lease of a spawn whose pane may land was released")
	}
	if rows := auditRows(t, s, r, "review-lease-released"); len(rows) != 0 {
		t.Fatalf("release audit=%v", rows)
	}
}

// (e) A successful spawn keeps its lease and owns it until the findings post.
func TestLeaseReleaseKeepsLeaseOnSuccess(t *testing.T) {
	runner := &panewireRunner{family: "openai", findings: `{"findings":[]}`}
	s, g, r := panewireSetup(t, runner)
	s.Runner = &hookRunner{runner, func() {
		rj := readReviewerJob(t, s, r, 1)
		if rj.LeaseSHA != shaA || !leaseHeld(g, 1) {
			t.Errorf("at spawn: record lease %q, held=%v", rj.LeaseSHA, leaseHeld(g, 1))
		}
	}}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 || !leaseHeld(g, 1) {
		t.Fatalf("posts=%d held=%v", g.postCount, leaseHeld(g, 1))
	}
	if rows := auditRows(t, s, r, "review-lease-released"); len(rows) != 0 {
		t.Fatalf("release audit=%v", rows)
	}
}
