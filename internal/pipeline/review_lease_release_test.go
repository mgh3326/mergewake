package pipeline

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
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

// leaseHeld reports whether the pass lease is still live: a released lease keeps
// its branch but is stamped expired.
func leaseHeld(g *fakeGH, pass int) bool {
	fakeGHMu.Lock()
	defer fakeGHMu.Unlock()
	_, active, _ := g.lease(claimRef(7, shaA, pass), time.Hour)
	return active
}

func assertLeaseReleased(t *testing.T, s *Service, g *fakeGH, r config.Repo, runner *panewireRunner, reason string) {
	t.Helper()
	if leaseHeld(g, 1) {
		t.Fatal("the pass lease is still live after a spawn that placed no pane")
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

// (a) The hub's active-job refusal (74) is an ordinary pre-start refusal now
// that a reviewer spawn takes no hub reservation: no reviewer-held row, no
// sweep, the lease goes back and the pass fails like any other refusal.
func TestLeaseReleaseOnActiveJobRefusal(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld}}
	s, g, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	if rows := auditRows(t, s, r, "reviewer-held"); len(rows) != 0 {
		t.Fatalf("held audit=%v", rows)
	}
	if rows := auditRows(t, s, r, "reviewer-held-sweep-failed"); len(rows) != 0 {
		t.Fatalf("sweep audit=%v", rows)
	}
	assertLeaseReleased(t, s, g, r, runner, "spawn-refused")
}

// A pass refused with 74 is a real review failure, not a wait: RetryReview
// counts it and backs off.
func TestActiveJobRefusalCountsAsReviewFailure(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld}}
	s, g, r := panewireSetup(t, runner)
	err := s.RetryReview(context.Background(), r, g.pull, Options{})
	if err == nil || errors.Is(err, ErrReviewHeld) || !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("retry err=%v, want a review-unavailable failure", err)
	}
	if st := retryStateOf(t, s, r); st.Failures != 1 || st.Backoffs != 1 || st.Held || st.WaitText != "" {
		t.Fatalf("retry state=%+v, want one counted failure and no wait", st)
	}
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
	runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
	s, g, r := panewireSetup(t, runner)
	s.Config.Notifier.Lane = "desk"
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
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

// raceClaim is the GitHub client when another host claims a pass between the
// pass list and this host's claim: the preflight reads no lease, the claim
// loses.
type raceClaim struct{ *fakeGH }

func (c *raceClaim) ClaimReviewWithSHA(context.Context, string, string, string, time.Duration) (string, bool, error) {
	return "", false, nil
}

// A pass the preflight found unclaimed, but which another host claimed before
// this host's claim, is not reviewed: the claim check in runPass keeps one
// reviewer per pass when two evaluations race past the pass list.
func TestLostClaimRacePlacesNoReviewer(t *testing.T) {
	runner := &panewireRunner{family: "openai", findings: `{"findings":[]}`}
	s, g, r := panewireSetup(t, runner)
	s.GH = &raceClaim{fakeGH: g}
	var output bytes.Buffer
	s.Out = &output
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("review: %v", err)
	}
	if !strings.Contains(output.String(), `"action":"review-pending"`) {
		t.Fatalf("output=%s, want the pass pending", output.String())
	}
	if len(runner.spawns) != 0 || g.postCount != 0 {
		t.Fatalf("spawns=%d posts=%d, want no reviewer", len(runner.spawns), g.postCount)
	}
}
