package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// leaseReleaseReason is the one audited release of pass 1, or "" for none.
func leaseReleaseReason(t *testing.T, s *Service, rr []map[string]any) string {
	t.Helper()
	switch len(rr) {
	case 0:
		return ""
	case 1:
		reason, _ := rr[0]["reason"].(string)
		return reason
	}
	t.Fatalf("release audit=%v", rr)
	return ""
}

// (a) A failed fetch of the PR head, after the claim and before any spawn,
// releases the lease and the audit names the step.
func TestLeaseReleaseOnFetchPRHeadFailure(t *testing.T) {
	runner := &panewireRunner{family: "openai", gitFails: "fetch"}
	s, g, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if len(runner.spawns) != 0 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	if leaseHeld(g, 1) {
		t.Fatal("the pass lease is still live after a skip that left no pane")
	}
	if reason := leaseReleaseReason(t, s, auditRows(t, s, r, "review-lease-released")); !strings.HasPrefix(reason, "fetch PR head:") {
		t.Fatalf("release reason=%q", reason)
	}
	// The next evaluation starts the pass at once.
	runner.gitFails = ""
	runner.findings = `{"findings":[]}`
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("next review: %v", err)
	}
	if g.postCount != 1 {
		t.Fatalf("posts=%d", g.postCount)
	}
}

// (b) A brief that cannot be published releases the lease too.
func TestLeaseReleaseOnPublishBriefFailure(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, runner)
	runner.sink.uploadErr = errors.New("attachment service unavailable")
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, context: strings.Repeat("context ", 10000)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plan := &passPlan{pass: 1, lens: "default"}
	claim := func() (string, bool, error) {
		return g.ClaimReviewWithSHA(ctx, r.Name, claimRef(7, shaA, 1), shaA, time.Hour)
	}
	out, e := x.runPass(ctx, plan, claim)
	if e != nil || len(runner.spawns) != 0 || len(out.skips) != 1 || !strings.Contains(out.skips[0].Reason, "publish brief: attachment service unavailable") {
		t.Fatalf("spawns=%d outcome=%+v error=%v", len(runner.spawns), out, e)
	}
	if leaseHeld(g, 1) {
		t.Fatal("the pass lease is still live after a skip that left no pane")
	}
	if reason := leaseReleaseReason(t, s, auditRows(t, s, r, "review-lease-released")); !strings.HasPrefix(reason, "publish brief:") {
		t.Fatalf("release reason=%q", reason)
	}
}

// (c) A panewire exit that places no pane (here 88, no usable account) releases.
func TestLeaseReleaseOnAuthUnavailable(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireAuthUnavailable}}
	s, g, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if leaseHeld(g, 1) {
		t.Fatal("the pass lease is still live after exit 88")
	}
	if reason := leaseReleaseReason(t, s, auditRows(t, s, r, "review-lease-released")); reason != "spawn-auth-unavailable" {
		t.Fatalf("release reason=%q", reason)
	}
}

// Every documented no-pane exit is covered, and the two that may follow a
// pane (76, 82) and the wait exits are not.
func TestSpawnNeverPlacedTable(t *testing.T) {
	for _, code := range []int{70, 74, 75, 77, 78, 79, 80, 81, 86, 87, 88, 89, 124} {
		if reason, ok := spawnNeverPlaced(code); !ok || reason == "" {
			t.Errorf("exit %d: reason=%q never=%v", code, reason, ok)
		}
	}
	for _, code := range []int{1, 76, 82, 83, 84, 85} {
		if _, ok := spawnNeverPlaced(code); ok {
			t.Errorf("exit %d may follow a pane but releases", code)
		}
	}
}

// (d), (e) A pane may exist after exit 76 (brief submission unconfirmed) or
// 82 (pane committed, lane unregistered): the lease stays.
func TestLeaseKeptOnPaneMayExistExits(t *testing.T) {
	for name, code := range map[string]int{"76 brief unconfirmed": panewireBriefUnconfirmed, "82 lane unregistered": panewireLaneUnregistered} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai", exits: []int{code}}
			s, g, r := panewireSetup(t, runner)
			if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
				t.Fatalf("err=%v", err)
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("spawns=%d", len(runner.spawns))
			}
			if !leaseHeld(g, 1) {
				t.Fatalf("the lease of a spawn that may have placed a pane was released (exit %d)", code)
			}
			if rows := auditRows(t, s, r, "review-lease-released"); len(rows) != 0 {
				t.Fatalf("release audit=%v", rows)
			}
		})
	}
}

// (f) A resumed pass (its record exists) that skips before the spawn keeps the
// lease it took again.
func TestLeaseKeptOnResumedPrespawnSkip(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
	s, g, r := panewireSetup(t, runner)
	s.Config.Notifier.Lane = "desk"
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	readReviewerJob(t, s, r, 1)
	// The lease expires, so the next run acquires it anew over the record.
	fakeGHMu.Lock()
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Unix(0, 0)
	fakeGHMu.Unlock()
	claims := g.claimCount
	t.Setenv(hubTokenEnv, "")
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("second run: %v", err)
	}
	if g.claimCount != claims+1 {
		t.Fatalf("claims=%d, want %d: the resumed pass did not acquire the lease", g.claimCount, claims+1)
	}
	if !leaseHeld(g, 1) {
		t.Fatal("a resumed pass released the lease on a pre-spawn skip")
	}
	if rows := auditRows(t, s, r, "review-lease-released"); len(rows) != 0 {
		t.Fatalf("release audit=%v", rows)
	}
}

// (g) A lease another host renewed while the pass prepared its spawn is not
// released: the compare-and-swap refuses.
func TestLeaseReleaseOnPrespawnSkipSkipsRenewedLease(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, runner)
	runner.gitFails = "fetch"
	s.Runner = &renewOnGit{runner, func() {
		fakeGHMu.Lock()
		defer fakeGHMu.Unlock()
		g.leaseIDs = map[string]string{claimRef(7, shaA, 1): shaB}
	}}
	if err := s.Review(context.Background(), r, 7, "", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if !leaseHeld(g, 1) {
		t.Fatal("a lease renewed by another host was released")
	}
	if rows := auditRows(t, s, r, "review-lease-released"); len(rows) != 0 {
		t.Fatalf("release audit=%v", rows)
	}
}

// renewOnGit runs before each git call, to stand in for another host.
type renewOnGit struct {
	*panewireRunner
	before func()
}

func (h *renewOnGit) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" && h.before != nil {
		h.before()
	}
	return h.panewireRunner.Run(ctx, command, args, input, dir)
}
