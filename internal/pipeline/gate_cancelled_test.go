package pipeline

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

var cancelNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

// A required check's cancellation holds the verdict: it waits for a rerun,
// then blocks. A skipped or neutral run holds nothing.
func TestGateCancelledRequiredCheck(t *testing.T) {
	for _, tt := range []struct {
		name          string
		vet, test     github.CI
		age           time.Duration
		done          bool
		state, reason string
		cancelled     []string
		skipped       []string
		summary       string
	}{
		{"cancelled 5 min ago waits", github.Green, github.Cancelled, 5 * time.Minute, true, "pending", "check_cancelled:test", []string{"test"}, nil, ""},
		{"cancelled 40 min ago blocks", github.Green, github.Cancelled, 40 * time.Minute, true, "blocked", "check_cancelled:test", []string{"test"}, nil, "check_cancelled:test (rerun it)"},
		{"cancelled at the bound blocks", github.Green, github.Cancelled, cancelledCheckWait, true, "blocked", "check_cancelled:test", []string{"test"}, nil, "check_cancelled:test (rerun it)"},
		{"skipped is ready", github.Green, github.Skipped, 0, true, "ready", "", nil, []string{"test"}, "1 required check(s) skipped: test"},
		{"queued", github.Green, github.Pending, 0, true, "pending", "", nil, nil, ""},
		{"red sibling blocks, cancelled waits", github.Red, github.Cancelled, 5 * time.Minute, true, "blocked", "check_failed:vet;check_cancelled:test", []string{"test"}, nil, ""},
		{"expired cancelled without review", github.Cancelled, github.Cancelled, 40 * time.Minute, false, "blocked", "check_cancelled:vet;check_cancelled:test;review_missing", []string{"vet", "test"}, nil, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			s.Now = func() time.Time { return cancelNow }
			g.required = []string{"vet", "test"}
			g.checkNames = map[string]map[string]github.CI{shaA: {"vet": tt.vet, "test": tt.test}}
			g.cancelledAt = map[string]time.Time{"vet": cancelNow.Add(-tt.age), "test": cancelNow.Add(-tt.age)}
			if !tt.done {
				g.refs = map[string]bool{}
			}
			v := gateAt(t, s, g, shaB)
			if v.State != tt.state || reasons(v) != tt.reason || !slices.Equal(v.Review.ChecksCancelled, tt.cancelled) || !slices.Equal(v.Review.ChecksSkipped, tt.skipped) {
				t.Fatalf("verdict=%+v", v)
			}
			if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if tt.name == "skipped is ready" {
				var output bytes.Buffer
				s.Out = &output
				if err := s.Land(context.Background(), r, 7, Options{DryRun: true}); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(output.String(), `"checks_skipped":["test"]`) {
					t.Fatalf("land record=%s", output.String())
				}
			}
			if tt.summary != "" && !strings.Contains(lastEvent(t, n), tt.summary) {
				t.Fatalf("notice=%s", lastEvent(t, n))
			}
			if tt.state == "pending" && n.count != 0 {
				t.Fatalf("pending notified: %+v", n.events)
			}
			failed, err := s.failedChecks(context.Background(), r.Name, shaA, g.required)
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(failed, "test") {
				t.Fatalf("cancelled or pending reported as failed: %v", failed)
			}
		})
	}
}

// A newer run on the head replaces the cancelled one: the fake models the
// client's latest-run-per-name result, here a green run after the cancellation.
func TestGateCancelledThenNewerGreenIsReady(t *testing.T) {
	s, g, _, _ := notifySetup(t)
	s.Now = func() time.Time { return cancelNow }
	g.required = []string{"vet", "test"}
	g.cancelledAt = map[string]time.Time{"test": cancelNow.Add(-40 * time.Minute)}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Green, "test": github.Cancelled}}
	if v := gateAt(t, s, g, shaB); v.State != "blocked" {
		t.Fatalf("before rerun: %+v", v)
	}
	g.checkNames[shaA]["test"] = github.Green
	if v := gateAt(t, s, g, shaB); v.State != "ready" || len(v.Reasons) != 0 || len(v.Review.ChecksCancelled) != 0 {
		t.Fatalf("after rerun: %+v", v)
	}
}

// Without a completion time the gate cannot age the cancellation, so it waits.
func TestGateCancelledWithoutTimeWaits(t *testing.T) {
	s, g, _, _ := notifySetup(t)
	s.Now = func() time.Time { return cancelNow }
	g.required = []string{"vet", "test"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Green, "test": github.Cancelled}}
	if v := gateAt(t, s, g, shaB); v.State != "pending" || reasons(v) != "check_cancelled:test" {
		t.Fatalf("verdict=%+v", v)
	}
}
