package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// stallAlerts records the text of every lane event the notifier is asked to
// send, and keeps the trace's notify rows as the recordingNotifier does.
type stallAlerts struct {
	trace *eventLog
	mu    sync.Mutex
	sent  []string
}

func (a *stallAlerts) Notify(ctx context.Context, e integrations.Event) error {
	a.mu.Lock()
	a.sent = append(a.sent, e.Text)
	a.mu.Unlock()
	return recordingNotifier{a.trace}.Notify(ctx, e)
}

func (a *stallAlerts) texts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.sent...)
}

// stallClock is the fixture's clock: a pass reads it, the test moves it.
type stallClock struct{ at time.Time }

func (c *stallClock) now() time.Time          { return c.at }
func (c *stallClock) advance(d time.Duration) { c.at = c.at.Add(d) }
func (c *stallClock) past(d time.Duration) time.Time {
	return c.at.Add(-d)
}

// stallFixture is one PR whose review never runs, with the clock the test
// advances and the lane events it records.
type stallFixture struct {
	f      *parallelFixture
	clock  *stallClock
	alerts *stallAlerts
	logs   *syncBuffer
	skips  *prSkips
}

func newStallFixture(t *testing.T) *stallFixture {
	t.Helper()
	f := newParallelFixture(t, 1, 1)
	// The clock starts at real now: a revisit deadline the pass pins while the
	// clock is behind would be overdue when the queue arms its real timer, and
	// the PR would be evaluated again at once.
	clock := &stallClock{at: time.Now().UTC().Truncate(time.Second)}
	f.s.Now = clock.now
	alerts := &stallAlerts{trace: f.trace}
	f.s.Notifier = alerts
	logs := &syncBuffer{}
	skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}
	return &stallFixture{f: f, clock: clock, alerts: alerts, logs: logs, skips: skips}
}

// pass runs one pr pass and fails unless it evaluated the PR and skipped none.
func (s *stallFixture) pass(t *testing.T) {
	t.Helper()
	skipped, evaluated := runSkippedPass(t, s.f, s.skips, s.logs)
	if skipped != 0 || evaluated < 1 {
		t.Fatalf("pass: skipped=%d evaluated=%d, want none skipped and the PR evaluated", skipped, evaluated)
	}
}

// waited records that this head has waited for its review since the given
// time, as the stall check's clock does. A test uses it where a second
// evaluation in the same wait would resume the reviewer and block on it.
func (s *stallFixture) waited(t *testing.T, since time.Time) {
	t.Helper()
	path := filepath.Join(s.f.s.Config.StateDir, s.f.repo.Name, "pr-review-wait.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	waits := map[string]map[string]string{fmt.Sprintf("1 %s", hubHead(1)): {"first": since.UTC().Format(time.RFC3339)}}
	b, err := json.Marshal(waits)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// The stall alert fires while the round hold keeps the review back, naming the
// hold: the path a held review takes returns before settle, so the notify-side
// stall check never ran for it.
func TestRoundHoldStallAlertsOnce(t *testing.T) {
	s := newStallFixture(t)
	holdRound(t, s)

	// Under reviewStallAfter the wait is young: no alert.
	s.pass(t)
	if got := s.alerts.texts(); len(got) != 0 {
		t.Fatalf("alerted before the stall window: %q", got)
	}

	s.clock.advance(61 * time.Minute)
	s.pass(t)
	got := s.alerts.texts()
	if len(got) != 1 {
		t.Fatalf("alerts=%q, want one held stall alert", got)
	}
	if !strings.Contains(got[0], "review stalled") || !strings.Contains(got[0], "held (round-hold)") {
		t.Fatalf("alert does not name the held wait: %q", got[0])
	}
	if !strings.Contains(got[0], "o/one#1") {
		t.Fatalf("alert does not name the PR: %q", got[0])
	}

	// Once per head: a second pass at the same head stays quiet.
	s.pass(t)
	if got := s.alerts.texts(); len(got) != 1 {
		t.Fatalf("a second pass alerted again: %q", got)
	}

	// Only the stall check ran: a held review delivers no findings and lands
	// nothing.
	for _, e := range s.f.trace.snapshot() {
		if e == "merge 1" {
			t.Fatalf("a held review landed: %v", s.f.trace.snapshot())
		}
	}
	for _, text := range s.alerts.texts() {
		if strings.Contains(text, "FINDINGS") {
			t.Fatalf("a held review delivered findings: %q", text)
		}
	}
}

// holdRound makes the PR's round hold active where the review pass reads it,
// with its comment baseline set at none.
func holdRound(t *testing.T, s *stallFixture) {
	t.Helper()
	path := filepath.Join(s.f.s.Config.StateDir, s.f.repo.Name, "round-rules", "1.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	at := s.clock.at.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	b := []byte(fmt.Sprintf(`{"hold_held":true,"hold_baseline_captured":true,"hold_comment_count":0,"hold_at":%q}`, at))
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A quota-stopped review is told the same way, naming the pool the reviewer
// stopped on.
func TestQuotaStoppedReviewStallNamesThePool(t *testing.T) {
	s := newStallFixture(t)
	// The review deadline outlasts the stall window: the placed reviewer is
	// found stopped on its pool when the wait crosses the threshold.
	s.f.s.Config.Review.Timeout = "3h"
	runner := &quotaRunner{}
	sink := &quotaSink{rec: integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}}
	s.f.s.Runner, s.f.s.Sink = runner, sink

	// A reviewer is placed and runs: pending, under the stall window.
	s.pass(t)
	if got := s.alerts.texts(); len(got) != 0 {
		t.Fatalf("alerted before the stall window: %q", got)
	}

	// Past the window the reviewer is found stopped on an exhausted pool.
	s.clock.advance(61 * time.Minute)
	sink.setRecord(integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=seat reset_at=" + s.clock.past(time.Minute).Format(time.RFC3339), Host: "node-a", PaneID: "p1"})
	// The give-up's replacement has no placement candidate left, so the pass
	// waits for the pool's reset and is reported as quota-stopped.
	runner.refuseSpawns(true)
	s.pass(t)
	got := s.alerts.texts()
	if len(got) != 1 {
		t.Fatalf("alerts=%q, want one quota-stopped stall alert", got)
	}
	if !strings.Contains(got[0], "quota-stopped (pool seat)") {
		t.Fatalf("alert does not name the quota stop and its pool: %q", got[0])
	}
}

// A pending review — a reviewer that runs on — is told the same way.
func TestPendingReviewStallAlerts(t *testing.T) {
	s := newStallFixture(t)
	// The review deadline outlasts the stall window: the placed reviewer is
	// still running when the wait is already past reviewStallAfter.
	s.f.s.Config.Review.Timeout = "3h"
	s.f.s.Runner = noMergeGitHistoryRunner{next: &quotaRunner{}}
	s.f.s.Sink = &quotaSink{rec: integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}}
	s.waited(t, s.clock.past(61*time.Minute))

	s.pass(t)
	got := s.alerts.texts()
	if len(got) != 1 || !strings.Contains(got[0], "pending") {
		t.Fatalf("alerts=%q, want one pending stall alert", got)
	}
}

// A skipped poll is a wait too: nothing reviewed the head, and it is told once
// the wait outlasts reviewStallAfter.
func TestSkippedPollStallAlerts(t *testing.T) {
	s := newStallFixture(t)
	// The hub reports two changed files and lists one, so the file list is
	// still settling: every pass is a skipped poll.
	s.f.hub.changedFiles = 2
	s.waited(t, s.clock.past(61*time.Minute))

	s.pass(t)
	got := s.alerts.texts()
	if len(got) != 1 || !strings.Contains(got[0], "skipped") {
		t.Fatalf("alerts=%q, want one skipped-poll stall alert", got)
	}

	s.pass(t)
	if got := s.alerts.texts(); len(got) != 1 {
		t.Fatalf("a second pass alerted again: %q", got)
	}
}
