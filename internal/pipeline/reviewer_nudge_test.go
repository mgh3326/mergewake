package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// nudgeRefused is a notifier whose nudge emit is refused; every other notice
// goes through.
type nudgeRefused struct{ integrations.Notifier }

func (n nudgeRefused) Notify(ctx context.Context, e integrations.Event) error {
	if strings.HasSuffix(e.ID, "-nudge") {
		return errors.New("emit refused")
	}
	return n.Notifier.Notify(ctx, e)
}

// nudgeEvents returns the nudge emits the fake notifier saw, by event id.
func nudgeEvents(n *fakeNotifier) []integrations.Event {
	var out []integrations.Event
	for _, e := range n.events {
		if strings.HasSuffix(e.ID, "-nudge") {
			out = append(out, e)
		}
	}
	return out
}

// nudgeSetup is an interrupted reviewer whose pane sits at status, with a
// review budget that leaves room for a nudge and a lease that has run down.
func nudgeSetup(t *testing.T, status string) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock, reviewerJob, *fakeReapHub, *fakeNotifier) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Review.Timeout = "30m"
	old := readReviewerJob(t, s, r, 1)
	hub := budgetPane(t, s, runner, old, status)
	n := s.Notifier.(*fakeNotifier)
	c.t = c.t.Add(time.Second)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	return s, g, r, runner, c, old, hub, n
}

// resumeAfter moves the clock by d and expires the lease, as the next run
// of a stuck pass sees it.
func resumeAfter(g *fakeGH, c *clock, d time.Duration) {
	c.t = c.t.Add(d)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
}

// reviewWaits runs one review and asserts that the pass waits on its reviewer:
// the run reports review-pending and returns no error.
func reviewWaits(t *testing.T, s *Service, r config.Repo) {
	t.Helper()
	var out bytes.Buffer
	s.Out = &out
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("review: %v", err)
	}
	if !strings.Contains(out.String(), `"action":"review-pending"`) {
		t.Fatalf("the pass did not wait on its reviewer: %s", out.String())
	}
}

func jobDirOf(s *Service, label string) string {
	return filepath.Join(s.Config.Review.Panewire.Jobs, label)
}

// (a) A finished pane with no result and enough budget is nudged once: one
// emit to the reviewer's lane names both output paths, the nudge time is
// saved, and the pass waits without a close or a second spawn.
func TestReviewerNudgedOnceWhenPaneFinishedWithoutResult(t *testing.T) {
	s, _, r, runner, _, old, _, n := nudgeSetup(t, "done")
	reviewWaits(t, s, r)
	events := nudgeEvents(n)
	if len(events) != 1 || events[0].Lane != old.Label || events[0].ID != old.Label+"-nudge" {
		t.Fatalf("nudge events=%+v", events)
	}
	for _, want := range []string{filepath.Join(jobDirOf(s, old.Label), "findings.json"), filepath.Join(jobDirOf(s, old.Label), "report.md"), "panewire done " + old.Label} {
		if !strings.Contains(events[0].Text, want) {
			t.Fatalf("nudge text %q lacks %q", events[0].Text, want)
		}
	}
	if got := readReviewerJob(t, s, r, 1); got.NudgedAt == nil {
		t.Fatal("record lacks nudged_at")
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 1 {
		t.Fatalf("closes=%v spawns=%d", runner.closes, len(runner.spawns))
	}
	rows := auditRows(t, s, r, "reviewer-nudge")
	if len(rows) != 1 || rows[0]["status"] != "sent" || rows[0]["label"] != old.Label {
		t.Fatalf("reviewer-nudge rows=%v", rows)
	}
}

// (b) The next resume finds the output the nudged reviewer wrote on its host:
// it is collected with no replacement, and the outcome is result_found.
func TestReviewerNudgedResultFoundIsCollected(t *testing.T) {
	s, g, r, runner, c, _, hub, _ := nudgeSetup(t, "done")
	reviewWaits(t, s, r)
	hub.mu.Lock()
	hub.jobResult = json.RawMessage(`{"found":true,"findings":{"findings":[]}}`)
	hub.mu.Unlock()
	resumeAfter(g, c, 30*time.Second)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	// The collected job is closed as completed, never abandoned, and no replacement spawns.
	if len(runner.spawns) != 1 || len(runner.closes) != 1 || arg(runner.closes[0], "--outcome") != "completed" || g.postCount != 1 {
		t.Fatalf("spawns=%d closes=%v posts=%d", len(runner.spawns), runner.closes, g.postCount)
	}
	if rows := auditRows(t, s, r, "reviewer-nudge-outcome"); len(rows) != 1 || rows[0]["outcome"] != "result_found" {
		t.Fatalf("outcome rows=%v", rows)
	}
	if len(auditRows(t, s, r, "review-result-lost")) != 0 {
		t.Fatal("a collected result was reported lost")
	}
	reviewerJobGone(t, s, r, 1)
}

// (b') A nudged reviewer that writes only its local findings file and runs no
// `panewire done --result` has produced nothing mergewake reads: the pass
// keeps waiting until nudgeWait is over, and is then lost and replaced.
func TestReviewerNudgedLocalFileOnlyIsLost(t *testing.T) {
	s, g, r, runner, c, old, _, _ := nudgeSetup(t, "done")
	reviewWaits(t, s, r)
	if err := os.WriteFile(filepath.Join(jobDirOf(s, old.Label), "findings.json"), []byte(legacyBody), 0600); err != nil {
		t.Fatal(err)
	}
	resumeAfter(g, c, 30*time.Second)
	reviewWaits(t, s, r)
	if len(runner.spawns) != 1 || len(runner.closes) != 0 || g.postCount != 0 {
		t.Fatalf("spawns=%d closes=%v posts=%d", len(runner.spawns), runner.closes, g.postCount)
	}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	resumeAfter(g, c, nudgeWait)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	noLegacyRead(t, s, r, runner, g)
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatalf("spawns=%v lost=%v", runner.spawns, auditRows(t, s, r, "review-result-lost"))
	}
	if rows := auditRows(t, s, r, "reviewer-nudge-outcome"); len(rows) != 1 || rows[0]["outcome"] != "lost" {
		t.Fatalf("outcome rows=%v", rows)
	}
}

// (c) The reviewer still idle once nudgeWait has passed is lost exactly as
// before the nudge: one close, a replacement, the lost rows, and the outcome
// lost.
func TestReviewerNudgedNoResultAfterWaitIsLost(t *testing.T) {
	s, g, r, runner, c, old, _, n := nudgeSetup(t, "done")
	reviewWaits(t, s, r)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	resumeAfter(g, c, nudgeWait+time.Second)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
		t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
	}
	if len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatal("missing lost audit")
	}
	if rows := auditRows(t, s, r, "reviewer-nudge-outcome"); len(rows) != 1 || rows[0]["outcome"] != "lost" {
		t.Fatalf("outcome rows=%v", rows)
	}
	if len(nudgeEvents(n)) != 1 {
		t.Fatalf("nudge events=%d, want one", len(nudgeEvents(n)))
	}
}

// (d) A resume before nudgeWait has passed keeps waiting: pending, and no
// second nudge for the same record.
func TestReviewerNudgedWaitsWithoutSecondNudge(t *testing.T) {
	s, g, r, runner, c, _, _, n := nudgeSetup(t, "done")
	reviewWaits(t, s, r)
	resumeAfter(g, c, time.Minute)
	reviewWaits(t, s, r)
	if len(nudgeEvents(n)) != 1 || len(runner.closes) != 0 || len(runner.spawns) != 1 {
		t.Fatalf("nudge events=%d closes=%v spawns=%d", len(nudgeEvents(n)), runner.closes, len(runner.spawns))
	}
	if rows := auditRows(t, s, r, "reviewer-nudge-outcome"); len(rows) != 0 {
		t.Fatalf("an undecided nudge has outcome rows: %v", rows)
	}
}

// (e) A review budget too short for nudgeWait sends no nudge: the pass is
// lost as before, with a skipped row.
func TestReviewerNudgeSkippedWhenBudgetShort(t *testing.T) {
	s, g, r, runner, c, old, _, n := nudgeSetup(t, "done")
	s.Config.Review.Timeout = "5m"
	c.t = old.Recorded.Add(3*time.Minute + 30*time.Second)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(nudgeEvents(n)) != 0 {
		t.Fatal("nudge sent with 1m30s of budget left, under nudgeWait")
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	if rows := auditRows(t, s, r, "reviewer-nudge"); len(rows) != 1 || rows[0]["status"] != "skipped" || rows[0]["reason"] != "budget" {
		t.Fatalf("reviewer-nudge rows=%v", rows)
	}
}

// (f) An emit refused for any reason but a duplicate event id sends no
// nudge: the pass is lost as before, with a skipped row naming the refusal.
func TestReviewerNudgeEmitRefusedIsLost(t *testing.T) {
	s, g, r, runner, _, old, _, n := nudgeSetup(t, "done")
	s.Notifier = nudgeRefused{n}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || g.postCount != 1 || len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	rows := auditRows(t, s, r, "reviewer-nudge")
	if len(rows) != 1 || rows[0]["status"] != "skipped" || !strings.Contains(rows[0]["reason"].(string), "emit refused") {
		t.Fatalf("reviewer-nudge rows=%v", rows)
	}
	if len(auditRows(t, s, r, "reviewer-nudge-outcome")) != 0 {
		t.Fatal("a refused nudge has an outcome")
	}
}

// (g) An overdue running pane and a pane that has disappeared are never
// nudged: both are lost as before.
func TestReviewerNotNudgedWhenOverdueOrGone(t *testing.T) {
	t.Run("overdue running pane", func(t *testing.T) {
		s, g, r, runner, c, _, _, n := nudgeSetup(t, "working")
		c.t = c.t.Add(31 * time.Minute)
		g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
		runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
		if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
			t.Fatal(err)
		}
		if len(nudgeEvents(n)) != 0 || len(auditRows(t, s, r, "reviewer-nudge")) != 0 {
			t.Fatalf("nudged an overdue pane: events=%d rows=%v", len(nudgeEvents(n)), auditRows(t, s, r, "reviewer-nudge"))
		}
		if len(runner.spawns) != 2 {
			t.Fatalf("spawns=%d", len(runner.spawns))
		}
	})
	t.Run("disappeared pane", func(t *testing.T) {
		s, _, r, runner, _, old, hub, n := nudgeSetup(t, "done")
		hub.agents["panes"] = []any{}
		hub.agents["agents"] = []any{}
		runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
		if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
			t.Fatal(err)
		}
		if len(nudgeEvents(n)) != 0 || len(auditRows(t, s, r, "reviewer-nudge")) != 0 {
			t.Fatalf("nudged a disappeared pane: events=%d", len(nudgeEvents(n)))
		}
		closedOnce(t, runner, old.Label)
		if len(runner.spawns) != 2 {
			t.Fatalf("spawns=%d", len(runner.spawns))
		}
	})
}

// (h) A record written before nudged_at existed loads as not nudged and is
// nudged once.
func TestReviewerRecordWithoutNudgedAtIsNudgedOnce(t *testing.T) {
	s, _, r, _, _, _, _, n := nudgeSetup(t, "done")
	raw, err := os.ReadFile(s.reviewerJobPath(r.Name, 7, 1))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "nudged_at") {
		t.Fatalf("a record never nudged carries nudged_at: %s", raw)
	}
	reviewWaits(t, s, r)
	if len(nudgeEvents(n)) != 1 {
		t.Fatalf("nudge events=%d, want one", len(nudgeEvents(n)))
	}
}

// (i) The nudge wait holds only a record that is still open. A nudged
// reviewer whose record turns completed without a result inside the wait is
// read on its host at once, and without the result there it is lost and
// replaced once, with no second nudge.
func TestReviewerNudgedRecordCompletedWithoutResultIsReplaced(t *testing.T) {
	s, g, r, runner, c, old, hub, n := nudgeSetup(t, "done")
	reviewWaits(t, s, r)
	before := len(hub.ops("job.result"))
	hub.mu.Lock()
	hub.jobResult = json.RawMessage(`{"found":false}`)
	hub.mu.Unlock()
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	resumeAfter(g, c, 30*time.Second)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if calls := hub.ops("job.result")[before:]; len(calls) != 1 || calls[0]["job"] != old.Label {
		t.Fatalf("host reads=%v, want one of %s", calls, old.Label)
	}
	rows := auditRows(t, s, r, "review-result-lost")
	if len(rows) != 1 || rows[0]["label"] != old.Label || !strings.Contains(rows[0]["collect_error"].(string), "not_found") {
		t.Fatalf("lost rows=%v", rows)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
		t.Fatalf("spawns=%v posts=%d, want one replacement that posts", runner.spawns, g.postCount)
	}
	if len(nudgeEvents(n)) != 1 {
		t.Fatalf("nudge events=%d, want the first one only", len(nudgeEvents(n)))
	}
	if rows := auditRows(t, s, r, "reviewer-nudge-outcome"); len(rows) != 1 || rows[0]["outcome"] != "lost" {
		t.Fatalf("outcome rows=%v", rows)
	}
}
