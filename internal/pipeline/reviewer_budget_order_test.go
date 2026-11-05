package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// budgetPane stands a fake hub in front of the reviewer's pane with the given
// agent status and records the job as claimed by an owner lane, as hk does.
func budgetPane(t *testing.T, s *Service, runner *panewireRunner, job reviewerJob, status string) {
	t.Helper()
	hub := newFakeReapHub()
	pane := map[string]any{"pane_id": "p1", "name": job.Label, "agent_status": status, "cwd": "/repo/" + job.Label}
	hub.agents["panes"] = []any{pane}
	hub.agents["agents"] = []any{pane}
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	runner.sink.jobs[job.Label] = integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
}

// pastBudget moves the clock beyond review.timeout from the recorded time and
// releases the pass lease, as the next run of a stuck pass sees it.
func pastBudget(g *fakeGH, c *clock) {
	c.t = c.t.Add(5 * time.Second)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
}

func closedOnce(t *testing.T, runner *panewireRunner, label string) {
	t.Helper()
	if len(runner.closes) != 1 || runner.closes[0][2] != label || arg(runner.closes[0], "--lane") != "owner" || arg(runner.closes[0], "--outcome") != "abandoned" {
		t.Fatalf("closes=%v, want one abandoned close of %s", runner.closes, label)
	}
}

// (a) A completed job is collected whatever the age of its record.
func TestReviewerBudgetCompletedJobIsCollected(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 || len(runner.closes) != 0 || g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d closes=%v posts=%d", len(runner.spawns), runner.closes, g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
}

// (b) A finished pane without a result is lost past the budget too: the job
// is closed and the pass replaced once.
func TestReviewerBudgetFinishedPaneIsReplaced(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	budgetPane(t, s, runner, old, "done")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 || len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
	}
}

// (c) A pane still working past the budget is lost the first time, not
// unavailable: its job is closed to release the hub claim and replaced once.
func TestReviewerBudgetRunningPaneIsReplaced(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	budgetPane(t, s, runner, old, "working")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 || len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
	}
}

// (d) The second consecutive loss is review-unavailable and leaves no record,
// so a following run does not fail with the same error.
func TestReviewerBudgetSecondLossDropsRecord(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	budgetPane(t, s, runner, old, "working")
	runner.exits = []int{panewireWaitTimeout}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("replacement run: %v", err)
	}
	replacement := readReviewerJob(t, s, r, 1)
	if replacement.Label == old.Label || replacement.Replacements != 1 {
		t.Fatalf("replacement=%+v", replacement)
	}
	budgetPane(t, s, runner, replacement, "working")
	pastBudget(g, c)
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || len(runner.closes) != 2 || len(runner.spawns) != 2 {
		t.Fatalf("err=%v closes=%v spawns=%d", err, runner.closes, len(runner.spawns))
	}
	reviewerJobGone(t, s, r, 1)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	runner.exits = []int{panewireWaitTimeout}
	if err = s.Review(context.Background(), r, 7, "", Options{}); errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("following run repeats the error: %v", err)
	}
}

// (e) A record inside the budget with its pane still running re-attaches with
// the bounded wait: same label, no close, record kept.
func TestReviewerBudgetYoungRunningPaneReattaches(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	budgetPane(t, s, runner, old, "working")
	runner.exits = []int{panewireWaitTimeout}
	c.t = c.t.Add(time.Second)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, errPassPending) && err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") != old.Label || len(runner.closes) != 0 {
		t.Fatalf("spawns=%v closes=%v", runner.spawns, runner.closes)
	}
	if got := readReviewerJob(t, s, r, 1); got.Label != old.Label {
		t.Fatalf("record %+v", got)
	}
}
