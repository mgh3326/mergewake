package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// lostPaneHub stands a fake hub in front of the lost reviewer's pane and gives
// pane.capture the screen outcome, so a forced close saves the screen and then
// closes the pane.
func lostPaneHub(t *testing.T, s *Service, runner *panewireRunner, job reviewerJob, status string) *fakeReapHub {
	t.Helper()
	hub := budgetPane(t, s, runner, job, status)
	hub.capture = map[string]any{"outcome": "screen", "saved": "/node/jobs/" + job.Label + "/pane-screen.txt"}
	return hub
}

// (a) A reviewer still working past the deadline is replaced: its screen is
// saved, its pane closes though it is working, and the replacement runs.
func TestLostOverdueReviewerScreenSavedAndPaneClosed(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "working")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	captures := hub.ops("pane.capture")
	if len(captures) != 1 || captures[0]["write"] != true || captures[0]["pane_id"] != "p1" {
		t.Fatalf("pane.capture = %v", captures)
	}
	if closes := hub.ops("pane.close"); len(closes) != 1 || closes[0]["pane_id"] != "p1" {
		t.Fatalf("pane.close = %v", closes)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns = %v", runner.spawns)
	}
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
}

// (b) A pane that no longer carries the pass's name and cwd is never closed;
// the skip is audited and the replacement still runs.
func TestLostOverdueReviewerMismatchedPaneIsKept(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "working")
	pane := map[string]any{"pane_id": "p1", "name": "mw-author", "agent_status": "working", "cwd": "/repo/" + old.Label}
	hub.agents["agents"] = []any{pane}
	hub.agents["panes"] = []any{pane}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if captures, closes := hub.ops("pane.capture"), hub.ops("pane.close"); len(captures)+len(closes) != 0 {
		t.Fatalf("a mismatched pane was touched: capture=%v close=%v", captures, closes)
	}
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 1 || !strings.Contains(rows[0]["reason"].(string), "name/cwd mismatch") {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}

// (c) A pane that finished without a result keeps today's behaviour: it is not
// captured or closed, and no pane skip is audited.
func TestLostFinishedPaneKeepsPaneHandling(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "done")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if captures, closes := hub.ops("pane.capture"), hub.ops("pane.close"); len(captures)+len(closes) != 0 {
		t.Fatalf("a finished pane was touched: capture=%v close=%v", captures, closes)
	}
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}

// (d) A forced close that the hub refuses is audited with its reason and never
// fails the pass: the replacement still runs.
func TestLostOverdueReviewerPaneCloseFailureIsAudited(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "working")
	hub.forbid = map[string]bool{"pane.close": true}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 1 || !strings.Contains(rows[0]["reason"].(string), "pane close") || rows[0]["pane"] != "p1" {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}

// (e) A forced close whose capture names a skip (the pane is no fleet job: its
// job dir is missing) is a refusal: the pane stays open, the skip and its
// reason are audited, and the replacement still runs.
func TestLostOverdueReviewerCaptureSkipKeepsPane(t *testing.T) {
	for _, skip := range []string{"not-a-job", "pane-gone", "pane-changed"} {
		t.Run(skip, func(t *testing.T) {
			s, g, r, runner, c := interruptedReviewer(t)
			old := readReviewerJob(t, s, r, 1)
			hub := lostPaneHub(t, s, runner, old, "working")
			hub.capture = map[string]any{"outcome": skip}
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			pastBudget(g, c)
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if captures := hub.ops("pane.capture"); len(captures) != 1 {
				t.Fatalf("pane.capture = %v", captures)
			}
			if closes := hub.ops("pane.close"); len(closes) != 0 {
				t.Fatalf("a pane the capture skipped was closed: %v", closes)
			}
			rows := auditRows(t, s, r, "reviewer-close-skipped")
			if len(rows) != 1 || !strings.Contains(rows[0]["reason"].(string), skip) || rows[0]["pane"] != "p1" {
				t.Fatalf("reviewer-close-skipped = %v", rows)
			}
			if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
				t.Fatalf("spawns = %v", runner.spawns)
			}
		})
	}
}
