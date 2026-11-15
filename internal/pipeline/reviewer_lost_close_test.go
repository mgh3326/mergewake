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

// (a) A reviewer still working past the deadline is replaced: its pane is left
// for panewire's node to close, never captured or closed here, and the
// replacement runs. The forced close that did not land is audited.
func TestLostOverdueReviewerPaneLeftForNode(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "working")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if captures, closes := hub.ops("pane.capture"), hub.ops("pane.close"); len(captures)+len(closes) != 0 {
		t.Fatalf("pane.capture = %v pane.close = %v", captures, closes)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns = %v", runner.spawns)
	}
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 1 {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
}

// (b) A pane that no longer carries the pass's name and cwd is still present
// by id, so the checkout stays; the skip is audited and the replacement runs.
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
	if len(rows) != 1 || !strings.Contains(rows[0]["reason"].(string), "pane still present") {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}

// (d) A refused close can no longer happen: nothing closes the pane, the
// pending checkout is audited and the replacement still runs.
func TestLostOverdueReviewerPaneCloseNeverRuns(t *testing.T) {
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
	if len(rows) != 1 || !strings.Contains(rows[0]["reason"].(string), "pane still present") || rows[0]["pane"] != "p1" {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}

// (e) Capture never runs anymore: a present pane keeps its checkout with a
// pending-cleanup row, no worktree is removed, and the replacement runs.
func TestLostOverdueReviewerCaptureNeverRuns(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "working")
	hub.detached = map[string]bool{"/repo/" + old.Label: true}
	hub.wtHead = shaA
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if captures, closes, removes := hub.ops("pane.capture"), hub.ops("pane.close"), hub.ops("worktree.remove"); len(captures)+len(closes)+len(removes) != 0 {
		t.Fatalf("capture=%v close=%v remove=%v", captures, closes, removes)
	}
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 1 || !strings.Contains(rows[0]["reason"].(string), "pane still present") || rows[0]["pane"] != "p1" {
		t.Fatalf("reviewer-close-skipped = %v", rows)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}

// (f) A pane panewire's node already closed counts as gone: no pane is left,
// so both the guarded remote worktree removal and the local mirror checkout
// removal run.
func TestLostOverdueReviewerGonePaneCountsAsClosed(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := lostPaneHub(t, s, runner, old, "working")
	// The job record still names the pane's cwd, but nothing lists the pane.
	rec := runner.sink.jobs[old.Label]
	rec.Cwd = "/repo/" + old.Label
	runner.sink.jobs[old.Label] = rec
	hub.agents["agents"] = []any{}
	hub.agents["panes"] = []any{}
	// The pass's detached checkout is still on the node at the reviewed head.
	hub.detached = map[string]bool{"/repo/" + old.Label: true}
	hub.wtHead = shaA
	// afterRun fires inside the replacement's spawn, so the mirror is seen as
	// removed before the replacement runs, not by the pass's own cleanup.
	mirror := false
	runner.afterRun = func() {
		for _, call := range gitCalls(runner, r.Root) {
			mirror = mirror || call == "worktree remove --force "+old.Checkout
		}
	}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != "/repo/"+old.Label || removes[0]["detached"] != true {
		t.Fatalf("worktree.remove = %v", removes)
	}
	if !mirror {
		t.Fatalf("the local mirror checkout was still there before the replacement: git %v", gitCalls(runner, r.Root))
	}
	var removed bool
	for _, row := range auditRows(t, s, r, "reviewer-cleanup") {
		removed = removed || row["result"] == "removed"
	}
	if !removed {
		t.Fatalf("reviewer-cleanup = %v", auditRows(t, s, r, "reviewer-cleanup"))
	}
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("a gone pane counted as a skipped close: %v", rows)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns = %v", runner.spawns)
	}
}
