package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// replacedHub stands a fake hub in front of a pane with the given id, named
// for label and in the given agent status, and points the panewire transport
// at it.
func replacedHub(t *testing.T, s *Service, label, paneID, status string) *fakeReapHub {
	t.Helper()
	hub := newFakeReapHub()
	pane := map[string]any{"pane_id": paneID, "name": label, "agent_status": status, "cwd": "/repo/" + label}
	hub.agents["panes"] = []any{pane}
	hub.agents["agents"] = []any{pane}
	hub.detached = map[string]bool{"/repo/" + label: true}
	hub.close = map[string]any{"outcome": "closed"}
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	return hub
}

// laterSweep is a later review run of the PR an hour on. The run's retry is
// what finishes the closes a replaced reviewer still owes. The run's own
// result is not asserted: the tests check the closes it made.
func laterSweep(t *testing.T, s *Service, r config.Repo, c *clock) {
	t.Helper()
	c.t = c.t.Add(time.Hour)
	_ = s.Review(context.Background(), r, 7, "anthropic", Options{})
}

// (a) A replaced pass's pane is force-closed in the same step as its job is
// closed, though the pane's first close answers pane-changed.
func TestReplacedPaneClosedInSameStep(t *testing.T) {
	for _, tt := range []struct{ name, status string }{
		{"finished without a result", "done"},
		{"running past the budget", "working"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, r, runner, c := interruptedReviewer(t)
			old := readReviewerJob(t, s, r, 1)
			hub := budgetPane(t, s, runner, old, tt.status)
			hub.dropClosed = true
			hub.close = map[string]any{"outcome": "closed"}
			hub.closeOutcomes = []string{"pane-changed"}
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			pastBudget(g, c)
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			closedOnce(t, runner, old.Label)
			if n := len(hub.agents["panes"].([]any)); n != 0 {
				t.Fatalf("replaced pane still listed (%d panes), closes=%v", n, hub.ops("pane.close"))
			}
		})
	}
}

// (a) A moved head's pane is force-closed with its job, not only the job.
func TestReplacedPaneClosedOnMovedHead(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	old := integrations.JobRecord{State: "claimed", OwnerLane: "owner", Host: "node-a", PaneID: "p1"}
	s, _, r, labels := movedHeadSetup(t, runner, old)
	hub := replacedHub(t, s, labels[0], "p1", "idle")
	hub.dropClosed = true
	hub.closeOutcomes = []string{"pane-changed"}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	closes := hub.ops("pane.close")
	if len(closes) == 0 || closes[0]["pane_id"] != "p1" || closes[0]["name"] != labels[0] {
		t.Fatalf("pane closes=%v, want a close of %s's pane p1", closes, labels[0])
	}
	if n := len(hub.agents["panes"].([]any)); n != 0 {
		t.Fatalf("moved head's pane still listed (%d panes), closes=%v", n, closes)
	}
	if len(runner.closes) != 2 {
		t.Fatalf("job closes=%v, want both moved-head jobs closed", runner.closes)
	}
}

// (b) A failed pane close is retried by the next reviewer sweep, which stops
// once the pane is gone.
func TestReplacedPaneCloseRetriedBySweep(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := budgetPane(t, s, runner, old, "done")
	hub.close = map[string]any{"outcome": "closed"}
	hub.dropClosed = true
	hub.closeFails = 1
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.agents["panes"].([]any)); n != 1 {
		t.Fatalf("pane closed despite a failed close, panes=%d", n)
	}
	laterSweep(t, s, r, c)
	if n := len(hub.agents["panes"].([]any)); n != 0 {
		t.Fatalf("later sweep left the replaced pane, closes=%v", hub.ops("pane.close"))
	}
}

// (b) A failed job close is retried by the next sweep even though the pane
// closed in the same step.
func TestReplacedJobCloseRetriedBySweep(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := budgetPane(t, s, runner, old, "done")
	hub.close = map[string]any{"outcome": "closed"}
	hub.dropClosed = true
	runner.closeErrs = []error{fmt.Errorf("hk unavailable")}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	laterSweep(t, s, r, c)
	jobCloses := 0
	for _, cl := range runner.closes {
		if cl[2] == old.Label {
			jobCloses++
		}
	}
	if jobCloses != 2 {
		t.Fatalf("job closes of %s = %d, want the failed one retried, closes=%v", old.Label, jobCloses, runner.closes)
	}
}

// (b) A replaced pane that is already gone when the sweep retries is not
// closed again, and the retry stops.
func TestReplacedPaneGoneStopsRetry(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := budgetPane(t, s, runner, old, "done")
	hub.close = map[string]any{"outcome": "closed"}
	hub.closeFails = 1
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	hub.agents["panes"] = []any{}
	hub.agents["agents"] = []any{}
	laterSweep(t, s, r, c)
	before := len(hub.ops("pane.close"))
	rows := len(auditRows(t, s, r, "reviewer-replaced-close"))
	laterSweep(t, s, r, c)
	if got := len(hub.ops("pane.close")); got != before {
		t.Fatalf("pane closed again after it was gone: %d -> %d", before, got)
	}
	if got := len(auditRows(t, s, r, "reviewer-replaced-close")); got != rows {
		t.Fatalf("retry continued after the pane was gone: %d -> %d audit rows", rows, got)
	}
}

// (c) A live reviewer's pane keeps the recheck: a pane-changed answer keeps
// it, with no replacement retry.
func TestLivePaneKeepsRecheck(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	f.hub.close = map[string]any{"outcome": "pane-changed"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(f.hub.ops("pane.close")) != 1 || len(rows) != 1 || rows[0].Reason != "pane close recheck: pane-changed" {
		t.Fatalf("closes=%v rows=%v", f.hub.ops("pane.close"), rowSummary(rows))
	}
	if n := len(auditRows(t, f.s, testRepo, "reviewer-replaced-close")); n != 0 {
		t.Fatalf("live pane got %d replaced-close rows", n)
	}
}

// (d) A replaced pass's pane is closed only by the id its own job record
// names: a pane of the same label under another id is left alone.
func TestReplacedPaneNotFromOwnRecordIsNotClosed(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := replacedHub(t, s, old.Label, "p9", "idle")
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	laterSweep(t, s, r, c)
	for _, cl := range hub.ops("pane.close") {
		if cl["pane_id"] == "p9" {
			t.Fatalf("pane p9 closed though the job record names p1: %v", hub.ops("pane.close"))
		}
	}
}

// (d) A replaced job with no host or pane in its record closes no pane.
func TestReplacedJobWithoutPaneClosesNoPane(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := replacedHub(t, s, old.Label, "p9", "idle")
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "claimed", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	laterSweep(t, s, r, c)
	if n := len(hub.ops("pane.close")); n != 0 {
		t.Fatalf("pane closed without a record pane id: %v", hub.ops("pane.close"))
	}
}

// closeRefusal is what panewire's job close prints with exit 5 when it refuses
// the close: the refusal is on stderr, the output of a failed close is empty.
func closeRefusal(stderr string) error {
	return &integrations.CommandError{Command: "panewire", Err: panewireExit(5), Stderr: stderr + "\n"}
}

// replacedMarker reads the owed-close marker of a replaced reviewer of PR 7,
// and reports whether one remains.
func replacedMarker(t *testing.T, s *Service, r config.Repo, label string) (replacedClose, bool) {
	t.Helper()
	raw, err := os.ReadFile(s.replacedClosePath(r.Name, 7, label))
	if os.IsNotExist(err) {
		return replacedClose{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var m replacedClose
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m, true
}

// jobClosesOf counts the job closes the runner made for one label.
func jobClosesOf(runner *panewireRunner, label string) int {
	n := 0
	for _, cl := range runner.closes {
		if cl[2] == label {
			n++
		}
	}
	return n
}

// (a) A job close that panewire refuses because the job already has a terminal
// record has ended the job: it is not owed again, and the marker goes once the
// pane is gone.
func TestAlreadyClosedJobRefusalIsClosed(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := budgetPane(t, s, runner, old, "done")
	hub.close = map[string]any{"outcome": "closed"}
	hub.dropClosed = true
	runner.closeErrs = []error{
		fmt.Errorf("hk unavailable"),
		closeRefusal("close refused: job " + old.Label + " already has a terminal record (00002-job.revoked.json) after its latest claim"),
	}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if m, owed := replacedMarker(t, s, r, old.Label); !owed || !m.JobOpen {
		t.Fatalf("failed job close is not owed: marker=%+v owed=%v", m, owed)
	}
	laterSweep(t, s, r, c)
	if m, owed := replacedMarker(t, s, r, old.Label); owed {
		t.Fatalf("refused close still owed: marker=%+v", m)
	}
	before := jobClosesOf(runner, old.Label)
	laterSweep(t, s, r, c)
	if got := jobClosesOf(runner, old.Label); got != before {
		t.Fatalf("job of %s closed again after the refusal: %d -> %d", old.Label, before, got)
	}
}

// (b) Refusals that are not "already has a terminal record" leave the job
// owed: the sweep retries the close.
func TestOtherCloseRefusalsStayOwed(t *testing.T) {
	for _, code := range []string{"owner_mismatch", "job_unknown", "lane_not_routed_to_caller", "scope_forbidden"} {
		t.Run(code, func(t *testing.T) {
			s, g, r, runner, c := interruptedReviewer(t)
			old := readReviewerJob(t, s, r, 1)
			hub := budgetPane(t, s, runner, old, "done")
			hub.close = map[string]any{"outcome": "closed"}
			hub.dropClosed = true
			refused := "close refused: job " + old.Label + " " + code
			runner.closeErrs = []error{closeRefusal(refused), closeRefusal(refused)}
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			pastBudget(g, c)
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			laterSweep(t, s, r, c)
			m, owed := replacedMarker(t, s, r, old.Label)
			if !owed || !m.JobOpen {
				t.Fatalf("refusal %s marked the job closed: marker=%+v owed=%v", code, m, owed)
			}
			if n := jobClosesOf(runner, old.Label); n != 2 {
				t.Fatalf("job closes of %s = %d, want both refused closes made, closes=%v", old.Label, n, runner.closes)
			}
		})
	}
}

// (c) A terminal job of an older head holds no reservation: no job close is
// sent, and its pane is still force-closed from the record's host and pane.
func TestTerminalOldHeadClosesPaneOnly(t *testing.T) {
	for _, state := range []string{"completed", "lost"} {
		t.Run(state, func(t *testing.T) {
			runner := &panewireRunner{family: "openai"}
			old := integrations.JobRecord{State: state, OwnerLane: "owner", Host: "node-a", PaneID: "p1"}
			s, _, r, labels := movedHeadSetup(t, runner, old)
			hub := replacedHub(t, s, labels[0], "p1", "idle")
			hub.dropClosed = true
			hub.closeOutcomes = []string{"pane-changed"}
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(runner.closes) != 0 {
				t.Fatalf("job closes=%v, want none for a %s job", runner.closes, state)
			}
			if n := len(hub.agents["panes"].([]any)); n != 0 {
				t.Fatalf("%s job's old pane still listed (%d), closes=%v", state, n, hub.ops("pane.close"))
			}
		})
	}
}

// (c) A failed pane close of a terminal old-head job is owed in the marker as
// the pane alone; the next sweep closes it.
func TestTerminalOldHeadPaneCloseOwedInMarker(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	old := integrations.JobRecord{State: "completed", OwnerLane: "owner", Host: "node-a", PaneID: "p1"}
	s, _, r, labels := movedHeadSetup(t, runner, old)
	hub := replacedHub(t, s, labels[0], "p1", "idle")
	hub.dropClosed = true
	hub.closeFails = 1
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	m, owed := replacedMarker(t, s, r, labels[0])
	if !owed || m.JobOpen || !m.PaneOpen {
		t.Fatalf("want only the pane owed: marker=%+v owed=%v", m, owed)
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.agents["panes"].([]any)); n != 0 {
		t.Fatalf("later sweep left the old head's pane, closes=%v", hub.ops("pane.close"))
	}
	if _, owed := replacedMarker(t, s, r, labels[0]); owed {
		t.Fatalf("marker still owed after its pane closed")
	}
}
