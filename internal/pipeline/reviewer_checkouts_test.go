package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A composer kept the checkout on pass one. Once the pane is gone and the
// reviewer job ended as lost, a later pass removes it through the hub.
func TestReviewerCheckoutKeptComposerRemovedOnceJobEnded(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	f.hub.composer = map[string]any{"outcome": "read", "composer": "has_text"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := f.hub.ops("worktree.remove"); len(got) != 0 {
		t.Fatalf("pass one removed the checkout: %v", got)
	}

	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	f.sink.recs[label] = integrations.JobRecord{State: "lost", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5", Cwd: "/repo/" + label}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != "/repo/"+label {
		t.Fatalf("removes: %v; rows: %v", removes, rowSummary(f.rows(t)))
	}
}

// A pending checkout with no pane and a reviewer job still running is held
// until its label is a day old; then it goes through the local removal.
func TestReviewerCheckoutPendingPastADayRemoved(t *testing.T) {
	for _, test := range []struct {
		name    string
		age     time.Duration
		removed bool
	}{
		{"under a day", time.Hour, false},
		{"past a day", 25 * time.Hour, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
			label := reviewerLabelFor(12, test.age)
			path := localCheckout(t, f, label, false)
			f.sink.recs[label] = integrations.JobRecord{State: "running", Role: "reviewer", Label: label}
			seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Worktree: path, Result: "pending-cleanup", Reason: "spawn ended before a pane result; the checkout may still run a pane"})
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			removed := f.run.called(key("git", "-C", "/repo", "worktree", "remove", path))
			if removed != test.removed {
				t.Fatalf("removed=%v calls %q; rows: %v", removed, f.run.calls, rowSummary(f.rows(t)))
			}
		})
	}
}

// A live pane keeps its checkout on every pass, however old the label is:
// the pane is never closed and the checkout never removed while it runs.
func TestReviewerCheckoutLivePaneKeptEveryPass(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, 25*time.Hour)
	f.addReviewer(label, "w1:p5")
	f.sink.recs[label] = integrations.JobRecord{State: "running", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5", Cwd: "/repo/" + label}
	for pass := 1; pass <= 3; pass++ {
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(f.hub.ops("pane.close")) + len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("live pane acted on: %v", f.hub.commands)
	}
	rows := f.rows(t)
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	for _, v := range rows {
		if v.Result != "kept" || v.Reason != "job:running" {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
	}
}

// The record lives on disk: a new process over the same state dir still
// revisits the checkout and removes it.
func TestReviewerCheckoutRecordSurvivesRestart(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
	label := reviewerLabelFor(12, 25*time.Hour)
	path := localCheckout(t, f, label, false)
	job := integrations.JobRecord{State: "running", Role: "reviewer", Label: label}
	f.sink.recs[label] = job
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Worktree: path, Result: "pending-cleanup", Reason: "spawn ended before a pane result; the checkout may still run a pane"})

	g := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
	g.s.Config.StateDir = f.s.Config.StateDir
	g.s.Config.Review.Panewire.Checkouts = f.s.Config.Review.Panewire.Checkouts
	g.sink.recs[label] = job
	for k, v := range f.run.outs {
		g.run.outs[k] = v
	}
	if err := g.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !g.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) {
		t.Fatalf("restarted pass did not remove; calls %q", g.run.calls)
	}
}

// Each UTC day the pass writes one summary per host: checkouts removed and
// kept (with reasons) on the day before. A second pass that day writes none.
func TestReviewerCheckoutDailySummaryPerHost(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
	f.run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	f.run.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	yesterday := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: yesterday, Action: "reviewer-reap", Repo: testRepo.Name, PR: 11, Label: reviewerLabelFor(11, 30*time.Hour), Host: "local", Result: "removed"},
		reapReceipt{Time: yesterday, Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Label: reviewerLabelFor(12, 30*time.Hour), Host: "local", Result: "kept", Reason: "job:running"},
	)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	summaries := dailySummaries(t, f)
	if len(summaries) != 2 {
		t.Fatalf("summaries (one per host, none on the second pass): %v", summaries)
	}
	byHost := map[string]map[string]any{}
	for _, row := range summaries {
		byHost[row["host"].(string)] = row
	}
	got := byHost["local"]
	if got == nil || got["day"] != "2026-11-03" || got["removed"] != float64(1) || got["kept"] != float64(1) {
		t.Fatalf("local summary: %v", got)
	}
	if reasons, _ := got["kept_reasons"].(map[string]any); reasons["job:running"] != float64(1) {
		t.Fatalf("local summary reasons: %v", got)
	}
	if node := byHost["node"]; node == nil || node["removed"] != float64(0) || node["kept"] != float64(0) {
		t.Fatalf("node summary: %v", node)
	}
}

// dailySummaries are the reviewer-checkouts-daily records in decisions.jsonl.
func dailySummaries(t *testing.T, f *reviewerFix) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.s.state(testRepo.Name), "decisions.jsonl"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var rec struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Command == "reviewer-checkouts-daily" {
			out = append(out, rec.Value)
		}
	}
	return out
}
