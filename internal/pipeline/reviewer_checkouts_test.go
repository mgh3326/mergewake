package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
// Rows have the shapes the cleanup writers produce: reviewer-cleanup rows carry
// no label (the worktree base is the label), name their host by machine id, or
// name none. With no local host configured, the local fallback records "local".
func TestReviewerCheckoutDailySummaryPerHost(t *testing.T) {
	f := newReviewerFix(t)
	f.run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	f.run.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	yesterday := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: yesterday, Action: "reviewer-reap", Repo: testRepo.Name, PR: 11, Label: reviewerLabelFor(11, 30*time.Hour), Host: "local", Result: "removed"},
		reapReceipt{Time: yesterday, Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Worktree: "/repo/" + reviewerLabelFor(12, 30*time.Hour), Host: "node-a", Result: "kept", Reason: "job:running"},
		reapReceipt{Time: yesterday, Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 14, Worktree: "/repo/" + reviewerLabelFor(14, 30*time.Hour), Host: "node-a", Result: "removed"},
		reapReceipt{Time: yesterday, Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 13, Worktree: "/repo/" + reviewerLabelFor(13, 30*time.Hour), Result: "pending-cleanup", Reason: "spawn ended before a pane result"},
	)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	summaries := dailySummaries(t, f)
	if len(summaries) != 3 {
		t.Fatalf("summaries (one per host, none on the second pass): %v", summaries)
	}
	byHost := map[string]map[string]any{}
	for _, row := range summaries {
		byHost[row["host"].(string)] = row
	}
	local := byHost["local"]
	if local == nil || local["day"] != "2026-11-03" || local["removed"] != float64(1) || local["kept"] != float64(0) {
		t.Fatalf("local summary: %v", local)
	}
	node := byHost["node"]
	if node == nil || node["removed"] != float64(1) || node["kept"] != float64(1) {
		t.Fatalf("node summary: %v", node)
	}
	if reasons, _ := node["kept_reasons"].(map[string]any); reasons["job:running"] != float64(1) {
		t.Fatalf("node summary reasons: %v", node)
	}
	unattributed := byHost["unattributed"]
	if unattributed == nil || unattributed["kept"] != float64(1) {
		t.Fatalf("unattributed summary: %v", unattributed)
	}
	if reasons, _ := unattributed["kept_reasons"].(map[string]any); reasons["spawn ended before a pane result"] != float64(1) {
		t.Fatalf("unattributed summary reasons: %v", unattributed)
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

// Concurrent passes on one repo write each day's summary once: the state is
// read and advanced under the repo lock.
func TestReviewerCheckoutDailySummaryConcurrentPassesWriteOnce(t *testing.T) {
	f := newReviewerFix(t)
	now := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	receipts := []reapReceipt{{Time: now.Add(-20 * time.Hour), Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 13, Worktree: "/repo/" + reviewerLabelFor(13, 30*time.Hour), Result: "pending-cleanup", Reason: "spawn ended before a pane result"}}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- f.s.reviewerDailySummary(testRepo.Name, receipts, nil, now)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	rows := 0
	for _, row := range dailySummaries(t, f) {
		if row["host"] == "unattributed" && row["day"] == "2026-11-03" {
			rows++
		}
	}
	if rows != 1 {
		t.Fatalf("unattributed summaries for 2026-11-03: %d, want 1", rows)
	}
}

// A host name seen only on reviewer rows gets rows through the day of its last
// row and no more: later passes write nothing for it.
func TestReviewerCheckoutDailySummaryStaleHostStopsEmitting(t *testing.T) {
	f := newReviewerFix(t)
	last := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	receipts := []reapReceipt{{Time: last, Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 21, Worktree: "/repo/" + reviewerLabelFor(21, 30*time.Hour), Host: "gone-host", Result: "removed"}}
	for _, day := range []int{3, 4, 10} {
		now := time.Date(2026, 11, day, 10, 0, 0, 0, time.UTC)
		if err := f.s.reviewerDailySummary(testRepo.Name, receipts, nil, now); err != nil {
			t.Fatal(err)
		}
	}
	var rows []map[string]any
	for _, row := range dailySummaries(t, f) {
		if row["host"] == "gone-host" {
			rows = append(rows, row)
		}
	}
	if len(rows) != 1 || rows[0]["day"] != "2026-11-02" || rows[0]["removed"] != float64(1) {
		t.Fatalf("gone-host summaries: %v, want one row for 2026-11-02", rows)
	}
}

// A checkout kept earlier in the day stays counted as kept when a later row for
// it that day has another result: a shadow pass's would-remove, or an error.
func TestReviewerCheckoutDayCountsKeepLastKeptOverLaterOutcome(t *testing.T) {
	day := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	row := func(pr, hour int, result, reason string) reapReceipt {
		return reapReceipt{Time: day.Add(time.Duration(hour) * time.Hour), Action: "reviewer-cleanup", Repo: testRepo.Name, PR: pr, Worktree: "/repo/" + reviewerLabelFor(pr, 30*time.Hour), Host: "local", Result: result, Reason: reason}
	}
	receipts := []reapReceipt{
		row(31, 10, "kept", "job:running"),
		row(31, 15, "would-remove", ""),
		row(32, 9, "kept", "job:running"),
		row(32, 11, "error", ""),
		row(33, 8, "kept", "composer-text"),
		row(33, 12, "removed", ""),
	}
	removed, kept, reasons := reviewerDayCounts(receipts, nil, "local", day)
	if removed != 1 || kept != 2 || reasons["job:running"] != 2 {
		t.Fatalf("removed=%d kept=%d reasons=%v; want removed 1, kept 2 (job:running 2)", removed, kept, reasons)
	}
}
