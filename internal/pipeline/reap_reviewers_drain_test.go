package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// seedDecisionRows writes the repo's decisions.jsonl, so a test can hand the
// sweep reviewer audit rows that predate the receipt's label field.
func seedDecisionRows(t *testing.T, s *Service, repo string, rows ...map[string]any) {
	t.Helper()
	if err := os.MkdirAll(s.state(repo), 0o700); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(s.state(repo), "decisions.jsonl"), []byte(buf.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A spec-placed pass end leaves a cleanup row that names its label and the
// node's <label>-checkout worktree. Once the pane is gone and the label is
// past settle, the row alone is a candidate: the sweep builds the worktree's
// path from the host's work root and removes it through the hub's guard.
func TestReapReviewerSpecCheckoutRowIsACandidateWithoutItsPane(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	label := reviewerLabelFor(12, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Host: "node-a", Pane: "w1:p5",
		Label: label, Worktree: label + reviewerCheckoutSuffix, Result: "pending-cleanup", Reason: "pane still present; panewire closes it and a later sweep cleans up"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path || removes[0]["detached"] != true || removes[0]["expect_head"] != headSHA {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "removed" || rows[0].Label != label || rows[0].Worktree != path {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// A label the receipts never named survives in decisions.jsonl: a reviewer
// audit row, here with no host, is a candidate, the job record names the host
// and cwd, and the guarded path removes it.
func TestReapReviewerLeakedLabelFromAuditRowIsRemoved(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(31, 5*24*time.Hour)
	cwd := "/repo/" + label
	f.hub.detached[cwd] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", Cwd: cwd}
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{
		"pr": 31, "label": label, "pane": "w1:p9", "result": "skipped", "reason": "pane gone; remote cwd unavailable or mismatched"}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != cwd || removes[0]["expect_head"] != headSHA {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "removed" || rows[0].Label != label {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
	// The journal offset advanced, so the next pass does not read the row again.
	nrows, naudits := len(f.rows(t)), len(f.audits(t))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != nrows || len(f.audits(t)) != naudits {
		t.Fatalf("second pass wrote: %v", rowSummary(f.rows(t)))
	}
}

// A drained host still holds its lane: with per_host 1, node-b's first label is
// removed, its second waits unvisited, and node-a's label is still removed in
// the same pass. The configured host_order puts node-b first.
func TestReapReviewerDrainCapsPerHostAndVisitsTheConfiguredOrder(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.hub.moreNodes = []any{map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true}}
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 1, HostOrder: []string{"node-b", "node"}}
	first := reviewerLabelFor(21, time.Hour)
	second := reviewerLabelFor(22, time.Hour)
	other := reviewerLabelFor(23, time.Hour)
	firstPath := "/work-b/" + first + reviewerCheckoutSuffix
	secondPath := "/work-b/" + second + reviewerCheckoutSuffix
	otherPath := "/repo/" + other
	for _, c := range []struct{ path, label, host string }{
		{firstPath, first, "node-b"},
		{secondPath, second, "node-b"},
		{otherPath, other, "node-a"},
	} {
		f.hub.detached[c.path] = true
		f.sink.recs[c.label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: c.label, Host: c.host, Cwd: c.path}
	}
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 21, Host: "node-b", Label: first, Worktree: firstPath, Result: "pending-cleanup", Reason: "x"},
		reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 22, Host: "node-b", Label: second, Worktree: secondPath, Result: "pending-cleanup", Reason: "x"},
		reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 23, Host: "node-a", Label: other, Worktree: otherPath, Result: "pending-cleanup", Reason: "x"},
	)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, v := range f.hub.ops("worktree.remove") {
		got = append(got, v["cwd"].(string))
	}
	if len(got) != 2 || got[0] != firstPath || got[1] != otherPath {
		t.Fatalf("removals %v, want node-b first then node-a (rows %v)", got, rowSummary(f.rows(t)))
	}
	for _, row := range f.rows(t) {
		if row.Label == second {
			t.Fatalf("a capped label was visited: %v", rowSummary(f.rows(t)))
		}
	}
}

// The hub's refusal at the work-root path stays a pending row with its reason,
// so the label is asked again once the retry interval has passed.
func TestReapReviewerGuardRefusalAtTheWorkRootPathStaysPending(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.hub.removeOutcome = "dirty"
	label := reviewerLabelFor(41, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 41, Host: "node-a",
		Label: label, Worktree: label + reviewerCheckoutSuffix, Result: "pending-cleanup", Reason: "x"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "pending-cleanup" || rows[0].Reason != reviewerRefused+"dirty" || rows[0].Worktree != path {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// reviewerWorktreePath is the node's own name for a spec-placed checkout, and
// "" when the host names no work root: the one case where no path can be made.
func TestReviewerWorktreePathNeedsAWorkRoot(t *testing.T) {
	label, ok := parseReviewerLabel(reviewerLabelFor(51, time.Hour))
	if !ok {
		t.Fatal("label does not parse")
	}
	got := reviewerWorktreePath(&reapHost{host: reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}}, label)
	if want := "/work-b/" + label.name + reviewerCheckoutSuffix; got != want {
		t.Fatalf("path %q, want %q", got, want)
	}
	if got := reviewerWorktreePath(&reapHost{host: reap.Host{Name: "node-b", Machine: "node-b"}}, label); got != "" {
		t.Fatalf("path %q without a work root", got)
	}
}

// The sweep respects a context that is already over before the first visit.
func TestReapReviewerStopsOnACancelledContext(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(61, time.Hour)
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 61, Host: "node-a", Label: label, Worktree: "/checkouts/" + label, Result: "pending-cleanup", Reason: "x"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.s.Reap(ctx, testRepo, 0, Options{}); err == nil {
		t.Fatal("a cancelled pass reported no error")
	}
}

// A leaked label whose job record is gone is still placed by the pane its old
// cleanup receipt recorded for the same PR: that receipt names no label, but
// its pane and host do.
func TestReapReviewerLeakedLabelJoinsItsPaneHostWhenTheRecordIsGone(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	label := reviewerLabelFor(32, 3*24*time.Hour)
	cwd := "/work/" + label + reviewerCheckoutSuffix
	f.hub.detached[cwd] = true
	delete(f.sink.recs, label)
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 32, Host: "node-a", Pane: "w1:p7",
		Result: "pending-cleanup", Reason: "pane still present; panewire closes it and a later sweep cleans up"})
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{
		"pr": 32, "label": label, "pane": "w1:p7", "result": "pending-cleanup"}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != cwd {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

// An aged label whose record is gone and whose rows name no host can hold no
// remote worktree this sweep can build a path for: one terminal row, and the
// next pass leaves it alone.
func TestReapReviewerAgedLabelWithoutAHostSettlesTerminal(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(33, 2*24*time.Hour)
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 33, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != reviewerHostUnknown {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
	nrows := len(f.rows(t))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != nrows {
		t.Fatalf("terminal label revisited: %v", rowSummary(f.rows(t)))
	}
}

// The review journal is read by byte offset: one pass reads each reviewer row
// once, a partial line waits for its newline, and a rewritten journal starts
// over instead of seeking past its end.
func TestReviewerAuditLabelsReadsEachRowOnce(t *testing.T) {
	dir := t.TempDir()
	s := &Service{Config: config.Config{StateDir: dir}}
	repo := "example/project"
	path := filepath.Join(dir, repo, "decisions.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	first := reviewerLabelFor(71, time.Hour)
	second := reviewerLabelFor(72, time.Hour)
	row := func(label string) string {
		b, _ := json.Marshal(map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 71, "label": label, "pane": "w1:p1", "host": "node-a"}})
		return string(b) + "\n"
	}
	if err := os.WriteFile(path, []byte(row(first)), 0o600); err != nil {
		t.Fatal(err)
	}
	labels, off, err := s.reviewerAuditLabels(repo)
	if err != nil || len(labels) != 1 || labels[first].host != "node-a" || off != int64(len(row(first))) {
		t.Fatalf("first scan labels=%v off=%d err=%v", labels, off, err)
	}
	if err := s.saveReviewerAuditOffset(repo, off); err != nil {
		t.Fatal(err)
	}
	// A half-written line is left for the next read.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(row(second)[:20]); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if labels, off2, err := s.reviewerAuditLabels(repo); err != nil || len(labels) != 0 || off2 != off {
		t.Fatalf("partial scan labels=%v off=%d (was %d) err=%v", labels, off2, off, err)
	}
	f, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	f.WriteString(row(second)[20:])
	f.Close()
	labels, off3, err := s.reviewerAuditLabels(repo)
	if err != nil || len(labels) != 1 || labels[second].host != "node-a" || off3 <= off {
		t.Fatalf("appended scan labels=%v off=%d err=%v", labels, off3, err)
	}
	// A journal rewritten shorter than the saved offset is read from the
	// start rather than seeking past its end.
	if err := s.saveReviewerAuditOffset(repo, off3); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(row(first)), 0o600); err != nil {
		t.Fatal(err)
	}
	if labels, off4, err := s.reviewerAuditLabels(repo); err != nil || len(labels) != 1 || off4 != int64(len(row(first))) {
		t.Fatalf("rewritten scan labels=%v off=%d err=%v", labels, off4, err)
	}
}
