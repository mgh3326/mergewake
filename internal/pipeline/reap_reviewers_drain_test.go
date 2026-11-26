package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestReapReviewerLegacyWorkRootCheckoutIsRemoved(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	label := reviewerLabelFor(52, 3*24*time.Hour)
	path := "/work/" + label
	f.hub.detached[path] = true
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 52,
		Host: "node-a", Label: label, Worktree: label, Result: "pending-cleanup", Reason: "old placement"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "removed" || rows[0].Worktree != path {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

func TestReapReviewerLegacyWorkRootCheckoutHonorsGuards(t *testing.T) {
	for _, tc := range []struct {
		name       string
		pane       bool
		removeWith string
		wantResult string
		wantRemove bool
		want       string
	}{
		{name: "dirty", removeWith: "dirty", wantResult: "pending-cleanup", wantRemove: true, want: reviewerRefused + "dirty"},
		{name: "pane in use", pane: true, wantResult: "kept", want: "pane-in-use"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReviewerFix(t)
			f.s.Config.Hosts[0].WorkRoot = "/work"
			label := reviewerLabelFor(53, 3*24*time.Hour)
			path := "/work/" + label
			f.hub.detached[path] = true
			f.hub.removeOutcome = tc.removeWith
			if tc.pane {
				f.addPane("w1:p8", "other", path)
			}
			seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 53,
				Host: "node-a", Label: label, Worktree: label, Result: "pending-cleanup", Reason: "old placement"})
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if got := len(f.hub.ops("worktree.remove")); (got > 0) != tc.wantRemove {
				t.Fatalf("removal count %d, want remove=%t: %v", got, tc.wantRemove, f.hub.ops("worktree.remove"))
			}
			rows := f.rows(t)
			if len(rows) != 1 || rows[0].Result != tc.wantResult || rows[0].Reason != tc.want || rows[0].Worktree != path {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

func TestReapReviewerLegacyPathRescansAlreadyGoneRow(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	label := reviewerLabelFor(54, 3*24*time.Hour)
	path := "/work/" + label
	f.hub.detached[path] = true
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 54,
		Host: "node-a", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
		Result: "skipped", Reason: reviewerAlreadyGone})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[1].Result != "removed" || rows[1].Worktree != path {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
	inspectCount := len(f.hub.ops("worktree.inspect"))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := len(f.rows(t)); got != 2 {
		t.Fatalf("legacy path was revisited after its row: %v", rowSummary(f.rows(t)))
	}
	if got := len(f.hub.ops("worktree.inspect")); got != inspectCount {
		t.Fatalf("settled legacy label was inspected again: %v", f.hub.ops("worktree.inspect"))
	}
}

func TestReapReviewerLegacyPathRescanUsesPerHostDrain(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 1, HostOrder: []string{"node"}}
	var labels []string
	var initial []reapReceipt
	for pr := 55; pr <= 56; pr++ {
		label := reviewerLabelFor(pr, 3*24*time.Hour)
		labels = append(labels, label)
		f.hub.detached["/work/"+label] = true
		initial = append(initial, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr,
			Host: "node-a", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
			Result: "skipped", Reason: reviewerAlreadyGone})
	}
	seedReapRows(t, f.s, testRepo.Name, initial...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := len(f.hub.ops("worktree.remove")); got != 1 {
		t.Fatalf("first-pass removals=%d, want 1: %v", got, f.hub.ops("worktree.remove"))
	}
	if rows := f.rows(t); len(rows) != 4 || rows[3].Result != "pending-cleanup" || rows[3].Reason != reviewerDrainHold || filepath.Base(rows[3].Worktree) != labels[1] {
		t.Fatalf("the guarded legacy removal did not wait on the drain cap: %v", rowSummary(rows))
	}
}

func TestReapReviewerAbsentLegacyPathsDoNotUsePerHostDrain(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 1, HostOrder: []string{"node"}}
	var initial []reapReceipt
	for pr := 58; pr <= 60; pr++ {
		label := reviewerLabelFor(pr, 3*24*time.Hour)
		if pr == 60 {
			f.hub.detached["/work/"+label] = true
		}
		initial = append(initial, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr,
			Host: "node-a", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
			Result: "skipped", Reason: reviewerAlreadyGone})
	}
	seedReapRows(t, f.s, testRepo.Name, initial...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	want := "/work/" + reviewerLabelFor(60, 3*24*time.Hour)
	if len(removes) != 1 || removes[0]["cwd"] != want {
		t.Fatalf("removals=%v; absent legacy paths consumed the slot (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

func TestReapReviewerLegacyDryRunDoesNotUsePerHostDrain(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 1, HostOrder: []string{"node"}}
	want := map[string]bool{}
	var initial []reapReceipt
	for pr := 61; pr <= 62; pr++ {
		label := reviewerLabelFor(pr, 3*24*time.Hour)
		want[label] = true
		f.hub.detached["/work/"+label] = true
		initial = append(initial, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr,
			Host: "node-a", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
			Result: "skipped", Reason: reviewerAlreadyGone})
	}
	seedReapRows(t, f.s, testRepo.Name, initial...)
	var out strings.Builder
	f.s.Out = &out
	if err := f.reap(t, 0, Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	for label := range want {
		if !strings.Contains(out.String(), `"label":"`+label+`"`) || !strings.Contains(out.String(), `"worktree":"/work/`+label+`","result":"would-remove"`) {
			t.Fatalf("dry run did not report legacy path %s as would-remove: %s", label, out.String())
		}
	}
	if strings.Contains(out.String(), reviewerDrainHold) {
		t.Fatalf("dry run reported a drain hold: %s", out.String())
	}
	if got := len(f.hub.ops("worktree.remove")); got != 0 {
		t.Fatalf("dry run removed worktrees: %v", f.hub.ops("worktree.remove"))
	}
}

func TestReapReviewerLegacyPathRefusalUsesItsOwnRetryWindow(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.hub.removeOutcome = "dirty"
	label := reviewerLabelFor(57, 3*24*time.Hour)
	legacy := "/work/" + label
	suffixed := legacy + reviewerCheckoutSuffix
	f.hub.detached[legacy] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", Cwd: suffixed}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 57,
		Host: "node-a", Label: label, Worktree: suffixed, Result: "pending-cleanup", Reason: "old attempt"})
	now := reviewerNow
	f.s.Now = func() time.Time { return now }
	for attempt := 1; attempt <= 4; attempt++ {
		if attempt > 1 {
			now = now.Add(reviewerRefusalRetry)
		}
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if got := len(f.hub.ops("worktree.remove")); got != attempt {
			t.Fatalf("attempt %d: remove calls=%d, want %d", attempt, got, attempt)
		}
	}
	rows := f.rows(t)
	if last := rows[len(rows)-1]; last.Result != "kept" || last.Reason != reviewerRefused+"dirty" || last.Worktree != legacy {
		t.Fatalf("final refusal row: %v", rowSummary(rows))
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := len(f.hub.ops("worktree.remove")); got != 4 {
		t.Fatalf("final refusal was retried immediately: removes=%v", f.hub.ops("worktree.remove"))
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

// A host with no work root leaves the row pending with the missing key named,
// and does not spend the drain cap; adding the work root removes the checkout
// on the next pass.
func TestReapReviewerMissingWorkRootStaysPendingThenIsRemoved(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(81, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 81, Host: "node-a",
		Label: label, Worktree: label + reviewerCheckoutSuffix, Result: "pending-cleanup", Reason: "pane still present"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "pending-cleanup" || rows[0].Reason != fmt.Sprintf(reviewerPathUnknown, "node") {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
	if len(f.hub.ops("worktree.remove")) != 0 {
		t.Fatalf("removal without a work root: %v", f.hub.ops("worktree.remove"))
	}
	// The operator configures the work root; the next pass releases it.
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.hub.detached[path] = true
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

// A row written since the label field exists names the node's checkout in
// Worktree while its pane is still present (no Cwd); that path is the one the
// sweep visits, not a guessed work-root path.
func TestReapReviewerNewCleanupRowWorktreeIsARemotePath(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	label := reviewerLabelFor(82, 2*24*time.Hour)
	path := "/node/checkouts/" + label + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 82, Host: "node-a",
		Label: label, Worktree: path, Result: "pending-cleanup", Reason: "pane still present"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

// An audit-only label with a live pane is not settled by that pane alone: no
// row is written before settle, so the journal window stays pending until a
// durable row exists.
func TestReapReviewerAuditLabelWithALivePaneKeepsTheWindow(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(83, 5*time.Minute)
	cwd := "/repo/" + label
	f.addPane("w1:p9", label, cwd)
	f.hub.detached[cwd] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", Cwd: cwd}
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 83, "label": label, "pane": "w1:p9"}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.s.reviewerAuditPath(testRepo.Name)); !os.IsNotExist(err) {
		t.Fatalf("journal offset advanced before a durable row: %v", err)
	}
	// Past settle, with the pane gone, the label is visited and removed.
	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	f.s.Now = func() time.Time { return reviewerNow.Add(time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.s.reviewerAuditPath(testRepo.Name)); err != nil {
		t.Fatalf("journal offset not saved after a durable row: %v", err)
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Result != "removed" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// A host named by a record but absent from the configured hosts is a temporary
// condition: the row stays non-terminal so a restored host still gets it.
func TestReapReviewerUnconfiguredHostStaysPending(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(84, 2*24*time.Hour)
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 84, "label": label, "host": "node-z", "pane": "w1:p1"}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "job-host-unconfigured" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// A journal larger than one read is read in windows forward from the saved
// offset: no row is skipped and no offset passes beyond what was handled.
func TestReviewerAuditScanReadsBoundedWindowsForward(t *testing.T) {
	old := reviewerAuditReadMax
	reviewerAuditReadMax = 150
	t.Cleanup(func() { reviewerAuditReadMax = old })
	dir := t.TempDir()
	s := &Service{Config: config.Config{StateDir: dir}}
	repo := "example/project"
	path := filepath.Join(dir, repo, "decisions.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	first := reviewerLabelFor(91, time.Hour)
	second := reviewerLabelFor(92, time.Hour)
	row := func(label string) string {
		b, _ := json.Marshal(map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 91, "label": label, "host": "node-a"}})
		return string(b) + "\n"
	}
	if err := os.WriteFile(path, []byte(row(first)+row(second)), 0o600); err != nil {
		t.Fatal(err)
	}
	labels, off, err := s.reviewerAuditLabels(repo)
	if err != nil || len(labels) != 1 || labels[first].label != first || off != int64(len(row(first))) {
		t.Fatalf("first window labels=%v off=%d err=%v", labels, off, err)
	}
	if err := s.saveReviewerAuditOffset(repo, off); err != nil {
		t.Fatal(err)
	}
	labels, off2, err := s.reviewerAuditLabels(repo)
	if err != nil || len(labels) != 1 || labels[second].label != second || off2 != int64(len(row(first)+row(second))) {
		t.Fatalf("second window labels=%v off=%d err=%v", labels, off2, err)
	}
}

// A receipt or audit row that cannot be written keeps the journal window: the
// label must not be consumed while nothing durable names it.
func TestReapReviewerWriteFailureKeepsTheJournalWindow(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(93, 2*time.Hour)
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 93, "label": label}})
	// The receipt log cannot be appended to, while the journal checkpoint
	// still can: the pass holds the window because its row was lost.
	if err := os.WriteFile(f.s.reapLog(testRepo.Name), nil, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err == nil {
		t.Fatal("a pass whose rows cannot be written reported no error")
	}
	if _, err := os.Stat(f.s.reviewerAuditPath(testRepo.Name)); !os.IsNotExist(err) {
		t.Fatalf("journal offset saved without its row: %v", err)
	}
}

// A host whose scan failed must not spend the audit visit budget: otherwise
// the labels sorted first would starve every healthy host's backlog.
func TestReapReviewerBlockedHostDoesNotStarveTheVisitBudget(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b"})
	f.hub.moreNodes = []any{map[string]any{"machine_id": "node-b", "state": "disconnected"}}
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 20, HostOrder: []string{"node-b", "node"}}
	rows := make([]map[string]any, 0, 251)
	for i := 0; i < 250; i++ {
		label := reviewerLabelFor(100+i, 2*24*time.Hour)
		rows = append(rows, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 100 + i, "label": label, "host": "node-b", "pane": "w1:p1"}})
	}
	healthy := reviewerLabelFor(400, 2*24*time.Hour)
	rows = append(rows, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 400, "label": healthy, "host": "node-a", "pane": "w1:p2"}})
	seedDecisionRows(t, f.s, testRepo.Name, rows...)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	path := "/work/" + healthy + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	f.sink.recs[healthy] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: healthy, Host: "node-a"}
	// The node-b scan error is the pass's own report; only the removal matters here.
	if err := f.reap(t, 0, Options{}); err != nil && !strings.Contains(err.Error(), "disconnected") {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("the healthy host's label was starved: removes %v (rows %d)", removes, len(f.rows(t)))
	}
}

// A label that once ended with "no host" is a candidate again when a later
// journal row names its host, so the checkout is still removed.
func TestReapReviewerHostUnknownReopensWhenTheJournalNamesAHost(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	label := reviewerLabelFor(401, 3*24*time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 401, Label: label,
		Worktree: label, Result: "kept", Reason: reviewerHostUnknown})
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{
		"pr": 401, "label": label, "host": "node-a", "pane": "w1:p3"}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

// A host whose drain budget is spent must not consume the audit visits either:
// its deferred labels are skipped without spending the allowance, so the next
// host still drains in the same pass.
func TestReapReviewerDrainedHostDoesNotSpendTheAuditAllowance(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.hub.moreNodes = []any{map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true}}
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 1, HostOrder: []string{"node-b", "node"}}
	rows := make([]map[string]any, 0, 251)
	for i := 0; i < 250; i++ {
		label := reviewerLabelFor(500+i, 2*24*time.Hour)
		f.hub.detached["/work-b/"+label+reviewerCheckoutSuffix] = true
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-b"}
		rows = append(rows, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 500 + i, "label": label, "host": "node-b", "pane": "w1:p1"}})
	}
	f.s.Config.Hosts[0].WorkRoot = "/work"
	healthy := reviewerLabelFor(900, 2*24*time.Hour)
	healthyPath := "/work/" + healthy + reviewerCheckoutSuffix
	f.hub.detached[healthyPath] = true
	f.sink.recs[healthy] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: healthy, Host: "node-a"}
	rows = append(rows, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 900, "label": healthy, "host": "node-a", "pane": "w1:p2"}})
	seedDecisionRows(t, f.s, testRepo.Name, rows...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var removed []string
	for _, v := range f.hub.ops("worktree.remove") {
		removed = append(removed, v["cwd"].(string))
	}
	if len(removed) != 2 || removed[1] != healthyPath {
		t.Fatalf("removals %v, want one on node-b and the healthy label after it", removed)
	}
}

// A reviewer row whose Result is "error" is still a durable row: it must not
// keep the journal checkpoint from advancing, or one repeating failure would
// stall the scan for every later label.
func TestReapReviewerErrorRowDoesNotBlockTheJournalCheckpoint(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(601, time.Hour)
	cwd := "/repo/" + label
	f.hub.detached[cwd] = true
	f.hub.forbid = map[string]bool{"worktree.remove": true}
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", Cwd: cwd}
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-cleanup", "value": map[string]any{"pr": 601, "label": label, "host": "node-a"}})
	err := f.reap(t, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "reviewer") {
		t.Fatalf("a failed removal reported success: %v", err)
	}
	var errored bool
	for _, row := range f.rows(t) {
		errored = errored || row.Result == "error"
	}
	if !errored {
		t.Fatalf("rows: %v", rowSummary(f.rows(t)))
	}
	if _, err := os.Stat(f.s.reviewerAuditPath(testRepo.Name)); err != nil {
		t.Fatalf("a durable error row stalled the journal checkpoint: %v", err)
	}
}

// A label whose only named host failed its scan is not inspected: the host's
// error is already reported, and a row here would repeat every pass.
func TestReapReviewerFailedHostIsNotInspected(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.hub.moreNodes = []any{map[string]any{"machine_id": "node-b", "state": "disconnected"}}
	label := reviewerLabelFor(602, 2*24*time.Hour)
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 602, Host: "node-b",
		Label: label, Worktree: label + reviewerCheckoutSuffix, Result: "pending-cleanup", Reason: "pane still present"})
	if err := f.reap(t, 0, Options{}); err == nil || !strings.Contains(err.Error(), "disconnected") {
		t.Fatalf("want the host scan error, got %v", err)
	}
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("a failed host was inspected: %v", rowSummary(rows))
	}
}
