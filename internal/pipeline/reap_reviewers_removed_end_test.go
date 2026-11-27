package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// removedEndFix seeds one reviewer label whose checkout was removed at path on
// host (hk 7019). Its job record names the path as its cwd.
func removedEndFix(t *testing.T, pr int, label, path string) *reviewerFix {
	t.Helper()
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host, Cwd: path}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)
	return f
}

// countRows counts the journal rows of one worktree path whose reason has prefix.
func countRows(rows []reapReceipt, path, prefix string) int {
	n := 0
	for _, r := range rows {
		if r.Worktree == path && strings.HasPrefix(r.Reason, prefix) {
			n++
		}
	}
	return n
}

// inspectsOf counts the worktree.inspect calls aimed at one path.
func inspectsOf(f *reviewerFix, path string) int {
	n := 0
	for _, op := range f.hub.ops("worktree.inspect") {
		if op["cwd"] == path {
			n++
		}
	}
	return n
}

// Rule 1 (hk 7019): a removed path is re-inspected at most three times after its
// removal, a day apart. The third absent answer settles the stream with a
// terminal row, and no later pass inspects it. Fails on the base, which writes an
// absent row and inspects the path again every day.
func TestReapReviewerRemovedPathSettlesAfterThreeReinspects(t *testing.T) {
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f := removedEndFix(t, pr, label, path)
	at := func(d time.Duration) { f.s.Now = func() time.Time { return reviewerNow.Add(d) } }
	for _, d := range []time.Duration{0, 25 * time.Hour, 49 * time.Hour} {
		at(d)
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	settledAt := inspectsOf(f, path)
	at(73 * time.Hour)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := inspectsOf(f, path) - settledAt; got != 0 {
		t.Fatalf("settled removed path inspected again after three re-inspects: %d inspects", got)
	}
	rows := f.rows(t)
	if n := countRows(rows, path, "already gone: removed path absent on 3 re-inspects"); n != 1 {
		t.Fatalf("terminal rows: %d, want 1: %v", n, rowSummary(rows))
	}
}

// Rule 2 (hk 7017, hk 7019): a label is admitted for a due removed stream, and
// that stream is re-inspected even when the job names another stream. The due
// absent answer here sits on the legacy path; the job's own spec path is settled.
// Fails on the base, which re-inspects only the job's stream.
func TestReapReviewerDueRemovedStreamOnAnotherPathIsProbed(t *testing.T) {
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	spec := "/work/" + label + reviewerCheckoutSuffix
	legacy := "/work/" + label
	f := removedEndFix(t, pr, label, spec)
	host := f.s.Config.Hosts[0].Name
	seedReapRows(t, f.s, testRepo.Name, append(removedStreamRows(pr, label, spec, host),
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr, Host: host, Label: label,
			Worktree: legacy, Cwd: legacy, Result: "skipped", Reason: "already gone: removed path re-inspected with no tree; inspected again later (2025-01-01T00:00:00Z)"})...)
	f.hub.detached[legacy] = true

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, op := range f.hub.ops("worktree.remove") {
		if op["cwd"] == legacy {
			return
		}
	}
	t.Fatalf("due removed stream on the legacy path was not re-inspected: removes %v, rows %v",
		f.hub.ops("worktree.remove"), rowSummary(f.rows(t)))
}

// Rule 3 (hk 7019): a removed path whose re-inspects keep failing is settled
// after three failed inspects, with a terminal row that names the last error.
// Fails on the base, which keeps a pending marker and inspects the path on every
// pass.
func TestReapReviewerRemovedPathFailingInspectsSettle(t *testing.T) {
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f := removedEndFix(t, pr, label, path)
	f.hub.inspectErr = map[string]string{path: "timeout"}
	for range 3 {
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	before := inspectsOf(f, path)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := inspectsOf(f, path) - before; got != 0 {
		t.Fatalf("failing removed path inspected after its three attempts: %d inspects", got)
	}
	rows := f.rows(t)
	if n := countRows(rows, path, "removed path re-inspect failed 3 times; last error:"); n != 1 {
		t.Fatalf("terminal rows naming the error: %d, want 1: %v", n, rowSummary(rows))
	}
}

// Rule 4 (hk 7019): after the legacy fallback switches the stream to the
// <work root>/<label> path, the removed proof belongs to that path's own rows. A
// tree that vanishes between two inspects of the spec path leaves no absent row
// on a legacy path that never held a removed row. Fails on the base.
func TestReapReviewerLegacyFallbackDropsRemovedProof(t *testing.T) {
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	spec := "/work/" + label + reviewerCheckoutSuffix
	legacy := "/work/" + label
	f := removedEndFix(t, pr, label, spec)
	f.hub.detached[spec] = true
	seen := 0
	f.hub.onInspect = func() {
		seen++
		if seen == 2 {
			// The second inspect of the spec path finds the tree gone.
			delete(f.hub.detached, spec)
		}
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if n := countRows(rows, legacy, "already gone: removed path re-inspected with no tree"); n != 0 {
		t.Fatalf("absent removed-path row written on the legacy stream: %v", rowSummary(rows))
	}
}

// Rule 5 (hk 7019): a row that holds a tree a re-inspect found keeps the removed
// proof, so the later removal is recorded as a recreated removal. Here the first
// pass finds the tree and a live pane keeps it; once the pane closes, the removal
// must still carry the recreated reason. Fails on the base, which records a plain
// removal.
func TestReapReviewerHeldRecreatedCheckoutKeepsProof(t *testing.T) {
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f := removedEndFix(t, pr, label, path)
	f.hub.detached[path] = true
	f.addPane("fixture-pane-b", "other", path)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removal while a pane uses the checkout: %v", removes)
	}

	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	closeSeededPane(t, f, pr, label, f.s.Config.Hosts[0].Name)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	last := rows[len(rows)-1]
	if last.Result != "removed" || last.Worktree != path || !strings.HasPrefix(last.Reason, "recreated checkout at a removed path removed again") {
		t.Fatalf("held recreated checkout removed as a plain removal: %v", rowSummary(rows))
	}
}
