package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// removedProbesPerHost is the per-host allowance of removed-path re-inspects
// (hk 6992): the probes one pass may take on a host.
const removedProbesPerHost = 5

// removedStreamRows are the rows one reviewer label leaves as a finished
// reviewer does: a pending pane row (so the label is still a candidate) and a
// removed row for its worktree at path (hk 6992).
func removedStreamRows(pr int, label, path, host string) []reapReceipt {
	return []reapReceipt{
		{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr, Host: host, Pane: fmt.Sprintf("fixture-pane-%d", pr), Label: label,
			Result: "pending-cleanup", Reason: "pane still present; panewire closes it and a later sweep cleans up"},
		{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr, Host: host, Label: label, Worktree: path, Cwd: path, Result: "removed"},
	}
}

// appendReapRows appends rows to the reap journal, as a later pass would write
// them. seedReapRows replaces the file, so it is for the first rows only.
func appendReapRows(t *testing.T, s *Service, rows ...reapReceipt) {
	t.Helper()
	f, err := os.OpenFile(s.reapLog(testRepo.Name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, row := range rows {
		b, _ := json.Marshal(row)
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
}

// closeSeededPane appends the terminal pane row a closed pane leaves, so the
// label's streams all end unless a re-inspect marker keeps it open (hk 6992).
func closeSeededPane(t *testing.T, f *reviewerFix, pr int, label, host string) {
	t.Helper()
	appendReapRows(t, f.s, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr, Host: host,
		Pane: fmt.Sprintf("fixture-pane-%d", pr), Label: label, Result: "closed", Reason: "closed by the sweep"})
}

// A removed-row re-inspect held by the host's allowance leaves a marker, so the
// label is still a candidate after its pane closes and the next pass inspects
// the recreated checkout and removes it (hk 6992). Fails without the marker.
func TestReapReviewerHeldRemovedReprobeIsRetried(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 10}
	count := removedProbesPerHost + 1
	labels := map[string]int{}
	paths := map[string]string{}
	var seeded []reapReceipt
	for i := range count {
		pr := 100 + i
		label := reviewerLabelFor(pr, time.Hour)
		path := "/work/" + label + reviewerCheckoutSuffix
		labels[label] = pr
		paths[label] = path
		f.hub.detached[path] = true
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
		seeded = append(seeded, removedStreamRows(pr, label, path, host)...)
	}
	seedReapRows(t, f.s, testRepo.Name, seeded...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removedNow := func() map[string]bool {
		out := map[string]bool{}
		for _, op := range f.hub.ops("worktree.remove") {
			out[op["cwd"].(string)] = true
		}
		return out
	}
	var held string
	for label, path := range paths {
		if !removedNow()[path] {
			held = label
		}
	}
	if held == "" || len(removedNow()) != removedProbesPerHost {
		t.Fatalf("pass 1 removes=%v, want %d and one held: %v", removedNow(), removedProbesPerHost, rowSummary(f.rows(t)))
	}
	for label, pr := range labels {
		closeSeededPane(t, f, pr, label, host)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !removedNow()[paths[held]] {
		t.Fatalf("held label %s was not probed again after its pane closed: removes %v, rows %v", held, removedNow(), rowSummary(f.rows(t)))
	}
}

// A removed-row re-inspect whose inspect fails leaves a marker: the label stays a
// candidate after its pane closes, so the next healthy pass removes the tree
// (hk 6992). Fails without the marker.
func TestReapReviewerRemovedReprobeErrorIsRetried(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)

	f.hub.inspectErr = map[string]string{path: "timeout"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removes after a failed inspect: %v", removes)
	}

	closeSeededPane(t, f, pr, label, host)
	f.hub.inspectErr = nil
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("healthy pass removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

// Removed-row re-inspects spend their own per-host allowance, so a backlog of
// them never takes the legacy-path probes' share (hk 6992). Fails without it.
func TestReapReviewerRemovedReprobesLeaveLegacyProbesTheirAllowance(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	var seeded []reapReceipt
	for i := range removedProbesPerHost + 1 {
		pr := 100 + i
		label := reviewerLabelFor(pr, time.Hour)
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
		seeded = append(seeded, removedStreamRows(pr, label, "/work/"+label+reviewerCheckoutSuffix, host)...)
	}

	legacyPR := 200
	legacy := reviewerLabelFor(legacyPR, 3*24*time.Hour)
	legacyPath := "/work/" + legacy
	f.hub.detached[legacyPath] = true
	f.sink.recs[legacy] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: legacy, Host: host}
	seeded = append(seeded, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: legacyPR, Host: host,
		Label: legacy, Worktree: legacyPath + reviewerCheckoutSuffix, Result: "skipped", Reason: reviewerAlreadyGone})
	seedReapRows(t, f.s, testRepo.Name, seeded...)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, op := range f.hub.ops("worktree.remove") {
		if op["cwd"] == legacyPath {
			return
		}
	}
	t.Fatalf("legacy label was starved by removed re-inspects: removes %v, rows %v", f.hub.ops("worktree.remove"), rowSummary(f.rows(t)))
}

// A revival within the day after an absent answer is found once the answer is
// due: the probe inspects the tree, and a pane using it keeps it pane-in-use
// (hk 6992). Inside the day nothing is written or inspected.
func TestReapReviewerAbsentSettleKeepsRevivalWithPane(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}

	f.hub.detached[path] = true
	f.addPane("fixture-pane-b", "other", path)
	before := len(f.rows(t))
	f.s.Now = func() time.Time { return reviewerNow.Add(time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 || len(f.rows(t)) != before {
		t.Fatalf("inside the day: removes %v, rows %v", removes, rowSummary(f.rows(t)))
	}

	f.s.Now = func() time.Time { return reviewerNow.Add(25 * time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removes: %v", removes)
	}
	rows := f.rows(t)
	if last := rows[len(rows)-1]; last.Result != "kept" || last.Reason != "pane-in-use" || last.Worktree != path {
		t.Fatalf("revival was not kept: %v", rowSummary(rows))
	}
}

// A pane on an absent removed path writes one absent answer and inspects the
// path once a day, not on every pass (hk 6992).
func TestReapReviewerAbsentPathUnderPaneSettlesOncePerDay(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)
	f.addPane("fixture-pane-b", "other", path)
	inspects := func() int {
		n := 0
		for _, op := range f.hub.ops("worktree.inspect") {
			if op["cwd"] == path {
				n++
			}
		}
		return n
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	after := inspects()
	f.s.Now = func() time.Time { return reviewerNow.Add(time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	// The host scan inspects every pane's cwd on each pass, so the pane on this
	// path costs one inspect; the removed-path probe itself must not run.
	if got := inspects() - after; got > 1 {
		t.Fatalf("absent path under a pane was probed again inside the day: %d inspects", got)
	}
	absent := 0
	for _, r := range f.rows(t) {
		if r.Result == "skipped" && strings.HasPrefix(r.Reason, reviewerRemovedAbsent) {
			absent++
		}
	}
	if absent != 1 {
		t.Fatalf("absent rows: %d, want 1: %v", absent, rowSummary(f.rows(t)))
	}
}

// A revival whose pane closed inside the day is found at the due re-inspect:
// the label's streams all end, so only the due absent answer brings it back
// (hk 6992). Fails without the due re-admission in candidates.
func TestReapReviewerAbsentSettleReadmitsClosedLabelAfterADay(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	closeSeededPane(t, f, pr, label, host)

	f.hub.detached[path] = true
	f.s.Now = func() time.Time { return reviewerNow.Add(25 * time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("revival after a closed pane was not removed: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}

// Each absent answer more than a day apart is a new row, so the stream settles
// again after the day: the third pass inside the next window inspects nothing
// (hk 6992). Fails when a repeated absent answer is dropped as a repeat.
func TestReapReviewerAbsentSettleRepeatsAfterADay(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)
	inspects := func() int {
		n := 0
		for _, op := range f.hub.ops("worktree.inspect") {
			if op["cwd"] == path {
				n++
			}
		}
		return n
	}
	at := func(d time.Duration) {
		f.s.Now = func() time.Time { return reviewerNow.Add(d) }
	}
	at(0)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	at(25 * time.Hour)
	before := inspects()
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if inspects() == before {
		t.Fatalf("second absent answer was not inspected after a day")
	}
	rows := f.rows(t)
	if n := 0; true {
		for _, r := range rows {
			if r.Result == "skipped" && strings.HasPrefix(r.Reason, reviewerRemovedAbsent) {
				n++
			}
		}
		if n != 2 {
			t.Fatalf("absent answers recorded: %d, want 2: %v", n, rowSummary(rows))
		}
	}
	at(26 * time.Hour)
	before = inspects()
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := inspects(); got != before {
		t.Fatalf("settled path inspected again inside the window: %d", got-before)
	}
}

// Held removed re-inspects take turns: a label the allowance held is the next
// full sweep's first candidate, so the failing paths that sort before it do not
// hold it forever (hk 6992). Fails without the sweep cursor for held labels.
func TestReapReviewerHeldRemovedLabelTakesNextTurn(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	f.s.Config.Reap.ReviewerDrain = config.ReviewerDrain{PerHost: 10}
	var seeded []reapReceipt
	errs := map[string]string{}
	var lastPath string
	for i := range removedProbesPerHost + 1 {
		pr := 100 + i
		label := reviewerLabelFor(pr, time.Hour)
		path := "/work/" + label + reviewerCheckoutSuffix
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
		seeded = append(seeded, removedStreamRows(pr, label, path, host)...)
		if i < removedProbesPerHost {
			errs[path] = "timeout"
		} else {
			f.hub.detached[path] = true
			lastPath = path
		}
	}
	seedReapRows(t, f.s, testRepo.Name, seeded...)
	f.hub.inspectErr = errs

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	f.hub.inspectErr = errs
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removed := false
	for _, op := range f.hub.ops("worktree.remove") {
		if op["cwd"] == lastPath {
			removed = true
		}
	}
	if !removed {
		t.Fatalf("held label never took its turn: removes %v", f.hub.ops("worktree.remove"))
	}
}

// A checkout recreated and removed again inside the day, after its label's pane
// closed, is found again at the due re-inspect: the recreated removal row
// shadows the absent row, so only its own day brings the label back (hk 6992).
// Fails without absentDue counting a recreated removal row.
func TestReapReviewerRecreatedRemovalReadmitsAfterADay(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	pr := 12
	label := reviewerLabelFor(pr, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f.hub.detached[path] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name, removedStreamRows(pr, label, path, host)...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 {
		t.Fatalf("first recreated removal: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
	closeSeededPane(t, f, pr, label, host)

	f.hub.detached[path] = true
	f.s.Now = func() time.Time { return reviewerNow.Add(25 * time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 2 {
		t.Fatalf("second recreation was not removed: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
}
