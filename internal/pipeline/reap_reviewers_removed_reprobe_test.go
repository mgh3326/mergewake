package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
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
