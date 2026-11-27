package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// A removed stream whose inspect fails keeps its removed proof: the failed probe
// proves nothing about the tree, so nothing is removed, the journal keeps the
// removed row, and the stream gets a pending re-inspect marker so the next pass
// inspects it again (hk 6983, hk 6992). The host is not failed for its labels.
func TestReapReviewerRemovedRowSurvivesInspectError(t *testing.T) {
	label := reviewerLabelFor(12, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Pane: "fixture-pane-a", Label: label,
			Result: "pending-cleanup", Reason: "pane still present; panewire closes it and a later sweep cleans up"},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Label: label, Worktree: path, Cwd: path,
			Result: "removed"},
	)
	before := len(f.rows(t))
	f.hub.detached[path] = true
	f.hub.inspectErr = map[string]string{path: "timeout"}

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removes: %v", removes)
	}
	rows := f.rows(t)
	if len(rows) != before+1 {
		t.Fatalf("rows: want %d, got %v", before+1, rowSummary(rows))
	}
	if last := rows[len(rows)-1]; last.Result != "pending-cleanup" || last.Reason != reviewerRemovedReprobe || last.Worktree != path {
		t.Fatalf("re-inspect marker: %v", rowSummary(rows))
	}
	if rows[before-1].Result != "removed" || rows[before-1].Worktree != path {
		t.Fatalf("removed proof was lost: %v", rowSummary(rows))
	}
}

// A checkout a same-label revival recreated at a removed path is removed again
// under a row of its own: the plain removed row would be dropped as a repeat.
func TestReapReviewerRecreatedCheckoutRemovalIsRecorded(t *testing.T) {
	label := reviewerLabelFor(12, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Pane: "fixture-pane-a", Label: label,
			Result: "pending-cleanup", Reason: "pane still present; panewire closes it and a later sweep cleans up"},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Label: label, Worktree: path, Cwd: path,
			Result: "removed"},
	)
	before := len(f.rows(t))
	f.hub.detached[path] = true

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != path {
		t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != before+1 {
		t.Fatalf("rows: want %d, got %v", before+1, rowSummary(rows))
	}
	last := rows[len(rows)-1]
	if last.Result != "removed" || last.Worktree != path || !strings.HasPrefix(last.Reason, reviewerRecreatedRemoved) {
		t.Fatalf("recreated removal row: %v", rowSummary(rows))
	}
	if audits := f.audits(t); len(audits) == 0 {
		t.Fatalf("no reviewer-reap audit record for the recreated removal")
	}
}

// Abandoned-and-removed labels on one host re-inspect their removed paths
// within the host's probe allowance each pass, not once per label (hk 6983).
func TestReapReviewerRemovedProbesStayWithinHostAllowance(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	host := f.s.Config.Hosts[0].Name
	count := reviewerLegacyProbesPerHost + 3
	paths := map[string]bool{}
	var rows []reapReceipt
	for i := range count {
		label := reviewerLabelFor(100+i, time.Hour)
		path := "/work/" + label + reviewerCheckoutSuffix
		paths[path] = true
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host, Cwd: path}
		rows = append(rows,
			reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 100 + i, Host: host, Label: label, Result: "abandoned", Reason: "abandoned by the sweep"},
			reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 100 + i, Host: host, Label: label, Worktree: path, Cwd: path, Result: "removed"},
		)
	}
	seedReapRows(t, f.s, testRepo.Name, rows...)

	probes := func() int {
		n := 0
		for _, c := range f.hub.ops("worktree.inspect") {
			if paths[c["cwd"].(string)] {
				n++
			}
		}
		return n
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := probes(); got == 0 || got > reviewerLegacyProbesPerHost {
		t.Fatalf("pass probes of removed paths=%d, want 1..%d for %d labels", got, reviewerLegacyProbesPerHost, count)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removes: %v", removes)
	}
}

// An abandon error row an older version wrote has no retry deadline. It is
// expired: in a fresh process the next pass tries the job close again instead
// of blocking the one-off reap (hk 6983).
func TestReapReviewerLegacyAbandonErrorRowWithoutDeadlineExpires(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.clearPanes()
	host := f.s.Config.Hosts[0].Name
	// The legacy row is the stream's newest outcome: a stuck marker after it
	// would replace it, since an error row without a deadline is not kept.
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Pane: "fixture-pane-a", Label: f.label,
			Result: "error", Reason: "abandon job: panewire exit status 5 owner_mismatch"},
	)

	refuseJobClose(f.reviewerFix, "close refused: job "+f.label+" owner_mismatch")
	f.s.reapRetryState = nil
	_ = f.passAt(t, 0)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("pass 1 job closes=%d, want the expired legacy row not to block the close: %v", n, rowSummary(f.rows(t)))
	}

	f.s.Runner = f.run
	f.allowJobClose()
	f.s.reapRetryState = nil
	if err := f.passAt(t, 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()); n != 2 {
		t.Fatalf("pass 2 job closes=%d, want the close retried: %v", n, rowSummary(f.rows(t)))
	}
	if rows := f.resultRows(t, "abandoned"); len(rows) != 1 {
		t.Fatalf("abandoned rows: %v", rowSummary(f.rows(t)))
	}
}
