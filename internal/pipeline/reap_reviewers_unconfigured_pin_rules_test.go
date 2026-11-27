package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// hk 7018: the unconfigured-host pin applies only while the host is still
// unconfigured, yields only to a scanned, readable, remote host, and does not
// hide that host's owed work when a job record names the unconfigured host.

// Rule 1 (findings 4715758323, 4715758377): the yield to an owed configured host
// applies only while the pinned host is unconfigured. Once node-z is configured,
// its open job-host-unconfigured row keeps the label (hk 7000). Here node-b holds
// a newer open row: on the base, the row competes by time and takes the label
// off node-z, so node-z's checkout is never targeted.
func TestReapReviewerReconfiguredPinHostKeepsItsCheckout(t *testing.T) {
	f := newReviewerFix(t,
		reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"},
		reap.Host{Name: "node-z", Machine: "node-z", WorkRoot: "/work-z"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-z", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(176, 2*24*time.Hour)
	pathZ := "/work-z/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathZ] = true
	// node-z's row was written while node-z was unconfigured; node-b's newer
	// open row follows it. The last receipt naming a host is node-z's.
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: reviewerNow.Add(-1 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 176, Label: label, Host: "node-b", Worktree: label,
			Result: "kept", Reason: "pane-in-use"},
		reapReceipt{Time: reviewerNow.Add(-3 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 176, Label: label, Host: "node-z", Worktree: pathZ, Cwd: pathZ,
			Result: "kept", Reason: "job-host-unconfigured"},
	)

	if err := f.reap(t, 0, Options{}); err != nil && !strings.Contains(err.Error(), "disconnected") {
		t.Fatal(err)
	}
	found := false
	for _, r := range f.hub.ops("worktree.remove") {
		if r["cwd"] == pathZ {
			found = true
		}
	}
	if !found {
		t.Fatalf("pass 1 never targeted node-z's worktree %s; removes %v; rows %v", pathZ, f.hub.ops("worktree.remove"), rowSummary(f.rows(t)))
	}
}

// Rule 2 (findings 4715758327, 4715758373): the yield picks only a configured
// remote host that is scanned and not failed this pass. Here node-b's newer
// open row sits on a host that is down, so the older owed checkout on the
// healthy node-c is the one the label must reach.
func TestReapReviewerUnconfiguredPinYieldsOnlyToAReadableRemoteHost(t *testing.T) {
	f := newReviewerFix(t,
		reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"},
		reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "disconnected"},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(177, 2*24*time.Hour)
	pathC := "/work-c/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathC] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: reviewerNow.Add(-3 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 177, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
		reapReceipt{Time: reviewerNow.Add(-2 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 177, Label: label, Host: "node-c", Worktree: pathC, Cwd: pathC,
			Result: "pending-cleanup", Reason: reviewerDrainHold},
		reapReceipt{Time: reviewerNow.Add(-1 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 177, Label: label, Host: "node-b", Worktree: label,
			Result: "kept", Reason: "pane-in-use"},
	)

	// The down node-b is the pass's own scan report; only the removal matters.
	if err := f.reap(t, 0, Options{}); err != nil && !strings.Contains(err.Error(), "disconnected") {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathC {
		t.Fatalf("pass 1 removes %v, want the healthy node-c's owed checkout %s; rows %v", removes, pathC, rowSummary(f.rows(t)))
	}
}

// Rule 3 (finding 4715758385): a job record that names the unconfigured host
// keeps that host's row open, and the visit still reaches the owed checkout on
// a healthy configured host.
func TestReapReviewerUnconfiguredJobRecordStillVisitsTheOwedHost(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(178, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-z"}
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: reviewerNow.Add(-3 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 178, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
		reapReceipt{Time: reviewerNow.Add(-2 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 178, Label: label, Host: "node-b", Worktree: pathB, Cwd: pathB,
			Result: "pending-cleanup", Reason: reviewerDrainHold},
	)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("pass 1 removes %v, want node-b's owed checkout %s despite the job record naming node-z; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}
	kept := false
	for _, row := range f.rows(t) {
		if row.Label == label && row.Host == "node-z" && row.Result == "kept" && row.Reason == "job-host-unconfigured" {
			kept = true
		}
	}
	if !kept {
		t.Fatalf("pass 1 rows %v, want node-z's kept row still open", rowSummary(f.rows(t)))
	}
}
