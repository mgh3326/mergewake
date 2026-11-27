package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// hk 7011: an open job-host-unconfigured row keeps its host as the label's
// host only while no other stream owes work on a configured host.

// The node-z / node-b case: the job record names node-z, which is not
// configured, and a drain-held removal is owed on node-b. Every pass pinned
// node-z, so node-b's checkout was never reached. The pass removes it.
func TestReapReviewerUnconfiguredPinYieldsToAConfiguredHostOwingWork(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(174, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 174, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 174, Label: label, Host: "node-b", Worktree: label,
			Result: "pending-cleanup", Reason: reviewerDrainHold},
	)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("pass 1 removes %v, want node-b's owed checkout %s; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}
}

// With only the unconfigured host holding owed work, it stays the label's
// host: no configured host is visited, and its own row stays owed.
func TestReapReviewerUnconfiguredPinStaysWhenOnlyItOwesWork(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(175, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 175, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 175, Label: label, Host: "node-b", Worktree: label,
			Result: "removed"},
	)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 1 removes %v, want none: the label's host stays node-z; rows %v", removes, rowSummary(f.rows(t)))
	}
	owed := false
	for _, row := range f.rows(t) {
		if row.Label == label && row.Host == "node-z" && row.Result == "kept" && row.Reason == "job-host-unconfigured" {
			owed = true
		}
	}
	if !owed {
		t.Fatalf("pass 1 rows %v, want node-z's kept row still owed", rowSummary(f.rows(t)))
	}
}
