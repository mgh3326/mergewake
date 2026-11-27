package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// hk 7024: the owed host is the newest open row that can progress this pass. A
// kept identity-changed row, or a refusal still inside its backoff, is passed
// over, so an older owed removal on a healthy host is still reached.

// Here node-c's newest row is a kept identity-changed checkout, and node-b holds
// an older owed removal. The unconfigured node-z pin yields to node-b, not to
// node-c, so node-b's checkout is removed.
func TestReapReviewerOwedHostSkipsAnIdentityChangedRow(t *testing.T) {
	f := newReviewerFix(t,
		reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"},
		reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(190, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	pathC := "/work-c/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: reviewerNow.Add(-4 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 190, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
		reapReceipt{Time: reviewerNow.Add(-3 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 190, Label: label, Host: "node-b", Worktree: pathB, Cwd: pathB,
			Result: "pending-cleanup", Reason: reviewerDrainHold},
		reapReceipt{Time: reviewerNow.Add(-1 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 190, Label: label, Host: "node-c", Worktree: pathC, Cwd: pathC,
			Result: "kept", Reason: "worktree identity changed"},
	)

	if err := f.reap(t, 0, Options{}); err != nil && !strings.Contains(err.Error(), "disconnected") {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("pass 1 removes %v, want node-b's owed checkout %s past node-c's identity-changed row; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}
}

// Here node-c's newest row is a refusal still inside its backoff, so it cannot
// progress this pass; node-b's older owed removal is the one reached.
func TestReapReviewerOwedHostSkipsARefusalInItsBackoff(t *testing.T) {
	f := newReviewerFix(t,
		reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"},
		reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(191, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	pathC := "/work-c/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Time: reviewerNow.Add(-4 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 191, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
		reapReceipt{Time: reviewerNow.Add(-3 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 191, Label: label, Host: "node-b", Worktree: pathB, Cwd: pathB,
			Result: "pending-cleanup", Reason: reviewerDrainHold},
		reapReceipt{Time: reviewerNow.Add(-1 * time.Hour), Action: "reviewer-reap", Repo: testRepo.Name, PR: 191, Label: label, Host: "node-c", Worktree: pathC, Cwd: pathC,
			Result: "pending-cleanup", Reason: reviewerRefused + "detached"},
	)

	if err := f.reap(t, 0, Options{}); err != nil && !strings.Contains(err.Error(), "disconnected") {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("pass 1 removes %v, want node-b's owed checkout %s past node-c's refusal in backoff; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}
}
