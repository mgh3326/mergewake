package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// An aged ambiguous host-less label whose only doubt this pass is a probe cap
// stays pending. The cap is not a failed host, so the label is neither settled
// terminal from its spawn nor given a bound it did not earn (review of 23fce75).
func TestReapReviewerHostlessAgedAmbiguousLabelHeldByProbeCapStaysPending(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	var rows []reapReceipt
	// Five ambiguous labels sort first and spend the ambiguous allowance on
	// both hosts.
	for i := range reviewerHostlessProbesPerHost {
		label := reviewerLabelFor(100+i, 2*24*time.Hour)
		f.hub.detached["/work-b/"+label+reviewerCheckoutSuffix] = true
		f.hub.detached["/work-c/"+label+reviewerCheckoutSuffix] = true
		rows = append(rows, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 100 + i, Label: label, Worktree: label,
			Result: "pending-cleanup", Reason: reviewerHostsSeveralPrefix + "node-b, node-c"})
	}
	// The aged label sorts after them, so both of its probes are capped.
	aged := reviewerLabelFor(200, reviewerHostlessMaxAge+time.Hour)
	f.hub.detached["/work-b/"+aged+reviewerCheckoutSuffix] = true
	rows = append(rows, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 200, Label: aged, Worktree: aged,
		Result: "pending-cleanup", Reason: reviewerHostsSeveralPrefix + "node-b, node-c"})
	seedReapRows(t, f.s, testRepo.Name, rows...)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var last *reapReceipt
	for _, r := range f.rows(t) {
		if r.Label == aged && r.Host == "" {
			r := r
			last = &r
		}
	}
	if last == nil {
		t.Fatalf("no host-less row for the aged label; rows %v", rowSummary(f.rows(t)))
	}
	if last.Result != "pending-cleanup" {
		t.Fatalf("probe cap settled the aged label: %v", rowSummary([]reapReceipt{*last}))
	}
}
