package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// Regression tests for the review findings on #627. Each fails on 2faf4c5.

// reviewHostlessFix is the two-host fixture the review tests share: node-a is
// the local reviewer host, node-b and node-c hold host-less checkouts.
func reviewHostlessFix(t *testing.T) *reviewerFix {
	t.Helper()
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	return f
}

// A pending host-less row written before FailedSince existed has the same
// reason the new pass writes. The row carrying FailedSince must still be
// persisted, or the bound never starts and the label never settles (hk 6969).
func TestReapReviewerHostlessBoundStartsOnADedupedPendingRow(t *testing.T) {
	f := reviewHostlessFix(t)
	label := reviewerLabelFor(140, 2*24*time.Hour)
	f.hub.inspectErr = map[string]string{"/work-b/" + label + reviewerCheckoutSuffix: "node_timeout"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 140, Label: label, Worktree: label,
		Result: "pending-cleanup", Reason: hostsPendingReason([]string{"node-b"})})

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	f.s.Now = func() time.Time { return reviewerNow.Add(reviewerHostlessMaxAge + time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	want := reviewerHostUnknown + reviewerHostUnprobed + "node-b"
	if !hasHostlessRow(f.rows(t), "kept", want) {
		t.Fatalf("label not settled after the bound from its first failed pass; rows %v", rowSummary(f.rows(t)))
	}
}

// Same persistence fault for an ambiguous row: the names stay the same when
// node-c goes down, so only FailedSince tells the rows apart (hk 6969).
func TestReapReviewerHostlessBoundStartsOnADedupedAmbiguousRow(t *testing.T) {
	f := reviewHostlessFix(t)
	label := reviewerLabelFor(141, 2*24*time.Hour)
	f.hub.detached["/work-b/"+label+reviewerCheckoutSuffix] = true
	f.hub.inspectErr = map[string]string{"/work-c/" + label + reviewerCheckoutSuffix: "node_timeout"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 141, Label: label, Worktree: label,
		Result: "pending-cleanup", Reason: reviewerHostsSeveralPrefix + "node-b, node-c"})

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removed an ambiguous copy: %v", removes)
	}
	f.s.Now = func() time.Time { return reviewerNow.Add(reviewerHostlessMaxAge + time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	want := reviewerHostUnknown + "; " + reviewerHostsSeveralPrefix + "node-b, node-c"
	if !hasHostlessRow(f.rows(t), "kept", want) {
		t.Fatalf("ambiguous label not settled after the bound; rows %v", rowSummary(f.rows(t)))
	}
}

// The sweep visits labels in (rank, name) order: resume locates its start with
// a sort.Search over that order. An ambiguous label must sort by name like any
// other, so it is visited first here, since its name sorts first (hk 6988).
func TestReapReviewerHostlessAmbiguousLabelsKeepNameOrder(t *testing.T) {
	f := reviewHostlessFix(t)
	ambiguous := reviewerLabelFor(150, 2*24*time.Hour)
	fresh := reviewerLabelFor(160, 2*24*time.Hour)
	f.hub.detached["/work-b/"+ambiguous+reviewerCheckoutSuffix] = true
	f.hub.detached["/work-c/"+ambiguous+reviewerCheckoutSuffix] = true
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 150, "label": ambiguous}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}

	f.hub.detached["/work-b/"+fresh+reviewerCheckoutSuffix] = true
	seedDecisionRows(t, f.s, testRepo.Name,
		map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 150, "label": ambiguous}},
		map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 160, "label": fresh}})
	before := len(f.hub.ops("worktree.inspect"))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	probes := f.hub.ops("worktree.inspect")[before:]
	// The first host is probed first for each label, so the first probe of the
	// pass belongs to the label that sorts first by name.
	if len(probes) == 0 || probes[0]["cwd"] != "/work-a/"+ambiguous+reviewerCheckoutSuffix {
		t.Fatalf("pass 2 probed %v first, want the ambiguous label's path first (name order)", probes)
	}
}
