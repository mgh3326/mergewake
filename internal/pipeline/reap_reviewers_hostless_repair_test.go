package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// Hostless repair tests: one per open host-less lookup finding. Each fails on
// the base it was written against and passes on the repaired head.

// hk 6880: a host-less label the old sweep ended with the bare unknown reason
// is visited once through the lookup after upgrade, so a clean remote checkout
// is released. The lookup then writes its own terminal row and the label is
// not visited again.
func TestReapReviewerHostlessBareUnknownRowIsRequeuedOnce(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(110, 2*reviewerHostlessMaxAge)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 110, Label: label, Worktree: label, Result: "kept", Reason: reviewerHostUnknown})

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("pass 1 removes %v, want %s; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}

	before := len(f.hub.ops("worktree.inspect"))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if probes := f.hub.ops("worktree.inspect")[before:]; len(probes) != 0 {
		t.Fatalf("pass 2 visited the requeued label again: %v; rows %v", probes, rowSummary(f.rows(t)))
	}
}

// hk 6969: the 7-day bound counts from the first pass that found the host
// down, not from the spawn. The aged label stays pending on that first failed
// pass and settles only once the bound has passed from it.
// (TestReapReviewerHostlessDownHostSettlesTerminalAfterTheBoundedWait in
// reap_reviewers_hostless_test.go carries the assertions.)

// hk 6971: a host-less discovery left open is resumed even when a later receipt
// names a host that is not configured. The owed host's copy is removed.
func TestReapReviewerHostlessOpenDiscoveryResumesWhenAReceiptNamesAnUnconfiguredHost(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(111, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 111, Label: label, Worktree: label,
			Result: "pending-cleanup", Reason: hostsPendingReason([]string{"node-b"})},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 111, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
	)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("removes %v, want the owed node-b copy %s; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}
}

// hk 6988: pending ambiguous labels do not starve host-less labels that have
// not been probed. The new label is probed first, so it gets the host's
// allowance even when five ambiguous labels sort before it by name.
func TestReapReviewerHostlessNewLabelIsProbedBeforeAmbiguousRevisits(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	var decisions []map[string]any
	for i := range reviewerHostlessProbesPerHost {
		label := reviewerLabelFor(120+i, 2*24*time.Hour)
		f.hub.detached["/work-b/"+label+reviewerCheckoutSuffix] = true
		f.hub.detached["/work-c/"+label+reviewerCheckoutSuffix] = true
		decisions = append(decisions, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 120 + i, "label": label}})
	}
	seedDecisionRows(t, f.s, testRepo.Name, decisions...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 1 removed an ambiguous copy: %v", removes)
	}

	// A single-copy label that sorts after the ambiguous ones is seen for the
	// first time on pass 2.
	fresh := reviewerLabelFor(190, 2*24*time.Hour)
	freshPath := "/work-b/" + fresh + reviewerCheckoutSuffix
	f.hub.detached[freshPath] = true
	decisions = append(decisions, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 190, "label": fresh}})
	seedDecisionRows(t, f.s, testRepo.Name, decisions...)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != freshPath {
		t.Fatalf("pass 2 removes %v, want the new label's copy %s; rows %v", removes, freshPath, rowSummary(f.rows(t)))
	}
}

// hk 6989: an ambiguous row names the hosts that hold a copy now, plus the
// named hosts that could not be probed this pass. A named host that could not
// answer keeps its name when a third host also holds a copy.
func TestReapReviewerHostlessAmbiguousNamesKeepAnUnansweredCopyHost(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"}, reap.Host{Name: "node-d", Machine: "node-d", WorkRoot: "/work-d"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-d", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(130, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	pathC := "/work-c/" + label + reviewerCheckoutSuffix
	pathD := "/work-d/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	f.hub.detached[pathC] = true
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 130, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if want := reviewerHostsSeveralPrefix + "node-b, node-c"; !hasHostlessRow(f.rows(t), "pending-cleanup", want) {
		t.Fatalf("pass 1 rows %v, want names %q", rowSummary(f.rows(t)), want)
	}

	// Pass 2: node-b still holds a copy, node-c cannot answer, and node-d now
	// holds one too. node-c must keep its name.
	f.hub.detached[pathD] = true
	f.hub.inspectErr = map[string]string{pathC: "node_timeout"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 2 removed an ambiguous copy: %v", removes)
	}
	if want := reviewerHostsSeveralPrefix + "node-b, node-c, node-d"; !hasHostlessRow(f.rows(t), "pending-cleanup", want) {
		t.Fatalf("pass 2 rows %v, want names %q", rowSummary(f.rows(t)), want)
	}
}

// hk 6989, second case: a named host that lost its copy is dropped from the
// names when one copy is seen now and another host cannot answer.
func TestReapReviewerHostlessAmbiguousNamesDropAHostThatLostItsCopy(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"}, reap.Host{Name: "node-d", Machine: "node-d", WorkRoot: "/work-d"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-d", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(131, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	pathC := "/work-c/" + label + reviewerCheckoutSuffix
	pathD := "/work-d/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	f.hub.detached[pathC] = true
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 131, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}

	// Pass 2: node-b lost its copy, node-c still holds one, node-d cannot answer.
	delete(f.hub.detached, pathB)
	f.hub.inspectErr = map[string]string{pathD: "node_timeout"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 2 removed while node-d could not answer: %v", removes)
	}
	if want := reviewerHostsSeveralPrefix + "node-c"; !hasHostlessRow(f.rows(t), "pending-cleanup", want) {
		t.Fatalf("pass 2 rows %v, want only node-c named as %q", rowSummary(f.rows(t)), want)
	}
}
