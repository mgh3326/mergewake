package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// Regression tests for the four open findings on the host-less reviewer lookup
// (hk 6998, 6999, 7000, 7001). Each fails on 95c9678 and passes on the head.

// hk 6998: an ambiguous row whose named hosts all answer with no copy, while an
// unnamed host is still down, keeps that host as owed. It never writes a
// "checkout found on several hosts: " row with no names.
func TestReapReviewerHostlessEmptyAmbiguousNamesOwesTheUnansweredHost(t *testing.T) {
	f := newReviewerFix(t,
		reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"},
		reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"},
		reap.Host{Name: "node-d", Machine: "node-d", WorkRoot: "/work-d"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-d", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(170, 2*24*time.Hour)
	f.hub.inspectErr = map[string]string{"/work-d/" + label + reviewerCheckoutSuffix: "node_timeout"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 170, Label: label, Worktree: label,
		Result: "pending-cleanup", Reason: reviewerHostsSeveralPrefix + "node-b, node-c"})

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if want := hostsPendingReason([]string{"node-d"}); !hasHostlessRow(f.rows(t), "pending-cleanup", want) {
		t.Fatalf("pass 1 rows %v, want the unanswered node-d owed as %q", rowSummary(f.rows(t)), want)
	}
}

// hk 6999: a bare-unknown label that a journal row names an unconfigured host
// for is visited with that host in the same pass. The host keeps its owed row;
// the lookup does not write the terminal "none holds the checkout" row.
func TestReapReviewerHostlessBareUnknownRequeueTakesTheJournalHost(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(171, 2*reviewerHostlessMaxAge)
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 171, Label: label, Worktree: label,
		Result: "kept", Reason: reviewerHostUnknown})
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 171, "label": label, "host": "node-z"}})

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	owed := false
	for _, row := range f.rows(t) {
		if row.Label == label && row.Host == "node-z" && row.Result == "kept" && row.Reason == "job-host-unconfigured" {
			owed = true
		}
	}
	if !owed || hasHostlessRow(f.rows(t), "kept", reviewerHostlessNoCopy) {
		t.Fatalf("pass 1 rows %v, want the journal's node-z kept as owed, not a terminal no-copy row", rowSummary(f.rows(t)))
	}
}

// hk 7000: discovery under an unconfigured job host finds a copy on node-b.
// The unconfigured host stays the label's host, so once node-z is configured
// again its worktree is the one removed.
func TestReapReviewerHostlessDiscoveryKeepsTheUnconfiguredJobHost(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(172, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	pathZ := "/work-z/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathB] = true
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 172, Label: label, Worktree: label,
			Result: "pending-cleanup", Reason: hostsPendingReason([]string{"node-b"})},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 172, Label: label, Host: "node-z", Worktree: label,
			Result: "kept", Reason: "job-host-unconfigured"},
	)

	// Pass 1: node-z is not configured; the owed node-b copy is removed.
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathB {
		t.Fatalf("pass 1 removes %v, want the owed node-b copy %s; rows %v", removes, pathB, rowSummary(f.rows(t)))
	}

	// Pass 2: node-z is configured again and holds its own copy.
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "node-z", Machine: "node-z", WorkRoot: "/work-z"})
	f.hub.moreNodes = append(f.hub.moreNodes, map[string]any{"machine_id": "node-z", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true})
	f.hub.detached[pathZ] = true
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range f.hub.ops("worktree.remove") {
		if r["cwd"] == pathZ {
			found = true
		}
	}
	if !found {
		t.Fatalf("pass 2 never targeted node-z's worktree %s; removes %v; rows %v", pathZ, f.hub.ops("worktree.remove"), rowSummary(f.rows(t)))
	}
}

// hk 7001: a blocked pass (a probe that is not a down host) carries the
// FailedSince of the first failed pass. The 7-day bound then counts from that
// first failure, so the label settles on the pass past the bound.
func TestReapReviewerHostlessBlockedPassCarriesTheFailedBound(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(173, 2*24*time.Hour)
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 173, Label: label, Worktree: label,
		Result: "pending-cleanup", Reason: hostsPendingReason([]string{"node-b"})})

	// Pass 1 at the first failure: node-b is down.
	f.hub.inspectErr = map[string]string{pathB: "node_timeout"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}

	// Pass 2, three days on: node-b answers with a non-unreachable error.
	f.s.Now = func() time.Time { return reviewerNow.Add(3 * 24 * time.Hour) }
	f.hub.inspectErr = map[string]string{pathB: "invalid_request"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}

	// Pass 3, past the bound counted from pass 1: node-b is down again.
	f.s.Now = func() time.Time { return reviewerNow.Add(reviewerHostlessMaxAge + time.Hour) }
	f.hub.inspectErr = map[string]string{pathB: "node_timeout"}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	want := reviewerHostUnknown + reviewerHostUnprobed + "node-b"
	if !hasHostlessRow(f.rows(t), "kept", want) {
		t.Fatalf("pass 3 rows %v, want the label settled from its first failed pass as %q", rowSummary(f.rows(t)), want)
	}
}
