package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A host-less label's checkout is held by one host. When that host is found
// and the only doubt is a host that is down, the checkout is removed on the
// found host, and the label stays pending for the down host alone: the next
// pass probes that host and no other.
func TestReapReviewerHostlessUniqueHostRemovesWhileADownHostStaysPending(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(90, 2*24*time.Hour)
	pathA := "/work-a/" + label + reviewerCheckoutSuffix
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathA] = true
	// A removed worktree is gone from the node, as the real node reports it.
	f.hub.dropRemoved = true
	f.hub.inspectErr = map[string]string{pathB: "node_timeout"}
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 90, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathA {
		t.Fatalf("pass 1 removes %v; rows %v", removes, rowSummary(f.rows(t)))
	}
	wantPending := reviewerHostsPendingPrefix + "node-b" + reviewerHostsPendingSuffix
	pending := 0
	for _, row := range f.rows(t) {
		if row.Result == "pending-cleanup" && row.Host == "" && row.Reason == wantPending {
			pending++
		}
	}
	if pending != 1 {
		t.Fatalf("want one pending row naming only node-b; rows %v", rowSummary(f.rows(t)))
	}

	before := len(f.hub.ops("worktree.inspect"))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	probes := f.hub.ops("worktree.inspect")[before:]
	if len(probes) == 0 {
		t.Fatalf("pass 2 probed no host; rows %v", rowSummary(f.rows(t)))
	}
	for _, p := range probes {
		// pathA's removed row is inspected again, read-only, so a checkout a
		// revival recreated there is seen (hk 6964); the removal guards still
		// decide whether anything is removed.
		if p["cwd"] != pathB && p["cwd"] != pathA {
			t.Fatalf("pass 2 probed %v, want only %s or %s", p["cwd"], pathB, pathA)
		}
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 {
		t.Fatalf("pass 2 removed again: %v", removes)
	}
}

// A host-less label whose only unanswered host is still down settles terminal
// once that host has been down for the bounded wait, and the row names the
// host that was never probed. The wait counts from the first pass that found
// the host down, not from the spawn, so the first failed pass only keeps the
// label pending.
func TestReapReviewerHostlessDownHostSettlesTerminalAfterTheBoundedWait(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(91, reviewerHostlessMaxAge+time.Hour)
	f.hub.inspectErr = map[string]string{"/work-b/" + label + reviewerCheckoutSuffix: "node_timeout"}
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 91, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "pending-cleanup" || rows[0].Reason != hostsPendingReason([]string{"node-b"}) {
		t.Fatalf("first failed pass rows %v, want one pending row owing node-b", rowSummary(rows))
	}

	// Past the bound, counted from that first failed pass, the label settles.
	f.s.Now = func() time.Time { return reviewerNow.Add(reviewerHostlessMaxAge + time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows = f.rows(t)
	want := reviewerHostUnknown + reviewerHostUnprobed + "node-b"
	if len(rows) != 2 || rows[1].Result != "kept" || rows[1].Reason != want {
		t.Fatalf("rows %v, want a kept row %q after the bound", rowSummary(rows), want)
	}
	nrows := len(rows)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != nrows {
		t.Fatalf("terminal label revisited: %v", rowSummary(f.rows(t)))
	}
}

// Host-less probes and legacy-path probes draw on separate allowances. The
// sweep resumes past the legacy labels, so the host-less labels are visited
// first on this host; the legacy labels still get their full allowance.
func TestReapReviewerHostlessProbesDoNotStarveLegacyPathProbes(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	var decisions []map[string]any
	for i := range reviewerHostlessProbesPerHost {
		label := reviewerLabelFor(100+i, 2*24*time.Hour)
		decisions = append(decisions, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 100 + i, "label": label}})
	}
	seedDecisionRows(t, f.s, testRepo.Name, decisions...)
	var legacy []string
	var receipts []reapReceipt
	for i := range reviewerLegacyProbesPerHost {
		pr := 200 + i
		label := reviewerLabelFor(pr, 2*24*time.Hour)
		legacy = append(legacy, label)
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a"}
		receipts = append(receipts, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: pr, Host: "node-a", Label: label,
			Worktree: "/work-a/" + label + reviewerCheckoutSuffix, Result: "skipped", Reason: reviewerAlreadyGone})
		f.hub.detached["/work-a/"+label] = true
	}
	// seedReapRows rewrites the log, so every legacy row goes in one call.
	seedReapRows(t, f.s, testRepo.Name, receipts...)
	// A cursor past every legacy label makes the pass visit the host-less
	// labels, which sort last, before the legacy labels on the host.
	if err := f.s.saveReviewerSweepCursor(testRepo.Name, reviewerCursor{Host: f.s.Config.Hosts[0].Name, Label: "zzz"}); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removed := map[string]bool{}
	for _, rm := range f.hub.ops("worktree.remove") {
		removed[rm["cwd"].(string)] = true
	}
	for _, label := range legacy {
		if !removed["/work-a/"+label] {
			t.Fatalf("legacy label %s was not removed; removes %v; rows %v", label, f.hub.ops("worktree.remove"), rowSummary(f.rows(t)))
		}
	}
}

// When the found host's removal stops (here a pane still uses the checkout),
// the host that is down is still owed its probe: the label's pending row names
// it before the removal, so the next pass probes it.
func TestReapReviewerHostlessOwedHostSurvivesAStoppedRemoval(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true}}
	label := reviewerLabelFor(92, 2*24*time.Hour)
	pathA := "/work-a/" + label + reviewerCheckoutSuffix
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathA] = true
	f.hub.inspectErr = map[string]string{pathB: "node_timeout"}
	f.addPane("w9:p9", "scratch", pathA)
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 92, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 1 removed a checkout in use: %v", removes)
	}
	wantPending := reviewerHostsPendingPrefix + "node-b" + reviewerHostsPendingSuffix
	found := false
	for _, row := range f.rows(t) {
		if row.Host == "" && row.Reason == wantPending {
			found = true
		}
	}
	if !found {
		t.Fatalf("no pending row names node-b; rows %v", rowSummary(f.rows(t)))
	}
	// Discovery waits for the found host's removal (it may still hold the
	// checkout), so the pane is released first; then the owed probe runs.
	keep := []any{}
	for _, p := range f.hub.agents["panes"].([]any) {
		if p.(map[string]any)["pane_id"] != "w9:p9" {
			keep = append(keep, p)
		}
	}
	f.hub.agents["panes"] = keep
	f.hub.agents["agents"] = keep
	before := len(f.hub.ops("worktree.inspect"))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range f.hub.ops("worktree.inspect")[before:] {
		if p["cwd"] == pathB {
			return
		}
	}
	t.Fatalf("pass 2 did not probe node-b; rows %v", rowSummary(f.rows(t)))
}

// A host whose scan fails this pass does not block the probes still owed to
// other hosts: the next pass probes the recovered host even though the found
// host is down.
func TestReapReviewerHostlessFailedFoundHostDoesNotBlockOwedProbe(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true}}
	label := reviewerLabelFor(93, 2*24*time.Hour)
	pathA := "/work-a/" + label + reviewerCheckoutSuffix
	pathB := "/work-b/" + label + reviewerCheckoutSuffix
	f.hub.detached[pathA] = true
	f.hub.inspectErr = map[string]string{pathB: "node_timeout"}
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 93, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 1 || removes[0]["cwd"] != pathA {
		t.Fatalf("pass 1 removes %v; rows %v", removes, rowSummary(f.rows(t)))
	}
	// Pass 2: node-a's scan fails (a pane's inspect times out) and node-b recovers.
	f.addPane("w9:p9", "scratch", "/scratch-x")
	f.hub.inspectErr = map[string]string{"/scratch-x": "node_timeout"}
	before := len(f.hub.ops("worktree.inspect"))
	// node-a's failed scan is reported as the pass's error; that is expected.
	_ = f.reap(t, 0, Options{})
	for _, p := range f.hub.ops("worktree.inspect")[before:] {
		if p["cwd"] == pathB {
			return
		}
	}
	t.Fatalf("pass 2 did not probe node-b while node-a failed; rows %v", rowSummary(f.rows(t)))
}

// Two reachable hosts both hold the host-less label's checkout, so neither copy
// is proven to be the spawn's: nothing is removed, the label stays pending with
// a reason that names both hosts, and the next pass visits it again.
func TestReapReviewerHostlessTwoCopiesStayPendingAndAreRevisited(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(94, 2*24*time.Hour)
	f.hub.detached["/work-b/"+label+reviewerCheckoutSuffix] = true
	f.hub.detached["/work-c/"+label+reviewerCheckoutSuffix] = true
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 94, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 1 removed a copy of an ambiguous checkout: %v; rows %v", removes, rowSummary(f.rows(t)))
	}
	want := reviewerHostsSeveralPrefix + "node-b, node-c"
	pending := 0
	for _, row := range f.rows(t) {
		if row.Result == "pending-cleanup" && row.Host == "" && row.Reason == want {
			pending++
		}
	}
	if pending != 1 {
		t.Fatalf("want one pending row naming node-b and node-c; rows %v", rowSummary(f.rows(t)))
	}

	before := len(f.hub.ops("worktree.inspect"))
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.hub.ops("worktree.inspect")[before:]) == 0 {
		t.Fatalf("pass 2 did not visit the ambiguous label; rows %v", rowSummary(f.rows(t)))
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("pass 2 removed a copy of an ambiguous checkout: %v", removes)
	}
}

// Past the bounded wait an ambiguous host-less label settles terminal, and its
// row names both hosts that hold a copy. Nothing is removed.
func TestReapReviewerHostlessTwoCopiesSettleTerminalAfterTheBoundedWait(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
	f.s.Config.Hosts[0].WorkRoot = "/work-a"
	f.hub.moreNodes = []any{
		map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
	}
	label := reviewerLabelFor(95, reviewerHostlessMaxAge+time.Hour)
	f.hub.detached["/work-b/"+label+reviewerCheckoutSuffix] = true
	f.hub.detached["/work-c/"+label+reviewerCheckoutSuffix] = true
	seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 95, "label": label}})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("aged ambiguous checkout was removed: %v", removes)
	}
	want := reviewerHostUnknown + "; " + reviewerHostsSeveralPrefix + "node-b, node-c"
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != want {
		t.Fatalf("rows %v, want one kept row %q", rowSummary(rows), want)
	}
	nrows := len(rows)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != nrows {
		t.Fatalf("terminal label revisited: %v", rowSummary(f.rows(t)))
	}
}

// Two hosts hold a host-less label's checkout. On the next pass one of them
// cannot answer. A single copy seen now proves nothing about that host, so no
// copy is removed on that pass or after the host recovers, and both hosts stay
// named as holding a copy.
func TestReapReviewerHostlessAmbiguityHoldsWhenACopyHostIsDown(t *testing.T) {
	for _, tc := range []struct{ name, down string }{{"node-b down", "node-b"}, {"node-c down", "node-c"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReviewerFix(t, reap.Host{Name: "node-b", Machine: "node-b", WorkRoot: "/work-b"}, reap.Host{Name: "node-c", Machine: "node-c", WorkRoot: "/work-c"})
			f.s.Config.Hosts[0].WorkRoot = "/work-a"
			f.hub.moreNodes = []any{
				map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
				map[string]any{"machine_id": "node-c", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
			}
			label := reviewerLabelFor(96, 2*24*time.Hour)
			pathB := "/work-b/" + label + reviewerCheckoutSuffix
			pathC := "/work-c/" + label + reviewerCheckoutSuffix
			f.hub.detached[pathB] = true
			f.hub.detached[pathC] = true
			seedDecisionRows(t, f.s, testRepo.Name, map[string]any{"command": "reviewer-held", "value": map[string]any{"pr": 96, "label": label}})
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
				t.Fatalf("pass 1 removed: %v", removes)
			}

			// Pass 2: the down host cannot be inspected.
			downPath := "/work-b/" + label + reviewerCheckoutSuffix
			if tc.down == "node-c" {
				downPath = pathC
			}
			f.hub.inspectErr = map[string]string{downPath: "node_timeout"}
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
				t.Fatalf("pass 2 removed a copy while %s could not answer: %v; rows %v", tc.down, removes, rowSummary(f.rows(t)))
			}
			want := reviewerHostsSeveralPrefix + "node-b, node-c"
			if !hasHostlessRow(f.rows(t), "pending-cleanup", want) {
				t.Fatalf("pass 2 lost the names of both copy hosts; rows %v", rowSummary(f.rows(t)))
			}

			// Pass 3: the host answers again; both copies are still there.
			f.hub.inspectErr = nil
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
				t.Fatalf("pass 3 removed an ambiguous copy: %v; rows %v", removes, rowSummary(f.rows(t)))
			}
		})
	}
}

// hasHostlessRow reports whether a host-less row with the result and reason exists.
func hasHostlessRow(rows []reapReceipt, result, reason string) bool {
	for _, row := range rows {
		if row.Host == "" && row.Result == result && row.Reason == reason {
			return true
		}
	}
	return false
}
