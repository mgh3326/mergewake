package pipeline

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// A reviewer checkout whose worktree inspect the repo budget cuts is a budget
// stop: the checkout is kept for a later sweep, no error row is written, and
// the pass's only stop is ReapBudgetStop with no other error.
func TestReapReviewerInspectBudgetCutIsAStopNotAnError(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	// The first pass sees the pane and writes the checkout's receipt; once the
	// pane is gone the second pass reaches the checkout's guarded removal.
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	f.hub.hangInspect = "/repo/" + label
	f.s.Config.Reap.RepoBudget = "1s"

	err := f.reap(t, 0, Options{})
	var stop *ReapBudgetStop
	if !errors.As(err, &stop) {
		t.Fatalf("the pass did not return a budget stop: %v", err)
	}
	if other := ReapNonBudgetErrors(err); other != nil {
		t.Fatalf("a budget-cut inspect added a non-budget error: %v", other)
	}
	if n := len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("a checkout whose inspect the budget cut was removed: %d guarded removals", n)
	}
	var held []reapReceipt
	for _, v := range f.rows(t) {
		if v.Result == "error" || v.Result == "removed" {
			t.Fatalf("row %s %s: %s", v.Label, v.Result, v.Reason)
		}
		if v.Result == "pending-cleanup" && v.Reason == reviewerBudgetHold {
			held = append(held, v)
		}
	}
	if len(held) != 1 {
		t.Fatalf("want one budget hold row, rows: %v", rowSummary(f.rows(t)))
	}
}

// A worktree inspect that fails for a reason other than the budget is still an
// error row, and a non-budget error of the pass.
func TestReapReviewerInspectFailureIsStillAnErrorRow(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	// A node the hub cannot serve is a host skip, not this error (see
	// TestReapReviewerGatewayInspectIsASkippedHost), so the failure is a body
	// error for this cwd.
	f.hub.inspectErr = map[string]string{"/repo/" + label: "invalid_request"}

	err := f.reap(t, 0, Options{})
	if ReapNonBudgetErrors(err) == nil || !strings.Contains(err.Error(), "worktree inspect") {
		t.Fatalf("a real inspect failure was not a pass error: %v", err)
	}
	var found bool
	for _, v := range f.rows(t) {
		if v.Result == "error" && strings.HasPrefix(v.Reason, "worktree inspect: ") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no error row for the failed inspect: %v", rowSummary(f.rows(t)))
	}
}
