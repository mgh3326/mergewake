package pipeline

import (
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// A removed row on a host+path does not hide a checkout recreated at that
// path (hk 6964). The label stays a candidate through its pane-stream row; the
// removed worktree row is the prior of the path. With the tree back and no
// pane, the guarded removal runs; with a pane on it, the pane keeps it.
func TestReapReviewerRemovedRowDoesNotHideRecreatedCheckout(t *testing.T) {
	label := reviewerLabelFor(12, time.Hour)
	path := "/work/" + label + reviewerCheckoutSuffix
	seed := func(f *reviewerFix) int {
		f.s.Config.Hosts[0].WorkRoot = "/work"
		host := f.s.Config.Hosts[0].Name
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: host}
		seedReapRows(t, f.s, testRepo.Name,
			reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Pane: "fixture-pane-a", Label: label,
				Result: "pending-cleanup", Reason: "pane still present; panewire closes it and a later sweep cleans up"},
			reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Label: label, Worktree: path, Cwd: path,
				Result: "removed"},
		)
		return len(f.rows(t))
	}

	t.Run("recreated tree with no pane is removed", func(t *testing.T) {
		f := newReviewerFix(t)
		seed(f)
		f.hub.detached[path] = true
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		removes := f.hub.ops("worktree.remove")
		if len(removes) != 1 || removes[0]["cwd"] != path {
			t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
		}
		rows := f.rows(t)
		if last := rows[len(rows)-1]; last.Result != "removed" || last.Worktree != path {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
	})

	t.Run("recreated tree with a pane is kept pane-in-use", func(t *testing.T) {
		f := newReviewerFix(t)
		seed(f)
		f.hub.detached[path] = true
		f.addPane("fixture-pane-b", "other", path)
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if removes := f.hub.ops("worktree.remove"); len(removes) != 0 {
			t.Fatalf("removes: %v (rows %v)", removes, rowSummary(f.rows(t)))
		}
		rows := f.rows(t)
		if last := rows[len(rows)-1]; last.Result != "kept" || last.Reason != "pane-in-use" || last.Worktree != path {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
	})

	t.Run("absent tree settles once and removes nothing", func(t *testing.T) {
		f := newReviewerFix(t)
		before := seed(f)
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
		if last := rows[len(rows)-1]; last.Result != "skipped" || last.Reason != reviewerRemovedAbsent || last.Worktree != path {
			t.Fatalf("settle row: %v", rowSummary(rows))
		}
		// Within a day the settled path is not inspected again: no row, no probe.
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if after := len(f.rows(t)); after != before+1 {
			t.Fatalf("settled path was revisited: %v", rowSummary(f.rows(t)))
		}
	})
}

// An abandon error row from a refused close, followed by a stuck-reviewer
// marker, must not block one-off reap runs forever (hk 6964). Each pass is a
// fresh process (no in-memory retry state). The first pass's refused close
// writes a row whose deadline is set; the second pass, past that deadline,
// retries the close and makes progress.
func TestReapReviewerAbandonErrorRowDoesNotBlockFreshReaps(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.clearPanes()
	host := f.s.Config.Hosts[0].Name
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Pane: "fixture-pane-a", Label: f.label,
			Result: "error", Reason: "abandon job: panewire exit status 5 owner_mismatch", RetryAfter: reviewerNow.Add(-time.Hour)},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Host: host, Pane: "fixture-pane-a", Label: f.label,
			Result: "stuck-reviewer", Reason: "job spawned with no result"},
	)

	refuseJobClose(f.reviewerFix, "close refused: job "+f.label+" owner_mismatch")
	f.s.reapRetryState = nil
	_ = f.passAt(t, 0)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("pass 1 job closes=%d, want the refused close attempted once: %v", n, rowSummary(f.rows(t)))
	}

	f.s.Runner = f.run
	f.allowJobClose()
	f.s.reapRetryState = nil
	if err := f.passAt(t, 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()); n != 2 {
		t.Fatalf("pass 2 job closes=%d, want the close retried and attempted: %v", n, rowSummary(f.rows(t)))
	}
	if rows := f.resultRows(t, "abandoned"); len(rows) != 1 {
		t.Fatalf("abandoned rows: %v", rowSummary(f.rows(t)))
	}
}
