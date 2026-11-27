package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// Item 1: a pane-only other-repository skip on a remote host is terminal. The
// #605 exception reopens a label only for a closed pane row; a skip for another
// repository's reviewer is never visited again, so an aged label whose record is
// gone is not removed for that repository.
func TestReapReviewerOtherRepoSkipOnRemoteHostStaysTerminal(t *testing.T) {
	f := newStuckFix(t)
	f.label = reviewerLabelFor(12, 25*time.Hour)
	f.clearPanes()
	seed := reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Host: "node-a", Result: "skipped", Reason: reviewerOtherRepo + "github.com/other/repo"}
	seedReapRows(t, f.s, testRepo.Name, seed)
	f.passAfter(t, 0)
	f.passAfter(t, time.Hour)
	if rows := f.rows(t); len(rows) != 1 {
		t.Fatalf("the other-repository skip was visited again: %v", rowSummary(rows))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("worktree.remove=%d for another repository's reviewer: %v", n, rowSummary(f.rows(t)))
	}
}

// Item 2: an aged label whose close is refused as job_unknown after its backoff
// has expired still gets its cleanup. The kept row and its backoff remain.
func TestReapReviewerAgedJobUnknownAfterBackoffStillReleasesCheckout(t *testing.T) {
	f := newStuckJobFix(t, 25*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	// A kept refusal whose backoff has passed, read by a fresh process: no
	// in-memory backoff is left, so the aged label's close is retried.
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "kept", Reason: reviewerNotClosable, RetryAfter: reviewerNow.Add(time.Hour)})
	f.clearPanes()
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d, want the expired backoff retried once: %v", n, rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d, want the aged checkout released past its backoff: %v", n, rowSummary(f.rows(t)))
	}
}

// Item 3: a stuck-reviewer marker written after a job_unknown refusal does not
// hide the refusal. The refusal is the newest close outcome on the pane stream,
// so the close stays backed off on a fresh pass and is not repeated.
func TestReapReviewerStuckMarkerDoesNotHideCloseRefusal(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, 8*time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f, jobUnknownStderr(label))
	seedReapRows(t, f.s, testRepo.Name,
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: label, Result: "kept", Reason: reviewerNotClosable, RetryAfter: reviewerNow.Add(time.Hour)},
		reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: label, Result: "stuck-reviewer", Reason: "job spawned with no result"},
	)

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("job closes=%d inside the refusal's backoff: %v", n, rowSummary(f.rows(t)))
	}
}
