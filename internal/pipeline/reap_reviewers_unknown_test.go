package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// jobUnknownStderr is the refusal panewire gives a close of a job its hub
// does not know (exit 5, as closeRefusal reports it).
func jobUnknownStderr(label string) string {
	return "close refused: job_unknown: " + label + " is not in the hub's job store"
}

// (a) A stuck reviewer job whose close the hub refuses as job_unknown, with a
// terminal event in its node's job dir, is the already-closed case: one closed
// row names the event, no error, no later close, and the worktree is removed
// as after any close.
func TestReapReviewerJobUnknownWithTerminalEventIsClosed(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":true}`)
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	rows := f.resultRows(t, "closed")
	if len(rows) != 1 || rows[0].Reason != "already closed (job_unknown, terminal event job.completed)" {
		t.Fatalf("closed rows: %v", rowSummary(f.rows(t)))
	}
	if errs := f.resultRows(t, "error"); len(errs) != 0 {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
}

// (b) and (c) A stuck reviewer job whose close the hub refuses as job_unknown,
// with no terminal event in its node's job dir or with that dir unreadable,
// gets one kept row, no error, and no later close. The worktree stays.
func TestReapReviewerJobUnknownWithoutTerminalEventIsKept(t *testing.T) {
	setups := map[string]func(*stuckFix){
		"no terminal event": func(f *stuckFix) {
			f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
		},
		"unreadable dir": func(f *stuckFix) {
			f.hub.jobResultError = "node unreachable"
		},
	}
	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			f := newStuckJobFix(t, 7*time.Hour)
			setup(f)
			refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
			f.passAfter(t, 0)
			f.clearPanes()
			f.passAfter(t, 30*time.Minute)
			f.passAfter(t, 3*time.Hour)
			f.passAfter(t, 6*time.Hour)
			if n := len(f.jobCloses()); n != 1 {
				t.Fatalf("job closes=%d, want one attempt and no retry: %v", n, rowSummary(f.rows(t)))
			}
			if got := notClosableRows(t, f); len(got) != 1 {
				t.Fatalf("not-closable rows=%d: %v", len(got), rowSummary(f.rows(t)))
			}
			if errs := f.resultRows(t, "error"); len(errs) != 0 {
				t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
			}
			if n := len(f.hub.ops("worktree.remove")); n != 0 {
				t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
			}
		})
	}
}

// A label whose not-closable row survives a restart is never closed again:
// the receipt, not the hub, decides.
func TestReapReviewerNotClosableIsNotClosedAfterRestart(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "kept", Reason: reviewerNotClosable})
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	if errs := f.resultRows(t, "error"); len(errs) != 0 {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
}

// A job_unknown close settled as closed names its terminal event; after a
// restart that closed row still ends the close, as the already-closed row does.
func TestReapReviewerJobUnknownClosedRowIsNotClosedAfterRestart(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "closed", Reason: "already closed (job_unknown, terminal event job.completed)"})
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
}

// (b) on the spawned path: a finished pane whose job the hub does not know is
// closed once, settled with one kept row, and not closed again on the next pass.
func TestReapReviewerSpawnedJobUnknownWithoutTerminalEventIsKept(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f, jobUnknownStderr(label))

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	var kept int
	for _, r := range f.rows(t) {
		if r.Result == "error" {
			t.Fatalf("error row: %v", rowSummary(f.rows(t)))
		}
		if r.Result == "kept" && r.Reason == reviewerNotClosable {
			kept++
		}
	}
	if kept != 1 {
		t.Fatalf("not-closable rows=%d: %v", kept, rowSummary(f.rows(t)))
	}
}

// (d) A close refused for another reason, here owned by another lane, is not
// job_unknown: it stays owed as today, an error row and a retry on a later pass,
// even when the node's job dir holds a terminal event.
func TestReapReviewerOwnedByLaneRefusalStaysOwed(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":true}`)
	refuseJobClose(f.reviewerFix, "close refused: job "+f.label+" owned by lane lane-y")
	f.passAfter(t, 0)
	f.clearPanes()
	if err := f.passAt(t, 30*time.Minute); err == nil {
		t.Fatalf("refused close did not fail its pass: %v", rowSummary(f.rows(t)))
	}
	if n := len(f.resultRows(t, "closed")); n != 0 {
		t.Fatalf("closed rows: %v", rowSummary(f.rows(t)))
	}
	errs := f.resultRows(t, "error")
	if len(errs) != 1 || !strings.HasPrefix(errs[0].Reason, "abandon job: ") {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
	_ = f.passAt(t, 30*time.Minute)
	_ = f.passAt(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 2 {
		t.Fatalf("job closes=%d, want the refused close retried: %v", n, rowSummary(f.rows(t)))
	}
}

// notClosableRows are the kept rows that name a job the hub cannot close.
func notClosableRows(t *testing.T, f *stuckFix) []reapReceipt {
	t.Helper()
	var out []reapReceipt
	for _, r := range f.rows(t) {
		if r.Result == "kept" && r.Reason == reviewerNotClosable {
			out = append(out, r)
		}
	}
	return out
}
