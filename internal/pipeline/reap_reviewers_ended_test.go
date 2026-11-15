package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// endedFix is a finished reviewer whose composer the hub reports unreadable, as
// herdr does for an omp pane, and whose hk job record the test edits.
func newEndedFix(t *testing.T, state string) *stuckFix {
	t.Helper()
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.setPane("w1:p5", "agent_status", "done")
	f.setJob(func(j *integrations.JobRecord) { j.State = state })
	return f
}

// (a) A reviewer pane whose composer cannot be read is no longer read at all:
// with the pane present the sweep keeps it for panewire's node to close,
// however finished the hk job record says the job is. The close reason is
// gone with the close.
func TestReapReviewerEndedJobKeepsUnreadablePaneForNode(t *testing.T) {
	for _, state := range []string{"completed", "lost"} {
		t.Run(state, func(t *testing.T) {
			f := newEndedFix(t, state)
			f.passAfter(t, 0)
			f.passAfter(t, 3*time.Hour)
			if f.closes() != 0 {
				t.Fatalf("closes=%d: %v", f.closes(), rowSummary(f.rows(t)))
			}
			rows := f.rows(t)
			if len(rows) != 1 || rows[0].Label != f.label || rows[0].Result != "kept" || rows[0].Reason != "pane-in-use" {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// A busy pane is never touched by that rule either: it keeps the worktree
// until panewire's node closes it.
func TestReapReviewerEndedJobKeepsWorkingPane(t *testing.T) {
	f := newEndedFix(t, "completed")
	f.setPane("w1:p5", "agent_status", "working")
	f.passAfter(t, 0)
	f.passAfter(t, 3*time.Hour)
	if f.closes() != 0 {
		t.Fatalf("closes=%d: %v", f.closes(), rowSummary(f.rows(t)))
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "pane-in-use" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// (c) A reviewer pane whose job record still reads spawned is abandoned once
// its review is over; the pane itself still waits for panewire's node.
func TestReapReviewerSpawnedJobAbandonsJobButKeepsPane(t *testing.T) {
	f := newEndedFix(t, "spawned")
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	f.allowJobClose()
	f.setJob(func(j *integrations.JobRecord) { j.OwnerLane = "lane-x" })
	f.passAfter(t, 0)
	f.passAfter(t, 30*time.Minute)
	if f.closes() != 0 {
		t.Fatalf("closed: %v", rowSummary(f.rows(t)))
	}
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[0].Result != "abandoned" || rows[1].Result != "kept" || rows[1].Reason != "pane-in-use" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// failingJobSink is an hk sink whose job read fails: an unreadable record is
// never read as ended.
type failingJobSink struct{ *jobSink }

func (s failingJobSink) Job(context.Context, string) (integrations.JobRecord, error) {
	return integrations.JobRecord{}, errors.New("hk unavailable")
}

// (d) An hk job read that fails keeps the pane and reports the error.
func TestReapReviewerUnreadableJobRecordKeepsPane(t *testing.T) {
	f := newEndedFix(t, "completed")
	f.s.Sink = failingJobSink{f.sink}
	if err := f.reap(t, 0, Options{}); err == nil {
		t.Fatal("a failed job read wrote no error")
	}
	f.now = reviewerNow.Add(3 * time.Hour)
	if err := f.reap(t, 0, Options{}); err == nil {
		t.Fatal("a failed job read wrote no error")
	}
	if f.closes() != 0 {
		t.Fatalf("closes=%d: %v", f.closes(), rowSummary(f.rows(t)))
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Result != "error" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// (a) A lost job whose role is not reviewer keeps its finished pane: the early
// release is reviewer-only, so the hold stays as job:lost and nothing closes.
func TestReapLostNonReviewerJobKeepsFinishedPane(t *testing.T) {
	for _, role := range []string{"worker", "implementer", "tester", "counsel", "consult"} {
		t.Run(role, func(t *testing.T) {
			f := newEndedFix(t, "lost")
			f.hub.composer = map[string]any{"outcome": "read", "composer": "empty"}
			f.setJob(func(j *integrations.JobRecord) { j.Role = role })
			f.passAfter(t, 0)
			f.passAfter(t, 3*time.Hour)
			rows := f.rows(t)
			if f.closes() != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "job:lost" {
				t.Fatalf("closes=%d: %v", f.closes(), rowSummary(rows))
			}
		})
	}
}

// (b) A lost reviewer job whose pane is still present keeps it for panewire's
// node; an empty composer no longer closes anything.
func TestReapLostReviewerJobKeepsPresentPane(t *testing.T) {
	f := newEndedFix(t, "lost")
	f.hub.composer = map[string]any{"outcome": "read", "composer": "empty"}
	f.passAfter(t, 0)
	rows := f.rows(t)
	if f.closes() != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "pane-in-use" {
		t.Fatalf("closes=%d rows: %v", f.closes(), rowSummary(rows))
	}
}
