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

// (a) A reviewer pane whose composer cannot be read closes on the first
// sighting when its hk job record says the job is completed, and the close row
// says why; (b) the same for a job the record says was lost. Neither waits for
// the two-hour composer-unknown row.
func TestReapReviewerEndedJobClosesUnreadablePaneAtOnce(t *testing.T) {
	for _, state := range []string{"completed", "lost"} {
		t.Run(state, func(t *testing.T) {
			f := newEndedFix(t, state)
			f.passAfter(t, 0)
			if f.closes() != 1 {
				t.Fatalf("closes=%d: %v", f.closes(), rowSummary(f.rows(t)))
			}
			rows := f.rows(t)
			if len(rows) == 0 || rows[0].Label != f.label || rows[0].Pane != "w1:p5" || rows[0].Result != "closed" || rows[0].Reason != "composer-unknown-job-ended" {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// A busy pane is never closed by that rule, however finished the job record is.
func TestReapReviewerEndedJobKeepsWorkingPane(t *testing.T) {
	f := newEndedFix(t, "completed")
	f.setPane("w1:p5", "agent_status", "working")
	f.passAfter(t, 0)
	f.passAfter(t, 3*time.Hour)
	if f.closes() != 0 {
		t.Fatalf("closes=%d: %v", f.closes(), rowSummary(f.rows(t)))
	}
}

// (c) A reviewer pane whose job record still reads spawned keeps today's rule:
// the pane waits for the two-hour composer-unknown row before it closes.
func TestReapReviewerSpawnedJobKeepsUnreadablePaneUntilTwoHours(t *testing.T) {
	f := newEndedFix(t, "spawned")
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	f.allowJobClose()
	f.setJob(func(j *integrations.JobRecord) { j.OwnerLane = "lane-x" })
	f.passAfter(t, 0)
	f.passAfter(t, 30*time.Minute)
	if f.closes() != 0 {
		t.Fatalf("closed before two idle hours: %v", rowSummary(f.rows(t)))
	}
	f.passAfter(t, 2*time.Hour)
	if f.closes() != 1 {
		t.Fatalf("closes=%d: %v", f.closes(), rowSummary(f.rows(t)))
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
