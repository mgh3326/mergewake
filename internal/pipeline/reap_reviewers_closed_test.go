package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// refusingClose is a runner whose panewire job closes are refused with refusal;
// every other command runs on the script runner, and the close calls are still
// recorded there so jobCloses sees them.
type refusingClose struct {
	*scriptRunner
	refusal error
}

func (r refusingClose) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if k := name + "\x00" + strings.Join(args, "\x00"); strings.Contains(k, "\x00job\x00close\x00") {
		r.calls = append(r.calls, k)
		return nil, r.refusal
	}
	return r.scriptRunner.Run(ctx, name, args, input, dir)
}

// refuseJobClose makes every job close of f's runner fail as panewire does,
// with stderr as its refusal text.
func refuseJobClose(f *reviewerFix, stderr string) {
	f.s.Runner = refusingClose{scriptRunner: f.run, refusal: closeRefusal(stderr)}
}

// passAt moves the clock d past the start and runs one acting pass, returning
// the pass's error: a refused close fails the pass.
func (f *stuckFix) passAt(t *testing.T, d time.Duration) error {
	f.now = reviewerNow.Add(d)
	return f.reap(t, 0, Options{})
}

const alreadyClosedStderr = "close refused: job %s already has a terminal record (00002-job.revoked.json) after its latest claim"

// (a) A spawned reviewer job whose PR merged is closed abandoned, but panewire
// refuses the close because the job already has a terminal record. The label
// gets one closed row, no error, and no later pass closes the job again.
func TestReapReviewerAlreadyClosedRefusalEndsClose(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	refuseJobClose(f, strings.ReplaceAll(alreadyClosedStderr, "%s", label))

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if n := len(f.jobCloses()); n != 1 || len(rows) < 1 || rows[0].Result != "closed" || rows[0].Reason != "already closed" {
		t.Fatalf("job closes=%d rows: %v", n, rowSummary(rows))
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows = f.rows(t)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closed again on the next pass: closes=%d rows: %v", n, rowSummary(rows))
	}
	for _, r := range rows {
		if r.Result == "error" {
			t.Fatalf("error row: %v", rowSummary(rows))
		}
	}
}

// The same refusal on a stuck job whose pane is gone ends its close as well,
// and the worktree cleanup still runs in that pass: the job has ended, so its
// checkout is released.
func TestReapReviewerAlreadyClosedStuckJobStillRemovesWorktree(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	refuseJobClose(f.reviewerFix, strings.ReplaceAll(alreadyClosedStderr, "%s", f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
	if rows := f.resultRows(t, "closed"); len(rows) != 1 || rows[0].Reason != "already closed" {
		t.Fatalf("closed rows: %v", rowSummary(f.rows(t)))
	}
	if errs := f.resultRows(t, "error"); len(errs) != 0 {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
}

// (b) A refusal that is not "already has a terminal record" stays owed as
// today: the close fails with an error row and a later pass retries it.
func TestReapReviewerOtherCloseRefusalStaysOwed(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	refuseJobClose(f.reviewerFix, "close refused: job "+f.label+" owner_mismatch")
	f.passAfter(t, 0)
	f.clearPanes()
	// The refused close fails its pass, as an owed close always has.
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
	// The next pass waits out the retry backoff; a later pass retries the close.
	_ = f.passAt(t, 30*time.Minute)
	_ = f.passAt(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 2 {
		t.Fatalf("job closes=%d, want the refused close retried: %v", n, rowSummary(f.rows(t)))
	}
}

// (c) A journal-only label, one with no pane in this repo's worktree, whose
// job record names another repository gets no close and no removal here. One
// skipped row says why, and the label is not visited again.
func TestReapReviewerOtherRepoJournalJobIsNotClosed(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.allowJobClose()
	f.setJob(func(j *integrations.JobRecord) {
		j.Worktree = &integrations.JobWorktree{Origin: "https://github.com/example/other.git"}
	})
	f.clearPanes()
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "kept", Reason: "job:spawned"})
	f.passAfter(t, 30*time.Minute)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
	rows := f.resultRows(t, "skipped")
	if len(rows) != 1 || rows[0].Reason != "job names another repository: example/other" {
		t.Fatalf("skipped rows: %v", rowSummary(f.rows(t)))
	}
}

// A journal-only job that names this repository is still closed as before.
func TestReapReviewerOwnRepoJournalJobIsClosed(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.allowJobClose()
	f.setJob(func(j *integrations.JobRecord) {
		j.Worktree = &integrations.JobWorktree{Origin: "https://github.com/example/project.git"}
	})
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
}
