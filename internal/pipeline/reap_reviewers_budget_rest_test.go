package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// hangingJobSink answers a job read from the reviewer fix's sink. With hang
// set, a read waits for its context, as a read the repo budget cuts does; with
// fail set, it answers that error.
type hangingJobSink struct {
	*jobSink
	hang bool
	fail error
}

func (s *hangingJobSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if s.hang {
		<-ctx.Done()
		return integrations.JobRecord{}, ctx.Err()
	}
	if s.fail != nil {
		return integrations.JobRecord{}, s.fail
	}
	return s.jobSink.Job(ctx, id)
}

// budgetCloseRunner answers panewire's job closes: with hang set each waits for
// its context, as a close the repo budget cuts does; otherwise it fails with
// fail. Every other command runs on the script runner.
type budgetCloseRunner struct {
	*scriptRunner
	hang bool
	fail error
}

func (r budgetCloseRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if k := name + "\x00" + strings.Join(args, "\x00"); strings.Contains(k, "\x00job\x00close\x00") {
		r.calls = append(r.calls, k)
		if r.hang {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, r.fail
	}
	return r.scriptRunner.Run(ctx, name, args, input, dir)
}

// requireBudgetHold asserts a pass that the repo budget stopped: the pass
// returns a budget stop with no other error, no row is an error, and the one
// reviewer step the budget cut is a budget hold row.
func requireBudgetHold(t *testing.T, f *reviewerFix, err error) {
	t.Helper()
	var stop *ReapBudgetStop
	if !errors.As(err, &stop) {
		t.Fatalf("the pass did not return a budget stop: %v", err)
	}
	if other := ReapNonBudgetErrors(err); other != nil {
		t.Fatalf("a budget cut added a non-budget error: %v", other)
	}
	holds := 0
	for _, v := range f.rows(t) {
		if v.Result == "error" {
			t.Fatalf("row %s error: %s", v.Label, v.Reason)
		}
		if v.Result == "pending-cleanup" && v.Reason == reviewerBudgetHold {
			holds++
		}
	}
	if holds != 1 {
		t.Fatalf("want one budget hold row, rows: %v", rowSummary(f.rows(t)))
	}
}

// requireErrorRow asserts a pass that a real failure stopped: the pass returns
// a non-budget error, and an error row carries the step's reason prefix.
func requireErrorRow(t *testing.T, f *reviewerFix, err error, prefix string) {
	t.Helper()
	if ReapNonBudgetErrors(err) == nil {
		t.Fatalf("a real failure was not a pass error: %v", err)
	}
	for _, v := range f.rows(t) {
		if v.Result == "error" && strings.HasPrefix(v.Reason, prefix) {
			return
		}
	}
	t.Fatalf("no error row %q: %v", prefix, rowSummary(f.rows(t)))
}

// The job-record read of a reviewer the repo budget cuts is a budget hold; a
// real read failure is still an error row.
func TestReapReviewerJobReadBudgetCutIsAHold(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.passAfter(t, 0)
	f.s.Sink = &hangingJobSink{jobSink: f.sink, hang: true}
	f.s.Config.Reap.RepoBudget = "1s"
	requireBudgetHold(t, f.reviewerFix, f.passAt(t, 30*time.Minute))
}

func TestReapReviewerJobReadFailureIsStillAnErrorRow(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.passAfter(t, 0)
	f.s.Sink = &hangingJobSink{jobSink: f.sink, fail: errors.New("job store unavailable")}
	requireErrorRow(t, f.reviewerFix, f.passAt(t, 30*time.Minute), "job record: ")
}

// spawnedMergedFix is a reviewer whose job still reads spawned for a merged PR
// and whose pane reads done: abandonSpawned closes the job, after a fresh pane
// read, which is the read these tests cut or fail.
func spawnedMergedFix(t *testing.T) *reviewerFix {
	t.Helper()
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	return f
}

// failingPanes is a host whose pane list read answers err: a node failure, or
// the repo budget cutting the read.
type failingPanes struct {
	reap.Ops
	err error
}

func (p failingPanes) AllPanes(context.Context) ([]reap.Pane, error) { return nil, p.err }

// abandonAfterPaneRead runs abandonSpawned for a merged PR's spawned reviewer
// whose fresh pane read answers readErr, on a context with the given cause. The
// hub client caches the node's pane list for a whole pass, so the fresh read
// cannot be cut through the fake hub; the call is made directly instead.
func abandonAfterPaneRead(t *testing.T, f *reviewerFix, ctx context.Context, readErr error) (bool, []reapReceipt) {
	t.Helper()
	merged := "2026-11-04T10:00:00Z"
	label := reviewerLabelFor(12, time.Hour)
	l, ok := parseReviewerLabel(label)
	if !ok {
		t.Fatalf("label %q does not parse", label)
	}
	var rows []reapReceipt
	w := &reviewerSweep{
		s: f.s, r: testRepo, acting: true,
		pulls:     map[int]github.Pull{12: {Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}},
		abandoned: map[string]bool{}, rowed: map[string]bool{},
		note: func(v reapReceipt) error { rows = append(rows, v); return nil },
	}
	c := reviewerCandidate{
		label: l,
		seen:  &reapHost{ops: failingPanes{err: readErr}},
		pane:  &reap.Pane{ID: "w1:p5", Cwd: "/repo/" + label, Status: "done"},
	}
	job := integrations.JobRecord{State: "spawned", Role: "reviewer", Label: label, OwnerLane: "lane-x"}
	abandoned := w.abandonSpawned(ctx, c, w.row(l), job)
	for _, call := range f.run.calls {
		if strings.Contains(call, "\x00job\x00close\x00") {
			t.Fatalf("a job closed after its fresh pane read failed: %q", call)
		}
	}
	return abandoned, rows
}

// The fresh pane read before a spawned job's close: a budget cut there is a
// hold, the job is not closed, and the row is pending-cleanup with the hold.
func TestReapReviewerAbandonPaneReadBudgetCutIsAHold(t *testing.T) {
	f := spawnedMergedFix(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrReapRepoBudget)
	abandoned, rows := abandonAfterPaneRead(t, f, ctx, ErrReapRepoBudget)
	if abandoned || len(rows) != 1 || rows[0].Result != "pending-cleanup" || rows[0].Reason != reviewerBudgetHold {
		t.Fatalf("abandoned=%v rows: %v", abandoned, rowSummary(rows))
	}
}

// A real fresh pane read failure is still an error row, and the job stays.
func TestReapReviewerAbandonPaneReadFailureIsStillAnErrorRow(t *testing.T) {
	f := spawnedMergedFix(t)
	abandoned, rows := abandonAfterPaneRead(t, f, context.Background(), errors.New("node_unavailable"))
	if abandoned || len(rows) != 1 || rows[0].Result != "error" || !strings.HasPrefix(rows[0].Reason, "abandon job: pane read: ") {
		t.Fatalf("abandoned=%v rows: %v", abandoned, rowSummary(rows))
	}
}

// The panewire close of a spawned reviewer job: a budget cut is a hold; a
// node failure is still an error row.
func TestReapReviewerAbandonCloseBudgetCutIsAHold(t *testing.T) {
	f := spawnedMergedFix(t)
	f.s.Runner = budgetCloseRunner{scriptRunner: f.run, hang: true}
	f.s.Config.Reap.RepoBudget = "1s"
	requireBudgetHold(t, f, f.reap(t, 0, Options{}))
}

func TestReapReviewerAbandonCloseFailureIsStillAnErrorRow(t *testing.T) {
	f := spawnedMergedFix(t)
	f.s.Runner = budgetCloseRunner{scriptRunner: f.run, fail: errors.New("node_busy")}
	requireErrorRow(t, f, f.reap(t, 0, Options{}), "abandon job: ")
}

// The panewire close of a stuck reviewer job (pane gone, 7h old): a budget cut
// is a hold; a node failure is still an error row.
func TestReapReviewerStuckCloseBudgetCutIsAHold(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.passAfter(t, 0)
	f.clearPanes()
	f.s.Runner = budgetCloseRunner{scriptRunner: f.run, hang: true}
	f.s.Config.Reap.RepoBudget = "1s"
	requireBudgetHold(t, f.reviewerFix, f.passAt(t, 30*time.Minute))
}

func TestReapReviewerStuckCloseFailureIsStillAnErrorRow(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.passAfter(t, 0)
	f.clearPanes()
	f.s.Runner = budgetCloseRunner{scriptRunner: f.run, fail: errors.New("node_busy")}
	requireErrorRow(t, f.reviewerFix, f.passAt(t, 30*time.Minute), "abandon job: ")
}
