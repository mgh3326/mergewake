package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// finalFix is a reviewerFix whose clock the test moves, and whose job reads
// are counted: a label the sweep no longer revisits makes no read.
type finalFix struct {
	*reviewerFix
	clock time.Time
	reads *atomic.Int64
	out   *strings.Builder
}

type countingJobs struct {
	*jobSink
	reads *atomic.Int64
}

func (c countingJobs) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	c.reads.Add(1)
	return c.jobSink.Job(ctx, id)
}

func newFinalFix(t *testing.T, hosts ...reap.Host) *finalFix {
	t.Helper()
	f := &finalFix{reviewerFix: newReviewerFix(t, hosts...), clock: reviewerNow, reads: &atomic.Int64{}, out: &strings.Builder{}}
	f.s.Now = func() time.Time { return f.clock }
	f.s.Sink = countingJobs{jobSink: f.sink, reads: f.reads}
	f.s.Out = f.out
	return f
}

// printed counts the reviewer-reap rows the sweep wrote to its output.
func (f *finalFix) printed() int {
	return strings.Count(f.out.String(), `"action":"reviewer-reap"`)
}

const (
	unknownPathReason = "remote worktree path unknown; left for the host's own sweep"
	refusedDirty      = "hub guarded removal refused: dirty"
)

// (c) A completed job whose pane is gone and whose remote path nobody knows:
// one terminal kept row, and the second sweep neither writes nor reads the job.
func TestReapReviewerFinal_C_UnknownRemotePathIsTerminalAndNotRevisited(t *testing.T) {
	f := newFinalFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Host: "node-a", Pane: "w1:p5", Worktree: "/checkouts/" + label, Result: "pending-cleanup", Reason: "spawn named no host and pane; checkout kept"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var terminal []reapReceipt
	for _, v := range f.rows(t) {
		if v.Result == "kept" && v.Reason == unknownPathReason {
			terminal = append(terminal, v)
		}
		if v.Result == "pending-cleanup" {
			t.Fatalf("a pending row is left behind: %v", rowSummary(f.rows(t)))
		}
	}
	if len(terminal) != 1 {
		t.Fatalf("want one terminal kept row, got rows %v", rowSummary(f.rows(t)))
	}
	rows, audits, reads, printed := len(f.rows(t)), len(f.audits(t)), f.reads.Load(), f.printed()
	for range 3 {
		f.clock = f.clock.Add(25 * time.Minute)
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.rows(t)) != rows || len(f.audits(t)) != audits || f.printed() != printed {
		t.Fatalf("later sweeps wrote: rows %v printed %d", rowSummary(f.rows(t)), f.printed())
	}
	if f.reads.Load() != reads {
		t.Fatalf("later sweeps revisited the label: %d job reads, was %d", f.reads.Load(), reads)
	}
}

// (d) The hub refuses a removal as dirty: the sweep asks again after 24 hours,
// twice more, and the last refusal is terminal. Only the hub ever removes.
func TestReapReviewerFinal_D_RefusedRemovalRetriesDailyThenSettles(t *testing.T) {
	f := newFinalFix(t)
	f.hub.dropClosed = true
	f.hub.removeOutcome = "dirty"
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	cwd := "/repo/" + label
	sweep := func(after time.Duration) {
		t.Helper()
		f.clock = f.clock.Add(after)
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	removes := func() int { return len(f.hub.ops("worktree.remove")) }
	last := func() reapReceipt { rows := f.rows(t); return rows[len(rows)-1] }

	sweep(0)
	if removes() != 1 || last().Result != "pending-cleanup" || last().Reason != refusedDirty {
		t.Fatalf("first refusal: removes %d rows %v", removes(), rowSummary(f.rows(t)))
	}
	rows, reads := len(f.rows(t)), f.reads.Load()
	sweep(time.Hour)
	sweep(10 * time.Hour)
	if removes() != 1 || len(f.rows(t)) != rows || f.reads.Load() != reads {
		t.Fatalf("revisited within a day: removes %d rows %v reads %d", removes(), rowSummary(f.rows(t)), f.reads.Load())
	}
	sweep(14 * time.Hour) // 25h after the refusal
	if removes() != 2 || last().Result != "pending-cleanup" || !strings.HasPrefix(last().Reason, refusedDirty) {
		t.Fatalf("day 1 retry: removes %d rows %v", removes(), rowSummary(f.rows(t)))
	}
	sweep(25 * time.Hour)
	if removes() != 3 || last().Result != "pending-cleanup" {
		t.Fatalf("day 2 retry: removes %d rows %v", removes(), rowSummary(f.rows(t)))
	}
	sweep(25 * time.Hour)
	if removes() != 4 || last().Result != "kept" || last().Reason != refusedDirty || last().Worktree != cwd {
		t.Fatalf("day 3 retry: removes %d rows %v", removes(), rowSummary(f.rows(t)))
	}
	rows, audits, reads := len(f.rows(t)), len(f.audits(t)), f.reads.Load()
	for range 4 {
		sweep(25 * time.Hour)
	}
	if removes() != 4 || len(f.rows(t)) != rows || len(f.audits(t)) != audits || f.reads.Load() != reads {
		t.Fatalf("after the terminal row: removes %d rows %v reads %d", removes(), rowSummary(f.rows(t)), f.reads.Load())
	}
	for _, c := range f.run.calls {
		if strings.Contains(c, cwd) {
			t.Fatalf("the remote worktree was touched another way: %q", c)
		}
	}
}

// (e) The same outcome on consecutive sweeps is one row in reap.jsonl, one
// audit record and one printed row.
func TestReapReviewerFinal_E_SamePendingOutcomeIsOneRow(t *testing.T) {
	f := newFinalFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	f.hub.composer = map[string]any{"outcome": "read", "composer": "has_text"}
	for range 3 {
		f.clock = f.clock.Add(25 * time.Minute)
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "composer-text" || len(f.audits(t)) != 1 || f.printed() != 1 {
		t.Fatalf("rows %v audits %d printed %d", rowSummary(rows), len(f.audits(t)), f.printed())
	}
}

// (f) A terminal label's local checkout that is clean and detached at the
// reviewed head is removed; a dirty one is kept.
func TestReapReviewerFinal_F_TerminalLabelLocalCheckout(t *testing.T) {
	for _, test := range []struct {
		name   string
		dirty  bool
		result string
		reason string
	}{
		{"clean", false, "removed", ""},
		{"dirty", true, "kept", "dirty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFinalFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
			label := reviewerLabelFor(12, time.Hour)
			cwd := "/repo/" + label
			path := localCheckout(t, f.reviewerFix, label, false)
			if test.dirty {
				f.run.outs[key("git", "-C", path, "status", "--porcelain")] = " M tool.go\n"
			}
			f.hub.detached[cwd] = true
			f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5", Cwd: cwd}
			old := reviewerNow.Add(-4 * 24 * time.Hour)
			seedReapRows(t, f.s, testRepo.Name,
				reapReceipt{Time: old, Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: label, Host: "node", Result: "skipped", Reason: "pane already gone"},
				reapReceipt{Time: old, Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: label, Host: "node", Worktree: cwd, Cwd: cwd, Result: "kept", Reason: refusedDirty},
			)
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if got := f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)); got != (test.result == "removed") {
				t.Fatalf("removed=%v calls %q", got, f.run.calls)
			}
			if len(f.hub.ops("worktree.remove")) != 0 {
				t.Fatalf("a terminal remote stream was asked again: %v", f.hub.commands)
			}
			rows := f.rows(t)
			if last := rows[len(rows)-1]; last.Host != "local" || last.Result != test.result || last.Reason != test.reason {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}
