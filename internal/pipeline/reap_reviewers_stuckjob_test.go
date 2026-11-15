package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// stuckJobFix is a reviewer spawned at the given label age whose hk record
// still reads spawned with no result. The PR stays open at the reviewed head,
// so abandonSpawned keeps the job: only the stuck fallback may act on it.
func newStuckJobFix(t *testing.T, age time.Duration) *stuckFix {
	t.Helper()
	f := newStuckFix(t)
	f.label = reviewerLabelFor(12, age)
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "open", Head: github.Ref{SHA: headSHA}}}
	f.addReviewer(f.label, "w1:p5")
	f.setJob(func(j *integrations.JobRecord) { j.State, j.OwnerLane = "spawned", "lane-x" })
	return f
}

func (f *stuckFix) clearPanes() {
	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
}

func (f *stuckFix) resultRows(t *testing.T, result string) []reapReceipt {
	var out []reapReceipt
	for _, r := range f.rows(t) {
		if r.Result == result {
			out = append(out, r)
		}
	}
	return out
}

// (a) An ended reviewer job whose pane is gone has its worktree removed in
// the same pass. A handed-off job reads lost with an end_reason, so lost
// covers it; abandoned is the job close outcome whose record still reads
// spawned until panewire's sweep ends it.
func TestReapReviewerEndedJobGoneRemovesInPass(t *testing.T) {
	for _, state := range []string{"completed", "lost", "escalated"} {
		t.Run(state, func(t *testing.T) {
			f := newStuckFix(t)
			f.addReviewer(f.label, "w1:p5")
			f.setJob(func(j *integrations.JobRecord) { j.State = state })
			f.passAfter(t, 0)
			f.clearPanes()
			f.passAfter(t, 30*time.Minute)
			if n := len(f.hub.ops("worktree.remove")); n != 1 {
				t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
			}
			if rows := f.resultRows(t, "removed"); len(rows) != 1 || rows[0].Label != f.label {
				t.Fatalf("rows: %v", rowSummary(f.rows(t)))
			}
		})
	}
	t.Run("abandoned", func(t *testing.T) {
		f := newStuckFix(t)
		f.addReviewer(f.label, "w1:p5")
		seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "abandoned", Reason: "PR #12 merged"})
		f.passAfter(t, 0)
		f.clearPanes()
		f.passAfter(t, 30*time.Minute)
		if n := len(f.hub.ops("worktree.remove")); n != 1 {
			t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
		}
	})
}

// (b) The same ended job with its pane still present keeps the worktree:
// one kept row, nothing removed, nothing closed.
func TestReapReviewerEndedJobPresentKeepsWorktree(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.passAfter(t, 0)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.hub.ops("worktree.remove")) + f.closes(); n != 0 {
		t.Fatalf("acted: %v", rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "kept" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// (c) A reviewer job still spawned 7h after its label, with no result and
// its pane gone, is closed abandoned and its worktrees removed in the same
// pass: the close is what lets panewire's sweep end the record.
func TestReapReviewerStuckSpawnedGoneClosesAndRemoves(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.allowJobClose()
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	closes := f.jobCloses()
	if len(closes) != 1 {
		t.Fatalf("job closes=%v: %v", closes, rowSummary(f.rows(t)))
	}
	joined := strings.Join(closes[0], " ")
	if !strings.HasPrefix(joined, "job close "+f.label+" ") || !strings.Contains(joined, "--outcome abandoned") {
		t.Fatalf("job close args: %v", closes[0])
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
	if rows := f.resultRows(t, "abandoned"); len(rows) != 1 {
		t.Fatalf("abandoned rows: %v", rowSummary(f.rows(t)))
	}
	if rows := f.resultRows(t, "removed"); len(rows) != 1 || rows[0].Label != f.label {
		t.Fatalf("rows: %v", rowSummary(f.rows(t)))
	}
}

// (d) The same job with its pane still present is left alone — panewire's
// node owns the pane — and the label is marked stuck-reviewer exactly once,
// with the age, however many passes run.
func TestReapReviewerStuckSpawnedPresentMarksOnce(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.passAfter(t, 0)
	f.passAfter(t, 30*time.Minute)
	f.passAfter(t, time.Hour)
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
	stuck := f.resultRows(t, "stuck-reviewer")
	if len(stuck) != 1 || !strings.Contains(stuck[0].Reason, "7h") {
		t.Fatalf("stuck rows: %v", rowSummary(f.rows(t)))
	}
}

// (e) A spawned job younger than the bound is untouched: kept job:spawned,
// no stuck-reviewer row, no close, no removal.
func TestReapReviewerStuckSpawnedYoungUntouched(t *testing.T) {
	f := newStuckJobFix(t, time.Hour)
	f.passAfter(t, 0)
	f.passAfter(t, 30*time.Minute)
	if n := len(f.jobCloses()) + len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("acted: %v", rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(f.resultRows(t, "stuck-reviewer")) != 0 {
		t.Fatalf("stuck row for a young job: %v", rowSummary(rows))
	}
	if len(rows) == 0 || rows[len(rows)-1].Result != "kept" || rows[len(rows)-1].Reason != "job:spawned" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// (f) A claimed job past the bound with its pane gone takes the same
// fallback: it is closed abandoned and its checkout removed.
func TestReapReviewerStuckClaimedGoneClosesAndRemoves(t *testing.T) {
	f := newStuckJobFix(t, 8*time.Hour)
	f.allowJobClose()
	f.setJob(func(j *integrations.JobRecord) { j.State = "claimed" })
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
}

// (g) The fallback is a plan until the pass acts: a dry run prints the
// would-abandon and would-remove rows and changes nothing.
func TestReapReviewerStuckFallbackDryRunPlans(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.passAfter(t, 0)
	f.clearPanes()
	var out strings.Builder
	f.s.Out = &out
	f.now = reviewerNow.Add(30 * time.Minute)
	if err := f.reap(t, 0, Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()) + len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("dry run acted: closes=%v removes=%v", f.jobCloses(), f.hub.ops("worktree.remove"))
	}
	for _, want := range []string{`"result":"would-abandon"`, `"result":"would-remove"`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s in %s", want, out.String())
		}
	}
}
