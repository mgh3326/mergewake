package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// slowJobSink is a pass whose job reads each take step, so the repo budget
// ends the worker stage between two candidates, as it does on a large
// repository. Every job id read is recorded.
type slowJobSink struct {
	*jobSink
	step time.Duration
	read map[string]bool
}

func (s *slowJobSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	s.read[id] = true
	select {
	case <-time.After(s.step):
	case <-ctx.Done():
	}
	return s.jobSink.Job(ctx, id)
}

// budgetWorkerFix is a reviewer fixture whose node-a also holds a clone with
// workers worker worktrees on wk/ branches. Each worker's job is still claimed,
// so every pass reads it and keeps the checkout.
func budgetWorkerFix(t *testing.T, workers int, budget string, step time.Duration) *reviewerFix {
	t.Helper()
	f := newReviewerFix(t)
	const common = "/work/project/.git"
	hub := f.hub
	hub.wtHead = headSHA
	hub.wtPaths, hub.branches, hub.commons = map[string]bool{}, map[string]string{}, map[string]string{}
	rows := []map[string]any{listedRow("/work/project", "main")}
	for i := 1; i <= workers; i++ {
		job := fmt.Sprintf("j%02d", i)
		path := "/work/" + job + ".wt"
		hub.wtPaths[path], hub.branches[path], hub.commons[path] = true, "wk/"+job, common
		rows = append(rows, listedRow(path, "wk/"+job))
		f.sink.recs[job] = integrations.JobRecord{State: "claimed", Host: "node-a", Cwd: path, Label: job}
	}
	hub.lists = map[string][]map[string]any{common: rows}
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.s.Config.Reap.RepoBudget = budget
	f.s.GH = &workerGH{legacyGH: legacyGH{}}
	f.s.Sink = &slowJobSink{jobSink: f.sink, step: step, read: map[string]bool{}}
	return f
}

// A worker stage with more candidates than its budget covers must leave the
// rest of the pass to the reviewer sweep that follows it, so a reviewer label
// is still swept in the same pass.
func TestWorkerStageBudgetLeavesReviewerSweepTime(t *testing.T) {
	f := budgetWorkerFix(t, 24, "600ms", 50*time.Millisecond)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	_ = f.reap(t, 0, Options{})
	if got := len(f.s.Sink.(*slowJobSink).read); got == 24 {
		t.Fatalf("the worker stage read every candidate: it did not fill its share of the budget")
	}
	for _, r := range f.rows(t) {
		if r.Label == label {
			return
		}
	}
	t.Fatalf("reviewer sweep was not reached in a pass whose worker stage was budget-cut; rows: %v", rowSummary(f.rows(t)))
}

// A budget-cut worker stage resumes after the candidate its last pass stopped
// at, so every worker worktree is reached within a bounded number of passes.
// Without that, the same first candidates use the budget on every pass.
func TestWorkerStageBudgetCutReachesEveryCandidate(t *testing.T) {
	const workers, passes = 24, 6
	f := budgetWorkerFix(t, workers, "600ms", 50*time.Millisecond)
	read := f.s.Sink.(*slowJobSink).read
	for pass := 1; pass <= passes; pass++ {
		_ = f.reap(t, 0, Options{})
		if len(read) == workers {
			return
		}
	}
	t.Fatalf("%d of %d worker candidates reached in %d budget-cut passes", len(read), workers, passes)
}

// cutGH answers the watch-verdict ancestry read only when the repo budget ends
// it, the way a GitHub request cut by the budget fails: with the context's
// own deadline error, not the budget cause.
type cutGH struct {
	*workerGH
	asked bool
}

func (g *cutGH) Ancestor(ctx context.Context, _ string, _, _ string) (bool, error) {
	g.asked = true
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
	}
	return false, ctx.Err()
}

// A GitHub read that the budget cuts short inside the worker stage is a budget
// stop, not a failure: the pass must not report it as one.
func TestWorkerStageGitHubReadCutByBudgetIsNotAFailure(t *testing.T) {
	s, _, _ := workerFixture(t, workerPR(40, true, "abc123"))
	gh := &cutGH{workerGH: s.GH.(*workerGH)}
	s.GH = gh
	if err := os.MkdirAll(s.state(testRepo.Name), 0o700); err != nil {
		t.Fatal(err)
	}
	// A verdict on another commit makes the watch check ask GitHub for ancestry.
	if err := os.WriteFile(filepath.Join(s.state(testRepo.Name), "last-green"), []byte("deadbeef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.RepoBudget = "400ms"
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if !gh.asked {
		t.Fatal("the watch verdict never asked GitHub; the budget did not reach the worker stage")
	}
	// The cut may end the pass (a budget stop) or only the worker stage's
	// share, after which the pass completes; either way it is not a failure.
	if ReapNonBudgetErrors(err) != nil {
		t.Fatalf("a GitHub read cut by the budget counted as a failure: %v", err)
	}
}
