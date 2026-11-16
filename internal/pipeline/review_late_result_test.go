package pipeline

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// lateFinding is a valid result a replaced reviewer finishes with after its
// replacement was spawned.
const lateFinding = `{"findings":[{"path":"tool","line":1,"body":"A late bug","severity":"medium"}]}`

// lostOverdueReviewer runs the first reviewer of pass 1 until its pane is
// still working past the budget, then runs the pass again: the reviewer is
// lost overdue and its replacement is spawned. It returns the replaced
// reviewer's record.
func lostOverdueReviewer(t *testing.T) (*Service, *fakeGH, *panewireRunner, config.Repo, reviewerJob) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	budgetPane(t, s, runner, old, "working")
	// The replacement is spawned in this run and stays pending, as its pane
	// has not finished; the first reviewer's pane is the one that timed out.
	runner.exits = []int{panewireWaitTimeout}
	pastBudget(g, c)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	rj := readReviewerJob(t, s, r, 1)
	if len(runner.spawns) != 2 || g.postCount != 0 || len(rj.Replaced) != 1 || rj.Replaced[0].Label != old.Label {
		t.Fatalf("spawns=%d posts=%d replaced=%+v", len(runner.spawns), g.postCount, rj.Replaced)
	}
	return s, g, runner, r, rj
}

// A reviewer replaced for an overdue loss whose result lands while its
// replacement still runs completes the pass: the replacement is not waited on,
// and the late result is posted with the audit row written.
func TestLateResultCompletesPassWhileReplacementRuns(t *testing.T) {
	s, g, runner, r, rj := lostOverdueReviewer(t)
	old := rj.Replaced[0].Label
	runner.sink.jobs[old] = integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(lateFinding)}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	cur := runner.sink.jobs[rj.Label]
	cur.Family = "anthropic"
	runner.sink.jobs[rj.Label] = cur
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || g.postCount != 1 || len(g.postedFindings) != 1 || !strings.Contains(g.postedFindings[0].Body, "A late bug") {
		t.Fatalf("spawns=%d posts=%d findings=%+v", len(runner.spawns), g.postCount, g.postedFindings)
	}
	if pass := g.passes[shaA]; len(pass) != 1 || pass[0].Family != "openai" {
		t.Fatalf("late result credited to the replacement's family: %+v", pass)
	}
	rows := auditRows(t, s, r, "review-result-late-collected")
	if len(rows) != 1 || rows[0]["late_label"] != old || rows[0]["label"] != rj.Label || rows[0]["head"] != shaA {
		t.Fatalf("late-collected rows=%v", rows)
	}
	if _, ok := rows[0]["seconds_past_budget"].(float64); !ok {
		t.Fatalf("late-collected row lacks seconds_past_budget: %v", rows[0])
	}
	reviewerJobGone(t, s, r, 1)
}

// A late result counts only for the head its reviewer ran on.
func TestLateResultForOtherHeadIsIgnored(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull}
	const old = "old-reviewer"
	runner.sink.jobs[old] = integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(lateFinding)}
	rj := reviewerJob{Replaced: []replacedReviewer{{Label: old, Head: shaA, Recorded: c.t}}}
	if b, _, _ := x.lateReviewerResult(context.Background(), &passPlan{pass: 1}, rj); b == nil {
		t.Fatal("same-head late result was not collected")
	}
	rj.Replaced[0].Head = "0000000000000000000000000000000000000000"
	if b, _, _ := x.lateReviewerResult(context.Background(), &passPlan{pass: 1}, rj); b != nil {
		t.Fatalf("late result for an older head was collected: %s", b)
	}
}

// Only a reviewer lost overdue joins the replacement's record; a loss for any
// other cause keeps the carried list as it was.
func TestReplacementCarriesOnlyOverdueLosses(t *testing.T) {
	s, g, _, r := notifySetup(t)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull}
	lost := reviewerJob{PR: g.pull.Number, Head: g.pull.Head.SHA, Pass: 1, Label: "lost-now", Replacements: 1, Replaced: []replacedReviewer{{Label: "earlier", Head: g.pull.Head.SHA}}}
	for _, tc := range []struct {
		cause string
		want  []string
	}{
		{"result missing", []string{"earlier"}},
		{lostOverdue, []string{"earlier", "lost-now"}},
	} {
		plan := &passPlan{pass: 1, lens: "default"}
		if _, err := x.replaceLostReviewer(context.Background(), plan, lost, integrations.JobRecord{}, tc.cause, ""); err == nil {
			t.Fatalf("%s: exhausted replacement unexpectedly continued", tc.cause)
		}
		var got []string
		for _, old := range plan.replaced {
			got = append(got, old.Label)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%s: carried reviewers = %v, want %v", tc.cause, got, tc.want)
		}
	}
}

// A replacement that finishes before the replaced reviewer does is the pass's
// result: the late one is not collected, and the replacement's findings post.
func TestReplacementFinishingFirstWins(t *testing.T) {
	s, g, runner, r, rj := lostOverdueReviewer(t)
	old := rj.Replaced[0].Label
	runner.sink.jobs[old] = integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(lateFinding)}
	runner.sink.jobs[rj.Label] = integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[{"path":"tool","line":1,"body":"A replacement bug","severity":"high"}]}`)}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 || len(g.postedFindings) != 1 || !strings.Contains(g.postedFindings[0].Body, "A replacement bug") {
		t.Fatalf("posts=%d findings=%+v", g.postCount, g.postedFindings)
	}
	if rows := auditRows(t, s, r, "review-result-late-collected"); len(rows) != 0 {
		t.Fatalf("late-collected rows=%v, want none", rows)
	}
}
