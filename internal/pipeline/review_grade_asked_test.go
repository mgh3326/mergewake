package pipeline

import (
	"context"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// A later round sends no --grade, so panewire routes the spawn at the task
// title's grade. The audit row must name that grade, not the PR grade (hk 6979).
func TestUnraisedGradeAskedIsTaskTitleGrade(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	s.Config.Review.FirstRoundGrade = map[string]string{"B": "A"}
	runner.sink.title = "Fix the thing (grade S, T1)"
	g.pull.Body += "Mergewake-Grade: B\n"
	// Round two: a patch of an earlier head was already reviewed, so no raise.
	g.passes = map[string][]github.PassReceipt{shaB: {{Pass: 1, Family: "openai", Patch: "earlier-patch"}}}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns = %d", len(runner.spawns))
	}
	if sp := runner.spawns[0]; slices.Contains(sp, "--grade") {
		t.Fatalf("--grade sent on an unraised round: %v", sp)
	}
	rows := auditRows(t, s, r, "review")
	if len(rows) != 1 {
		t.Fatalf("review rows = %v", rows)
	}
	if rows[0]["pr_grade"] != "B" || rows[0]["grade_asked"] != "S" || rows[0]["first_round"] != false {
		t.Fatalf("audit row = %v, want PR grade B, grade asked S (the title's grade), round two", rows[0])
	}
}

// A task title without a grade marker never reaches a spawn: the pass is
// skipped, so no --grade and no grade_asked are recorded for it (hk 6979).
func TestUnraisedPassWithoutTitleGradeIsSkipped(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	s.Config.Review.FirstRoundGrade = map[string]string{"B": "A"}
	runner.sink.title = "Fix the thing (T1)"
	g.pull.Body += "Mergewake-Grade: B\n"
	g.passes = map[string][]github.PassReceipt{shaB: {{Pass: 1, Family: "openai", Patch: "earlier-patch"}}}
	_ = s.Review(context.Background(), r, 7, "", Options{})
	if len(runner.spawns) != 0 {
		t.Fatalf("spawns = %v, want none for a title without a grade", runner.spawns)
	}
}
