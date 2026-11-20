package pipeline

import (
	"context"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// Round one of a grade B or C PR asks for a reviewer one grade up; every later
// round, and a grade the map does not name, asks at the PR grade (hk 5668).
func TestFirstRoundGradeAskedPerRound(t *testing.T) {
	defaults := map[string]string{"B": "A", "C": "B"}
	tests := []struct {
		name      string
		prGrade   string
		firstMap  map[string]string
		earlier   bool // a patch of an earlier head was already reviewed
		wantFlag  string
		wantAsked string
	}{
		{"B round 1", "B", defaults, false, "A", "A"},
		{"B round 2", "B", defaults, true, "", "B"},
		{"C round 1", "C", defaults, false, "B", "B"},
		{"C round 2", "C", defaults, true, "", "C"},
		{"A round 1", "A", defaults, false, "", "A"},
		{"configured C to A", "C", map[string]string{"C": "A"}, false, "A", "A"},
		{"empty map turns the raise off", "B", map[string]string{}, false, "", "B"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			s.Config.Review.FirstRoundGrade = tc.firstMap
			// The title's grade is the floor: equal to the PR grade, it leaves the policy under test alone.
			runner.sink.title = "Fix the thing (grade " + tc.prGrade + ", T2)"
			g.pull.Body += "Mergewake-Grade: " + tc.prGrade + "\n"
			if tc.earlier {
				if g.passes == nil {
					g.passes = map[string][]github.PassReceipt{}
				}
				g.passes[shaB] = []github.PassReceipt{{Pass: 1, Family: "openai", Patch: "earlier-patch"}}
			}
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("spawns = %d", len(runner.spawns))
			}
			sp := runner.spawns[0]
			if got := arg(sp, "--grade"); got != tc.wantFlag || (tc.wantFlag == "" && slices.Contains(sp, "--grade")) {
				t.Fatalf("--grade = %q, want %q in %v", got, tc.wantFlag, sp)
			}
			rows := auditRows(t, s, r, "review")
			if len(rows) != 1 {
				t.Fatalf("review rows = %v", rows)
			}
			row := rows[0]
			if row["pr_grade"] != tc.prGrade || row["grade_asked"] != tc.wantAsked || row["first_round"] != !tc.earlier || row["seat"] != "unknown" {
				t.Fatalf("audit row = %v", row)
			}
		})
	}
}

// A whole-diff review of a PR that already has a reviewed head is not round
// one, whatever the round plan fell back to.
func TestFirstReviewRoundCountsOtherHeadsOnly(t *testing.T) {
	own := github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Patch: "p1"}}
	other := github.ReviewedPass{Head: shaB, PassReceipt: github.PassReceipt{Pass: 1, Patch: "p0"}}
	for name, tc := range map[string]struct {
		history []github.ReviewedPass
		want    bool
	}{
		"no history":                  {nil, true},
		"own passes only":             {[]github.ReviewedPass{own}, true},
		"an earlier head":             {[]github.ReviewedPass{other}, false},
		"an earlier head and own one": {[]github.ReviewedPass{other, own}, false},
		// The PR moved to other, then reset to shaA: shaA's first pass is still round one.
		"reset to the first head": {[]github.ReviewedPass{own, other}, true},
	} {
		if got := firstReviewRound(tc.history, shaA); got != tc.want {
			t.Errorf("%s: firstReviewRound = %v, want %v", name, got, tc.want)
		}
	}
}

// The task title's grade is a floor: panewire's explicit --grade overrides it,
// so a round-one raise is not stated when the title grade is stronger.
func TestFirstRoundGradeFloorIsTaskTitle(t *testing.T) {
	for name, tc := range map[string]struct {
		title     string
		wantFlag  string
		wantAsked string
	}{
		"title stronger than the raise": {"Fix the thing (grade S, T1)", "", "A"},
		"title equal to the raise":      {"Fix the thing (grade A, T2)", "A", "A"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			s.Config.Review.FirstRoundGrade = map[string]string{"B": "A"}
			runner.sink.title = tc.title
			g.pull.Body += "Mergewake-Grade: B\n"
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("spawns = %d", len(runner.spawns))
			}
			sp := runner.spawns[0]
			if got := arg(sp, "--grade"); got != tc.wantFlag || (tc.wantFlag == "" && slices.Contains(sp, "--grade")) {
				t.Fatalf("--grade = %q, want %q in %v", got, tc.wantFlag, sp)
			}
			rows := auditRows(t, s, r, "review")
			if len(rows) != 1 || rows[0]["grade_asked"] != tc.wantAsked || rows[0]["first_round"] != true {
				t.Fatalf("audit rows = %v", rows)
			}
		})
	}
}
