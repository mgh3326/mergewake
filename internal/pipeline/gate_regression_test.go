package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestRoutineRequiresExactHeadReview(t *testing.T) {
	for _, which := range []string{"missing", "stale", "current"} {
		t.Run(which, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			head := shaB
			if which == "current" {
				head = g.pull.Head.SHA
			}
			if which != "missing" {
				g.passes = map[string][]github.PassReceipt{head: {{Pass: 1}}}
			}
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			want := 0
			if which == "current" {
				want = 1
			}
			if g.mergeCount != want {
				t.Fatalf("merges=%d action=%s", g.mergeCount, landAction(t, s, r.Name))
			}
		})
	}
}

func TestLandOptionalBotsNeverHold(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent gate", true: "configured gate"}[configured], func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.reviewed = true
			g.gate = configured
			g.feedback.ReviewAuthors = nil
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 1 || landAction(t, s, r.Name) != "merge" {
				t.Fatal("optional bots held landing")
			}
		})
	}
}

func TestOwnBlockingFindingsHold(t *testing.T) {
	for _, severity := range []string{"", "medium", "low"} {
		for _, configured := range []bool{false, true} {
			t.Run(severity+map[bool]string{false: " absent", true: " configured"}[configured], func(t *testing.T) {
				s, g, _, _, r := setup(t)
				g.enabled = true
				g.blocking = true
				g.reviewed = true
				g.gate = configured
				body := "finding"
				if severity != "" {
					body = "<!-- mergewake-severity:" + severity + " --> finding"
				}
				g.feedback.Threads = []github.ReviewThread{{Path: "tool", Line: 1, Comments: []github.ReviewComment{{Author: "review-bot", CommitSHA: shaA, Body: body}, {Author: "dev", AuthorType: "User", Body: "ok"}}}}
				if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
					t.Fatal(err)
				}
				if g.mergeCount != 0 || landAction(t, s, r.Name) != "wait-for-blocking-findings" {
					t.Fatal("unresolved own finding landed")
				}
			})
		}
	}
}

func TestDistinctPassesAndLenses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		passes []github.PassReceipt
		want   int
	}{
		{"duplicate pass one", []github.PassReceipt{{Pass: 1, Family: "openai", Lens: "correctness"}, {Pass: 1, Family: "openai", Lens: "correctness"}}, 0},
		{"same family same lens", []github.PassReceipt{{Pass: 1, Family: "openai", Lens: "correctness", SameFamily: true}, {Pass: 2, Family: "openai", Lens: "correctness", SameFamily: true}}, 0},
		{"same family distinct lenses", []github.PassReceipt{{Pass: 1, Family: "openai", Lens: "correctness", SameFamily: true}, {Pass: 2, Family: "openai", Lens: "state-risk", SameFamily: true}}, 1},
		{"missing family unknown", []github.PassReceipt{{Pass: 1}, {Pass: 2}}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.blocking = true
			g.passes = map[string][]github.PassReceipt{g.pull.Head.SHA: tt.passes}
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != tt.want {
				t.Fatalf("merges=%d", g.mergeCount)
			}
		})
	}
}

func TestReviewReclaimsExpiredClaim(t *testing.T) {
	for _, age := range []time.Duration{time.Second, 2 * time.Minute} {
		t.Run(age.String(), func(t *testing.T) {
			s, g, _, _, r := setup(t)
			ref := claimRef(7, g.pull.Head.SHA, 1)
			g.refs = map[string]bool{ref: true}
			g.leaseTimes = map[string]time.Time{ref: time.Now().Add(-age)}
			var out strings.Builder
			s.Out = &out
			if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
				t.Fatal(err)
			}
			want := 0
			if age == 2*time.Minute {
				want = 1
			}
			if g.postCount != want || s.Runner.(*fakeRunner).calls != want {
				t.Fatalf("posts=%d output=%s", g.postCount, out.String())
			}
		})
	}
}

func TestBlockFindingsApplyWithoutGateSection(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "present"}[configured], func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.reviewed = true
			g.gate = configured
			g.feedback.Threads = []github.ReviewThread{{Path: "tool", Line: 1, IsResolved: true, ResolvedBy: "pr-author", ResolvedByType: "User", Comments: []github.ReviewComment{
				{Author: "chatgpt-codex-connector", CommitSHA: shaA, AuthorType: "Bot", Body: "P1 concrete bug"},
				{Author: "pr-author", AuthorType: "User", Body: "ok"},
			}}}
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 0 || landAction(t, s, r.Name) != "wait-for-blocking-findings" {
				t.Fatal("answered, manually resolved P1 landed")
			}
		})
	}
}
