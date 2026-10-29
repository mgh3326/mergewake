package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestOptionalBotsAbsentSilently(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent gate", true: "explicit defaults"}[configured], func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.reviewed = true
			g.gate = configured
			g.feedback.ReviewAuthors = nil
			var out strings.Builder
			s.Out = &out
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 1 {
				t.Fatal("optional absence held landing", out.String())
			}
			state, err := s.ledger().Read()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			for _, text := range []string{out.String(), string(encoded)} {
				for _, forbidden := range []string{"coverage_gap", "missing_required_bots", "coderabbitai", "chatgpt-codex-connector", "grace"} {
					if strings.Contains(text, forbidden) {
						t.Fatalf("optional absence was recorded: %s", text)
					}
				}
			}
		})
	}
}

func TestRequiredBotsOptInUsesCurrentHead(t *testing.T) {
	for _, tt := range []struct {
		name, threadHead string
		review           bool
		action           string
		merges           int
	}{
		{name: "absent", action: "wait-for-required-bots"},
		{name: "old thread", threadHead: shaB, action: "wait-for-required-bots"},
		{name: "current thread", threadHead: shaA, action: "merge", merges: 1},
		{name: "current submitted review", review: true, action: "merge", merges: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.reviewed = true
			g.gate = true
			g.requiredBots = []string{"coderabbitai"}
			g.feedback.ReviewAuthors = nil
			if tt.review {
				g.feedback.ReviewAuthors = map[string]int{"coderabbitai": 1}
			}
			if tt.threadHead != "" {
				g.feedback.Threads = []github.ReviewThread{{Comments: []github.ReviewComment{{Author: "coderabbitai", CommitSHA: tt.threadHead, Body: "Minor finding"}}}}
			}
			var out strings.Builder
			s.Out = &out
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != tt.merges || landAction(t, s, r.Name) != tt.action {
				t.Fatal(out.String())
			}
			if tt.merges == 0 && !strings.Contains(out.String(), `"missing_required_bots":["coderabbitai"]`) {
				t.Fatal(out.String())
			}
			if strings.Contains(out.String(), "chatgpt-codex-connector") || strings.Contains(out.String(), "coverage_gap") {
				t.Fatal("optional bot absence leaked", out.String())
			}
		})
	}
}

func TestOptionalCurrentHeadFindingsStillApply(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		for _, tt := range []struct {
			name, author, body, head string
			answer                   bool
			action                   string
			merges                   int
		}{
			{"critical", "coderabbitai", "Critical finding", shaA, false, "wait-for-blocking-findings", 0},
			{"P1", "chatgpt-codex-connector", "P1 finding", shaA, false, "wait-for-blocking-findings", 0},
			{"major unanswered", "coderabbitai", "Major finding", shaA, false, "wait-for-triage-reply", 0},
			{"major answered", "coderabbitai", "Major finding", shaA, true, "merge", 1},
			{"minor", "coderabbitai", "Minor finding", shaA, false, "merge", 1},
			{"nitpick", "coderabbitai", "Nitpick finding", shaA, false, "merge", 1},
			{"old critical", "coderabbitai", "Critical old finding", shaB, false, "merge", 1},
		} {
			t.Run(map[bool]string{false: "routine/", true: "blocking/"}[blocking]+tt.name, func(t *testing.T) {
				s, g, _, _, r := setup(t)
				g.enabled = true
				g.reviewed = true
				g.blocking = blocking
				g.feedback.ReviewAuthors = nil
				comments := []github.ReviewComment{{Author: tt.author, CommitSHA: tt.head, Body: tt.body}}
				if tt.answer {
					comments = append(comments, github.ReviewComment{Author: "maintainer", AuthorType: "User", Body: "fixed"})
				}
				g.feedback.Threads = []github.ReviewThread{{Comments: comments}}
				if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
					t.Fatal(err)
				}
				if g.mergeCount != tt.merges || landAction(t, s, r.Name) != tt.action {
					t.Fatalf("action=%s merges=%d", landAction(t, s, r.Name), g.mergeCount)
				}
			})
		}
	}
}

func TestOwnReviewNeedsNewHeadAndPass(t *testing.T) {
	for _, actorType := range []string{"User", "Bot"} {
		t.Run(actorType, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.blocking = true
			g.reviewed = true
			g.feedback.RecheckedAuthors = map[string]bool{"review-bot": true}
			g.feedback.Threads = []github.ReviewThread{{IsResolved: true, IsOutdated: true, ResolvedBy: "review-bot", ResolvedByType: actorType, Comments: []github.ReviewComment{{Author: "review-bot", CommitSHA: shaA, Body: "<!-- mergewake-severity:high --> finding"}}}}
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 0 || landAction(t, s, r.Name) != "wait-for-blocking-findings" {
				t.Fatal("same head resolution cleared own finding")
			}
			g.reviewed = false
			g.pull.Head.SHA = shaC
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 0 || landAction(t, s, r.Name) != "wait-for-independent-review" {
				t.Fatal("new head without review landed")
			}
			g.reviewed = true
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 1 {
				t.Fatal("new head with review did not clear old own finding")
			}
			g.feedback.Threads[0].Comments[0].CommitSHA = shaC
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != 1 || landAction(t, s, r.Name) != "wait-for-blocking-findings" {
				t.Fatal("new own finding was cleared by resolution")
			}
		})
	}
}

type changedFeedbackGH struct{ *fakeGH }

func (g changedFeedbackGH) ReviewFeedback(context.Context, string, int) (github.ReviewFeedback, error) {
	return github.ReviewFeedback{HeadSHA: shaB}, nil
}
func TestFeedbackHeadMustMatchDecision(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.reviewed = true
	s.GH = changedFeedbackGH{g}
	if err := s.Land(context.Background(), r, 7, Options{}); err == nil || g.mergeCount != 0 {
		t.Fatal("unbound feedback accepted", err)
	}
}
