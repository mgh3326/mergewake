package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// A PR body without a Mergewake-Author-Family line takes the family from the
// same trailer in the head commit, ahead of the Co-Authored-By inference.
func TestReviewCommitTrailerSetsFamily(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.pull.Body = "no family line\n"
	g.messages = map[string]string{shaA: "subject\n\nCo-Authored-By: Claude <noreply@anthropic.com>\nMergewake-Author-Family: deepseek\n"}
	s.Config.Review.Profiles = map[string]config.Reviewer{
		"ds": {Family: "deepseek", Command: "pi", Args: []string{"--model", "ds-model"}},
	}
	s.Runner.(*fakeRunner).recommend = []string{"ds"}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	pass := g.passes[shaA]
	if len(pass) != 1 || pass[0].Reviewer != "ds" || !pass[0].SameFamily {
		t.Fatalf("%+v", pass)
	}
}

// The PR body line still wins over the head commit's trailer.
func TestReviewBodyFamilyBeatsCommitTrailer(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.pull.Body = "Mergewake-Author-Family: openai\n"
	g.messages = map[string]string{shaA: "subject\n\nMergewake-Author-Family: deepseek\n"}
	s.Config.Review.Profiles = map[string]config.Reviewer{
		"gpt": {Family: "openai", Command: "pi", Args: []string{"--model", "gpt-model"}},
	}
	s.Runner.(*fakeRunner).recommend = []string{"gpt"}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	pass := g.passes[shaA]
	if len(pass) != 1 || pass[0].Reviewer != "gpt" || !pass[0].SameFamily {
		t.Fatalf("%+v", pass)
	}
}

// With no family in the body or the commit and an unknown identity, the
// review stays unavailable and the error names both sources.
func TestReviewFamilyErrorNamesSources(t *testing.T) {
	s, g, _, _, r := setup(t)
	s.Config.Review.DefaultFamily = ""
	g.pull.Body = "no family line\n"
	g.authorName, g.authorEmail = "mgh3326", "m@example.com"
	g.committerName, g.committerEmail = "GitHub", "noreply@github.com"
	e := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "PR body or head commit") {
		t.Fatal(e)
	}
}

// Resolving the family reads the head commit once: a body with no family line
// fetches it exactly once more than a body that already carries one.
func TestReviewFetchesHeadCommitOncePerFamilyResolution(t *testing.T) {
	calls := func(body string) int {
		s, g, _, _, r := setup(t)
		g.pull.Body = body
		g.messages = map[string]string{shaA: "subject\n\nMergewake-Author-Family: deepseek\n"}
		s.Config.Review.Profiles = map[string]config.Reviewer{
			"ds": {Family: "deepseek", Command: "pi", Args: []string{"--model", "ds-model"}},
		}
		s.Runner.(*fakeRunner).recommend = []string{"ds"}
		g.commitCalls = 0
		if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
			t.Fatal(e)
		}
		return g.commitCalls
	}
	withBody, noBody := calls("Mergewake-Author-Family: deepseek\n"), calls("no family line\n")
	if noBody != withBody+1 {
		t.Fatalf("commit fetches: body family %d, no body family %d", withBody, noBody)
	}
}
