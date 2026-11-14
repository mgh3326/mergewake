package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

// reviewProfiles is one reviewer profile whose family differs from the
// default family, so a profile that is not read changes the verdict.
func reviewProfiles(s *Service) {
	s.Config.Review.Profiles = map[string]config.Reviewer{
		"ds": {Family: "deepseek", Command: "pi", Args: []string{"--model", "ds-model"}},
	}
}

// A PR body without trailers takes the author profile from the head commit:
// the profile's family is the author family, so a deepseek reviewer is
// same-family.
func TestTrailersReviewTakesProfileFromHeadCommit(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	g.pull.Body = "no trailers\n"
	g.messages = map[string]string{shaA: "subject\n\nMergewake-Author-Profile: ds@high\n"}
	reviewProfiles(s)
	s.Runner.(*fakeRunner).pane.family = "deepseek"
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	pass := g.passes[shaA]
	if len(pass) != 1 || !pass[0].SameFamily {
		t.Fatalf("%+v", pass)
	}
}

// A family and a profile that disagree are a conflict wherever they are
// written: here both are only in the head commit.
func TestTrailersReviewFamilyAndProfileBothFromHeadCommit(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	g.pull.Body = "no trailers\n"
	g.messages = map[string]string{shaA: "subject\n\nMergewake-Author-Family: openai\nMergewake-Author-Profile: ds@high\n"}
	reviewProfiles(s)
	e := s.Review(context.Background(), r, 7, "", Options{})
	if e == nil || !strings.Contains(e.Error(), "author family conflicts with configured profile") {
		t.Fatalf("want a family/profile conflict: %v", e)
	}
}

// Where body and commit differ, the body's family and profile win: the
// commit's openai family would conflict with the body's deepseek profile.
func TestTrailersReviewBodyBeatsHeadCommit(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	g.pull.Body = "Mergewake-Author-Family: deepseek\nMergewake-Author-Profile: ds@high\n"
	g.messages = map[string]string{shaA: "subject\n\nMergewake-Author-Family: openai\nMergewake-Author-Profile: gpt@high\n"}
	reviewProfiles(s)
	s.Runner.(*fakeRunner).pane.family = "deepseek"
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	pass := g.passes[shaA]
	if len(pass) != 1 || !pass[0].SameFamily {
		t.Fatalf("%+v", pass)
	}
}

const commitScaleMessage = "subject\n\nMergewake-Author-Family: openai\nMergewake-Author-Profile: gpt@high\nMergewake-Author-Model: commit-model\nMergewake-Experiment: exp/arm-c\nMergewake-Grade: C\nMergewake-Tier: T1\n"

// observeTrailers observes one merged PR whose body and head commit carry the
// given text, and returns its outcome row.
func observeTrailers(t *testing.T, body, message, title string) outcomes.Outcome {
	t.Helper()
	s, g, _, _, repo := setup(t)
	merged := "2026-11-02T00:00:00Z"
	g.pull.CreatedAt = "2026-11-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = body
	g.messages = map[string]string{g.pull.Head.SHA: message}
	s.GH = &outcomeGH{fakeGH: g, first: true}
	s.Sink = &titleSink{title: title}
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	return state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
}

// A body with no trailers records the grade, tier and author identity the
// head commit declares, ahead of the task title.
func TestTrailersOutcomeRowFromHeadCommit(t *testing.T) {
	v := observeTrailers(t, "no trailers\n", commitScaleMessage, "do it (grade B, T2)")
	if v.Grade != "C" || v.Tier != "T1" || v.Family != "openai" || v.Profile != "gpt" || v.Effort != "high" || v.Model != "commit-model" || v.Experiment != "exp" || v.Arm != "arm-c" {
		t.Fatalf("%+v", v)
	}
}

// Where body and commit differ, the body wins for every trailer.
func TestTrailersOutcomeRowBodyBeatsHeadCommit(t *testing.T) {
	body := "Mergewake-Author-Family: google\nMergewake-Author-Profile: gem@low\nMergewake-Author-Model: body-model\nMergewake-Experiment: other/arm-b\nMergewake-Grade: A\nMergewake-Tier: T2\n"
	v := observeTrailers(t, body, commitScaleMessage, "do it (grade B, T3)")
	if v.Grade != "A" || v.Tier != "T2" || v.Family != "google" || v.Profile != "gem" || v.Effort != "low" || v.Model != "body-model" || v.Experiment != "other" || v.Arm != "arm-b" {
		t.Fatalf("%+v", v)
	}
}

// With no trailer anywhere, the row is as before: no identity, and the grade
// and tier come from the task title.
func TestTrailersOutcomeRowNeitherUnchanged(t *testing.T) {
	v := observeTrailers(t, "Mergewake-Task: 2981\n", "subject\n", "do it (grade B, T2)")
	if v.Grade != "B" || v.Tier != "T2" || v.Family != "" || v.Profile != "" || v.Model != "" || v.Experiment != "" {
		t.Fatalf("%+v", v)
	}
}

// One-task readers keep the first Mergewake-Task line; later lines and Closes
// references do not change it.
func TestPRTaskUsesFirstOfSeveralTrailers(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.pull.Body = "Mergewake-Task: 11\nMergewake-Task: 12\nCloses hk 13\n"
	task, err := s.prTask(context.Background(), r, g.pull)
	if err != nil || task != "11" {
		t.Fatalf("task %q, %v; want 11", task, err)
	}
}

// A task trailer only in the head commit does not block the first pass of a
// new head: the review runs and no BLOCKED comment is posted.
func TestTrailersTaskInHeadCommitDoesNotBlockFirstPass(t *testing.T) {
	f := blockSetup(t)
	f.g.messages = map[string]string{shaA: "subject\n\nMergewake-Task: 42\n"}
	if e := f.review(t); e != nil {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || f.n.count != 0 || f.blocked(t, 7) != "" || len(f.runner.spawns) != 1 || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("posts=%d events=%+v block=%q spawns=%d", f.gh.posts, f.n.events, f.blocked(t, 7), len(f.runner.spawns))
	}
}
