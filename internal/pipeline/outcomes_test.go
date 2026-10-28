package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

type outcomeGH struct {
	*fakeGH
	recovery github.Pull
	first    bool
	calls    int
}

func (g *outcomeGH) FirstPRCI(context.Context, string, github.Pull, []string) (*bool, error) {
	g.calls++
	v := g.first
	return &v, nil
}
func (g *outcomeGH) Pull(_ context.Context, _ string, n int) (github.Pull, error) {
	if n == 9 {
		return g.recovery, nil
	}
	return g.pull, nil
}
func TestShadowOutcomesAndSevenDayFollowup(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Now().UTC()
	opened := now.Add(-10 * 24 * time.Hour)
	merged := now.Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	g.pull.CreatedAt = opened.Format(time.RFC3339)
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = "Mergewake-Author-Profile: example@high\nMergewake-Author-Model: concrete-model\nMergewake-Experiment: effort-test/control\nMergewake-Grade: A\n"
	source := &outcomeGH{fakeGH: g, first: true}
	s.GH = source
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.recordReview(repo.Name, 7, g.pull.Head.SHA, []github.Finding{}, github.PassReceipt{}, false); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 7)]
	if v.Evidence() != "clean_pass" || v.TimeToMerge == nil || v.Experiment != "effort-test" || !v.Shadow {
		t.Fatalf("%+v", v)
	}
	source.first = false
	if err = s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 {
		t.Fatal("overwrote initial CI")
	}
	// An actual watch recovery merge, rather than merely opening it, fails the PR.
	merged = now.Add(-24 * time.Hour).Format(time.RFC3339)
	g.pull.MergedAt = &merged
	recovered := now.Add(-time.Hour).Format(time.RFC3339)
	source.recovery.MergedAt = &recovered
	if err = s.ledger().Update(repo.Name, 7, func(v *outcomes.Outcome) { v.Reverted = nil; v.RevertPR = 9 }); err != nil {
		t.Fatal(err)
	}
	if err = s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	state, _ = s.ledger().Read()
	v = state.Latest[outcomes.Key(repo.Name, 7)]
	if v.Evidence() != "failure" || v.Reverted == nil || !*v.Reverted {
		t.Fatal(v)
	}
	if g.mergeCount != 0 || g.revertCount != 0 || g.claimCount != 0 {
		t.Fatal("shadow observation mutated GitHub")
	}
}
