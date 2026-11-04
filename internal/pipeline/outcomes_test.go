package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
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

func TestAuthorProfile(t *testing.T) {
	for _, tt := range []struct {
		value, profile, effort string
	}{
		{"opus", "opus", ""},
		{"opus@high", "opus", "high"},
		{"codex-sol@xhigh", "codex-sol", "xhigh"},
		{"codex@bogus", "codex", ""},
		{"a@b@c", "", ""},
		{"bad name", "", ""},
		{"", "", ""},
	} {
		profile, effort := authorProfile(tt.value)
		if profile != tt.profile || effort != tt.effort {
			t.Errorf("authorProfile(%q) = %q, %q; want %q, %q", tt.value, profile, effort, tt.profile, tt.effort)
		}
	}
}

// fleetPRBody keeps the trailer block of mgh3326/panewire#143 (merged
// 2026-11-01), written to the rstack wake brief without an @effort suffix.
const fleetPRBody = "Synthetic summary of the change.\n" +
	"\n" +
	"Mergewake-Author-Family: anthropic\n" +
	"Mergewake-Author-Profile: opus\n" +
	"\n" +
	"🤖 Generated with [Claude Code](https://claude.com/claude-code)"

func TestObservePRRecordsProfileWithoutEffort(t *testing.T) {
	for _, tt := range []struct {
		name, body, model string
	}{
		{"derived from profile", fleetPRBody, "opus-model"},
		{"explicit model wins", fleetPRBody + "\nMergewake-Author-Model: concrete-model\n", "concrete-model"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, repo := setup(t)
			s.Config.Review.Profiles["opus"] = config.Reviewer{Family: "anthropic", Command: "claude", Args: []string{"--model", "opus-model", "--effort", "high"}}
			g.pull.Body = tt.body
			s.GH = &outcomeGH{fakeGH: g, first: true}
			if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
				t.Fatal(err)
			}
			state, err := s.ledger().Read()
			if err != nil {
				t.Fatal(err)
			}
			v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
			if v.Profile != "opus" || v.Effort != "" || v.Model != tt.model || v.Family != "anthropic" {
				t.Fatalf("profile=%q effort=%q model=%q family=%q", v.Profile, v.Effort, v.Model, v.Family)
			}
		})
	}
}

// Rows written before profile-only trailers were accepted stay unattributed,
// so ReconcileOutcomes revisits them and records the profile on its next pass.
func TestReconcileBackfillsProfileWithoutEffort(t *testing.T) {
	s, g, _, _, repo := setup(t)
	s.Config.Review.Profiles["opus"] = config.Reviewer{Family: "anthropic", Command: "claude", Args: []string{"--model", "opus-model"}}
	merged := time.Now().UTC().Format(time.RFC3339)
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = fleetPRBody
	s.GH = &outcomeGH{fakeGH: g, first: true}
	if err := s.ledger().Update(repo.Name, g.pull.Number, func(v *outcomes.Outcome) { v.Final = true; v.Decision = "landed" }); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileOutcomes(context.Background(), repo, map[int]bool{}, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	if v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]; v.Profile != "opus" || v.Model != "opus-model" {
		t.Fatalf("profile=%q model=%q", v.Profile, v.Model)
	}
}
