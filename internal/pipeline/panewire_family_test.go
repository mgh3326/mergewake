package pipeline

import (
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestReviewerFamilyFallbackIsStableAcrossProfiles(t *testing.T) {
	profiles := map[string]config.Reviewer{}
	for _, name := range []string{"zeta", "alpha", "mid", "beta", "omega"} {
		profiles[name] = config.Reviewer{Family: "family-" + name, GateProfile: "seat-a"}
	}
	x := &panewireExecutor{s: &Service{Config: config.Config{Review: config.Review{Profiles: profiles}}}}
	for range 50 {
		if got := x.reviewerFamily(integrations.JobRecord{}, t.TempDir(), "seat-a"); got != "family-alpha" {
			t.Fatalf("seat shared by several profiles resolved to %q, want the first by name", got)
		}
	}
}

func TestPanewireAuthorFamilyCanonicalizesAliases(t *testing.T) {
	for _, tc := range []struct {
		name   string
		family string
		want   string
	}{
		{name: "claude", family: "claude", want: "anthropic"},
		{name: "capital claude", family: "Claude", want: "anthropic"},
		{name: "padded opus", family: " opus ", want: "anthropic"},
		{name: "codex", family: "codex", want: "openai"},
		{name: "gemini", family: "gemini", want: "google"},
		{name: "agy", family: "agy", want: "google"},
		{name: "grok", family: "grok", want: "xai"},
		{name: "anthropic", family: "anthropic", want: "anthropic"},
		{name: "cognition", family: "cognition", want: "cognition"},
		{name: "deepseek", family: "deepseek", want: "deepseek"},
		{name: "google", family: "google", want: "google"},
		{name: "meta", family: "meta", want: "meta"},
		{name: "minimax", family: "minimax", want: "minimax"},
		{name: "mistral", family: "mistral", want: "mistral"},
		{name: "mixed", family: "mixed", want: "mixed"},
		{name: "openai", family: "openai", want: "openai"},
		{name: "other", family: "other", want: "other"},
		{name: "qwen", family: "qwen", want: "qwen"},
		{name: "upstage", family: "upstage", want: "upstage"},
		{name: "xai", family: "xai", want: "xai"},
		{name: "zhipu", family: "zhipu", want: "zhipu"},
		{name: "devin", family: "devin", want: "cognition"},
		{name: "kimi", family: "kimi", want: "other"},
		{name: "moonshot", family: "moonshot", want: "other"},
		{name: "unknown", family: "unknown-x", want: "other"},
		{name: "foo", family: "foo", want: "other"},
		{name: "empty", family: "", want: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := panewireAuthorFamily(tc.family); got != tc.want {
				t.Fatalf("panewireAuthorFamily(%q)=%q, want %q", tc.family, got, tc.want)
			}
		})
	}
}
