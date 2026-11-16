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

func TestPanewireBuilderFamilyCanonicalizesAliases(t *testing.T) {
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
		{name: "deepseek", family: "deepseek", want: "deepseek"},
		{name: "anthropic", family: "anthropic", want: "anthropic"},
		{name: "openai", family: "openai", want: "openai"},
		{name: "devin", family: "devin", want: "other"},
		{name: "cognition", family: "cognition", want: "other"},
		{name: "kimi", family: "kimi", want: "other"},
		{name: "unknown", family: "unknown-x", want: "other"},
		{name: "empty", family: "", want: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := panewireBuilderFamily(tc.family); got != tc.want {
				t.Fatalf("panewireBuilderFamily(%q)=%q, want %q", tc.family, got, tc.want)
			}
		})
	}
}
