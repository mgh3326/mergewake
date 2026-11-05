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
