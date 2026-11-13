package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// deployChecksGH answers every check read from an explicit per-name map: a
// name it does not list is Absent, unlike fakeGH's blanket fallback. It records
// the names each read asked for.
type deployChecksGH struct {
	*fakeGH
	names map[string]map[string]github.CI
	reads []string
}

func (g *deployChecksGH) Check(_ context.Context, _, sha string, required []string) (github.CI, error) {
	g.reads = append(g.reads, strings.Join(required, ","))
	for _, n := range required {
		if g.names[sha][n] != github.Green {
			return github.Red, nil
		}
	}
	return github.Green, nil
}

func (g *deployChecksGH) CheckStates(_ context.Context, _, sha string, required []string) (map[string]github.CI, error) {
	out := make(map[string]github.CI, len(required))
	for _, n := range required {
		v, ok := g.names[sha][n]
		if !ok {
			v = github.Absent
		}
		out[n] = v
	}
	return out, nil
}

func (g *deployChecksGH) CheckEvidence(ctx context.Context, repo, sha string, required []string) ([]github.CheckEvidence, error) {
	states, e := g.CheckStates(ctx, repo, sha, required)
	if e != nil {
		return nil, e
	}
	out := make([]github.CheckEvidence, 0, len(required))
	for _, n := range required {
		out = append(out, github.CheckEvidence{Name: n, State: states[n], URL: "https://checks.example/" + n})
	}
	return out, nil
}

// A missing deploy result does not prevent the independent main checkpoint.
func TestWatchMainGreenWithMissingDeployOK(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.required = []string{"ci-ok"}
	g.policyExtra = "main_checks: [ci-ok]\ndeploy_checks: [deploy-ok]\n"
	g.head = shaB
	adapter := &deployChecksGH{fakeGH: g, names: map[string]map[string]github.CI{
		shaB: {"ci-ok": github.Green},
	}}
	s.GH = adapter

	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(adapter.reads, "ci-ok") || !slices.Contains(adapter.reads, "deploy-ok") {
		t.Fatalf("watch reads %v", adapter.reads)
	}
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "last-green"))
	if e != nil || string(b) != shaB {
		t.Fatalf("missing deploy-ok was not green: %q %v", b, e)
	}
}
