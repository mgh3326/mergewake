package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

// idleUsage is a quota snapshot whose single pool is unused, so pace keeps the
// recommendation order in tests about family and lens selection.
const idleUsage = `{"providers":[{"id":"idle","pool_class":"spend","status":"ok","buckets":[{"label":"7d","window":"7d","used_pct":0,"scope":{"kind":"account"}}]}]}`

// paceRunner serves one recommendation (profile → pool) and a usage snapshot
// built from per-pool (used %, time to a 7d reset) pairs.
type paceRunner struct {
	rows    [][2]string
	used    map[string][2]float64
	usage   int
	gates   []string
	reviews int
}

func (r *paceRunner) Run(_ context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "scopefuel" {
		switch args[0] {
		case "--json":
			r.usage++
			pools := []string{}
			for id, u := range r.used {
				reset := time.Now().Add(time.Duration(u[1] * float64(time.Hour))).UTC().Format(time.RFC3339)
				pools = append(pools, fmt.Sprintf(`{"id":%q,"status":"ok","buckets":[{"label":"7d","window":"7d","used_pct":%v,"resets_at":%q,"scope":{"kind":"account"}}]}`, id, u[0], reset))
			}
			return []byte(`{"providers":[` + strings.Join(pools, ",") + `]}`), nil
		case "--recommend":
			rows := []string{}
			for _, row := range r.rows {
				rows = append(rows, fmt.Sprintf(`{"profile":%q,"kind":"candidate","pool":%q}`, row[0], row[1]))
			}
			return []byte(`{"rows":[` + strings.Join(rows, ",") + `]}`), nil
		}
		r.gates = append(r.gates, args[2])
		return nil, nil
	}
	r.reviews++
	for _, a := range args {
		if a == "--input-format" {
			return []byte(`{"event":"result","result":{"status":"SUCCESS","response":"{\"findings\":[]}","num_turns":1}}`), nil
		}
	}
	return []byte(`{"findings":[]}`), nil
}

func paceSetup(t *testing.T, profiles map[string]config.Reviewer, runner *paceRunner) (*Service, *fakeGH, *bytes.Buffer) {
	t.Helper()
	s, g, _, _, _ := setup(t)
	s.Config.Review.Pairings = nil
	s.Config.Review.Profiles = profiles
	s.Runner = runner
	out := &bytes.Buffer{}
	s.Out = out
	return s, g, out
}

func TestPacePrefersWasteOverRawUsage(t *testing.T) {
	// "low" has used only 20% but is 10% into its week (200% at reset); "high"
	// has used 60% with 90% elapsed (67%) and would otherwise waste a third.
	runner := &paceRunner{
		rows: [][2]string{{"low", "fast"}, {"high", "slow"}},
		used: map[string][2]float64{"fast": {20, 151.2}, "slow": {60, 16.8}},
	}
	s, g, out := paceSetup(t, map[string]config.Reviewer{
		"low":  {Family: "google", Command: "agy", Args: []string{"--model", "m-low"}},
		"high": {Family: "xai", Command: "grok", Args: []string{"--model", "m-high"}},
	}, runner)
	repo := config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml"}
	if err := s.Review(context.Background(), repo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if runner.usage != 1 || strings.Join(runner.gates, ",") != "high" || runner.reviews != 1 || g.claimCount != 2 {
		t.Fatal(runner.usage, runner.gates, runner.reviews, g.claimCount)
	}
	if !strings.Contains(out.String(), `"reviewer":"high"`) || !strings.Contains(out.String(), `"pace":{"pool":"slow","tier":2,"projected_pct":66.`) {
		t.Fatal(out.String())
	}
}

func TestPaceKeepsFamilyRuleAboveWaste(t *testing.T) {
	// The author's family has an idle pool, but a different family under 100%
	// still reviews first; same family is only a fallback.
	runner := &paceRunner{
		rows: [][2]string{{"same", "idle"}, {"other", "busy"}},
		used: map[string][2]float64{"idle": {0, 100}, "busy": {85, 16.8}},
	}
	s, _, _ := paceSetup(t, map[string]config.Reviewer{
		"same":  {Family: "anthropic", Command: "pi", Args: []string{"--model", "anthropic/model-a"}},
		"other": {Family: "deepseek", Command: "cmd", Args: []string{"--model", "model-b"}},
	}, runner)
	repo := config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml"}
	if err := s.Review(context.Background(), repo, 7, "anthropic", Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if runner.reviews != 0 || len(runner.gates) != 0 {
		t.Fatal("dry run acted")
	}
	if !strings.Contains(s.Out.(*bytes.Buffer).String(), `"candidates":["other","same"]`) {
		t.Fatal(s.Out)
	}
}

func TestPaceFailsClearlyWhenAllTight(t *testing.T) {
	runner := &paceRunner{
		rows: [][2]string{{"burning", "fast"}, {"unmeasured", "nowhere"}},
		used: map[string][2]float64{"fast": {50, 151.2}},
	}
	s, g, _ := paceSetup(t, map[string]config.Reviewer{
		"burning":    {Family: "google", Command: "agy", Args: []string{"--model", "m-a"}},
		"unmeasured": {Family: "xai", Command: "grok", Args: []string{"--model", "m-b"}},
	}, runner)
	repo := config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml"}
	err := s.Review(context.Background(), repo, 7, "anthropic", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), "burning (fast skipped: projected 500% at 7d reset)") || !strings.Contains(err.Error(), "unmeasured (nowhere skipped: pool not measured)") {
		t.Fatal(err)
	}
	if runner.reviews != 0 || len(runner.gates) != 0 || g.claimCount != 0 {
		t.Fatal("acted without headroom", runner.reviews, runner.gates, g.claimCount)
	}
}

func TestPaceNeverPicksUnknownPoolSilently(t *testing.T) {
	runner := &paceRunner{
		rows: [][2]string{{"unknown", ""}, {"known", "slow"}},
		used: map[string][2]float64{"slow": {95, 1}},
	}
	s, _, _ := paceSetup(t, map[string]config.Reviewer{
		"unknown": {Family: "google", Command: "agy", Args: []string{"--model", "m-a"}},
		"known":   {Family: "xai", Command: "grok", Args: []string{"--model", "m-b"}},
	}, runner)
	repo := config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml"}
	if err := s.Review(context.Background(), repo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(runner.gates, ",") != "known" {
		t.Fatal(runner.gates)
	}
}

func TestRankSelectSkipsUsage(t *testing.T) {
	runner := &paceRunner{rows: [][2]string{{"first", ""}, {"second", "slow"}}}
	s, _, _ := paceSetup(t, map[string]config.Reviewer{
		"first":  {Family: "google", Command: "agy", Args: []string{"--model", "m-a"}},
		"second": {Family: "xai", Command: "grok", Args: []string{"--model", "m-b"}},
	}, runner)
	s.Config.Review.Select = "rank"
	repo := config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml"}
	if err := s.Review(context.Background(), repo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if runner.usage != 0 || strings.Join(runner.gates, ",") != "first" {
		t.Fatal(runner.usage, runner.gates)
	}
}
