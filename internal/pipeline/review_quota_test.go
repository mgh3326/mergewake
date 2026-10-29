package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

type rankingRunner struct {
	gates   []string
	input   string
	args    []string
	reviews int
}

func (r *rankingRunner) Run(_ context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "scopefuel" {
		if args[0] == "--recommend" {
			if args[1] != "A" {
				return nil, errors.New("wrong grade")
			}
			return []byte(`{"rows":[{"profile":"same","kind":"candidate"},{"profile":"kimi","kind":"unsubscribed"},{"profile":"limited","kind":"candidate"},{"profile":"good","kind":"candidate"}]}`), nil
		}
		r.gates = append(r.gates, args[2])
		if args[2] == "limited" {
			return nil, errors.New("quota denied")
		}
		return nil, nil
	}
	r.reviews++
	r.input = input
	r.args = args
	return []byte(`{"findings":[]}`), nil
}
func TestRankedFallbackAndOptionalPairings(t *testing.T) {
	s, g, _, _, repo := setup(t)
	g.pull.Body = "Mergewake-Grade: A\n"
	s.Config.Review.Pairings = nil
	s.Config.Review.Profiles = map[string]config.Reviewer{
		"same": {Family: "openai", Command: "codex"}, "kimi": {Family: "moonshot", Command: "kimi"}, "limited": {Family: "anthropic", Command: "pi"}, "good": {Family: "deepseek", Command: "cmd"},
	}
	runner := &rankingRunner{}
	s.Runner = runner
	if err := s.Review(context.Background(), repo, 7, "openai", Options{}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(runner.gates, ",") != "limited,good" || runner.reviews != 1 || g.claimCount != 2 {
		t.Fatal(runner.gates, runner.reviews, g.claimCount)
	}
	if !strings.Contains(strings.Join(runner.args, " "), "--safe-mode") {
		t.Fatal(runner.args)
	}
}
func TestPairingCannotEnableUnsubscribedPool(t *testing.T) {
	s, g, _, _, repo := setup(t)
	g.pull.Body = "Mergewake-Grade: A\n"
	s.Config.Review.Profiles = map[string]config.Reviewer{"kimi": {Family: "moonshot", Command: "kimi"}}
	s.Config.Review.Pairings = map[string][]string{"openai": {"kimi"}}
	runner := &rankingRunner{}
	s.Runner = runner
	if err := s.Review(context.Background(), repo, 7, "openai", Options{}); err == nil {
		t.Fatal("unsubscribed override admitted")
	}
	if runner.reviews != 0 || len(runner.gates) > 0 || g.claimCount > 0 {
		t.Fatal("acted for unsubscribed pool")
	}
}
