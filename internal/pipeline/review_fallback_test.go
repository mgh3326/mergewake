package pipeline

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

const devinLimit = "Agent error: Reached free model rate limit for SWE-2. Your limit will reset in 1 hour 2 minutes (at 11:01 UTC)\n\"cognition.ai/errorKind\": \"unavailable\", \"retryable\": true"

// fallbackRunner serves a free devin pool ranked ahead of a paid codex one. A
// reviewer fails with the stderr set for its CLI; the others return no findings.
type fallbackRunner struct {
	stderr   map[string]string // reviewer CLI → stderr of its exit-1 failure
	observes [][]string
	reviewed []string
	observe  error
}

func (r *fallbackRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command == "scopefuel" {
		switch args[0] {
		case "--json":
			return []byte(idleUsage), nil
		case "--recommend":
			return []byte(`{"rows":[` +
				`{"profile":"devin-swe2","kind":"candidate","pool":"devin","billing":"free"},` +
				`{"profile":"devin-swe2-medium","kind":"candidate","pool":"devin","billing":"free"},` +
				`{"profile":"codex","kind":"candidate","pool":"idle"},` +
				`{"profile":"grok","kind":"candidate","pool":"idle"}]}`), nil
		case "observe":
			r.observes = append(r.observes, args)
			return nil, r.observe
		}
		return nil, nil // gate
	}
	cli := ""
	for _, a := range args {
		switch filepath.Base(a) {
		case "devin", "codex", "grok":
			cli = filepath.Base(a)
		}
	}
	r.reviewed = append(r.reviewed, cli)
	if text, ok := r.stderr[cli]; ok {
		return nil, &integrations.CommandError{Command: "env", Err: errors.New("exit status 1"), Stderr: text}
	}
	return []byte(`{"findings":[]}`), nil
}

func fallbackSetup(t *testing.T, runner *fallbackRunner) (*Service, *fakeGH, *bytes.Buffer) {
	t.Helper()
	s, g, out := paceSetup(t, map[string]config.Reviewer{
		"devin-swe2":        {Family: "cognition", Command: "devin", Args: []string{"--model", "swe-2"}},
		"devin-swe2-medium": {Family: "cognition", Command: "devin", Args: []string{"--model", "swe-2-medium"}},
		"codex":             {Family: "openai", Command: "codex", Args: []string{"--model", "gpt-x"}},
		"grok":              {Family: "xai", Command: "grok", Args: []string{"--model", "grok-x"}},
	}, nil)
	s.Runner = runner
	return s, g, out
}

var fallbackRepo = config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml"}

func TestRateLimitedDevinIsRecordedAndNextReviewerRuns(t *testing.T) {
	runner := &fallbackRunner{stderr: map[string]string{"devin": devinLimit}}
	s, g, out := fallbackSetup(t, runner)
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	// The sibling SWE-2 rung shares the exhausted free pool and is not tried.
	if strings.Join(runner.reviewed, ",") != "devin,codex" || g.postCount != 1 {
		t.Fatal(runner.reviewed, g.postCount)
	}
	if len(runner.observes) != 1 {
		t.Fatal(runner.observes)
	}
	o := strings.Join(runner.observes[0], " ")
	for _, want := range []string{"observe devin-free-limit", "--reset ", "--observed ", "--rung swe-2", "--source mergewake:example/project#7"} {
		if !strings.Contains(o, want) {
			t.Fatalf("observe args %q lack %q", o, want)
		}
	}
	text := out.String()
	for _, want := range []string{`"reviewer":"codex"`, `"skipped":[`, `"reviewer":"devin-swe2","reason":"quota exhausted"`, `"reviewer":"devin-swe2-medium","reason":"free pool rate-limited earlier in this review"`, `"reset":"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("event lacks %s: %s", want, text)
		}
	}
	if g.passes[shaA][0].Reviewer != "codex" {
		t.Fatal(g.passes)
	}
}

func TestRateLimitWithoutResetIsSkippedNotObserved(t *testing.T) {
	runner := &fallbackRunner{stderr: map[string]string{"devin": "Agent error: Reached free model rate limit"}}
	s, g, out := fallbackSetup(t, runner)
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.observes) != 0 || g.postCount != 1 || !strings.Contains(out.String(), "reset time not reported") {
		t.Fatal(runner.observes, g.postCount, out.String())
	}
}

func TestObserveFailureStillFallsBack(t *testing.T) {
	runner := &fallbackRunner{stderr: map[string]string{"devin": devinLimit}, observe: errors.New("offline")}
	s, g, out := fallbackSetup(t, runner)
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 || !strings.Contains(out.String(), "recording the limit failed") {
		t.Fatal(g.postCount, out.String())
	}
}

func TestGenericFailureTriesNextCandidateOnce(t *testing.T) {
	runner := &fallbackRunner{stderr: map[string]string{"devin": "segfault"}}
	s, g, out := fallbackSetup(t, runner)
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	// A crash is not a quota event: nothing is observed, the failed profile is
	// not retried, and its sibling profile gets its own single attempt.
	if len(runner.observes) != 0 || strings.Join(runner.reviewed, ",") != "devin,devin,codex" {
		t.Fatal(runner.observes, runner.reviewed)
	}
	if g.postCount != 1 || !strings.Contains(out.String(), `"reason":"review failed"`) {
		t.Fatal(g.postCount, out.String())
	}
}

func TestEveryReviewerFailingIsAClearUnavailableError(t *testing.T) {
	runner := &fallbackRunner{stderr: map[string]string{"devin": devinLimit, "codex": "429 Too Many Requests", "grok": "boom"}}
	s, g, out := fallbackSetup(t, runner)
	err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{})
	if !errors.Is(err, ErrReviewUnavailable) {
		t.Fatal(err)
	}
	for _, want := range []string{"devin-swe2: quota exhausted until ", "codex: quota exhausted", "grok: review failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error lacks %q: %v", want, err)
		}
	}
	// Each reviewer ran at most once, and nothing was posted from a failed run.
	if len(runner.reviewed) != 3 || g.postCount != 0 {
		t.Fatal(runner.reviewed, g.postCount)
	}
	if !strings.Contains(out.String(), `"review-skipped"`) {
		t.Fatal(out.String())
	}
}

func TestInvalidOutputFallsBack(t *testing.T) {
	runner := &garbageRunner{fallbackRunner: &fallbackRunner{}}
	s, g, _ := fallbackSetup(t, runner.fallbackRunner)
	s.Runner = runner
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 || g.passes[shaA][0].Reviewer == "devin-swe2" {
		t.Fatal(g.postCount, g.passes)
	}
}

// garbageRunner makes devin answer with something that is not a review.
type garbageRunner struct{ *fallbackRunner }

func (r *garbageRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	for _, a := range args {
		if filepath.Base(a) == "devin" {
			return []byte("not json"), nil
		}
	}
	return r.fallbackRunner.Run(ctx, command, args, input, dir)
}
