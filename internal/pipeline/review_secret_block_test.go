package pipeline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type secretRefusalRunner struct {
	*panewireRunner
	output string
	stderr bool
}

func (r *secretRefusalRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" {
		r.spawns = append(r.spawns, args)
		if r.stderr {
			return nil, &integrations.CommandError{Command: command, Err: panewireExit(81), Stderr: r.output}
		}
		return []byte(r.output), panewireExit(81)
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

func TestSecretLikeSpawnBlock(t *testing.T) {
	for _, stderr := range []bool{true, false} {
		t.Run(map[bool]string{true: "stderr", false: "stdout"}[stderr], func(t *testing.T) {
			const private = "fixture-sensitive-text-do-not-log"
			f := blockSetup(t)
			f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
			runner := &secretRefusalRunner{panewireRunner: f.runner, output: "secret_like_content " + private, stderr: stderr}
			f.s.Runner = runner
			var log bytes.Buffer
			f.s.Out = &log
			if err := f.review(t); !errors.Is(err, ErrReviewBlocked) {
				t.Fatalf("expected ErrReviewBlocked, got %v", err)
			}
			if got := f.blocked(t, 7); got != "blocked: secret-like-content" {
				t.Fatalf("block = %q", got)
			}
			if f.gh.posts != 1 {
				t.Fatalf("comments = %d", f.gh.posts)
			}
			for _, want := range []string{"credential", "remove", "fixture", "placeholder", "push"} {
				if !strings.Contains(strings.ToLower(f.gh.comments[0].Body), want) {
					t.Errorf("comment missing %q", want)
				}
			}
			x := &panewireExecutor{s: f.s, repo: f.r, pr: f.g.pull}
			if _, exists, err := x.ownReviewerJob(1); err != nil || exists {
				t.Fatalf("reviewer record exists=%v err=%v", exists, err)
			}
			for range 2 {
				if err := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); err != nil {
					t.Fatal(err)
				}
			}
			var state reviewRetryState
			if _, err := fsutil.ReadJSON(filepath.Join(f.s.state(f.r.Name), "review-retry-7.json"), &state); err != nil || state.Failures != 0 {
				t.Fatalf("retry state=%+v err=%v", state, err)
			}
			if f.gh.posts != 1 {
				t.Fatalf("repeat comments = %d", f.gh.posts)
			}
			if got := f.blocked(t, 7); got != "blocked: secret-like-content" {
				t.Fatalf("repeat block = %q", got)
			}
			before := len(runner.spawns)
			f.g.pull.Head.SHA = shaC
			if err := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); err != nil {
				t.Fatal(err)
			}
			if len(runner.spawns) != before+1 || f.gh.posts != 2 {
				t.Fatalf("new head spawns=%d comments=%d", len(runner.spawns)-before, f.gh.posts)
			}
			data, err := os.ReadFile(filepath.Join(f.s.state(f.r.Name), "decisions.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), private) || strings.Contains(log.String(), private) {
				t.Fatal("refusal text appeared in audit or output")
			}
			_, events := reviewRecords(t, f.s, f.r, "review-blocked")
			if strings.Contains(mustJSON(t, events), private) || strings.Contains(mustJSON(t, f.n.events), private) {
				t.Fatal("refusal text appeared in events")
			}
		})
	}
}

func TestSecretLikeSpawnOtherExit81Fails(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
	f.s.Runner = &secretRefusalRunner{panewireRunner: f.runner, output: "another refusal", stderr: true}
	err := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{})
	if !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("expected normal failure, got %v", err)
	}
	var state reviewRetryState
	if _, e := fsutil.ReadJSON(filepath.Join(f.s.state(f.r.Name), "review-retry-7.json"), &state); e != nil || state.Failures != 1 {
		t.Fatalf("retry state=%+v err=%v", state, e)
	}
	if f.gh.posts != 0 {
		t.Fatalf("comments = %d", f.gh.posts)
	}
}
