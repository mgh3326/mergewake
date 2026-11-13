package pipeline

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

type compatibilitySpawnRunner struct {
	spawns       [][]string
	refuseModern bool
	refuseAll    bool
	otherErr     error
}

func (r *compatibilitySpawnRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command != "panewire" {
		return nil, errors.New("unexpected command")
	}
	r.spawns = append(r.spawns, slices.Clone(args))
	if r.otherErr != nil {
		return nil, r.otherErr
	}
	if r.refuseAll || r.refuseModern && (slices.Contains(args, "--author-family") || arg(args, "--role") == "implementer") {
		return nil, &integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "flag provided but not defined: --author-family"}
	}
	return []byte("OK pane=w1:p1 model=fixture"), nil
}

func TestPanewireSpawnArgsTranslateRecordedNames(t *testing.T) {
	got := panewireSpawnArgs([]string{"spawn", "--builder-family", "anthropic", "--role", "worker", "--routing-role", "implementer"})
	if arg(got, "--author-family") != "anthropic" || slices.Contains(got, "--builder-family") || slices.Contains(got, "--routing-role") || arg(got, "--role") != "implementer" {
		t.Fatalf("translated spawn args: %v", got)
	}
	for _, old := range []string{"worker", "tester"} {
		if got := arg(panewireSpawnArgs([]string{"spawn", "--role", old}), "--role"); got != "implementer" {
			t.Errorf("role %q translated to %q", old, got)
		}
	}
	if got := arg(panewireSpawnArgs([]string{"spawn", "--role", "counsel"}), "--role"); got != "consult" {
		t.Errorf("counsel translated to %q", got)
	}
}

func TestPanewireSpawnCompatibilityRetriesAndRemembersOldVersion(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	compat := &panewireSpawnCompatibility{now: func() time.Time { return now }}
	runner := &compatibilitySpawnRunner{refuseModern: true}
	args := []string{"spawn", "--builder-family", "anthropic", "--role", "worker"}

	if _, err := compat.launch(context.Background(), runner, "panewire", args); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[0], "--author-family") != "anthropic" || arg(runner.spawns[1], "--builder-family") != "anthropic" || arg(runner.spawns[1], "--role") != "worker" {
		t.Fatalf("first spawn attempts: %v", runner.spawns)
	}
	if _, err := compat.launch(context.Background(), runner, "panewire", args); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 3 || arg(runner.spawns[2], "--builder-family") != "anthropic" {
		t.Fatalf("remembered spelling was not used without retry: %v", runner.spawns)
	}

	now = now.Add(panewireSpawnCompatibilityTTL + time.Second)
	if _, err := compat.launch(context.Background(), runner, "panewire", args); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 5 || arg(runner.spawns[3], "--author-family") != "anthropic" || arg(runner.spawns[4], "--builder-family") != "anthropic" {
		t.Fatalf("expired preference did not retry modern then old: %v", runner.spawns)
	}
}

func TestPanewireSpawnCompatibilityDoesNotRetryOtherFailures(t *testing.T) {
	for name, err := range map[string]error{
		"non-usage exit":  &integrations.CommandError{Command: "panewire", Err: panewireExit(1), Stderr: "unknown --author-family"},
		"unrelated usage": &integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "--task value is malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &compatibilitySpawnRunner{otherErr: err}
			compat := &panewireSpawnCompatibility{}
			if _, got := compat.launch(context.Background(), runner, "panewire", []string{"spawn", "--author-family", "openai", "--role", "reviewer"}); got == nil {
				t.Fatal("launch succeeded")
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("unexpected retry: %v", runner.spawns)
			}
		})
	}
}

func TestPanewireSpawnCompatibilityRetriesAtMostOnce(t *testing.T) {
	runner := &compatibilitySpawnRunner{refuseAll: true}
	compat := &panewireSpawnCompatibility{}
	if _, err := compat.launch(context.Background(), runner, "panewire", []string{"spawn", "--author-family", "openai", "--role", "reviewer"}); err == nil {
		t.Fatal("refused spawn succeeded")
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("got %d attempts, want modern and one legacy retry: %v", len(runner.spawns), runner.spawns)
	}
}
