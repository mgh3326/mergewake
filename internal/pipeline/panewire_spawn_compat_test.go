package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

type compatibilitySpawnRunner struct {
	spawns       [][]string
	refuseModern bool
	refuseLegacy bool
	refuseAll    bool
	otherErr     error
	failures     []error
}

func (r *compatibilitySpawnRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command != "panewire" {
		return nil, errors.New("unexpected command")
	}
	r.spawns = append(r.spawns, slices.Clone(args))
	if len(r.failures) > 0 {
		err := r.failures[0]
		r.failures = r.failures[1:]
		return nil, err
	}
	if r.otherErr != nil {
		return nil, r.otherErr
	}
	if r.refuseLegacy && (slices.Contains(args, "--builder-family") || arg(args, "--role") == "worker") {
		return nil, &integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "--builder-family is not a flag the node accepts for a hub spawn"}
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
		"non-usage exit":          &integrations.CommandError{Command: "panewire", Err: panewireExit(1), Stderr: "unknown --author-family"},
		"unrelated usage":         &integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "--task value is malformed"},
		"role word in other flag": &integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "--consult-mode is not a flag"},
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

func TestPanewireSpawnCompatibilityRecoversWhenCachedCLIChanges(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	compat := &panewireSpawnCompatibility{now: func() time.Time { return now }}
	runner := &compatibilitySpawnRunner{refuseModern: true}
	args := []string{"spawn", "--author-family", "openai", "--role", "reviewer"}
	if _, err := compat.launch(context.Background(), runner, "panewire", args); err != nil {
		t.Fatal(err)
	}
	runner.refuseModern = false
	runner.refuseLegacy = true
	if _, err := compat.launch(context.Background(), runner, "panewire", args); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 4 || !slices.Contains(runner.spawns[2], "--builder-family") || !slices.Contains(runner.spawns[3], "--author-family") {
		t.Fatalf("cached old CLI did not retry with modern args: %v", runner.spawns)
	}
	if _, err := compat.launch(context.Background(), runner, "panewire", args); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 5 || !slices.Contains(runner.spawns[4], "--author-family") {
		t.Fatalf("modern spelling was not remembered after recovery: %v", runner.spawns)
	}
}

func TestPanewireSpawnCompatibilityPreservesRetryFailureAndInitialRefusal(t *testing.T) {
	modernErr := &integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "--author-family is not a flag"}
	legacyErr := &integrations.CommandError{Command: "panewire", Err: panewireExit(77), Stderr: "reviewer seat refused"}
	runner := &compatibilitySpawnRunner{failures: []error{modernErr, legacyErr}}
	compat := &panewireSpawnCompatibility{}
	_, err := compat.launch(context.Background(), runner, "panewire", []string{"spawn", "--author-family", "openai", "--role", "reviewer"})
	if err == nil || exitStatus(err) != 77 || !strings.Contains(integrations.Detail(err), "--author-family") || !strings.Contains(integrations.Detail(err), "reviewer seat refused") {
		t.Fatalf("retry error lost original refusal or retry result: %v (%s)", err, integrations.Detail(err))
	}
}

func TestPanewireRenamedArgsUsageErrorRequiresRoleContext(t *testing.T) {
	for _, stderr := range []string{
		"--role value must be implementer or reviewer",
		"invalid role: worker",
		"panewire: --role=consult is not accepted",
	} {
		if !panewireRenamedArgsUsageError(&integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: stderr}) {
			t.Errorf("role usage error was not recognized: %q", stderr)
		}
	}
	if panewireRenamedArgsUsageError(&integrations.CommandError{Command: "panewire", Err: panewireExit(2), Stderr: "unknown --consult-mode flag"}) {
		t.Fatal("unrelated role-like flag was classified as renamed-role usage")
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
