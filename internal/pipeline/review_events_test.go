package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// fakeSecrets are credential-shaped fixtures a reviewer's stderr carries;
// none may reach the decision log or the fleet events.
var fakeSecrets = []string{"ghp_FAKEfakeFAKEfakeFAKEfake0123456789", "hunter2pass", "tok3nVALUEfake99"}

const fakeSecretText = "token=tok3nVALUEfake99 ghp_FAKEfakeFAKEfakeFAKEfake0123456789 https://ops:hunter2pass@reviewer.invalid/v1"

// crashRunner fails only the SWE-2 devin rung, with exit status 3 and a
// stderr that explains the crash; every other reviewer behaves as in
// fallbackRunner.
type crashRunner struct{ *fallbackRunner }

func (r crashRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command != "scopefuel" && slices.Contains(args, "swe-2") {
		r.reviewed = append(r.reviewed, "devin")
		stderr := strings.Repeat("noise line\n", 200) + "panic: reviewer crashed " + fakeSecretText + "\n"
		return nil, &integrations.CommandError{Command: "env", Err: panewireExit(3), Stderr: stderr}
	}
	return r.fallbackRunner.Run(ctx, command, args, input, dir)
}

func readLines(t *testing.T, path string) (string, []map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("not JSON: %q", line)
		}
		rows = append(rows, row)
	}
	return string(raw), rows
}

func TestFailingReviewerRecordsExitCodeAndStderrTail(t *testing.T) {
	inner := &fallbackRunner{}
	s, g, _ := fallbackSetup(t, inner)
	s.Runner = crashRunner{inner}
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 {
		t.Fatalf("the next reviewer must still post: %d", g.postCount)
	}

	// Decision log: the review record's skip carries the cause.
	logRaw, rows := readLines(t, filepath.Join(s.Config.StateDir, "example/project", "decisions.jsonl"))
	var skip map[string]any
	for _, row := range rows {
		if row["command"] != "review" {
			continue
		}
		value, _ := row["value"].(map[string]any)
		skipped, _ := value["skipped"].([]any)
		if len(skipped) > 0 {
			skip, _ = skipped[0].(map[string]any)
		}
	}
	if skip == nil {
		t.Fatalf("no skip in the decision log: %s", logRaw)
	}
	tail, _ := skip["stderr_tail"].(string)
	if skip["reviewer"] != "devin-swe2" || skip["reason"] != "review failed" || skip["exit_code"] != float64(3) || skip["host"] != "host-a" {
		t.Fatalf("skip: %v", skip)
	}
	if !strings.Contains(tail, "panic: reviewer crashed") || len(tail) > skipStderrTail {
		t.Fatalf("stderr tail: %d bytes %q", len(tail), tail)
	}

	// Fleet events: exactly one, in the v1 shape.
	evRaw, events := readLines(t, filepath.Join(s.Config.StateDir, "events", "fleet-events.jsonl"))
	if len(events) != 1 {
		t.Fatalf("want one event: %s", evRaw)
	}
	ev := events[0]
	if len(ev) != 8 {
		t.Fatalf("event is not the v1 shape: %s", evRaw)
	}
	for _, key := range []string{"ts", "host", "tool", "kind", "job", "outcome", "reason", "detail"} {
		if _, ok := ev[key].(string); !ok {
			t.Fatalf("field %s missing: %s", key, evRaw)
		}
	}
	if ev["tool"] != "mergewake" || ev["kind"] != "reviewer" || ev["job"] != "example/project#7" || ev["outcome"] != "failed" || ev["reason"] != "review_failed" {
		t.Fatalf("event: %v", ev)
	}
	detail := ev["detail"].(string)
	for _, want := range []string{"reviewer=devin-swe2", "host=host-a", "exit=3", "stderr: ", "panic: reviewer crashed"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("detail %q lacks %q", detail, want)
		}
	}
	for _, secret := range fakeSecrets {
		if strings.Contains(logRaw, secret) || strings.Contains(evRaw, secret) {
			t.Fatalf("secret %q leaked", secret)
		}
	}
}

func TestNoReviewerRecordsReviewMissingEvent(t *testing.T) {
	runner := &fallbackRunner{stderr: map[string]string{"devin": "boom", "codex": "boom", "grok": "boom"}}
	s, _, _ := fallbackSetup(t, runner)
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatal(err)
	}
	raw, events := readLines(t, filepath.Join(s.Config.StateDir, "events", "fleet-events.jsonl"))
	missing := 0
	failed := 0
	for _, ev := range events {
		switch {
		case ev["kind"] == "review" && ev["outcome"] == "missing" && ev["reason"] == "review_missing":
			missing++
		case ev["kind"] == "reviewer" && ev["outcome"] == "failed":
			failed++
		}
	}
	if missing != 1 || failed != len(runner.reviewed) || failed == 0 {
		t.Fatalf("missing=%d failed=%d reviewed=%v: %s", missing, failed, runner.reviewed, raw)
	}
}

func TestSkipReasonCodes(t *testing.T) {
	for reason, want := range map[string]string{
		"review failed":                      "review_failed",
		"quota gate refused: pool exhausted": "quota_gate_refused",
		"panewire spawn exit 70: boom":       "panewire_spawn_exit",
		"task 2478 unreadable: x":            "task_unreadable",
		"":                                   "unknown",
	} {
		if got := skipReasonCode(reason); got != want {
			t.Fatalf("%q: got %q want %q", reason, got, want)
		}
	}
}

// A refused panewire reviewer spawn records its exit code, a redacted stderr
// tail and the requested host in the decision log and as one event, and the
// review still completes headless.
func TestPanewireSpawnFailureRecordsCause(t *testing.T) {
	runner := &panewireRunner{refuse: map[string]int{"reviewer": 70}, refuseStderr: "panewire: quota gate refused " + fakeSecretText + "\n"}
	s, _, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
		t.Fatal(err)
	}
	logRaw, rows := readLines(t, filepath.Join(s.Config.StateDir, r.Name, "decisions.jsonl"))
	var skip map[string]any
	for _, row := range rows {
		value, _ := row["value"].(map[string]any)
		skipped, _ := value["skipped"].([]any)
		for _, raw := range skipped {
			if sk, _ := raw.(map[string]any); sk["reviewer"] == "panewire" {
				skip = sk
			}
		}
	}
	tail, _ := skip["stderr_tail"].(string)
	if skip == nil || skip["exit_code"] != float64(70) || skip["host"] != s.Config.Review.Panewire.Host || !strings.Contains(tail, "panewire: quota gate refused") {
		t.Fatalf("panewire skip lacks its cause: %v\n%s", skip, logRaw)
	}
	evRaw, events := readLines(t, filepath.Join(s.Config.StateDir, "events", "fleet-events.jsonl"))
	if len(events) != 1 || events[0]["kind"] != "reviewer" || events[0]["outcome"] != "failed" || events[0]["reason"] != "panewire_spawn_exit" {
		t.Fatalf("events: %s", evRaw)
	}
	for _, secret := range fakeSecrets {
		if strings.Contains(logRaw, secret) || strings.Contains(evRaw, secret) {
			t.Fatalf("secret %q leaked", secret)
		}
	}
}

// reviewerEventDetails returns the detail of every reviewer fleet event.
func reviewerEventDetails(t *testing.T, stateDir string) []string {
	t.Helper()
	_, events := readLines(t, filepath.Join(stateDir, "events", "fleet-events.jsonl"))
	var details []string
	for _, ev := range events {
		if ev["kind"] == "reviewer" {
			details = append(details, ev["detail"].(string))
		}
	}
	return details
}

// A spawned panewire pane that fails names, in the event, the launch profile
// its hk job record carries; the other fields keep their names and order.
func TestPanewireFailureEventNamesJobRecordProfile(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, _, r := panewireSetup(t, runner)
	runner.sink.missing = &integrations.JobRecord{State: "escalated", Family: "openai", Profile: "codex-high"}
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
		t.Fatal(err)
	}
	details := reviewerEventDetails(t, s.Config.StateDir)
	want := `pass=1 reviewer=panewire profile=codex-high host=auto reason="reviewer job record escalated"`
	if len(details) != 1 || details[0] != want {
		t.Fatalf("details %q, want [%q]", details, want)
	}
	_, rows := readLines(t, filepath.Join(s.Config.StateDir, r.Name, "decisions.jsonl"))
	for _, row := range rows {
		value, _ := row["value"].(map[string]any)
		skipped, _ := value["skipped"].([]any)
		for _, raw := range skipped {
			if sk, _ := raw.(map[string]any); sk["reviewer"] == "panewire" && sk["profile"] != "codex-high" {
				t.Fatalf("decision-log skip lacks the profile: %v", sk)
			}
		}
	}
}

// A skip before any pane was spawned has no profile yet: the field is
// present and empty.
func TestPanewireSkipBeforeSpawnLeavesProfileEmpty(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`}
	s, g, r := panewireSetup(t, runner)
	g.pull.Body = "Mergewake-Author-Family: openai\n"
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
		t.Fatal(err)
	}
	details := reviewerEventDetails(t, s.Config.StateDir)
	want := `pass=1 reviewer=panewire profile= host=auto reason="no Mergewake-Task trailer"`
	if len(details) != 1 || details[0] != want {
		t.Fatalf("details %q, want [%q]", details, want)
	}
}

// A headless reviewer that fails is named by the profile it ran as.
func TestHeadlessFailureEventNamesProfile(t *testing.T) {
	inner := &fallbackRunner{}
	s, _, _ := fallbackSetup(t, inner)
	s.Runner = crashRunner{inner}
	if err := s.Review(context.Background(), fallbackRepo, 7, "anthropic", Options{}); err != nil {
		t.Fatal(err)
	}
	details := reviewerEventDetails(t, s.Config.StateDir)
	if len(details) != 1 || !strings.HasPrefix(details[0], `pass=1 reviewer=devin-swe2 profile=devin-swe2 host=host-a exit=3 reason="review failed" stderr: `) {
		t.Fatalf("details %q", details)
	}
}
