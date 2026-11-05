package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// fakeSecrets are credential-shaped fixtures a reviewer's stderr carries;
// none may reach the decision log or the fleet events.
var fakeSecrets = []string{"ghp_FAKEfakeFAKEfakeFAKEfake0123456789", "hunter2pass", "tok3nVALUEfake99"}

const fakeSecretText = "token=tok3nVALUEfake99 ghp_FAKEfakeFAKEfakeFAKEfake0123456789 https://ops:hunter2pass@reviewer.invalid/v1"

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
// tail and the requested host in the decision log and as one reviewer event,
// and the pass fails without a headless review.
func TestPanewireSpawnFailureRecordsCause(t *testing.T) {
	runner := &panewireRunner{refuse: map[string]int{"reviewer": 70}, refuseStderr: "panewire: quota gate refused " + fakeSecretText + "\n"}
	s, _, r := panewireSetup(t, runner)
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); !errors.Is(err, ErrReviewUnavailable) || runner.headless != 0 {
		t.Fatalf("err=%v headless=%d", err, runner.headless)
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
	if len(events) != 2 || events[0]["kind"] != "reviewer" || events[0]["outcome"] != "failed" || events[0]["reason"] != "panewire_spawn_exit" || events[1]["kind"] != "review" || events[1]["reason"] != "review_missing" {
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
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); !errors.Is(err, ErrReviewUnavailable) {
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
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatal(err)
	}
	details := reviewerEventDetails(t, s.Config.StateDir)
	want := `pass=1 reviewer=panewire profile= host=auto reason="no Mergewake-Task trailer"`
	if len(details) != 1 || details[0] != want {
		t.Fatalf("details %q, want [%q]", details, want)
	}
}
