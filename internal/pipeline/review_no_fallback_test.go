package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// With review.executor: panewire, panewire is the pass's only executor: a
// pass its reviewer pane did not produce fails the run, and nothing runs
// headless (hk 2882).

// paneDeadlineRunner is a panewireRunner whose spawn waits on its pane until the
// review deadline, then exits non-zero as a timed-out --wait does.
type paneDeadlineRunner struct{ *panewireRunner }

func (r paneDeadlineRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" {
		r.spawns = append(r.spawns, args)
		<-ctx.Done()
		return nil, panewireExit(1)
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

// reviewRecords returns the decision-log rows of command and the fleet events.
func reviewRecords(t *testing.T, s *Service, r config.Repo, command string) (rows, events []map[string]any) {
	t.Helper()
	_, all := readLines(t, filepath.Join(s.state(r.Name), "decisions.jsonl"))
	for _, row := range all {
		if row["command"] == command {
			rows = append(rows, row["value"].(map[string]any))
		}
	}
	if path := filepath.Join(s.Config.StateDir, "events", "fleet-events.jsonl"); fileExists(path) {
		_, events = readLines(t, path)
	}
	return rows, events
}

func fileExists(path string) bool {
	_, e := os.Stat(path)
	return e == nil
}

func missingReason(events []map[string]any) string {
	for _, ev := range events {
		if ev["kind"] == "review" && ev["outcome"] == "missing" {
			return ev["reason"].(string)
		}
	}
	return ""
}

// assertPanewirePassFailed checks a run whose pane produced no pass: no
// headless reviewer, no posted review, no done marker, a review_missing event
// and a review-skipped audit naming the panewire skip.
func assertPanewirePassFailed(t *testing.T, s *Service, g *fakeGH, r config.Repo, runner *panewireRunner, err error, reason string) {
	t.Helper()
	if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), "(panewire: ") || !strings.Contains(err.Error(), reason) {
		t.Fatalf("err=%v, want ErrReviewUnavailable naming %q", err, reason)
	}
	if runner.headless != 0 || g.postCount != 0 || g.refs[doneRef(7, shaA)] || len(g.passes[shaA]) != 0 {
		t.Fatalf("headless=%d posts=%d refs=%v passes=%+v", runner.headless, g.postCount, g.refs, g.passes[shaA])
	}
	skipped, events := reviewRecords(t, s, r, "review-skipped")
	if len(skipped) != 1 || !strings.Contains(mustJSON(t, skipped[0]["skipped"]), `"reviewer":"panewire"`) || !strings.Contains(mustJSON(t, skipped[0]["skipped"]), reason) {
		t.Fatalf("review-skipped audit=%v", skipped)
	}
	if strings.Contains(mustJSON(t, skipped[0]["skipped"]), `"reviewer":"headless"`) {
		t.Fatalf("headless skip recorded: %v", skipped)
	}
	if got := missingReason(events); got != "review_missing" {
		t.Fatalf("missing event reason=%q events=%v", got, events)
	}
	if reviews, _ := reviewRecords(t, s, r, "review"); len(reviews) != 0 {
		t.Fatalf("review audit written: %v", reviews)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

// (a) A spawn that exits 70 may leave its pane running: the pass fails and is
// not re-run headless while that pane may still review the same head.
func TestPanewireSpawnExit70RunsNoHeadless(t *testing.T) {
	runner := &panewireRunner{refuse: map[string]int{"reviewer": 70}}
	s, g, r := panewireSetup(t, runner)
	err := s.Review(context.Background(), r, 7, "openai", Options{})
	assertPanewirePassFailed(t, s, g, r, runner, err, "panewire spawn exit 70")
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
}

// (b) No findings and a pane refusal fail the pass the same way; a spawn cut
// off by the review deadline keeps its review_deadline event. None runs
// headless.
func TestPanewireSkipsRunNoHeadless(t *testing.T) {
	for name, tc := range map[string]struct {
		runner *panewireRunner
		reason string
	}{
		"no findings":  {&panewireRunner{family: "openai"}, "no findings"},
		"pane refusal": {&panewireRunner{family: "openai", findings: `{"refused":"HEAD is 1234567, expected ` + shaA + `"}`}, "refused: HEAD is 1234567"},
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r := panewireSetup(t, tc.runner)
			err := s.Review(context.Background(), r, 7, "openai", Options{})
			assertPanewirePassFailed(t, s, g, r, tc.runner, err, tc.reason)
			if len(tc.runner.spawns) != 1 {
				t.Fatalf("spawns=%d", len(tc.runner.spawns))
			}
		})
	}
	t.Run("spawn deadline", func(t *testing.T) {
		runner := &panewireRunner{family: "openai"}
		s, g, r := panewireSetup(t, runner)
		s.Runner = paneDeadlineRunner{runner}
		s.Config.Review.Timeout = "2s"
		err := s.Review(context.Background(), r, 7, "openai", Options{})
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err=%v", err)
		}
		if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 0 || g.refs[doneRef(7, shaA)] {
			t.Fatalf("spawns=%d headless=%d posts=%d refs=%v", len(runner.spawns), runner.headless, g.postCount, g.refs)
		}
		_, events := reviewRecords(t, s, r, "review-skipped")
		if got := missingReason(events); got != "review_deadline" {
			t.Fatalf("missing event reason=%q events=%v", got, events)
		}
	})
}

// (c) Through RetryReview a failed pane backs the review off; the next run
// past the backoff retries the pass through panewire and posts. A refused
// spawn started no job, so the retry spawns a new reviewer pane under a new
// label; a spawn that failed after it may have started keeps its job record,
// so the retry resumes that job instead (reviewer_job.go). Nothing runs
// headless in any run.
func TestRetryReviewRespawnsPanewireAfterFailedPane(t *testing.T) {
	for _, tc := range []struct {
		name          string
		refuse        map[string]int
		failedSpawns  int // spawn calls in the failed run
		retrySpawns   int // spawn calls in the retry
		newLabelRetry bool
	}{
		{"refused", map[string]int{"reviewer": panewireRefused, "worker": panewireRefused}, 2, 1, true},
		{"spawn exit 70", map[string]int{"reviewer": 70}, 1, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai", refuse: tc.refuse}
			s, g, r := panewireSetup(t, runner)
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			s.Now = c.now
			s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 3}
			if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewUnavailable) {
				t.Fatalf("first run: %v", err)
			}
			if len(runner.spawns) != tc.failedSpawns || g.postCount != 0 {
				t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
			}
			// Inside the backoff nothing runs.
			if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil || len(runner.spawns) != tc.failedSpawns {
				t.Fatalf("backoff run: err=%v spawns=%d", err, len(runner.spawns))
			}
			c.t = c.t.Add(time.Minute)
			// The failed run's pass lease also expires before the next attempt.
			g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
			runner.refuse = nil
			if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
				t.Fatalf("second run: %v", err)
			}
			if len(runner.spawns) != tc.failedSpawns+tc.retrySpawns {
				t.Fatalf("retry spawns: %q", runner.spawns)
			}
			if tc.newLabelRetry && arg(runner.spawns[0], "-l") == arg(runner.spawns[len(runner.spawns)-1], "-l") {
				t.Fatalf("no new reviewer spawned: %q", runner.spawns)
			}
			got := g.passes[shaA]
			if runner.headless != 0 || g.postCount != 1 || !g.refs[doneRef(7, shaA)] || len(got) != 1 || got[0].Executor != "panewire" || got[0].Fallback != "" {
				t.Fatalf("headless=%d posts=%d refs=%v passes=%+v", runner.headless, g.postCount, g.refs, got)
			}
		})
	}
}

// (d) The panewire executor needs no headless profiles and no quota tool: with
// none configured and a failing quota command the review still spawns its
// reviewer, and the dry-run plan names the executor without candidates.
func TestPanewireNeedsNoProfilesOrQuota(t *testing.T) {
	for name, profiles := range map[string]map[string]config.Reviewer{"missing": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai", quotaErr: errors.New("scopefuel: not found")}
			s, g, r := panewireSetup(t, runner)
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			s.Config.Review.Profiles = profiles
			s.Config.Review.Pairings = nil
			out := &bytes.Buffer{}
			s.Out = out
			if err := s.Review(context.Background(), r, 7, "openai", Options{DryRun: true}); err != nil {
				t.Fatal(err)
			}
			var plan struct {
				Passes      []map[string]any `json:"passes"`
				PaceSkipped []string         `json:"pace_skipped"`
			}
			if e := json.Unmarshal(out.Bytes(), &plan); e != nil {
				t.Fatalf("%v: %s", e, out)
			}
			if len(plan.Passes) != 1 || plan.Passes[0]["executor"] != "panewire" || plan.PaceSkipped != nil {
				t.Fatalf("plan: %s", out)
			}
			for _, key := range []string{"candidates", "pace", "same_family"} {
				if _, ok := plan.Passes[0][key]; ok {
					t.Fatalf("plan names headless %s: %s", key, out)
				}
			}
			if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
				t.Fatal(err)
			}
			got := g.passes[shaA]
			if runner.quotaCalls != 0 || runner.headless != 0 || len(runner.spawns) != 1 || g.postCount != 1 || len(got) != 1 || got[0].Executor != "panewire" {
				t.Fatalf("quota=%d headless=%d spawns=%d posts=%d passes=%+v", runner.quotaCalls, runner.headless, len(runner.spawns), g.postCount, got)
			}
			reviews, _ := reviewRecords(t, s, r, "review")
			if len(reviews) != 1 || reviews[0]["executor"] != "panewire" || reviews[0]["pace"] != nil || reviews[0]["fallback"] != nil {
				t.Fatalf("review audit=%v", reviews)
			}
		})
	}
}
