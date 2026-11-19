package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestReviewResultLost(t *testing.T) {
	for _, tc := range []struct{ name, result, reason string }{
		{"missing", "", "completed without a result"},
		{"invalid", `{"unexpected":true}`, "completed with a result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &panewireRunner{}
			s, g, r := panewireSetup(t, runner)
			s.Config.Notifier.Lane = "desk"
			runner.sink.missing = &integrations.JobRecord{State: "completed", Profile: "reviewer-profile", Result: json.RawMessage(tc.result)}
			err := s.Review(context.Background(), r, 7, "", Options{})
			if err == nil || errors.Is(err, ErrReviewBlocked) || runner.headless != 0 || g.postCount != 0 || g.refs[doneRef(7, shaA)] {
				t.Fatalf("err=%v headless=%d posts=%d refs=%v", err, runner.headless, g.postCount, g.refs)
			}
			sp := runner.spawns[0]
			_, rows := readLines(t, filepath.Join(s.state(r.Name), "decisions.jsonl"))
			var lost []map[string]any
			for _, row := range rows {
				if row["command"] == "review-result-lost" {
					lost = append(lost, row["value"].(map[string]any))
				}
			}
			if len(lost) != 1 {
				t.Fatalf("lost audit=%v", lost)
			}
			want := map[string]any{"pr": float64(7), "head": shaA, "pass": float64(1), "lens": "default", "label": arg(sp, "-l"), "record": tc.reason, "profile": "reviewer-profile", "report": arg(sp, "--report")}
			for k, v := range want {
				if lost[0][k] != v {
					t.Errorf("%s=%v want %v", k, lost[0][k], v)
				}
			}
			if lost[0]["collect_error"] == "" || lost[0]["collect_error"] == nil {
				t.Errorf("missing error: %v", lost[0])
			}
			// A single lost result is counted by cause and not told (hk 5361).
			if n := s.Notifier.(*fakeNotifier); n.count != 0 {
				t.Fatalf("events=%v", n.events)
			}
			causeRows := auditRows(t, s, r, "notice-cause")
			if len(causeRows) != 1 || causeRows[0]["sent"] != false || causeRows[0]["cause"] != "lost:other" || causeRows[0]["pr"] != float64(7) || causeRows[0]["head"] != shaA {
				t.Fatalf("notice-cause rows=%v", causeRows)
			}
		})
	}
}

func TestRetryReviewResultLostBelowThresholdIsQuiet(t *testing.T) {
	runner := &panewireRunner{}
	s, g, r := panewireSetup(t, runner)
	s.Config.Notifier.Lane = "desk"
	runner.sink.missing = &integrations.JobRecord{State: "completed"}
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 3}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("first review did not fail")
	}
	c.t = c.t.Add(time.Minute)
	// The review lease also expires before the next attempt.
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	restarted := *s
	if err := restarted.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("second review did not fail")
	}
	n := s.Notifier.(*fakeNotifier)
	if len(runner.spawns) != 2 || runner.headless != 0 || n.count != 0 {
		t.Fatalf("spawns=%d headless=%d events=%v", len(runner.spawns), runner.headless, n.events)
	}
}

func TestReviewResultCompletedAndSkips(t *testing.T) {
	for _, tc := range []struct {
		name, result string
		exit         int
		pane         bool
	}{
		{"valid", `{"findings":[]}`, 0, true},
		{"refused", `{"refused":"cannot review"}`, 0, false},
		{"spawn failure", "", 70, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &panewireRunner{}
			if tc.exit != 0 {
				runner.refuse = map[string]int{"reviewer": tc.exit}
			}
			s, g, r := panewireSetup(t, runner)
			runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(tc.result)}
			err := s.Review(context.Background(), r, 7, "", Options{})
			// A pane that produced no pass fails it; nothing runs headless.
			if (err == nil) != tc.pane || (!tc.pane && !errors.Is(err, ErrReviewUnavailable)) {
				t.Fatal(err)
			}
			want := map[bool]int{true: 1, false: 0}[tc.pane]
			if g.postCount != want || g.refs[doneRef(7, shaA)] != tc.pane || runner.headless != 0 {
				t.Fatal(fmt.Sprintf("posts=%d headless=%d refs=%v", g.postCount, runner.headless, g.refs))
			}
		})
	}
}
