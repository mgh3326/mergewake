package pipeline

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestReviewerExpiresFormat(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "1m"},
		{0, "1m"},
		{-time.Minute, "1m"},
		{20 * time.Minute, "20m"},
		{20*time.Minute + time.Millisecond, "21m"},
		{time.Hour, "1h"},
		{90 * time.Minute, "90m"},
		{24 * time.Hour, "24h"},
		{48 * time.Hour, "24h"},
	} {
		if got := reviewerExpires(tc.d); got != tc.want {
			t.Errorf("reviewerExpires(%v)=%q, want %q", tc.d, got, tc.want)
		}
	}
}

// A fresh reviewer spawn carries the time left in its pass as --expires.
func TestPanewireFreshSpawnExpiresWithPass(t *testing.T) {
	for _, tc := range []struct {
		left time.Duration
		want string
	}{
		{20 * time.Minute, "20m"},
		{30 * time.Second, "1m"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}
			diff, _ := g.Diff(context.Background(), r.Name, 7)
			x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: diff, authorFamily: "anthropic"}
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(tc.left))
			defer cancel()
			if _, e := x.runPass(ctx, &passPlan{pass: 1, lens: "default"}, func() (string, bool, error) { return "lease", true, nil }); e != nil {
				t.Fatal(e)
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("spawns=%d", len(runner.spawns))
			}
			if got := arg(runner.spawns[0], "--expires"); got != tc.want {
				t.Fatalf("--expires=%q, want %q: %v", got, tc.want, runner.spawns[0])
			}
		})
	}
}

// A resumed replay keeps its recorded --expires: panewire fingerprints the
// submission, so a replay with another value is refused (errSpawnClaimed).
// Only --wait-timeout may differ from the recorded argv.
func TestPanewireResumedReplayKeepsRecordedExpires(t *testing.T) {
	s, _, r, runner, c := interruptedReviewer(t)
	first := runner.spawns[0]
	// The first pass ran on the test's short budget. The replay reads the
	// budget as configured now, so its remaining budget differs from the
	// recorded --expires: a rewrite would show up as a different value.
	s.Config.Review.Timeout = "25m"
	firstExpires := arg(first, "--expires")
	if firstExpires == "" {
		t.Fatalf("first spawn has no --expires: %v", first)
	}
	c.t = c.t.Add(10 * time.Minute)
	_ = s.Review(context.Background(), r, 7, "", Options{})
	if len(runner.spawns) < 2 {
		t.Fatalf("no replay: %d spawns", len(runner.spawns))
	}
	replay := runner.spawns[1]
	if got := arg(replay, "--expires"); got != firstExpires {
		t.Fatalf("replay --expires=%q, want the recorded %q: %v", got, firstExpires, replay)
	}
	if !slices.Equal(withoutWait(replay), withoutWait(first)) {
		t.Fatalf("replay changed recorded args beyond --wait-timeout:\n%v\n%v", first, replay)
	}
}
