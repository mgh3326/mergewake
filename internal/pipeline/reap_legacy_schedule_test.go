package pipeline

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

// legacyRestart is a fresh Service over the same config and state dir, as a
// daemon start would build, with its own GitHub read counter.
func legacyRestart(s *Service, gh GitHub) *Service {
	return &Service{Config: s.Config, GH: gh, Runner: s.Runner, Sink: s.Sink, Notifier: s.Notifier, Out: io.Discard, Now: s.Now}
}

// legacyRowsOf are one repo's legacy-sweep rows.
func legacyRowsOf(t *testing.T, s *Service, repo string) []map[string]any {
	t.Helper()
	if _, e := os.Stat(s.reapLog(repo)); e != nil {
		return nil
	}
	var out []map[string]any
	for _, r := range reapLogRows(t, s, repo) {
		if r["action"] == legacyAction {
			out = append(out, r)
		}
	}
	return out
}

// (a) The interval gates the sweep: it runs when due, skips inside the
// interval, and a restart inside the interval does not sweep again.
func TestReapLegacyScheduledHoldsTheIntervalAcrossRestarts(t *testing.T) {
	s, _, gh := legacyFixture(t)
	s.Config.Reap.LegacyEvery = "24h"
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	repos := s.Config.Repos
	ctx := context.Background()

	// No attempt recorded yet, so the first pass sweeps.
	if e := s.ReapLegacyScheduled(ctx, repos, plan()); e != nil {
		t.Fatal(e)
	}
	if gh.closedCalls != 1 {
		t.Fatalf("first pass swept %d times, want 1", gh.closedCalls)
	}
	// The next pass inside the interval does not.
	now = now.Add(23 * time.Hour)
	if e := s.ReapLegacyScheduled(ctx, repos, plan()); e != nil {
		t.Fatal(e)
	}
	if gh.closedCalls != 1 {
		t.Fatalf("pass inside the interval swept %d times, want 1", gh.closedCalls)
	}
	// A restart inside the interval reads the recorded attempt, not its own
	// empty memory.
	restarted := legacyRestart(s, &legacyGH{closed: gh.closed})
	if e := restarted.ReapLegacyScheduled(ctx, repos, plan()); e != nil {
		t.Fatal(e)
	}
	if calls := restarted.GH.(*legacyGH).closedCalls; calls != 0 {
		t.Fatalf("restart inside the interval swept %d times", calls)
	}
	// Once the interval has passed on the same clock, the restarted daemon
	// sweeps on its next pass.
	now = now.Add(2 * time.Hour)
	if e := restarted.ReapLegacyScheduled(ctx, repos, plan()); e != nil {
		t.Fatal(e)
	}
	if calls := restarted.GH.(*legacyGH).closedCalls; calls != 1 {
		t.Fatalf("pass after the interval swept %d times, want 1", calls)
	}
}

// (b) "0" and "off" disable the scheduled sweep: nothing is read and no
// schedule state is written.
func TestReapLegacyScheduledOffNeverRuns(t *testing.T) {
	for _, off := range []string{"0", "off", "OFF"} {
		t.Run(off, func(t *testing.T) {
			s, _, gh := legacyFixture(t)
			s.Config.Reap.LegacyEvery = off
			if e := s.ReapLegacyScheduled(context.Background(), s.Config.Repos, plan()); e != nil {
				t.Fatal(e)
			}
			if gh.closedCalls != 0 {
				t.Fatalf("disabled sweep read closed PRs %d times", gh.closedCalls)
			}
			if _, e := os.Stat(s.legacyScheduleFile("example/project")); !os.IsNotExist(e) {
				t.Fatalf("disabled sweep wrote schedule state: %v", e)
			}
		})
	}
}

// (c) Under shadow the scheduled sweep plans and writes would-remove rows; the
// node is never asked to remove anything.
func TestReapLegacyScheduledShadowPlansOnly(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	s.Config.Reap.LegacyEvery = "1h"
	if e := s.ReapLegacyScheduled(context.Background(), s.Config.Repos, Options{Shadow: true}); e != nil {
		t.Fatal(e)
	}
	wantRow(t, legacyOnly(t, s), "would-remove", legacyGuardNote)
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("shadow scheduled %d removals", n)
	}
}

// (d) One repo's failure is returned and audited, and the next repo still
// sweeps.
func TestReapLegacyScheduledContinuesAfterAFailedRepo(t *testing.T) {
	s, _, gh := legacyFixture(t)
	s.Config.Reap.LegacyEvery = "24h"
	broken := config.Repo{Name: "broken/project", Branch: "main"}
	s.Config.Repos = []config.Repo{broken, testRepo}
	gh.closedErr = map[string]error{broken.Name: errors.New("github unavailable")}

	e := s.ReapLegacyScheduled(context.Background(), s.Config.Repos, Options{Shadow: true})
	if e == nil || !strings.Contains(e.Error(), broken.Name) {
		t.Fatalf("error = %v, want one naming %s", e, broken.Name)
	}
	if strings.Contains(e.Error(), testRepo.Name) {
		t.Fatalf("the repo after the failure reported an error too: %v", e)
	}
	if rows := legacyRowsOf(t, s, testRepo.Name); len(rows) == 0 {
		t.Fatal("the repo after the failure did not sweep")
	}
	decisions, e := os.ReadFile(filepath.Join(s.state(broken.Name), "decisions.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(decisions)), "\n") {
		if strings.Contains(line, "reap-legacy-error") {
			return
		}
	}
	t.Fatalf("no reap-legacy-error audit row for %s:\n%s", broken.Name, decisions)
}
