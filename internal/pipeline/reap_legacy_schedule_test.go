package pipeline

import (
	"context"
	"encoding/json"
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

// legacyErrorRows counts one repo's reap-legacy-error audit rows.
func legacyErrorRows(t *testing.T, s *Service, repo string) int {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(s.state(repo), "decisions.jsonl"))
	if e != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.Contains(line, "reap-legacy-error") {
			n++
		}
	}
	return n
}

// (e) A corrupt legacy-sweep.json does not disable the sweep: it is treated
// like a missing record, audited once, and the stamp write repairs the file.
func TestReapLegacyScheduledRepairsCorruptSchedule(t *testing.T) {
	s, _, gh := legacyFixture(t)
	s.Config.Reap.LegacyEvery = "24h"
	file := s.legacyScheduleFile(testRepo.Name)
	if e := os.MkdirAll(filepath.Dir(file), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(file, []byte("{not json"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := s.ReapLegacyScheduled(context.Background(), s.Config.Repos, apply()); e != nil {
		t.Fatal(e)
	}
	if gh.closedCalls != 1 {
		t.Fatalf("corrupt schedule swept %d times, want 1", gh.closedCalls)
	}
	b, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	var last legacySchedule
	if e := json.Unmarshal(b, &last); e != nil || last.LastRun.IsZero() {
		t.Fatalf("schedule file not repaired: %q (%v)", b, e)
	}
	if n := legacyErrorRows(t, s, testRepo.Name); n != 1 {
		t.Fatalf("want one reap-legacy-error audit row, got %d", n)
	}
	decisions, _ := os.ReadFile(filepath.Join(s.state(testRepo.Name), "decisions.jsonl"))
	if !strings.Contains(string(decisions), `"file":"legacy-sweep.json"`) {
		t.Fatalf("audit row does not name the file:\n%s", decisions)
	}
}

// (f) An unreadable legacy-sweep.json stays not-due — the stamp write would
// fail the same way — but the failure is still audited and returned.
func TestReapLegacyScheduledAuditsUnreadableSchedule(t *testing.T) {
	s, _, gh := legacyFixture(t)
	s.Config.Reap.LegacyEvery = "24h"
	if e := os.MkdirAll(s.legacyScheduleFile(testRepo.Name), 0700); e != nil {
		t.Fatal(e)
	}
	e := s.ReapLegacyScheduled(context.Background(), s.Config.Repos, apply())
	if e == nil || !strings.Contains(e.Error(), testRepo.Name) {
		t.Fatalf("error = %v, want one naming %s", e, testRepo.Name)
	}
	if gh.closedCalls != 0 {
		t.Fatalf("unreadable schedule swept %d times, want 0", gh.closedCalls)
	}
	if n := legacyErrorRows(t, s, testRepo.Name); n != 1 {
		t.Fatalf("want one reap-legacy-error audit row, got %d", n)
	}
}
