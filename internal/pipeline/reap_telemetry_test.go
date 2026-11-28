package pipeline

import (
	"testing"
	"time"
)

// fakeReapClock drives a ReapTelemetry's stage clock and its GitHub counters, so
// the stage walls and categories in a summary are exact.
type fakeReapClock struct {
	now     time.Time
	ghTime  time.Duration
	ghCalls int
}

func newFakeReapClock(t *testing.T) (*ReapTelemetry, *fakeReapClock) {
	t.Helper()
	c := &fakeReapClock{now: time.Unix(1_700_000_000, 0)}
	tel := &ReapTelemetry{times: map[string]time.Duration{}, calls: map[string]int{}, stages: map[string]*reapStageTotals{}}
	tel.now = func() time.Time { return c.now }
	tel.SetGitHub(func() time.Duration { return c.ghTime }, func() int { return c.ghCalls })
	return tel, c
}

func stageRow(t *testing.T, summary map[string]any, name string) map[string]any {
	t.Helper()
	stages, ok := summary["stages"].(map[string]any)
	if !ok {
		t.Fatalf("summary has no stages map: %#v", summary)
	}
	row, ok := stages[name].(map[string]any)
	if !ok {
		t.Fatalf("summary has no stage %q: %#v", name, stages)
	}
	return row
}

// TestReapPassSummaryCarriesStageAndCategoryFields drives two stages of one
// repository and checks the summary row: every stage carries its categories, the
// categories of a stage add up to its wall time, and the pass's stages plus the
// unstaged time add up to the pass wall time.
func TestReapPassSummaryCarriesStageAndCategoryFields(t *testing.T) {
	tel, c := newFakeReapClock(t)
	ended := 0

	tel.Stage("host scan", func() { ended++ })
	tel.record("hosts", "panes", 10*time.Millisecond)
	c.ghTime, c.ghCalls = 5*time.Millisecond, 2
	c.now = c.now.Add(30 * time.Millisecond)

	tel.Stage("worker reap", nil)
	tel.record("git", "git worktree", 4*time.Millisecond)
	tel.Wait("host-scan-lock", 3*time.Millisecond)
	c.now = c.now.Add(20 * time.Millisecond)
	tel.Finish()

	if ended != 1 {
		t.Fatalf("stage end callback ran %d times, want 1 (ended when the next stage started)", ended)
	}

	pass := NewReapPass()
	pass.Add(tel)
	summary := pass.Summary(50*time.Millisecond, time.Minute)

	if got := summary["wall_ms"]; got != int64(50) {
		t.Fatalf("wall_ms = %v, want 50", got)
	}
	if got := summary["staged_ms"]; got != int64(50) {
		t.Fatalf("staged_ms = %v, want 50", got)
	}
	if got := summary["unstaged_ms"]; got != int64(0) {
		t.Fatalf("unstaged_ms = %v, want 0", got)
	}
	if got := summary["rate_wait_ms"]; got != int64(time.Minute.Milliseconds()) {
		t.Fatalf("rate_wait_ms = %v, want one minute", got)
	}

	host := stageRow(t, summary, "host scan")
	if got := host["wall_ms"]; got != int64(30) {
		t.Fatalf("host scan wall_ms = %v, want 30", got)
	}
	hostCats := host["categories"].(map[string]int64)
	if hostCats["hosts"] != 10 || hostCats["github"] != 5 || hostCats["other"] != 15 {
		t.Fatalf("host scan categories = %v, want hosts 10, github 5, other 15", hostCats)
	}
	if sum := sumCategories(hostCats); sum != 30 {
		t.Fatalf("host scan categories add to %d ms, want its wall 30", sum)
	}
	if host["calls"].(map[string]int)["github"] != 2 {
		t.Fatalf("host scan github calls = %v, want 2", host["calls"])
	}
	if host["detail"].(map[string]int64)["hosts/panes"] != 10 {
		t.Fatalf("host scan detail = %v, want hosts/panes 10", host["detail"])
	}

	reap := stageRow(t, summary, "worker reap")
	reapCats := reap["categories"].(map[string]int64)
	if reapCats["git"] != 4 || reapCats["wait"] != 3 || reapCats["other"] != 13 {
		t.Fatalf("worker reap categories = %v, want git 4, wait 3, other 13", reapCats)
	}
	if sum := sumCategories(reapCats); sum != 20 {
		t.Fatalf("worker reap categories add to %d ms, want its wall 20", sum)
	}
}

// TestReapPassSumsStagesAcrossRepositories checks that a stage of several
// repositories of one pass adds up, and that time outside every stage is the
// unstaged residual.
func TestReapPassSumsStagesAcrossRepositories(t *testing.T) {
	pass := NewReapPass()
	for _, wall := range []time.Duration{10 * time.Millisecond, 15 * time.Millisecond} {
		tel, c := newFakeReapClock(t)
		tel.Stage("worker reap", nil)
		c.now = c.now.Add(wall)
		pass.Add(tel)
	}
	summary := pass.Summary(40*time.Millisecond, 0)

	if got := stageRow(t, summary, "worker reap")["wall_ms"]; got != int64(25) {
		t.Fatalf("worker reap wall_ms = %v, want 25 across two repositories", got)
	}
	if got := summary["unstaged_ms"]; got != int64(15) {
		t.Fatalf("unstaged_ms = %v, want 15 (pass wall 40 less staged 25)", got)
	}
}

func sumCategories(cats map[string]int64) int64 {
	var sum int64
	for _, v := range cats {
		sum += v
	}
	return sum
}
