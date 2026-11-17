package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/reap"
)

// balanceFixture is a Service with two watched repos and a fixed clock, for
// exercising the daily reviewer balance across repos and hosts.
type balanceFixture struct {
	s     *Service
	n     *fakeNotifier
	repos []config.Repo
	now   time.Time
}

func newBalanceFixture(t *testing.T, now time.Time) *balanceFixture {
	t.Helper()
	s, _, _, n, _ := setup(t)
	repoA := config.Repo{Name: "example/a", Branch: "main", Lane: "design-lane"}
	repoB := config.Repo{Name: "example/b", Branch: "main", Lane: "design-lane"}
	s.Config.Repos = []config.Repo{repoA, repoB}
	s.Config.Hosts = []reap.Host{{Name: "node-a"}, {Name: "node-b"}}
	s.Now = func() time.Time { return now }
	return &balanceFixture{s: s, n: n, repos: []config.Repo{repoA, repoB}, now: now}
}

func balanceLabel(pr int, at time.Time) string {
	return balanceLabelUnix(pr, at.Unix())
}

func balanceLabelUnix(pr int, unix int64) string {
	return "mwr-" + strconv.Itoa(pr) + "-abcdef1-p1-" + strconv.FormatInt(unix, 10)
}

func balanceDayTime(day string, hour int) time.Time {
	d, err := time.Parse(reviewerSummaryDay, day)
	if err != nil {
		panic(err)
	}
	return d.Add(time.Duration(hour) * time.Hour)
}

// spawn seeds a reviewer-spawn audit row: the pass placed label on host.
func (f *balanceFixture) spawn(t *testing.T, repo, host, label string, at time.Time) {
	t.Helper()
	f.s.Now = func() time.Time { return at }
	if err := f.s.audit(repo, reviewerSpawnCommand, map[string]any{"pr": 99, "pass": 1, "label": label, "host": host}, false); err != nil {
		t.Fatal(err)
	}
}

// removed seeds a reviewer-reap removed audit row for label on host.
func (f *balanceFixture) removed(t *testing.T, repo, host, label string, at time.Time) {
	t.Helper()
	f.s.Now = func() time.Time { return at }
	if err := f.s.audit(repo, "reviewer-reap", map[string]any{"pr": 99, "label": label, "host": host, "result": "removed", "reason": "pane gone"}, false); err != nil {
		t.Fatal(err)
	}
}

// cleanupRemoved seeds a hostless reviewer-cleanup removed row (as the pass
// writes it) plus a reap receipt carrying the host, for label.
func (f *balanceFixture) cleanupRemoved(t *testing.T, repo, host, label string, at time.Time) {
	t.Helper()
	f.s.Now = func() time.Time { return at }
	if err := f.s.audit(repo, "reviewer-cleanup", map[string]any{"pr": 99, "label": label, "result": "removed", "reason": "pane gone"}, false); err != nil {
		t.Fatal(err)
	}
	f.s.Now = func() time.Time { return f.now }
	rec := reapReceipt{Time: at, Action: "reviewer-cleanup", Repo: repo, PR: 99, Host: host, Worktree: "/checkouts/" + label, Result: "removed", Reason: "pane gone"}
	if err := f.s.reapNote(rec, false); err != nil {
		t.Fatal(err)
	}
}

func balanceRows(t *testing.T, f *balanceFixture, repo string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.s.state(repo), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.Command == balanceDailyCommand {
			out = append(out, row.Value)
		}
	}
	return out
}

func balanceRowByHost(rows []map[string]any, host string) map[string]any {
	for _, r := range rows {
		if r["host"] == host {
			return r
		}
	}
	return nil
}

// A stalled host (many created, almost none removed) gets exactly one notice
// per day while a healthy host stays silent; counts aggregate across repos.
func TestReapBalanceStallNoticesOnce(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	ctx := context.Background()
	day := "2026-11-17"
	// node-a stalls: 25 created across both repos, only 2 removed.
	for i := range 25 {
		at := balanceDayTime(day, i%24)
		repo := f.repos[i%2].Name
		f.spawn(t, repo, "node-a", balanceLabel(100+i, at), at)
	}
	f.removed(t, f.repos[0].Name, "node-a", balanceLabel(100, balanceDayTime(day, 0)), balanceDayTime(day, 20))
	f.cleanupRemoved(t, f.repos[1].Name, "node-a", balanceLabel(101, balanceDayTime(day, 1)), balanceDayTime(day, 21))
	// node-b keeps up: 25 created, 24 removed.
	for i := range 25 {
		at := balanceDayTime(day, i%24)
		repo := f.repos[(i+1)%2].Name
		label := "mwr-7-1234567-p1-" + strconv.FormatInt(balanceDayTime(day, 0).Unix()+int64(1000+i), 10)
		f.spawn(t, repo, "node-b", label, at)
		if i < 24 {
			f.removed(t, repo, "node-b", label, balanceDayTime(day, 23))
		}
	}
	f.s.Now = func() time.Time { return f.now }

	if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := balanceRows(t, f, f.repos[0].Name)
	stalled, healthy := balanceRowByHost(rows, "node-a"), balanceRowByHost(rows, "node-b")
	if stalled == nil || healthy == nil {
		t.Fatalf("daily rows: %v", rows)
	}
	if stalled["created"] != float64(25) || stalled["removed"] != float64(2) {
		t.Fatalf("node-a row: %v", stalled)
	}
	if healthy["created"] != float64(25) || healthy["removed"] != float64(24) {
		t.Fatalf("node-b row: %v", healthy)
	}
	if len(f.n.events) != 1 {
		t.Fatalf("events=%+v", f.n.events)
	}
	text := f.n.events[0].Text
	for _, want := range []string{"node-a", "created 25", "removed 2"} {
		if !strings.Contains(text, want) {
			t.Fatalf("notice %q missing %q", text, want)
		}
	}
	if f.n.events[0].Lane != "design-lane" {
		t.Fatalf("lane=%q", f.n.events[0].Lane)
	}
	// A second pass the same day writes no new rows and sends no new notice.
	before, _ := os.ReadFile(filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl"))
	if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl"))
	if string(before) != string(after) {
		t.Fatal("second pass rewrote the day")
	}
	if len(f.n.events) != 1 {
		t.Fatalf("repeat notice sent: %+v", f.n.events)
	}
}

// When removals keep up on every host, the day still gets its rows but no
// notice goes out.
func TestReapBalanceSilentWhenHealthy(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	ctx := context.Background()
	day := "2026-11-17"
	for i := range 22 {
		at := balanceDayTime(day, i%24)
		for hi, host := range []string{"node-a", "node-b"} {
			label := balanceLabelUnix(80+hi, at.Unix()+int64(i)*60)
			f.spawn(t, f.repos[(i+hi)%2].Name, host, label, at)
			f.removed(t, f.repos[(i+hi)%2].Name, host, label, balanceDayTime(day, 23))
		}
	}
	f.s.Now = func() time.Time { return f.now }
	if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := balanceRows(t, f, f.repos[0].Name)
	if len(rows) != 2 {
		t.Fatalf("rows=%v", rows)
	}
	if len(f.n.events) != 0 {
		t.Fatalf("healthy day noticed: %+v", f.n.events)
	}
}

// Labels without spawn rows count by their label timestamp once they left
// checkout evidence; labels that never had checkouts do not count.
func TestReapBalanceFallbackCounts(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	day := "2026-11-17"
	at := balanceDayTime(day, 3)
	label := balanceLabel(42, at)
	// No spawn row: a cleanup row gives it checkout evidence.
	f.s.Now = func() time.Time { return at }
	if err := f.s.audit(f.repos[0].Name, "reviewer-cleanup", map[string]any{"pr": 42, "label": label, "result": "kept", "reason": "pane present"}, false); err != nil {
		t.Fatal(err)
	}
	// A label with no evidence at all (quota-stopped spawn) is not a checkout.
	ghostAt := balanceDayTime(day, 4)
	ghost := balanceLabel(43, ghostAt)
	if err := f.s.audit(f.repos[0].Name, "review-quota-stopped", map[string]any{"pr": 43, "label": ghost}, false); err != nil {
		t.Fatal(err)
	}
	f.s.Now = func() time.Time { return f.now }
	res, err := f.s.reviewerBalanceForDay(f.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	if res.UnattributedCreated != 1 || len(res.Hosts) != 0 {
		t.Fatalf("fallback created: %+v", res)
	}
}
