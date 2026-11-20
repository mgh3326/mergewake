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
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
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

// noticeRowByHost returns the reviewer-balance-notice row value for the
// host's day, or nil when the target repo holds none.
func noticeRowByHost(t *testing.T, f *balanceFixture, repo, day, host string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.s.state(repo), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
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
		if row.Command == balanceNoticeCommand && row.Value["day"] == day && row.Value["host"] == host {
			return row.Value
		}
	}
	return nil
}

// A stalled host (many created, almost none removed) gets exactly one notice
// per day while a healthy host stays silent; counts aggregate across repos.
func TestReapBalanceStallNoticesOnce(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	// No balance_lane key: the notice defaults to the desk lane.
	f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
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
	if f.n.events[0].Lane != "fleet-desk" {
		t.Fatalf("lane=%q, want desk lane fleet-desk", f.n.events[0].Lane)
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

// seedStalled makes node-a stall on day: 25 created across both repos, 2
// removed.
func seedStalled(t *testing.T, f *balanceFixture, day string) {
	t.Helper()
	for i := range 25 {
		at := balanceDayTime(day, i%24)
		f.spawn(t, f.repos[i%2].Name, "node-a", balanceLabel(100+i, at), at)
	}
	f.removed(t, f.repos[0].Name, "node-a", balanceLabel(100, balanceDayTime(day, 0)), balanceDayTime(day, 20))
	f.cleanupRemoved(t, f.repos[1].Name, "node-a", balanceLabel(101, balanceDayTime(day, 1)), balanceDayTime(day, 21))
	f.s.Now = func() time.Time { return f.now }
}

// The stall notice goes to reap.balance_lane; when the key is unset it goes
// to the desk lane, never the first repo's lane. Audit rows stay in the
// first repo either way.
func TestReapBalanceNoticeLane(t *testing.T) {
	day := "2026-11-17"
	t.Run("balance lane", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Repos[0].Lane = "repo-design-lane"
		f.s.Config.Reap.BalanceLane = "balance-desk"
		seedStalled(t, f, day)
		if err := f.s.reviewerBalance(context.Background(), Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("events=%+v", f.n.events)
		}
		if f.n.events[0].Lane != "balance-desk" {
			t.Fatalf("lane=%q, want balance-desk", f.n.events[0].Lane)
		}
		if rows := balanceRows(t, f, f.repos[0].Name); len(rows) == 0 {
			t.Fatal("no audit rows in first repo")
		}
	})
	t.Run("desk lane when unset", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Repos[0].Lane = "repo-design-lane"
		f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
		seedStalled(t, f, day)
		if err := f.s.reviewerBalance(context.Background(), Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("events=%+v", f.n.events)
		}
		if f.n.events[0].Lane != "fleet-desk" {
			t.Fatalf("lane=%q, want fleet-desk", f.n.events[0].Lane)
		}
	})
}

// A host with an open balance from earlier in the window but no activity
// today still gets a row with its trailing open balance — and no notice.
func TestReapBalanceBacklogHostStillReported(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	prev := "2026-11-16"
	for i := range 25 {
		at := balanceDayTime(prev, i%24)
		f.spawn(t, f.repos[i%2].Name, "node-c", balanceLabelUnix(90, at.Unix()+int64(i)), at)
	}
	for i := range 5 {
		label := balanceLabelUnix(90, balanceDayTime(prev, i).Unix()+int64(i))
		f.removed(t, f.repos[i%2].Name, "node-c", label, balanceDayTime(prev, 23))
	}
	f.s.Now = func() time.Time { return f.now }
	ctx := context.Background()
	if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := balanceRows(t, f, f.repos[0].Name)
	row := balanceRowByHost(rows, "node-c")
	if row == nil {
		t.Fatalf("no backlog row: %v", rows)
	}
	if row["created"] != float64(0) || row["removed"] != float64(0) || row["open_balance"] != float64(20) {
		t.Fatalf("backlog row: %v", row)
	}
	if len(f.n.events) != 0 {
		t.Fatalf("backlog noticed: %+v", f.n.events)
	}
}

// A retried day (rows written, notice failed) writes no duplicate rows:
// reviewerBalanceDay skips the rows the target log already holds.
func TestReapBalanceDayRetryNoDuplicates(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
	day := "2026-11-17"
	seedStalled(t, f, day)
	ctx := context.Background()
	if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("retried day duplicated audit rows")
	}
	if len(f.n.events) != 1 {
		t.Fatalf("events=%+v", f.n.events)
	}
}

// A delivered notice whose audit row was lost is repaired, not redelivered:
// deleting the reviewer-balance-notice row and rerunning the day rewrites
// only that row, with no new lane event and no duplicate daily rows.
func TestReapBalanceNoticeAuditRepaired(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
	day := "2026-11-17"
	seedStalled(t, f, day)
	ctx := context.Background()
	if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	notices := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.Command == "reviewer-balance-notice" {
			notices++
			continue
		}
		kept = append(kept, line)
	}
	if notices != 1 {
		t.Fatalf("notices=%d", notices)
	}
	if err := os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(after), `"reviewer-balance-notice"`); got != 1 {
		t.Fatalf("notice rows=%d", got)
	}
	if len(f.n.events) != 1 {
		t.Fatalf("redelivered: %+v", f.n.events)
	}
	for _, row := range balanceRows(t, f, f.repos[0].Name) {
		if row["host"] == "node-a" && (row["created"] != float64(25) || row["removed"] != float64(2)) {
			t.Fatalf("daily row changed: %v", row)
		}
	}
}

// sabotageNotifier delivers through the fixture notifier, then runs sabotage
// once mid-run so a later persist in the same pass fails.
type sabotageNotifier struct {
	inner    *fakeNotifier
	fired    bool
	sabotage func() error
	err      error
}

func (n *sabotageNotifier) Notify(ctx context.Context, e integrations.Event) error {
	if err := n.inner.Notify(ctx, e); err != nil {
		return err
	}
	if n.fired {
		return nil
	}
	n.fired = true
	if err := n.sabotage(); err != nil {
		n.err = err
	}
	return nil
}

// A delivered notice whose audit row fails is repaired before the day
// advances, through reviewerBalance: the first pass delivers, persists the
// key, then fails the notice audit, so the checkpoint stays behind. The
// retry appends the missing row with no second delivery and advances. A
// second case fails the checkpoint persist after the notice row is written;
// the retry repeats neither effect.
func TestReapBalanceNoticeRepairThroughReviewerBalance(t *testing.T) {
	day := "2026-11-17"
	ctx := context.Background()

	t.Run("audit fails after delivery", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
		seedStalled(t, f, day)
		decisions := filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl")
		var stash []byte
		sab := &sabotageNotifier{inner: f.n, sabotage: func() error {
			raw, err := os.ReadFile(decisions)
			if err != nil {
				return err
			}
			stash = raw
			if err := os.Remove(decisions); err != nil {
				return err
			}
			return os.Mkdir(decisions, 0700)
		}}
		f.s.Notifier = sab

		if err := f.s.reviewerBalance(ctx, Options{}); err == nil {
			t.Fatal("reviewerBalance succeeded despite failing notice audit")
		}
		if sab.err != nil {
			t.Fatalf("sabotage failed: %v", sab.err)
		}
		if _, err := os.Stat(f.s.reviewerBalanceStateFile()); !os.IsNotExist(err) {
			t.Fatalf("checkpoint advanced despite audit failure: %v", err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("deliveries=%d, want 1", len(f.n.events))
		}
		sent, err := loadSentKeys(f.s.reviewerBalanceSentFile(), 1024)
		if err != nil {
			t.Fatal(err)
		}
		if !sent.has("balance " + day + " node-a") {
			t.Fatal("sent key missing after delivery")
		}
		if err := os.Remove(decisions); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(decisions, stash, 0600); err != nil {
			t.Fatal(err)
		}
		if noticed, err := f.s.balanceNoticeWritten(f.repos[0].Name, day, "node-a"); err != nil || noticed {
			t.Fatalf("notice row present despite audit failure: %v %v", noticed, err)
		}
		rows := balanceRows(t, f, f.repos[0].Name)
		if len(rows) != 1 {
			t.Fatalf("daily rows=%v", rows)
		}
		if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("retry redelivered: %+v", f.n.events)
		}
		rows = balanceRows(t, f, f.repos[0].Name)
		if len(rows) != 1 || rows[0]["host"] != "node-a" || rows[0]["created"] != float64(25) || rows[0]["removed"] != float64(2) {
			t.Fatalf("daily rows=%v", rows)
		}
		got := noticeRowByHost(t, f, f.repos[0].Name, day, "node-a")
		if got == nil {
			t.Fatal("repaired notice row missing")
		}
		if got["created"] != float64(25) || got["removed"] != float64(2) || got["lane"] != "fleet-desk" {
			t.Fatalf("repaired row lost delivery payload: %v", got)
		}
		var st reviewerBalanceState
		if _, err := fsutil.ReadJSON(f.s.reviewerBalanceStateFile(), &st); err != nil {
			t.Fatal(err)
		}
		if st.Last != day {
			t.Fatalf("checkpoint=%q, want %q", st.Last, day)
		}
	})

	t.Run("checkpoint fails after notice row", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
		seedStalled(t, f, day)
		decisions := filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl")
		stateFile := f.s.reviewerBalanceStateFile()
		// The checkpoint is read before the day runs and written after the
		// notice row, so the sabotage runs mid-pass from the notifier.
		sab := &sabotageNotifier{inner: f.n, sabotage: func() error {
			return os.MkdirAll(stateFile, 0700)
		}}
		f.s.Notifier = sab

		if err := f.s.reviewerBalance(ctx, Options{}); err == nil {
			t.Fatal("reviewerBalance succeeded despite failing checkpoint")
		}
		if sab.err != nil {
			t.Fatalf("sabotage failed: %v", sab.err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("deliveries=%d, want 1", len(f.n.events))
		}
		if noticed, err := f.s.balanceNoticeWritten(f.repos[0].Name, day, "node-a"); err != nil || !noticed {
			t.Fatalf("notice row missing after first pass: %v %v", noticed, err)
		}
		before, err := os.ReadFile(decisions)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(f.s.reviewerBalanceStateFile()); err != nil {
			t.Fatal(err)
		}
		if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
			t.Fatal(err)
		}
		after, err := os.ReadFile(decisions)
		if err != nil {
			t.Fatal(err)
		}
		if string(before) != string(after) {
			t.Fatal("retry repeated audit effects")
		}
		if len(f.n.events) != 1 {
			t.Fatalf("retry redelivered: %+v", f.n.events)
		}
		var st reviewerBalanceState
		if _, err := fsutil.ReadJSON(f.s.reviewerBalanceStateFile(), &st); err != nil {
			t.Fatal(err)
		}
		if st.Last != day {
			t.Fatalf("checkpoint=%q, want %q", st.Last, day)
		}
	})

	t.Run("key host no longer stalled", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
		f.s.Config.Reap.BalanceLane = "lane-one"
		seedStalled(t, f, day)
		if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("deliveries=%d, want 1", len(f.n.events))
		}
		// Lose the notice row, then change the world: the host no longer
		// stalls and the lane moves. The sent key still demands its row,
		// written with the original delivery payload.
		path := filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var kept []string
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) == "" || strings.Contains(line, `"reviewer-balance-notice"`) {
				continue
			}
			kept = append(kept, line)
		}
		if err := os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		f.s.Config.Reap.BalanceMinCreated = 100
		f.s.Config.Reap.BalanceLane = "lane-two"
		if err := f.s.reviewerBalance(ctx, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("repair redelivered: %+v", f.n.events)
		}
		got := noticeRowByHost(t, f, f.repos[0].Name, day, "node-a")
		if got == nil {
			t.Fatal("sent key without a notice row was not repaired")
		}
		if got["created"] != float64(25) || got["removed"] != float64(2) || got["lane"] != "lane-one" {
			t.Fatalf("repaired row lost delivery payload: %v", got)
		}
		rows := balanceRows(t, f, f.repos[0].Name)
		if len(rows) != 1 {
			t.Fatalf("daily rows=%v", rows)
		}
		var st reviewerBalanceState
		if _, err := fsutil.ReadJSON(f.s.reviewerBalanceStateFile(), &st); err != nil {
			t.Fatal(err)
		}
		if st.Last != day {
			t.Fatalf("checkpoint=%q, want %q", st.Last, day)
		}
	})
}

// A stale orphan receipt for the same key must not overwrite the fresh
// delivery: after planting a stale receipt (sent file without the key, as
// after a sent-file loss) and delivering, the receipts file and a later
// repair of the lost notice row carry the fresh counts.
func TestReapBalanceFreshReceiptWinsOverStale(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
	day := "2026-11-17"
	seedStalled(t, f, day)
	key := "balance " + day + " node-a"
	stale, err := json.Marshal(map[string]balanceNoticeReceipt{
		key: {Created: 1, Removed: 1, Open: 1, Window: 7, Lane: "stale-lane", Via: "reviewer balance"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.s.reviewerBalanceReceiptsFile(), append(stale, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
		t.Fatal(err)
	}
	all, err := loadBalanceReceipts(f.s.reviewerBalanceReceiptsFile())
	if err != nil {
		t.Fatal(err)
	}
	if got := all[key]; got.Created != 25 || got.Removed != 2 || got.Lane != "fleet-desk" {
		t.Fatalf("stored receipt not fresh: %+v", got)
	}
	path := filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.Contains(line, `"reviewer-balance-notice"`) {
			continue
		}
		kept = append(kept, line)
	}
	if err := os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.n.events) != 1 {
		t.Fatalf("redelivered: %+v", f.n.events)
	}
	got := noticeRowByHost(t, f, f.repos[0].Name, day, "node-a")
	if got == nil {
		t.Fatal("repaired notice row missing")
	}
	if got["created"] != float64(25) || got["removed"] != float64(2) || got["lane"] != "fleet-desk" {
		t.Fatalf("repaired row carried stale payload: %v", got)
	}
}

// A receipt-less repair (pre-upgrade sent key, or a key whose receipt save
// failed) records the lane the notice would go to now instead of an empty
// lane: reap.balance_lane when set, else the desk lane.
func TestReapBalanceReceiptlessRepairLane(t *testing.T) {
	day := "2026-11-17"
	ctx := context.Background()
	loseNoticeAndReceipts := func(t *testing.T, f *balanceFixture) {
		t.Helper()
		path := filepath.Join(f.s.state(f.repos[0].Name), "decisions.jsonl")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var kept []string
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) == "" || strings.Contains(line, `"reviewer-balance-notice"`) {
				continue
			}
			kept = append(kept, line)
		}
		if err := os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(f.s.reviewerBalanceReceiptsFile()); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	t.Run("balance lane", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
		f.s.Config.Reap.BalanceLane = "balance-desk"
		seedStalled(t, f, day)
		if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
			t.Fatal(err)
		}
		loseNoticeAndReceipts(t, f)
		if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("redelivered: %+v", f.n.events)
		}
		got := noticeRowByHost(t, f, f.repos[0].Name, day, "node-a")
		if got == nil {
			t.Fatal("repaired notice row missing")
		}
		if got["lane"] != "balance-desk" || got["via"] != "reviewer balance" {
			t.Fatalf("repaired row lost resolved lane: %v", got)
		}
		if got["created"] != float64(25) || got["removed"] != float64(2) {
			t.Fatalf("repaired row lost daily counts: %v", got)
		}
	})
	t.Run("desk lane", func(t *testing.T) {
		f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
		f.s.Config.Notifier = config.Notifier{Type: "noop", Lane: "fleet-desk"}
		seedStalled(t, f, day)
		if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
			t.Fatal(err)
		}
		loseNoticeAndReceipts(t, f)
		if err := f.s.reviewerBalanceDay(ctx, f.repos[0], day, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.n.events) != 1 {
			t.Fatalf("redelivered: %+v", f.n.events)
		}
		got := noticeRowByHost(t, f, f.repos[0].Name, day, "node-a")
		if got == nil {
			t.Fatal("repaired notice row missing")
		}
		if got["lane"] != "fleet-desk" || got["via"] != "reviewer balance" {
			t.Fatalf("repaired row lost resolved lane: %v", got)
		}
	})
}

// hk 5665: a reviewer checkout whose guarded removal found the worktree
// already gone left the disk, so it counts as removed once. The fixture is
// shaped like the 2026-11-19 receipts: 41 checkouts created on node-a, 3 removed
// by mergewake, the rest skipped as "worktree already gone". It also covers a
// label gone on two days, a real removal that is gone again later, and a
// "pane already gone" skip, which is not a checkout removal.
func TestReapBalanceAlreadyGoneCountsRemoved(t *testing.T) {
	f := newBalanceFixture(t, balanceDayTime("2026-11-18", 10))
	day := "2026-11-17"
	repo := f.repos[0].Name
	labels := make([]string, 41)
	for i := range labels {
		at := balanceDayTime(day, i%24)
		labels[i] = balanceLabelUnix(200+i, at.Unix())
		f.spawn(t, f.repos[i%2].Name, "node-a", labels[i], at)
	}
	// mergewake's own removals: decisions rows, as the pass writes them.
	for i := range 3 {
		f.removed(t, repo, "node-a", labels[i], balanceDayTime(day, 20))
	}
	gone := func(i int, at time.Time, reason string) {
		t.Helper()
		f.s.Now = func() time.Time { return at }
		rec := reapReceipt{Action: "reviewer-reap", Repo: repo, PR: 200 + i, Label: labels[i], Host: "node-a", Result: "skipped", Reason: reason}
		if err := f.s.reapNote(rec, false); err != nil {
			t.Fatal(err)
		}
	}
	for i := 3; i < 39; i++ {
		gone(i, balanceDayTime(day, 22), reviewerAlreadyGone)
	}
	gone(39, balanceDayTime(day, 23), reviewerAlreadyGone)
	gone(39, balanceDayTime("2026-11-18", 1), reviewerAlreadyGone) // repeat on a later day
	gone(0, balanceDayTime("2026-11-18", 2), reviewerAlreadyGone)  // real removal gone again
	gone(40, balanceDayTime(day, 21), "pane already gone")         // not a checkout removal
	f.s.Now = func() time.Time { return f.now }

	res, err := f.s.reviewerBalanceForDay(f.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	c := res.Hosts["node-a"]
	if c == nil {
		t.Fatalf("node-a missing: %+v", res.Hosts)
	}
	if c.Created != 41 || c.Removed != 40 || c.Open != 1 {
		t.Fatalf("node-a created=%d removed=%d open=%d, want 41/40/1", c.Created, c.Removed, c.Open)
	}
	if res.Noticed["node-a"] {
		t.Fatal("node-a noticed: removals keep up once already-gone checkouts count")
	}
	next, err := f.s.reviewerBalanceForDay(f.repos, "2026-11-18")
	if err != nil {
		t.Fatal(err)
	}
	if n := next.Hosts["node-a"]; n == nil || n.Removed != 0 || n.Created != 0 {
		t.Fatalf("repeat days counted again: %+v", next.Hosts["node-a"])
	}
}
