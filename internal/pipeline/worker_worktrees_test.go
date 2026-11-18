package pipeline

import (
	"testing"
)

// seedWorkerRow writes one reap row for a worker worktree on the fixture day.
func seedWorkerRow(t *testing.T, f *livenessFixture, action, host, wt, result, reason string, pr int) {
	t.Helper()
	rec := reapReceipt{
		Time:     f.now,
		Action:   action,
		Repo:     f.repos[0].Name,
		PR:       pr,
		Host:     host,
		Worktree: wt,
		Result:   result,
		Reason:   reason,
	}
	if err := f.s.reapNote(rec, false); err != nil {
		t.Fatal(err)
	}
}

func workerMechanism(t *testing.T, f *livenessFixture, day string) LivenessMechanismResult {
	t.Helper()
	res, err := f.s.livenessForDay(f.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range res.Mechanisms {
		if m.Mechanism == MechWorkerWorktrees {
			return m
		}
	}
	t.Fatalf("no %s mechanism in %+v", MechWorkerWorktrees, res.Mechanisms)
	return LivenessMechanismResult{}
}

func workerHost(t *testing.T, m LivenessMechanismResult, host string) WorkerWorktreeHost {
	t.Helper()
	for _, h := range m.Hosts {
		if h.Host == host {
			return h
		}
	}
	t.Fatalf("no host %q in %+v", host, m.Hosts)
	return WorkerWorktreeHost{}
}

// A stalled host raises while a healthy one stays quiet, and the kept list
// groups staying worktrees with their reasons per host.
func TestWorkerWorktrees_PerHostStallAndKept(t *testing.T) {
	day := "2026-11-17"
	f := newLivenessFixture(t, balanceDayTime(day, 12))

	// host-a stalls: 10 taken into custody, 2 removed (20% < 50%).
	for i := 1; i <= 10; i++ {
		wt := "/work/example.wa" + itoa(i)
		if i <= 2 {
			seedWorkerRow(t, f, "remove-worktree", "host-a", wt, "removed", "", i)
			continue
		}
		reason := "dirty"
		if i%2 == 0 {
			reason = "pane-in-use"
		}
		seedWorkerRow(t, f, "remove-worktree", "host-a", wt, "skipped", reason, i)
	}
	// host-b is healthy: 10 in custody, 8 removed.
	for i := 1; i <= 10; i++ {
		wt := "/work/example.wb" + itoa(i)
		if i <= 8 {
			seedWorkerRow(t, f, "remove-worktree", "host-b", wt, "removed", "", 100+i)
			continue
		}
		seedWorkerRow(t, f, "legacy-sweep", "host-b", wt, "kept", "too-young", 100+i)
	}

	m := workerMechanism(t, f, day)
	if !m.Stalled {
		t.Fatalf("worker-worktrees should stall on host-a: %+v", m)
	}
	if m.Made != 20 || m.Done != 10 {
		t.Fatalf("worker-worktrees totals: %+v", m)
	}

	a := workerHost(t, m, "host-a")
	if !a.Stalled || a.Made != 10 || a.Removed != 2 {
		t.Fatalf("host-a: %+v", a)
	}
	if len(a.Kept) != 8 {
		t.Fatalf("host-a kept %d, want 8: %+v", len(a.Kept), a.Kept)
	}
	reasons := map[string]int{}
	for _, k := range a.Kept {
		reasons[k.Reason]++
	}
	if reasons["dirty"] != 4 || reasons["pane-in-use"] != 4 {
		t.Fatalf("host-a kept reasons group per host: %v", reasons)
	}

	b := workerHost(t, m, "host-b")
	if b.Stalled || b.Made != 10 || b.Removed != 8 {
		t.Fatalf("host-b should stay quiet: %+v", b)
	}
	if len(b.Kept) != 2 {
		t.Fatalf("host-b kept %d, want 2: %+v", len(b.Kept), b.Kept)
	}
	for _, k := range b.Kept {
		if k.Reason != "too-young" {
			t.Fatalf("host-b kept reason %q: %+v", k.Reason, b.Kept)
		}
	}
}

// Reviewer checkouts and consult checkouts never count as worker worktrees,
// and hostless rows stay unattributed without stalling.
func TestWorkerWorktrees_ExclusionsAndUnattributed(t *testing.T) {
	day := "2026-11-17"
	f := newLivenessFixture(t, balanceDayTime(day, 12))

	seedWorkerRow(t, f, "remove-worktree", "host-a", "/work/mwr-7-abc1234-p1-1", "skipped", "pane-in-use", 7)
	seedWorkerRow(t, f, "remove-worktree", "host-a", "/work/panewire.consult-claims-task", "skipped", "pane-in-use", 8)
	seedWorkerRow(t, f, "legacy-sweep", "host-a", "/work/example.consult-other", "kept", "open-pr", 9)
	for i := 1; i <= 3; i++ {
		seedWorkerRow(t, f, "remove-worktree", "", "", "skipped", "merge-commit-unknown", i)
	}

	m := workerMechanism(t, f, day)
	if m.Stalled || m.Made != 0 || m.Done != 0 {
		t.Fatalf("excluded rows must not count: %+v", m)
	}
	if len(m.Hosts) != 0 {
		t.Fatalf("no attributed hosts expected: %+v", m.Hosts)
	}
}

// already-gone (either field) and gone proofs count as removed.
func TestWorkerWorktrees_GoneProofsCountDone(t *testing.T) {
	day := "2026-11-17"
	f := newLivenessFixture(t, balanceDayTime(day, 12))

	for i := 1; i <= 10; i++ {
		wt := "/work/example.wg" + itoa(i)
		switch {
		case i <= 4:
			seedWorkerRow(t, f, "remove-worktree", "host-a", wt, "removed", "", i)
		case i <= 6:
			seedWorkerRow(t, f, "remove-worktree", "host-a", wt, "already-gone", "", i)
		case i <= 8:
			seedWorkerRow(t, f, "remove-worktree", "host-a", wt, "skipped", "worktree-gone", i)
		default:
			seedWorkerRow(t, f, "legacy-sweep", "host-a", wt, "kept", "gone", i)
		}
	}
	m := workerMechanism(t, f, day)
	if m.Stalled || m.Made != 10 || m.Done != 10 {
		t.Fatalf("gone proofs are done: %+v", m)
	}
	a := workerHost(t, m, "host-a")
	if len(a.Kept) != 0 {
		t.Fatalf("nothing stays: %+v", a.Kept)
	}
}

// One worker-worktrees-daily row per host, append-only across retries.
func TestWorkerWorktrees_DailyRowsPerHost(t *testing.T) {
	day := "2026-11-17"
	f := newLivenessFixture(t, balanceDayTime(day, 12))
	seedWorkerRow(t, f, "remove-worktree", "host-a", "/work/example.wa1", "skipped", "dirty", 1)
	seedWorkerRow(t, f, "remove-worktree", "host-b", "/work/example.wb1", "removed", "", 2)

	hosts, _, err := f.s.workerWorktreesForDay(f.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.auditWorkerWorktreesDay(f.repos[0], day, hosts, false); err != nil {
		t.Fatal(err)
	}
	rows := livenessRows(t, f, f.repos[0].Name, workerWorktreesDailyCommand)
	if len(rows) != 2 {
		t.Fatalf("one row per host, got %d: %v", len(rows), rows)
	}
	byHost := map[string]map[string]any{}
	for _, r := range rows {
		h, _ := r["host"].(string)
		byHost[h] = r
	}
	ra := byHost["host-a"]
	if ra["made"] != float64(1) || ra["removed"] != float64(0) || ra["open"] != float64(1) {
		t.Fatalf("host-a row: %v", ra)
	}
	kept, _ := ra["kept"].([]any)
	if len(kept) != 1 {
		t.Fatalf("host-a kept list: %v", ra)
	}
	entry, _ := kept[0].(map[string]any)
	if entry["worktree"] != "/work/example.wa1" || entry["reason"] != "dirty" {
		t.Fatalf("host-a kept entry: %v", entry)
	}

	// A retried day writes nothing twice.
	if err := f.s.auditWorkerWorktreesDay(f.repos[0], day, hosts, false); err != nil {
		t.Fatal(err)
	}
	if rows := livenessRows(t, f, f.repos[0].Name, workerWorktreesDailyCommand); len(rows) != 2 {
		t.Fatalf("retried day duplicated rows: %d", len(rows))
	}
}
