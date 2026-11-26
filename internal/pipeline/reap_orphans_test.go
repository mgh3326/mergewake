package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// orphanCheckout is a local checkout no reviewer-job record names: a clean,
// detached worktree of the test repo whose directory is `age` old.
func orphanCheckout(t *testing.T, f *reviewerFix, age time.Duration) (label, path string) {
	t.Helper()
	label = reviewerLabelFor(12, age)
	path = localCheckout(t, f, label, false)
	f.run.outs[key("git", "-C", path, "rev-parse", "--symbolic-full-name", "HEAD")] = "HEAD\n"
	mtime := reviewerNow.Add(-age)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	return label, path
}

// newOrphanFix stubs the process scan empty, so the test host's own processes
// do not decide the outcome; the process-scan tests set their own.
func newOrphanFix(t *testing.T) *reviewerFix {
	t.Helper()
	saved := processScan
	processScan = func(context.Context, integrations.Runner) (procEvidence, error) { return procEvidence{}, nil }
	t.Cleanup(func() { processScan = saved })
	return newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
}

// scanProc stands in for the host's process scan for one test.
func scanProc(t *testing.T, ev procEvidence, err error) {
	t.Helper()
	saved := processScan
	processScan = func(context.Context, integrations.Runner) (procEvidence, error) { return ev, err }
	t.Cleanup(func() { processScan = saved })
}

// lsofScanRunner answers the two commands the lsof process scan makes.
type lsofScanRunner struct {
	lsof  []byte
	ps    []byte
	calls []string
}

func (r *lsofScanRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, command+" "+strings.Join(args, " "))
	switch command {
	case "lsof":
		return r.lsof, nil
	case "ps":
		return r.ps, nil
	}
	return nil, errors.New("unexpected command " + command)
}

// scanLsofProc runs the real lsof scan against the fake command runner.
func scanLsofProc(t *testing.T, run *lsofScanRunner) {
	t.Helper()
	saved := processScan
	processScan = func(ctx context.Context, _ integrations.Runner) (procEvidence, error) {
		return scanProcessesLsof(ctx, run)
	}
	t.Cleanup(func() { processScan = saved })
}

// orphanRows are the reviewer-cleanup rows of the orphan sweep: they name a
// checkout with no PR, which the reviewer rows do not.
func orphanRows(t *testing.T, f *reviewerFix) []reapReceipt {
	t.Helper()
	all, err := f.s.reapReceipts(testRepo.Name)
	if err != nil {
		t.Fatal(err)
	}
	var out []reapReceipt
	for _, v := range all {
		if v.Action == "reviewer-cleanup" && v.PR == 0 {
			out = append(out, v)
		}
	}
	return out
}

func removedCalled(f *reviewerFix, path string) bool {
	return f.run.called(key("git", "-C", "/repo", "worktree", "remove", path))
}

// An orphan older than the stuck bound, clean, idle and detached is removed
// with one reap row; a younger one is kept.
func TestOrphanCheckoutRemovedWhenOldIdleClean(t *testing.T) {
	for _, test := range []struct {
		name    string
		age     time.Duration
		removed bool
	}{
		{"seven hours", 7 * time.Hour, true},
		{"one hour", time.Hour, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrphanFix(t)
			_, path := orphanCheckout(t, f, test.age)
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if got := removedCalled(f, path); got != test.removed {
				t.Fatalf("removed=%v calls %q", got, f.run.calls)
			}
			rows := orphanRows(t, f)
			if !test.removed {
				if len(rows) != 0 {
					t.Fatalf("young orphan wrote rows: %v", rowSummary(f.rows(t)))
				}
				return
			}
			if len(rows) != 1 || rows[0].Result != "removed" || rows[0].Reason != "orphan-checkout" || rows[0].Worktree != path {
				t.Fatalf("rows: %+v", rows)
			}
		})
	}
}

func TestOrphanStageRunsAfterReviewerSweepUsesItsShare(t *testing.T) {
	f := newOrphanFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	f.s.Config.Reap.RepoBudget = "3s"
	checkoutsFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(checkoutsFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.s.Config.Review.Panewire.Checkouts = checkoutsFile
	label := reviewerLabelFor(67, 3*24*time.Hour)
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 67,
		Host: "node", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
		Result: "skipped", Reason: reviewerAlreadyGone})
	f.hub.hangInspect = "/work/" + label
	for pass := 1; pass <= 2; pass++ {
		err := f.reap(t, 0, Options{})
		if err == nil || !strings.Contains(err.Error(), "orphan checkouts") {
			t.Fatalf("pass %d: orphan stage did not run after the reviewer sweep's share ended", pass)
		}
	}
}

// A checkout a reviewer-job record names is not this path's: the record's
// reviewer sweep owns it, so nothing is removed or receipted here.
func TestOrphanCheckoutNamedByRecordNotTouched(t *testing.T) {
	f := newOrphanFix(t)
	label, path := orphanCheckout(t, f, 7*time.Hour)
	dir := filepath.Join(f.s.state(testRepo.Name), "reviewer-jobs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(reviewerJob{PR: 12, Pass: 1, Label: label, Checkout: path})
	if err := os.WriteFile(filepath.Join(dir, "12-p1.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
		t.Fatalf("named checkout touched: calls %q", f.run.calls)
	}
}

// A record that cannot be read may name any checkout, so the whole sweep
// stops for the pass: nothing is removed and the pass reports the error.
func TestOrphanCheckoutUnreadableRecordStopsSweep(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	dir := filepath.Join(f.s.state(testRepo.Name), "reviewer-jobs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "12-p1.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.reap(t, 0, Options{}); err == nil || !strings.Contains(err.Error(), "orphan checkouts") {
		t.Fatalf("err = %v, want an orphan-checkouts error", err)
	}
	if removedCalled(f, path) {
		t.Fatalf("removed past an unreadable record: calls %q", f.run.calls)
	}
}

// A pane on this host that runs in the checkout keeps it.
func TestOrphanCheckoutPaneInUseKept(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	f.run.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[{"pane_id":"orphan-test-pane","cwd":"` + path + `"}]}}`
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
		t.Fatalf("checkout with a pane removed: calls %q", f.run.calls)
	}
}

// A process whose working directory is inside the checkout keeps it, even
// when no pane runs there.
func TestOrphanCheckoutProcessCwdKept(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	scanProc(t, procEvidence{cwds: []string{path + "/internal", "/"}}, nil)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
		t.Fatalf("checkout with a process inside removed: calls %q", f.run.calls)
	}
}

// Uncommitted changes keep the checkout; so does a branch checkout, which is
// someone's work rather than a detached review.
func TestOrphanCheckoutDirtyOrBranchKept(t *testing.T) {
	for _, test := range []struct {
		name   string
		status string
		branch string
	}{
		{"dirty", " M main.go\n", ""},
		{"branch", "", "feature/x\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrphanFix(t)
			_, path := orphanCheckout(t, f, 7*time.Hour)
			if test.status != "" {
				f.run.outs[key("git", "-C", path, "status", "--porcelain")] = test.status
			}
			if test.branch != "" {
				f.run.outs[key("git", "-C", path, "branch", "--show-current")] = test.branch
			}
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
				t.Fatalf("kept checkout removed: calls %q", f.run.calls)
			}
		})
	}
}

// A directory that is not a git worktree is left and reported once, however
// many passes see it.
func TestOrphanCheckoutNotWorktreeReportedOnce(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	if f.run.errs == nil {
		f.run.errs = map[string]error{}
	}
	f.run.errs[key("git", "-C", path, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")] = gitRefusal{}
	for pass := 1; pass <= 3; pass++ {
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if removedCalled(f, path) {
		t.Fatalf("non-worktree removed: calls %q", f.run.calls)
	}
	var n int
	for _, v := range orphanRows(t, f) {
		if v.Result == "kept" && v.Reason == "not-a-git-worktree" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("reported %d times, want once; rows: %+v", n, orphanRows(t, f))
	}
}

// A worktree of another repo is that repo's pass to remove, not this one's.
func TestOrphanCheckoutOtherRepoKept(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	f.run.outs[key("git", "-C", path, "remote", "get-url", "origin")] = "git@github.com:other/repo.git\n"
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
		t.Fatalf("other repo's checkout removed: calls %q", f.run.calls)
	}
}

// Shadow mode plans the removal and removes nothing.
func TestOrphanCheckoutShadowPlansOnly(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	if err := f.reap(t, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) {
		t.Fatalf("shadow removed: calls %q", f.run.calls)
	}
	rows := orphanRows(t, f)
	if len(rows) != 1 || rows[0].Result != "would-remove" || rows[0].Reason != "orphan-checkout" {
		t.Fatalf("rows: %+v", rows)
	}
}

// A PR-scoped pass is not a whole-repo pass: it never sweeps the directory.
func TestOrphanCheckoutSkippedForPRScopedPass(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	if err := f.reap(t, 12, Options{}); err != nil {
		t.Logf("pass err (PR 12 is not merged in this fixture): %v", err)
	}
	if removedCalled(f, path) {
		t.Fatalf("PR-scoped pass swept the directory: calls %q", f.run.calls)
	}
}

// A host with no process scan (neither /proc nor lsof) removes nothing and
// fails no pass: the sweep cannot prove that no process holds a checkout. It
// writes one skipped row naming the failure, so a sweep that can never run is
// visible (hk 4792).
func TestOrphanCheckoutNoProcessScanFailsClosed(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	scanProc(t, procEvidence{}, errProcessScanUnavailable)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatalf("pass failed on a host without a process scan: %v", err)
	}
	if removedCalled(f, path) {
		t.Fatalf("removed without a process scan: calls %q", f.run.calls)
	}
	rows := orphanRows(t, f)
	if len(rows) != 1 || rows[0].Result != "skipped" || !strings.HasPrefix(rows[0].Reason, orphanScanUnavailable) {
		t.Fatalf("rows: %+v", rows)
	}
}

// A same-user process the scan cannot rule out fails the sweep closed: nothing
// is removed, and one skipped row names the process — once per UTC day, so a
// pass every minute does not bury the log. A new day reports the condition
// again, even when the blocking process is the same one.
func TestOrphanCheckoutUnresolvedProcessReportsOncePerDay(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	pid := 4711
	saved := processScan
	processScan = func(context.Context, integrations.Runner) (procEvidence, error) {
		return procEvidence{unknown: []procUnknown{{pid: pid, comm: "sudo"}}}, nil
	}
	t.Cleanup(func() { processScan = saved })
	for _, p := range []int{4711, 4722} {
		pid = p
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if removedCalled(f, path) {
		t.Fatalf("removed under a process the scan cannot rule out: calls %q", f.run.calls)
	}
	rows := orphanRows(t, f)
	if len(rows) != 1 || rows[0].Result != "skipped" || !strings.Contains(rows[0].Reason, "pid 4711") || !strings.Contains(rows[0].Reason, "sudo") {
		t.Fatalf("rows: %+v", rows)
	}
	// The same process on the next day is a new day's row, not a repeat.
	f.s.Now = func() time.Time { return reviewerNow.Add(24 * time.Hour) }
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows = orphanRows(t, f)
	if len(rows) != 2 || !strings.Contains(rows[1].Reason, "pid 4722") {
		t.Fatalf("rows after a day = %+v, want the day's second row for the same process", rows)
	}
}

// On the lsof path a same-user pid with no reported cwd is unknown: the sweep
// fails closed with the day's one row, even when its argv0 names an
// allow-listed program — that name is process-chosen and is not evidence. A
// process lsof did report does not block the sweep.
func TestOrphanCheckoutLsofUnreadableCwd(t *testing.T) {
	uid := strconv.Itoa(os.Getuid())
	cwdError := "ncwd|rtd info error: Operation not permitted\n"
	for _, test := range []struct {
		name    string
		lsof    string
		ps      string
		removed bool
		reason  string
	}{
		{
			name:   "same-user pid without cwd fails closed",
			lsof:   "p101\nfcwd\nn/\np102\nfcwd\n" + cwdError,
			ps:     " 101 " + uid + " S worker --serve\n 102 " + uid + " S sudo --serve\n",
			reason: "pid 102 (sudo)",
		},
		{
			// argv0 is process-chosen, so it cannot satisfy the no-work-dir
			// allow-list: the pid stays unresolved and the sweep fails closed.
			name:   "sshd argv0 without cwd fails closed",
			lsof:   "p101\nfcwd\nn/\np102\nfcwd\n" + cwdError,
			ps:     " 101 " + uid + " S worker --serve\n 102 " + uid + " S sshd\n",
			reason: "pid 102 (sshd)",
		},
		{
			name:    "all cwds present proceeds",
			lsof:    "p101\nfcwd\nn/\n",
			ps:      " 101 " + uid + " S worker --serve\n",
			removed: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrphanFix(t)
			_, path := orphanCheckout(t, f, 7*time.Hour)
			scanLsofProc(t, &lsofScanRunner{lsof: []byte(test.lsof), ps: []byte(test.ps)})
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if got := removedCalled(f, path); got != test.removed {
				t.Fatalf("removed=%v calls %q", got, f.run.calls)
			}
			rows := orphanRows(t, f)
			if test.removed {
				if len(rows) != 1 || rows[0].Result != "removed" {
					t.Fatalf("rows: %+v", rows)
				}
				return
			}
			if len(rows) != 1 || rows[0].Result != "skipped" || !strings.HasPrefix(rows[0].Reason, orphanScanUnavailable) || !strings.Contains(rows[0].Reason, test.reason) {
				t.Fatalf("rows: %+v", rows)
			}
		})
	}
}

// A non-dumpable same-user process whose open files point at nothing under the
// checkouts directory cannot hold one: the sweep proceeds (hk 4792).
func TestOrphanCheckoutUnreadableCwdOpenFilesElsewhereProceeds(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	scanProc(t, procEvidence{unknown: []procUnknown{
		{pid: 4711, comm: "sudo", fdsKnown: true, fds: []string{"/tmp/notes", "socket:[7]"}},
	}}, nil)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !removedCalled(f, path) {
		t.Fatalf("checkout no process holds was kept: calls %q", f.run.calls)
	}
	if rows := orphanRows(t, f); len(rows) != 1 || rows[0].Result != "removed" {
		t.Fatalf("rows: %+v", rows)
	}
}

// An open file under the checkout is proof that its process holds it: kept.
func TestOrphanCheckoutUnreadableCwdOpenFileInCheckoutKept(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	scanProc(t, procEvidence{unknown: []procUnknown{
		{pid: 4711, comm: "sudo", fdsKnown: true, fds: []string{"/tmp/notes", path + "/.git/index"}},
	}}, nil)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
		t.Fatalf("checkout held by an open file removed: calls %q", f.run.calls)
	}
}

// A process whose working directory and open files both deny this user is judged
// by its executable or name only when that never enters a work directory; any
// other name fails the sweep closed.
func TestOrphanCheckoutUnreadableCwdNameAllowList(t *testing.T) {
	for _, test := range []struct {
		name    string
		proc    procUnknown
		removed bool
	}{
		{"parenthesised helper", procUnknown{pid: 4711, comm: "(sd-pam)", commTrusted: true}, true},
		{"readable executable", procUnknown{pid: 4712, comm: "sshd", exe: "/usr/sbin/sshd"}, true},
		{"trusted allow-list name", procUnknown{pid: 4714, comm: "sshd", commTrusted: true}, true},
		{"argv0 name is not evidence", procUnknown{pid: 4715, comm: "sshd"}, false},
		{"unknown name", procUnknown{pid: 4713, comm: "sudo", commTrusted: true}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOrphanFix(t)
			_, path := orphanCheckout(t, f, 7*time.Hour)
			scanProc(t, procEvidence{unknown: []procUnknown{test.proc}}, nil)
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if got := removedCalled(f, path); got != test.removed {
				t.Fatalf("removed=%v calls %q", got, f.run.calls)
			}
			rows := orphanRows(t, f)
			switch {
			case test.removed && (len(rows) != 1 || rows[0].Result != "removed"):
				t.Fatalf("rows: %+v", rows)
			case !test.removed && (len(rows) != 1 || rows[0].Result != "skipped"):
				t.Fatalf("rows: %+v", rows)
			}
		})
	}
}

// lsof -F pn output carries one cwd per pid's n record; other fields are
// ignored, and the pid set records which processes had a readable cwd.
func TestParseLsofCwdPids(t *testing.T) {
	out := []byte("p101\nfcwd\nn/Users/x/checkouts/mwr-1\np202\nfcwd\nn/\n" +
		"p303\nfcwd\nncwd|rtd info error: Operation not permitted\np404\nfcwd\nnrelative/path\n")
	got := parseLsofCwdPids(out)
	if len(got.cwds) != 2 || got.cwds[0] != "/Users/x/checkouts/mwr-1" || got.cwds[1] != "/" {
		t.Fatalf("cwds = %q", got.cwds)
	}
	if !got.pids[101] || !got.pids[202] || got.pids[303] || got.pids[404] {
		t.Fatalf("pids = %v", got.pids)
	}
}

// ps lists all users; the scan keeps only the effective uid, skips zombies,
// keeps a live process whose command contains spaces, and excludes its own
// scanner command. A malformed same-user line makes the listing unreliable.
func TestParsePsProcesses(t *testing.T) {
	out := []byte(" 101 501 S worker --serve\n" +
		" 102 501 R ps -A -o pid=,uid=,state=,command=\n" +
		" 103 0 Ss login\n" +
		" 104 501 Z old <defunct>\n" +
		" 105 501 S Build Zeta --serve\n")
	procs, err := parsePsProcesses(out, "501")
	if err != nil {
		t.Fatal(err)
	}
	if len(procs) != 2 || procs[0].pid != 101 || procs[0].comm != "worker" || procs[1].pid != 105 || procs[1].comm != "Build" {
		t.Fatalf("procs = %+v", procs)
	}
	if _, err := parsePsProcesses([]byte("PID UID STATE COMMAND\n"), "501"); err == nil {
		t.Fatal("malformed ps output parsed")
	}
}

// A shadow pass leaves a would-remove orphan row. On a later acting pass the
// reviewer sweep must not turn that row into a candidate: a label past a day
// with no job would otherwise reach removeLocal, which has no process guard.
func TestOrphanCheckoutShadowRowNotReviewerCandidate(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 25*time.Hour)
	scanProc(t, procEvidence{}, nil)
	if err := f.reap(t, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	scanProc(t, procEvidence{cwds: []string{path + "/work"}}, nil)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) {
		t.Fatalf("a process-held orphan was removed on a later pass: calls %q", f.run.calls)
	}
}

// A process that starts in the checkout after the sweep's first scan, while the
// candidate is being checked, keeps it: the scan is repeated before removal.
func TestOrphanCheckoutProcessStartingMidSweepKept(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	calls := 0
	saved := processScan
	processScan = func(context.Context, integrations.Runner) (procEvidence, error) {
		calls++
		if calls == 1 {
			return procEvidence{}, nil
		}
		return procEvidence{cwds: []string{path}}, nil
	}
	t.Cleanup(func() { processScan = saved })
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if removedCalled(f, path) {
		t.Fatalf("removed under a process that started mid-sweep: calls %q", f.run.calls)
	}
}

// A failed HEAD read is not evidence of a detached checkout: the directory is
// kept, and nothing is removed.
func TestOrphanCheckoutHeadUnreadableKept(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	if f.run.errs == nil {
		f.run.errs = map[string]error{}
	}
	f.run.errs[key("git", "-C", path, "rev-parse", "--symbolic-full-name", "HEAD")] = errors.New("transient")
	if err := f.reap(t, 0, Options{}); err == nil {
		t.Fatal("expected the head read error to surface")
	}
	if removedCalled(f, path) {
		t.Fatalf("removed without a head read: calls %q", f.run.calls)
	}
}

// A directory under a state subdirectory that cannot be read stops the sweep:
// its records may name any checkout.
func TestRecordFilesUnreadableDirIsError(t *testing.T) {
	state := t.TempDir()
	bad := filepath.Join(state, "owner", "repo", "reviewer-jobs")
	if err := os.MkdirAll(bad, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(bad, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(bad, 0o700) })
	if _, err := recordFiles(state); err == nil {
		t.Fatal("unreadable reviewer-jobs dir read as empty")
	}
	if _, err := recordFiles(filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Fatalf("a missing state dir is empty, not an error: %v", err)
	}
}
