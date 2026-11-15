package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
// do not decide the outcome; the process-cwd tests set their own.
func newOrphanFix(t *testing.T) *reviewerFix {
	t.Helper()
	saved := processCwds
	processCwds = func(context.Context, integrations.Runner) ([]string, error) { return nil, nil }
	t.Cleanup(func() { processCwds = saved })
	return newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
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
	saved := processCwds
	processCwds = func(context.Context, integrations.Runner) ([]string, error) {
		return []string{path + "/internal", "/"}, nil
	}
	t.Cleanup(func() { processCwds = saved })
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
// fails no pass: the sweep cannot prove that no process holds a checkout.
func TestOrphanCheckoutNoProcessScanFailsClosed(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 7*time.Hour)
	saved := processCwds
	processCwds = func(context.Context, integrations.Runner) ([]string, error) {
		return nil, errProcessScanUnavailable
	}
	t.Cleanup(func() { processCwds = saved })
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatalf("pass failed on a host without a process scan: %v", err)
	}
	if removedCalled(f, path) || len(orphanRows(t, f)) != 0 {
		t.Fatalf("removed without a process scan: calls %q", f.run.calls)
	}
}

// lsof -F n output carries one cwd per n record; other fields are ignored.
func TestParseLsofCwds(t *testing.T) {
	out := []byte("p101\nfcwd\nn/Users/x/checkouts/mwr-1\np202\nfcwd\nn/\n")
	got := parseLsofCwds(out)
	if len(got) != 2 || got[0] != "/Users/x/checkouts/mwr-1" || got[1] != "/" {
		t.Fatalf("cwds = %q", got)
	}
}

// A shadow pass leaves a would-remove orphan row. On a later acting pass the
// reviewer sweep must not turn that row into a candidate: a label past a day
// with no job would otherwise reach removeLocal, which has no process guard.
func TestOrphanCheckoutShadowRowNotReviewerCandidate(t *testing.T) {
	f := newOrphanFix(t)
	_, path := orphanCheckout(t, f, 25*time.Hour)
	saved := processCwds
	t.Cleanup(func() { processCwds = saved })
	processCwds = func(context.Context, integrations.Runner) ([]string, error) { return nil, nil }
	if err := f.reap(t, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	processCwds = func(context.Context, integrations.Runner) ([]string, error) { return []string{path + "/work"}, nil }
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
	saved := processCwds
	t.Cleanup(func() { processCwds = saved })
	calls := 0
	processCwds = func(context.Context, integrations.Runner) ([]string, error) {
		calls++
		if calls == 1 {
			return nil, nil
		}
		return []string{path}, nil
	}
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
