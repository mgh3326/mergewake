package pipeline

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

// passGH has no pull requests in any repository: the pass's host scans are what
// these tests count.
type passGH struct{ reapGH }

func (passGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return nil, nil
}

// passRunCalls is how many runner calls ran the command k.
func passRunCalls(run *scriptRunner, k string) int {
	n := 0
	for _, c := range run.calls {
		if c == k {
			n++
		}
	}
	return n
}

// A reap pass visits its repositories one after another. Each host is read
// once for the whole pass: the second repository reuses the first one's scan.
func TestReapPassScansEachHostOnce(t *testing.T) {
	run := &scriptRunner{outs: worktreeOuts("/repo.wt")}
	s := svc(t, &passGH{}, run)
	s.Out = io.Discard
	s.Config.Repos = append(s.Config.Repos, config.Repo{Name: "example/other", Branch: "dev"})
	ctx := WithReapPassHosts(context.Background())
	for _, r := range s.Config.Repos {
		if err := s.Reap(ctx, r, 0, Options{Shadow: true}); err != nil {
			t.Fatalf("reap %s: %v", r.Name, err)
		}
	}
	if n := passRunCalls(run, key("herdr", "pane", "list")); n != 1 {
		t.Fatalf("pane list scans for two repositories of one pass = %d, want 1", n)
	}
}

// A host that failed its scan stays failed for the pass: every repository
// reports it and none reads it again, so its removals stay refused.
func TestReapPassFailedHostIsReportedOnceScanned(t *testing.T) {
	run := &scriptRunner{outs: worktreeOuts("/repo.wt"), errs: map[string]error{key("herdr", "pane", "list"): errors.New("host down")}}
	s := svc(t, &passGH{}, run)
	s.Out = io.Discard
	s.Config.Repos = append(s.Config.Repos, config.Repo{Name: "example/other", Branch: "dev"})
	ctx := WithReapPassHosts(context.Background())
	for _, r := range s.Config.Repos {
		if err := s.Reap(ctx, r, 0, Options{Shadow: true}); err == nil {
			t.Fatalf("reap %s returned no error for an unreadable host", r.Name)
		}
	}
	if n := passRunCalls(run, key("herdr", "pane", "list")); n != 1 {
		t.Fatalf("pane list reads of a failed host in one pass = %d, want 1", n)
	}
}

// A host that a repository's own sweep marks failed (a reviewer inspect the
// hub answers 502) fails for that repository only: the next repository of the
// pass gets its own copy of the scan, unmarked, and sweeps the host.
func TestReapPassMidSweepFailureStaysWithItsRepository(t *testing.T) {
	run := &scriptRunner{outs: worktreeOuts("/repo.wt")}
	s := svc(t, &passGH{}, run)
	s.Out = io.Discard
	ctx := WithReapPassHosts(context.Background())
	h := s.Config.Hosts[0]
	first, _ := s.reapHostScan(ctx, nil, h)
	first.failed = true
	second, errs := s.reapHostScan(ctx, nil, h)
	if second.failed || len(errs) != 0 {
		t.Fatalf("a mid-sweep failure of the first repository reached the second: failed=%v errs=%v", second.failed, errs)
	}
	if n := passRunCalls(run, key("herdr", "pane", "list")); n != 1 {
		t.Fatalf("pane list scans for two repositories of one pass = %d, want 1", n)
	}
}

// A host the worker stage skips (its scan failed) was never discovered, so the
// stage does not finish: its resume cursor survives for the next pass.
func TestReapWorkerSkippedHostKeepsCursor(t *testing.T) {
	s := svc(t, &passGH{}, &scriptRunner{})
	repo := s.Config.Repos[0]
	saved := workerCursor{Host: "local", Path: "/repo.wt/wk/x"}
	if err := s.saveWorkerStageCursor(repo.Name, saved); err != nil {
		t.Fatal(err)
	}
	skipped := &reapHost{host: s.Config.Hosts[0], failed: true, trees: map[string]*reap.Worktree{}}
	s.reapWorkerWorktrees(context.Background(), repo, []*reapHost{skipped}, &passJobRecords{}, false, true, func(reapReceipt) {})
	got, err := s.workerStageCursor(repo.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got != saved {
		t.Fatalf("a skipped host cleared the worker cursor: got %+v, want %+v", got, saved)
	}
}

// An unreadable worker cursor has no position to keep, so a pass that skips a
// host still rewrites it: the next pass does not report the same error again.
func TestReapWorkerUnreadableCursorIsResetWithSkippedHost(t *testing.T) {
	s := svc(t, &passGH{}, &scriptRunner{})
	repo := s.Config.Repos[0]
	path := filepath.Join(s.state(repo.Name), "worker-worktree-cursor.json")
	writeCorrupt(t, path)
	skipped := &reapHost{host: s.Config.Hosts[0], failed: true, trees: map[string]*reap.Worktree{}}
	pass := func() []error {
		return s.reapWorkerWorktrees(context.Background(), repo, []*reapHost{skipped}, &passJobRecords{}, false, true, func(reapReceipt) {})
	}
	cursorErr := func(errs []error) bool {
		for _, e := range errs {
			if strings.Contains(e.Error(), "worker worktrees: resume cursor") {
				return true
			}
		}
		return false
	}
	if errs := pass(); !cursorErr(errs) {
		t.Fatalf("the unreadable worker cursor was not reported on its pass: %v", errs)
	}
	if !decodes(path, &workerCursor{}) {
		t.Fatalf("a pass with a skipped host left the unreadable cursor %s in place", path)
	}
	if errs := pass(); cursorErr(errs) {
		t.Fatalf("the next pass still reports the worker cursor: %v", errs)
	}
}

// An unreadable cursor is still rewritten when one host lists a candidate
// while another is skipped: the cursor holds no position to keep, so the next
// pass must not report the same error again. One listed candidate puts total
// above zero, exercising the candidate-path rewrite; the skipped-only test
// above covers the early return.
func TestReapWorkerUnreadableCursorIsResetWithListedCandidate(t *testing.T) {
	s, _, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	repo := s.Config.Repos[0]
	path := filepath.Join(s.state(repo.Name), "worker-worktree-cursor.json")
	writeCorrupt(t, path)
	ctx := context.Background()
	hub, err := s.reapHub()
	if err != nil {
		t.Fatal(err)
	}
	listed, scanErrs := s.reapScanHost(ctx, hub, s.Config.Hosts[0])
	if len(scanErrs) != 0 {
		t.Fatalf("listed host scan: %v", scanErrs)
	}
	skipped := &reapHost{host: reap.Host{Name: "down", Jobs: "/jobs"}, failed: true, trees: map[string]*reap.Worktree{}}
	hosts := []*reapHost{skipped, listed}
	pass := func() []error {
		return s.reapWorkerWorktrees(ctx, repo, hosts, &passJobRecords{}, false, true, func(reapReceipt) {})
	}
	cursorErr := func(errs []error) bool {
		for _, e := range errs {
			if strings.Contains(e.Error(), "worker worktrees: resume cursor") {
				return true
			}
		}
		return false
	}
	if errs := pass(); !cursorErr(errs) {
		t.Fatalf("the unreadable worker cursor was not reported on its pass: %v", errs)
	}
	if !decodes(path, &workerCursor{}) {
		t.Fatalf("a pass with a listed candidate left the unreadable cursor %s in place", path)
	}
	if errs := pass(); cursorErr(errs) {
		t.Fatalf("the next pass still reports the worker cursor: %v", errs)
	}
}
