package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
)

// (a) While one PR's merge request is stuck at GitHub, another PR of the same
// repository settles: its findings are delivered and its READY verdict is
// recorded (a READY is recorded, not sent: see recordOnlyEvent). Only its own
// merge waits for the first one's.
func TestSettleOfOnePRDoesNotWaitForAnotherPRsMerge(t *testing.T) {
	e, f := engineFixture(t, 2)
	close(f.runner.release)
	started, hold := make(chan struct{}), make(chan struct{})
	var requests atomic.Int32
	f.hub.mergeProbe = func() func() {
		if requests.Add(1) == 1 {
			close(started)
			<-hold
		}
		return func() {}
	}
	e.q.Enqueue(daemon.SourceWebhook, daemon.PRKey{Repo: f.repo.Name, PR: 1}, daemon.PRKey{Repo: f.repo.Name, PR: 2})
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatalf("no merge request reached GitHub: %v", f.trace.snapshot())
	}
	clocks := filepath.Join(f.s.Config.StateDir, f.repo.Name, "pr-ready-renotify.json")
	deadline := time.Now().Add(10 * time.Second)
	for {
		// Each PR's READY clock is written by its NotifyPR, after its findings
		// delivery, and before its Land.
		b, _ := os.ReadFile(clocks)
		if strings.Contains(string(b), "ready 1 ") && strings.Contains(string(b), "ready 2 ") {
			break
		}
		if time.Now().After(deadline) {
			close(hold)
			t.Fatalf("a PR's verdict waited for the other PR's merge: clocks %q, trace %v", b, f.trace.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("%d merge requests while the first was held, want 1", got)
	}
	close(hold)
	e.q.Wait()
	merges := 0
	for _, ev := range f.trace.snapshot() {
		if ev == "merge 1" || ev == "merge 2" {
			merges++
		}
	}
	if merges != 2 {
		t.Fatalf("%d merges, want both PRs: %v", merges, f.trace.snapshot())
	}
}
