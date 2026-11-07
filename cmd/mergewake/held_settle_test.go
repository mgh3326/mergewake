package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// heldSpawnExit is panewire exit 74: another reviewer job holds the
// reservation.
type heldSpawnExit struct{}

func (heldSpawnExit) Error() string { return "exit 74" }
func (heldSpawnExit) ExitCode() int { return 74 }

// heldSpawnBody names a holder of another PR, so no older-head sweep runs, and
// names a wait the next evaluation must not cache the PR through.
const heldSpawnBody = "hub refused spawn: job_already_claimed (held by request=req-1 job=mwr-8-1111111-p1-1700000000) retry_after_s=240"

// heldRunner is panewire for a reviewer spawn the hub always refuses as held;
// every other command succeeds with no output, as the parallel fixture's runner
// does.
type heldRunner struct {
	mu     sync.Mutex
	spawns int
}

func (r *heldRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command != "panewire" || len(args) == 0 || args[0] != "spawn" {
		return nil, nil
	}
	r.mu.Lock()
	r.spawns++
	r.mu.Unlock()
	return nil, &integrations.CommandError{Command: "panewire", Err: heldSpawnExit{}, Stderr: heldSpawnBody}
}

func (r *heldRunner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spawns
}

// runSkippedPass runs one pr pass with skips and returns its evaluated and skipped
// counts from the pass line.
func runSkippedPass(t *testing.T, f *parallelFixture, skips *prSkips, logs *syncBuffer) (skipped, evaluated int) {
	t.Helper()
	if err := pollPRsSkipping(context.Background(), f.s, f.gh, f.repo, pipeline.Options{}, skips); err != nil {
		t.Fatalf("pass: %v", err)
	}
	for _, line := range strings.Split(logs.take(), "\n") {
		var rec struct {
			Msg       string
			Skipped   int `json:"prs_skipped"`
			Evaluated int `json:"prs_evaluated"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Msg != "pr pass" {
			continue
		}
		skipped, evaluated = rec.Skipped, rec.Evaluated
	}
	return skipped, evaluated
}

func cachedEvaluation(k *prSkips, repo string, n int) (prEvaluation, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	p, ok := k.last[prKey{repo, n}]
	return p, ok
}

// A head with no completion marker is not "handled", so the pass would settle
// it. A held review must not reach settlePR: no findings delivery, no verdict
// notice and no land decision for a review that posted nothing.
func TestHeldReviewNeverSettles(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	runner := &heldRunner{}
	f.s.Runner = runner
	logs := &syncBuffer{}
	skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}

	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if runner.count() != 1 {
		t.Fatalf("spawns=%d, want 1", runner.count())
	}
	for _, e := range f.trace.snapshot() {
		if e == "notify 1" || e == "merge 1" {
			t.Fatalf("a held review settled: %v", f.trace.snapshot())
		}
	}
	// settlePR always audits through DeliverFindings, NotifyPR and Land.
	for _, cmd := range []string{`"command":"pr-notify`, `"command":"land`} {
		if strings.Contains(f.out.String(), cmd) {
			t.Fatalf("a held review reached settlePR (%s): %s", cmd, f.out)
		}
	}
}

// (c) An evaluation whose only outcome was a held refusal is not cached as
// settled: the next evaluation runs, and it spawns again once the hold clears.
func TestHeldOnlyEvaluationIsNotSettled(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	runner := &heldRunner{}
	f.s.Runner = runner
	// The head has a completion marker but no review receipt: the stale marker
	// is not evidence that a review ran.
	f.hub.refs["mergewake/reviews/1/"+hubHead(1)+"-done"] = hubHead(1)

	now := time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC)
	f.s.Now = func() time.Time { return now }
	logs := &syncBuffer{}
	skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}

	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("first pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if _, ok := cachedEvaluation(skips, f.repo.Name, 1); ok {
		t.Fatal("a held-only evaluation was cached as settled")
	}
	if runner.count() != 1 {
		t.Fatalf("spawns=%d, want 1", runner.count())
	}

	// Inside the hold the PR is still evaluated; it must not be skipped as
	// unchanged until the retry succeeds.
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("held wait pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if _, ok := cachedEvaluation(skips, f.repo.Name, 1); ok {
		t.Fatal("a held wait was cached as settled")
	}

	now = now.Add(241 * time.Second)
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("after the hold: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if runner.count() != 2 {
		t.Fatalf("spawns=%d, want a retry once the hold clears", runner.count())
	}
}
