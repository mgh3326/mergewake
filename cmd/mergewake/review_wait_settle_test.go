package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// runSkippedPass runs one pr pass with skips and returns its evaluated and skipped
// counts from the pass line.
func runSkippedPass(t *testing.T, f *parallelFixture, skips *prSkips, logs *syncBuffer) (skipped, evaluated int) {
	t.Helper()
	return runSkippedPassWithin(t, context.Background(), "pass", f, skips, logs)
}

// runSkippedPassWithin is runSkippedPass bounded by ctx, so a pass that never
// returns fails the test with what it was doing instead of at the package
// timeout.
func runSkippedPassWithin(t *testing.T, ctx context.Context, what string, f *parallelFixture, skips *prSkips, logs *syncBuffer) (skipped, evaluated int) {
	t.Helper()
	if err := pollPRsSkipping(ctx, f.s, f.gh, f.repo, pipeline.Options{}, skips); err != nil {
		if ctx.Err() != nil {
			t.Fatalf("%s did not finish within its deadline: %v", what, ctx.Err())
		}
		t.Fatalf("%s: %v", what, err)
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
// it. A review that produced no result — its reviewer still runs here — must
// not reach settlePR: no findings delivery, no verdict notice and no land
// decision for a review that posted nothing.
func TestWaitReviewNeverSettles(t *testing.T) {
	s := newStallFixture(t)
	// The review deadline outlasts the pass: the placed reviewer still runs
	// when the pass ends pending, so the evaluation produced no result.
	s.f.s.Config.Review.Timeout = "3h"
	s.f.s.Runner = noMergeGitHistoryRunner{next: &quotaRunner{}}
	s.f.s.Sink = &quotaSink{rec: integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}}

	s.pass(t)
	for _, e := range s.f.trace.snapshot() {
		if e == "notify 1" || e == "merge 1" {
			t.Fatalf("a review wait settled: %v", s.f.trace.snapshot())
		}
	}
	// settlePR always audits through DeliverFindings, NotifyPR and Land.
	for _, cmd := range []string{`"command":"pr-notify`, `"command":"land`} {
		if strings.Contains(s.f.out.String(), cmd) {
			t.Fatalf("a review wait reached settlePR (%s): %s", cmd, s.f.out)
		}
	}
}
