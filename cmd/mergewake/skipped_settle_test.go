package main

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// The hub lists one changed file for a PR that reports two, so the file list
// is still settling: the review is a poll the GitHub client skips. A stale
// completion marker must not turn that skip into a settled evaluation.
func TestSkippedPollUnderStaleDoneMarkerIsNotSettled(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	f.hub.changedFiles = 2
	f.hub.refs["mergewake/reviews/1/"+hubHead(1)+"-done"] = hubHead(1)
	logs := &syncBuffer{}
	skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}

	proxy := httptest.NewServer(etagProxy{f.hub})
	t.Cleanup(proxy.Close)
	f.gh.BaseURL = proxy.URL
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("first pass: skipped=%d evaluated=%d", skipped, evaluated)
	}
	if _, ok := cachedEvaluation(skips, f.repo.Name, 1); ok {
		t.Fatal("a skipped poll was cached as a settled evaluation")
	}
	if skipped, evaluated := runSkippedPass(t, f, skips, logs); evaluated != 1 || skipped != 0 {
		t.Fatalf("second pass: skipped=%d evaluated=%d, want the PR evaluated again", skipped, evaluated)
	}
	for _, e := range f.trace.snapshot() {
		if e == "merge 1" || e == "notify 1" {
			t.Fatalf("a skipped poll settled the PR: %v", f.trace.snapshot())
		}
	}
}

// A pass that ends on a skipped poll is no review, so it must not cancel the
// timer another reviewer armed before the pass.
func TestEngineSkippedPollLeavesThePendingTimerArmed(t *testing.T) {
	a := asyncEngineFixture(t, 1, 1)
	repo := a.f.repo.Name
	a.f.hub.changedFiles = 2
	a.e.revisit(repo, 1, "armed-before", time.Now().Add(time.Hour))
	a.e.q.EnqueueNamed(daemon.SourceWebhook, repo, 1, "")
	a.idle(t, "the skipped pass ends")

	if ids := a.e.q.TimerIDs(repo, 1); !slices.Equal(ids, []string{"armed-before"}) {
		t.Fatalf("timer ids %v after a skipped pass, want the armed timer kept", ids)
	}
}

// reviewPR on a skipped poll reports the PR unreviewed and clean, with no
// error, and settles nothing even though the caller asks it to settle.
func TestReviewPRSkippedPollIsUnreviewed(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	f.hub.changedFiles = 2
	pr, err := f.gh.Pull(context.Background(), f.repo.Name, 1)
	if err != nil {
		t.Fatal(err)
	}
	errs, clean, reviewed, unreviewed := reviewPR(context.Background(), f.s, f.repo, pr, pipeline.Options{}, true)
	if len(errs) != 0 || !clean || reviewed || !unreviewed {
		t.Fatalf("errs=%v clean=%v reviewed=%v unreviewed=%v, want a clean unreviewed skip", errs, clean, reviewed, unreviewed)
	}
	for _, e := range f.trace.snapshot() {
		if e == "merge 1" || e == "notify 1" {
			t.Fatalf("a skipped poll settled the PR: %v", f.trace.snapshot())
		}
	}
}
