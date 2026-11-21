package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A kept not-closable row carries its backoff deadline. Each standalone reap is
// a fresh process, so the deadline on the row, not in-memory state, decides
// whether a later invocation retries; when the refusal repeats the new row's
// deadline moves out again (hk 6014).
func TestReapReviewerKeptNotClosableDeadlineElapsesAcrossInvocations(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	f.clearPanes()
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "kept", Reason: reviewerNotClosable, RetryAfter: reviewerNow.Add(time.Hour)})
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))

	invoke := func(at time.Duration) {
		t.Helper()
		f.s.reapRetryState = nil
		f.now = reviewerNow.Add(at)
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	invoke(30 * time.Minute)
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("closed before the persisted deadline: closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	invoke(2 * time.Hour)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("not retried after the persisted deadline: closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	kept := f.resultRows(t, "kept")
	if len(kept) == 0 {
		t.Fatalf("no kept row after the repeated refusal: %v", rowSummary(f.rows(t)))
	}
	deadline := kept[len(kept)-1].RetryAfter
	if !deadline.After(reviewerNow.Add(2 * time.Hour)) {
		t.Fatalf("repeated refusal did not move the deadline out: retry_after=%v", deadline)
	}
	invoke(deadline.Sub(reviewerNow) - time.Second)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("closed again before the moved-out deadline: closes=%d: %v", n, rowSummary(f.rows(t)))
	}
}

// cancelAtClose cancels the reap context at the at-th job close, so the
// reviewer loop exits early on cancellation (hk 6014).
type cancelAtClose struct {
	refusingClose
	at     int
	closes int
	cancel context.CancelFunc
}

func (r *cancelAtClose) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	out, err := r.refusingClose.Run(ctx, name, args, input, dir)
	if strings.Contains(name+"\x00"+strings.Join(args, "\x00"), "\x00job\x00close\x00") {
		r.closes++
		if r.closes == r.at {
			r.cancel()
		}
	}
	return out, err
}

// The credential-wide ERROR is logged once per pass even when the reviewer loop
// exits early on cancellation after three scope_forbidden refusals (hk 6014).
func TestReapReviewerScopeForbiddenCredentialErrorOnEarlyExit(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	f.s.Config.Review.Panewire.Owner = "own-lane"
	for i, age := range []time.Duration{time.Hour, 2 * time.Hour, 3 * time.Hour, 4 * time.Hour} {
		label := reviewerLabelFor(12, age+time.Duration(i)*time.Minute)
		f.addReviewer(label, fmt.Sprintf("w1:p%d", 5+i))
		rec := f.sink.recs[label]
		rec.State, rec.OwnerLane = "spawned", "lane-other"
		f.sink.recs[label] = rec
	}
	for _, list := range []string{"agents", "panes"} {
		for _, p := range f.hub.agents[list].([]any) {
			p.(map[string]any)["agent_status"] = "done"
		}
	}
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.s.Runner = &cancelAtClose{
		refusingClose: refusingClose{scriptRunner: f.run, refusal: closeRefusal("panewire job: close refused: scope_forbidden")},
		at:            3,
		cancel:        cancel,
	}
	var logs bytes.Buffer
	f.s.Log = slog.New(slog.NewTextHandler(&logs, nil))

	_ = f.s.Reap(ctx, testRepo, 0, Options{})
	if n := len(f.jobCloses()); n != 3 {
		t.Fatalf("job closes=%d, want 3 before the cancel: %v", n, rowSummary(f.rows(t)))
	}
	out := logs.String()
	if got := strings.Count(out, "may have lost its close grant"); got != 1 {
		t.Fatalf("ERROR lines naming the credential on an early exit = %d, want 1: %q", got, out)
	}
}

// blockingNotifier holds each notice until its context ends, as a slow lane does.
type blockingNotifier struct{}

func (blockingNotifier) Notify(ctx context.Context, _ integrations.Event) error {
	<-ctx.Done()
	return ctx.Err()
}

// A scope_forbidden notice is bounded by the reap context: a slow lane cannot
// hold the serial repo sweep past its deadline (hk 6014).
func TestReapReviewerScopeForbiddenNoticeBoundedByReapContext(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Notifier = blockingNotifier{}
	w := &reviewerSweep{s: f.s, r: testRepo}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	l := reviewerLabel{name: reviewerLabelFor(12, time.Hour), pr: 12, sha7: headSHA[:7], pass: 1}
	start := time.Now()
	w.noteScopeForbidden(ctx, l, integrations.JobRecord{OwnerLane: "lane-x"}, closeRefusal("scope_forbidden"))
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("notice held the sweep %v past a 50ms reap deadline", d)
	}
}
