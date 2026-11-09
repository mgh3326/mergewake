package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// escRecorder is the fixture notifier with the conflict ESCs kept apart so a
// test can count them. With refuse set, every ESC is rejected the way
// panewire rejects a lane before recording anything.
type escRecorder struct {
	trace  *eventLog
	mu     sync.Mutex
	refuse bool
	tries  int
	escs   []integrations.Event
}

func (n *escRecorder) Notify(ctx context.Context, e integrations.Event) error {
	if !strings.HasPrefix(e.Text, "ESC ") {
		return recordingNotifier{n.trace}.Notify(ctx, e)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tries++
	if n.refuse {
		return &integrations.CommandError{Command: "panewire", Err: errors.New("exit status 2"), Stderr: "emit: invalid --lane " + e.Lane}
	}
	n.escs = append(n.escs, e)
	return nil
}

func (n *escRecorder) counts() (tries int, escs []integrations.Event) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tries, append([]integrations.Event(nil), n.escs...)
}

// wantEngineEscs checks that want ESCs reached the desk for PR 1.
func wantEngineEscs(t *testing.T, n *escRecorder, want int) {
	t.Helper()
	tries, escs := n.counts()
	if len(escs) != want {
		t.Fatalf("%d conflict ESCs (%d attempted), want %d: %+v", len(escs), tries, want, escs)
	}
	for _, e := range escs {
		if e.Lane != "desk" || e.Type != "decision" || !strings.Contains(e.Text, "o/one#1 ") || !strings.Contains(e.Text, "conflict") {
			t.Fatalf("ESC %+v", e)
		}
	}
}

func escSkips() (*prSkips, *syncBuffer) {
	logs := &syncBuffer{}
	return &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}, logs
}

// (B2, held) A conflicting PR whose reviewer the hub holds never reaches
// settlePR, so NotifyPR never sees it. It still gets its ESC, once, and the
// reviewer placement is what it would be without the conflict.
func TestConflictEscEngineHeldReview(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	f.hub.conflicting = true
	runner := &heldRunner{}
	f.s.Runner = runner
	n := &escRecorder{trace: f.trace}
	f.s.Notifier = n
	skips, logs := escSkips()

	runSkippedPass(t, f, skips, logs)
	if runner.count() != 1 {
		t.Fatalf("spawns=%d, want 1", runner.count())
	}
	wantEngineEscs(t, n, 1)
	if strings.Contains(f.out.String(), `"command":"pr-notify`) {
		t.Fatalf("a held review reached settlePR: %s", f.out)
	}
	runSkippedPass(t, f, skips, logs)
	wantEngineEscs(t, n, 1)
}

// (B2, pending and quota) A conflicting PR whose reviewer is still running,
// then stopped on an exhausted pool, gets one ESC and no other reviewer.
func TestConflictEscEnginePendingAndQuotaReview(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	f.hub.conflicting = true
	runner := &quotaRunner{}
	sink := &quotaSink{rec: integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}}
	f.s.Runner, f.s.Sink = runner, sink
	n := &escRecorder{trace: f.trace}
	f.s.Notifier = n
	// The fixture clock starts now, as in TestQuotaStoppedOnlyEvaluationIsNotSettled.
	now := time.Now().UTC().Truncate(time.Second)
	f.s.Now = func() time.Time { return now }
	skips, logs := escSkips()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runSkippedPassWithin(t, ctx, "pending pass", f, skips, logs)
	if runner.count() != 1 {
		t.Fatalf("spawns=%d, want 1", runner.count())
	}
	wantEngineEscs(t, n, 1)

	reset := now.Add(10 * time.Minute)
	f.hub.mu.Lock()
	f.hub.refs["mergewake/reviews/1/"+hubHead(1)+"-done"] = hubHead(1)
	f.hub.mu.Unlock()
	sink.setRecord(integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=seat reset_at=" + reset.Format(time.RFC3339), Host: "node-a", PaneID: "p1"})
	runSkippedPassWithin(t, ctx, "quota pass", f, skips, logs)
	if runner.count() != 1 {
		t.Fatalf("spawns=%d, want 1", runner.count())
	}
	wantEngineEscs(t, n, 1)
}

// (B3) A conflicting draft gets its ESC and no reviewer. Marked ready, it gets
// its reviewer and no second ESC.
func TestConflictEscEngineDraftThenReady(t *testing.T) {
	f := newParallelFixture(t, 1, 1)
	f.hub.conflicting, f.hub.draft = true, true
	runner := &quotaRunner{}
	sink := &quotaSink{rec: integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}}
	f.s.Runner, f.s.Sink = runner, sink
	n := &escRecorder{trace: f.trace}
	f.s.Notifier = n
	now := time.Now().UTC().Truncate(time.Second)
	f.s.Now = func() time.Time { return now }
	skips, logs := escSkips()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runSkippedPassWithin(t, ctx, "draft pass", f, skips, logs)
	if runner.count() != 0 {
		t.Fatalf("a draft got %d reviewers", runner.count())
	}
	wantEngineEscs(t, n, 1)

	f.hub.mu.Lock()
	f.hub.draft = false
	f.hub.mu.Unlock()
	runSkippedPassWithin(t, ctx, "ready pass", f, skips, logs)
	if runner.count() != 1 {
		t.Fatalf("spawns=%d once ready, want 1", runner.count())
	}
	wantEngineEscs(t, n, 1)
}

// reviewsOf counts the reviewer starts for PR pr in a fixture trace.
func reviewsOf(events []string, pr int) int {
	n := 0
	for _, e := range events {
		if e == fmt.Sprintf("review %d", pr) {
			n++
		}
	}
	return n
}

// (D1) Recovery does not wait on the notice: after an ESC that was delivered
// and after one every destination refused, a push to a new, clean head gets
// its review as any new head does.
func TestConflictEscEngineNewHeadReviewedAfterEsc(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(fmt.Sprintf("refused=%t", refuse), func(t *testing.T) {
			f := newParallelFixture(t, 1, 1)
			close(f.runner.release)
			f.hub.conflicting = true
			n := &escRecorder{trace: f.trace, refuse: refuse}
			f.s.Notifier = n

			if err := <-f.poll(context.Background()); err != nil && !strings.Contains(err.Error(), "land") {
				t.Fatal(err)
			}
			if got := reviewsOf(f.trace.snapshot(), 1); got != 1 {
				t.Fatalf("%d reviews of the conflicting head, want 1: %q", got, f.trace.snapshot())
			}
			if refuse {
				wantEngineEscs(t, n, 0)
				if tries, _ := n.counts(); tries == 0 || !strings.Contains(f.out.String(), `"command":"conflict-esc-failed"`) {
					t.Fatalf("a refused ESC was not attempted and audited (tries %d): %s", tries, f.out)
				}
			} else {
				wantEngineEscs(t, n, 1)
			}

			f.hub.mu.Lock()
			f.hub.conflicting = false
			f.hub.heads = map[int]string{1: strings.Repeat("e", 40)}
			f.hub.mu.Unlock()
			if err := <-f.poll(context.Background()); err != nil && !strings.Contains(err.Error(), "land") {
				t.Fatal(err)
			}
			if got := reviewsOf(f.trace.snapshot(), 1); got != 2 {
				t.Fatalf("%d reviews after the push, want 2 (the new head reviewed): %q", got, f.trace.snapshot())
			}
			if refuse {
				wantEngineEscs(t, n, 0)
			} else {
				wantEngineEscs(t, n, 1)
			}
		})
	}
}
