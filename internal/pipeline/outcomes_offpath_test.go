package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// hangingReps is an outcomes command whose every call waits until its context
// ends or the test releases it, and counts the calls.
type hangingReps struct {
	mu      sync.Mutex
	calls   []string
	entered chan struct{}
	release chan struct{}
	adds    int
}

func newHangingReps() *hangingReps {
	return &hangingReps{entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (h *hangingReps) Run(ctx context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	h.mu.Lock()
	h.calls = append(h.calls, strings.Join(args, " "))
	if len(args) > 1 && args[1] == "add" {
		h.adds++
	}
	h.mu.Unlock()
	h.entered <- struct{}{}
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if len(args) > 1 && args[1] == "add" {
		return []byte("recorded rep id=srv:1\n"), nil
	}
	return nil, nil
}

func (h *hangingReps) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

func (h *hangingReps) unblock() {
	select {
	case <-h.release:
	default:
		close(h.release)
	}
}

// returnsWithin fails the test when fn has not returned after d.
func returnsWithin(t *testing.T, d time.Duration, what string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not return within %s while an outcome publish hung", what, d)
		return nil
	}
}

// (a) Land never waits for a publish: with an outcomes command that hangs, it
// returns at once and has called the command zero times.
func TestLandDoesNotPublishOutcomes(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	finalRow(t, s, r, 99)
	reps := newHangingReps()
	t.Cleanup(reps.unblock)
	s.Runner = reps
	var nudges int
	s.OutcomesChanged = func() { nudges++ }
	if err := returnsWithin(t, 3*time.Second, "Land", func() error { return s.Land(context.Background(), r, 7, Options{}) }); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 {
		t.Fatalf("merged %d times", g.mergeCount)
	}
	if n := reps.callCount(); n != 0 {
		t.Fatalf("Land ran the outcomes command %d times: %v", n, reps.calls)
	}
	if nudges != 1 {
		t.Fatalf("Land nudged the publisher %d times, want 1", nudges)
	}
}

// (a) ReconcileOutcomes does not publish either.
func TestReconcileOutcomesDoesNotPublish(t *testing.T) {
	s, _, _, _, r := setup(t)
	finalRow(t, s, r, 99)
	reps := newHangingReps()
	t.Cleanup(reps.unblock)
	s.Runner = reps
	if err := returnsWithin(t, 3*time.Second, "ReconcileOutcomes", func() error {
		return s.ReconcileOutcomes(context.Background(), r, map[int]bool{}, Options{Shadow: true})
	}); err != nil {
		t.Fatal(err)
	}
	if n := reps.callCount(); n != 0 {
		t.Fatalf("ReconcileOutcomes ran the outcomes command %d times: %v", n, reps.calls)
	}
}

// The publisher publishes what a nudge announces, once, and keeps publishing
// on its timer without one (a row written by another process).
func TestOutcomePublisherPublishesOnNudgeAndTimer(t *testing.T) {
	s, _, _, _, r := setup(t)
	runner := &repsRunner{}
	s.Runner = runner
	p := s.NewOutcomePublisher([]string{r.Name}, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	finalRow(t, s, r, 1)
	p.Nudge()
	p.Nudge() // a burst coalesces
	waitFor(t, "nudged outcome published", func() bool { state, _ := s.ledger().Read(); return len(state.Published) == 1 })
	if len(runner.adds) != 1 {
		t.Fatalf("reps add ran %d times", len(runner.adds))
	}

	s2, _, _, _, r2 := setup(t)
	runner2 := &repsRunner{}
	s2.Runner = runner2
	p2 := s2.NewOutcomePublisher([]string{r2.Name}, 50*time.Millisecond)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { defer close(done2); p2.Run(ctx2) }()
	t.Cleanup(func() { cancel2(); <-done2 })
	finalRow(t, s2, r2, 1)
	waitFor(t, "timer-published outcome", func() bool { state, _ := s2.ledger().Read(); return len(state.Published) == 1 })
}

// The publisher stops on context cancel even with a publish in flight: the
// publish's context ends with it.
func TestOutcomePublisherStopsWithContextDuringPublish(t *testing.T) {
	s, _, _, _, r := setup(t)
	finalRow(t, s, r, 1)
	reps := newHangingReps()
	t.Cleanup(reps.unblock)
	s.Runner = reps
	p := s.NewOutcomePublisher([]string{r.Name}, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	p.Nudge()
	select {
	case <-reps.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher never ran the outcomes command")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher still running after its context ended")
	}
}

// One publish run is bounded: a command that never answers is cut off by the
// run's own deadline, and the outcome stays in the outbox.
func TestOutcomePublisherRunIsBounded(t *testing.T) {
	s, _, _, _, r := setup(t)
	finalRow(t, s, r, 1)
	reps := newHangingReps()
	t.Cleanup(reps.unblock)
	s.Runner = reps
	p := s.NewOutcomePublisher([]string{r.Name}, time.Hour)
	p.Timeout = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	p.Nudge()
	<-reps.entered
	// The run ended by its deadline, so the next nudge starts another run.
	time.Sleep(500 * time.Millisecond)
	p.Nudge()
	waitFor(t, "second run", func() bool { return reps.callCount() >= 2 })
	if state, _ := s.ledger().Read(); len(state.Published) != 0 {
		t.Fatalf("a cut-off publish was acknowledged: %v", state.Published)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
