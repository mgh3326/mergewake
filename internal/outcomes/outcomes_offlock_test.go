package outcomes

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockingReps is a Reps whose Publish waits for release, so a test can act
// while a publish is in flight. It counts every Publish call; it has no
// idempotency check of its own.
type blockingReps struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func newBlockingReps() *blockingReps {
	return &blockingReps{entered: make(chan struct{}, 16), release: make(chan struct{})}
}

func (b *blockingReps) Publish(ctx context.Context, _ Outcome) (string, error) {
	b.mu.Lock()
	b.calls++
	n := b.calls
	b.mu.Unlock()
	b.entered <- struct{}{}
	select {
	case <-b.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return fmt.Sprintf("srv:%d", n), nil
}
func (b *blockingReps) Read(context.Context) ([]Outcome, error) { return nil, nil }
func (b *blockingReps) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// within runs fn and fails the test when it has not returned after d.
func within(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

// (b) While a publish is in flight the ledger is free: a writer, a reader and
// the next revision of the outcome being published all go through, and the
// revision published afterwards is the one the flush read.
func TestFlushDoesNotHoldLedgerLockWhilePublishing(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	reps := newBlockingReps()
	t.Cleanup(func() {
		select {
		case <-reps.release:
		default:
			close(reps.release)
		}
	})
	flushed := make(chan error, 1)
	go func() { flushed <- ledger.Flush(context.Background(), reps, "") }()
	select {
	case <-reps.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush never published")
	}
	within(t, 2*time.Second, "Update during a publish", func() {
		if err := ledger.Update("example/project", 2, complete); err != nil {
			t.Error(err)
		}
	})
	within(t, 2*time.Second, "Read during a publish", func() {
		if _, err := ledger.Read(); err != nil {
			t.Error(err)
		}
	})
	close(reps.release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	state, err := ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	// PR 2 was written after the flush read the outbox; it waits for the next flush.
	if len(state.Published) != 1 || reps.count() != 1 {
		t.Fatalf("published=%v calls=%d", state.Published, reps.count())
	}
	if err := ledger.Flush(context.Background(), &fakeReps{}, ""); err != nil {
		t.Fatal(err)
	}
	if state, _ = ledger.Read(); len(state.Published) != 2 {
		t.Fatalf("second outcome never published: %v", state.Published)
	}
}

// (c) Two flushes started at the same time publish an outcome once: the second
// waits for the first and then finds the outcome published.
func TestConcurrentFlushesPublishOnce(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	reps := newBlockingReps()
	t.Cleanup(func() {
		select {
		case <-reps.release:
		default:
			close(reps.release)
		}
	})
	errs := make(chan error, 2)
	go func() { errs <- ledger.Flush(context.Background(), reps, "") }()
	select {
	case <-reps.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first flush never published")
	}
	go func() { errs <- ledger.Flush(context.Background(), reps, "") }()
	// A second flush that did not wait would be inside Publish by now.
	time.Sleep(300 * time.Millisecond)
	close(reps.release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if reps.count() != 1 {
		t.Fatalf("outcome published %d times by two concurrent flushes", reps.count())
	}
}

// A flush waiting for another flush stops waiting when its context ends.
func TestFlushWaitEndsWithContext(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	reps := newBlockingReps()
	first := make(chan struct{})
	// The first flush ends, receipt written, before the ledger's directory goes.
	t.Cleanup(func() { close(reps.release); <-first })
	go func() { defer close(first); ledger.Flush(context.Background(), reps, "") }()
	<-reps.entered
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	waited := make(chan error, 1)
	go func() { waited <- ledger.Flush(ctx, &fakeReps{}, "") }()
	select {
	case err := <-waited:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiting flush returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting flush did not return within 3s of its context ending")
	}
}

// crashAfterPublish publishes through the real CLI path, then fails as if the
// process died before the ledger could record the receipt.
type crashAfterPublish struct{ CLI }

func (c crashAfterPublish) Publish(ctx context.Context, v Outcome) (string, error) {
	if _, err := c.CLI.Publish(ctx, v); err != nil {
		return "", err
	}
	return "", errors.New("crash between publish and ledger append")
}

// (c) A flush after a crash between the publish and the append finds the
// earlier rep by its event ID and adds nothing.
func TestFlushAfterCrashBetweenPublishAndAppendPublishesOnce(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	r := &benchRunner{t: t, prefix: "srv"}
	cli := CLI{Runner: r, Command: "pacelever"}
	err := ledger.Flush(context.Background(), crashAfterPublish{cli}, "")
	if err == nil || !strings.Contains(err.Error(), "crash between") {
		t.Fatalf("simulated crash: %v", err)
	}
	if state, _ := ledger.Read(); len(state.Published) != 0 || r.adds != 1 {
		t.Fatalf("after the crash: published=%v adds=%d", state.Published, r.adds)
	}
	if err := ledger.Flush(context.Background(), cli, ""); err != nil {
		t.Fatal(err)
	}
	if r.adds != 1 {
		t.Fatalf("reps add ran %d times for one outcome across a crash", r.adds)
	}
	if state, _ := ledger.Read(); len(state.Published) != 1 {
		t.Fatalf("receipt not recorded after the crash: %v", state.Published)
	}
}
