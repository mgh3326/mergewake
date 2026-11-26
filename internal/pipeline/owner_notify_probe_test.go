package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// probeRunner answers each version probe with the next scripted result and
// counts the probes it saw.
type probeRunner struct {
	mu      sync.Mutex
	answers []probeAnswer
	probes  int
	gate    chan struct{}
}

type probeAnswer struct {
	out string
	err error
}

func (r *probeRunner) Run(_ context.Context, _ string, _ []string, _, _ string) ([]byte, error) {
	if r.gate != nil {
		<-r.gate
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes++
	if len(r.answers) == 0 {
		return nil, errors.New("no scripted probe answer")
	}
	a := r.answers[0]
	if len(r.answers) > 1 {
		r.answers = r.answers[1:]
	}
	return []byte(a.out), a.err
}

func withOwnerNotifyClock(t *testing.T, start time.Time) *time.Time {
	t.Helper()
	clock := start
	prev := ownerNotifyClock
	ownerNotifyClock = func() time.Time { return clock }
	t.Cleanup(func() { ownerNotifyClock = prev })
	return &clock
}

func TestOwnerNotifyProbeFailureIsNotCached(t *testing.T) {
	withOwnerNotifyClock(t, time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC))
	runner := &probeRunner{answers: []probeAnswer{
		{err: errors.New("transient: version timed out")},
		{out: `{"capabilities":["owner_notify_v1"]}`},
	}}
	const command = "panewire-test-owner-notify-probe-retry"

	if supportsOwnerNotify(context.Background(), runner, command) {
		t.Fatal("first probe failed, so the flag must be left out")
	}
	if !supportsOwnerNotify(context.Background(), runner, command) {
		t.Fatalf("second spawn did not re-probe after a failed probe: probes=%d", runner.probes)
	}
	if runner.probes != 2 {
		t.Fatalf("probe count = %d, want 2", runner.probes)
	}
}

func TestOwnerNotifyMalformedAndCancelledProbesAreNotCached(t *testing.T) {
	withOwnerNotifyClock(t, time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC))
	runner := &probeRunner{answers: []probeAnswer{
		{out: `not json`},
		{err: context.Canceled},
		{out: `{"capabilities":["owner_notify_v1"]}`},
	}}
	const command = "panewire-test-owner-notify-probe-malformed"

	for i, want := range []bool{false, false, true} {
		if got := supportsOwnerNotify(context.Background(), runner, command); got != want {
			t.Fatalf("probe %d: supported=%v, want %v", i+1, got, want)
		}
	}
	if runner.probes != 3 {
		t.Fatalf("probe count = %d, want 3", runner.probes)
	}
}

func TestOwnerNotifyCachedAnswerExpiresAfterTTL(t *testing.T) {
	clock := withOwnerNotifyClock(t, time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC))
	runner := &probeRunner{answers: []probeAnswer{
		{out: `{"capabilities":["owner_notify_v1"]}`},
		{out: `{"capabilities":[]}`},
	}}
	const command = "panewire-test-owner-notify-probe-ttl"

	if !supportsOwnerNotify(context.Background(), runner, command) {
		t.Fatal("first probe should report support")
	}
	*clock = clock.Add(ownerNotifyProbeTTL - time.Second)
	if !supportsOwnerNotify(context.Background(), runner, command) || runner.probes != 1 {
		t.Fatalf("answer within the TTL must be served from cache: probes=%d", runner.probes)
	}
	*clock = clock.Add(2 * time.Second)
	if supportsOwnerNotify(context.Background(), runner, command) {
		t.Fatal("a capability dropped after the TTL must not keep the flag")
	}
	if runner.probes != 2 {
		t.Fatalf("expired answer was not re-probed: probes=%d", runner.probes)
	}
}

func TestOwnerNotifyConcurrentSpawnsShareOneProbe(t *testing.T) {
	withOwnerNotifyClock(t, time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC))
	runner := &probeRunner{
		answers: []probeAnswer{{out: `{"capabilities":["owner_notify_v1"]}`}},
		gate:    make(chan struct{}),
	}
	const command = "panewire-test-owner-notify-probe-shared"

	var wg sync.WaitGroup
	var supported atomic.Int32
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if supportsOwnerNotify(context.Background(), runner, command) {
				supported.Add(1)
			}
		}()
	}
	close(runner.gate)
	wg.Wait()

	if runner.probes != 1 {
		t.Fatalf("concurrent spawns ran %d probes, want 1", runner.probes)
	}
	if supported.Load() == 0 {
		t.Fatal("no concurrent spawn saw the shared answer")
	}
}
