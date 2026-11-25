package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// phaseClockPullGH advances a fake phase clock on every PR read, so the phase
// a slow post-lock read books to is deterministic without sleeping.
type phaseClockPullGH struct {
	*fakeGH
	advance func()
}

func (g *phaseClockPullGH) Pull(ctx context.Context, repo string, n int) (github.Pull, error) {
	g.advance()
	return g.fakeGH.Pull(ctx, repo, n)
}

// With a pending attempt, a Land takes the merge lock; the settle and
// everything after it must book to settle, not lock_wait.
func TestLandSettlePhaseBooksPostLockWorkToSettle(t *testing.T) {
	s, g, _, _, r := setup(t)
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 7), mergeAttempt{PR: 7, Head: shaA, Base: shaB, At: time.Now()}); e != nil {
		t.Fatal(e)
	}
	now := time.Unix(1000, 0)
	p := daemon.NewPhases(func() time.Time { return now })
	s.GH = &phaseClockPullGH{fakeGH: g, advance: func() { now = now.Add(5 * time.Second) }}
	if e := s.Land(daemon.WithPhases(context.Background(), p), r, 7, Options{}); e != nil {
		t.Fatalf("Land: %v", e)
	}
	p.End()
	if got := p.Took(daemon.PhaseLockWait); got != 0 {
		t.Fatalf("lock_wait = %v, want 0", got)
	}
	if got := p.Took(daemon.PhaseSettle); got < 5*time.Second {
		t.Fatalf("settle = %v, want >= 5s", got)
	}
}

// revertHoldNotifier blocks the first notice until released, so the test can
// probe the merge lock while the notice is still blocked.
type revertHoldNotifier struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (n *revertHoldNotifier) Notify(ctx context.Context, _ integrations.Event) error {
	n.once.Do(func() { close(n.entered) })
	select {
	case <-n.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A mergeRevert whose notifier blocks must not hold the merge lock: a
// concurrent lock acquisition on the same repo returns while the notify is
// still blocked.
func TestMergeRevertBlockedNotifyReleasesMergeLock(t *testing.T) {
	s, rg, _, _, r := recoverySetup(t, nil)
	n := &revertHoldNotifier{entered: make(chan struct{}), release: make(chan struct{})}
	s.Notifier = n
	ws := &watchStream{Service: s}
	done := make(chan error, 1)
	go func() {
		done <- ws.mergeRevert(context.Background(), r, shaC, nil, rg.revert, recoveryIncident{}, watchResult{}, Options{})
	}()
	select {
	case <-n.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("mergeRevert did not reach the blocked notify")
	}
	locked := make(chan error, 1)
	go func() {
		unlock, e := s.mergeLock(r.Name)
		if e == nil {
			unlock()
		}
		locked <- e
	}()
	select {
	case e := <-locked:
		if e != nil {
			t.Fatalf("merge lock while notify blocked: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("merge lock still held while revert notice is blocked")
	}
	close(n.release)
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("mergeRevert: %v", e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mergeRevert did not return after notify released")
	}
	if rg.merges != 1 {
		t.Fatalf("revert merges = %d, want 1", rg.merges)
	}
}
