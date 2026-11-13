package daemon

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeTimers is a clock the test moves: After records, fire runs.
type fakeTimers struct {
	mu    sync.Mutex
	calls []fakeTimer
}

type fakeTimer struct {
	d       time.Duration
	f       func()
	stopped *bool
}

func (c *fakeTimers) after(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	stopped := new(bool)
	c.calls = append(c.calls, fakeTimer{d, f, stopped})
	return func() bool { *stopped = true; return true }
}

func (c *fakeTimers) get(i int) fakeTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[i]
}

// A timer queues its PR with SourceTimer when it fires, and a later timer for
// the same PR and id replaces it: the replaced one's late callback does nothing.
func TestEnqueueAtFiresOnceAndReplaces(t *testing.T) {
	ev := newEvals()
	ck := &fakeTimers{}
	now := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	q := &Queue{Run: ev.Run, After: ck.after, Now: func() time.Time { return now }}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); q.Wait() }()
	q.Start(ctx)
	k := PRKey{"o/one", 1}
	q.EnqueueAt(SourceTimer, k.Repo, k.PR, "mwr-1", now.Add(25*time.Minute))
	q.EnqueueAt(SourceTimer, k.Repo, k.PR, "mwr-1", now.Add(26*time.Minute))
	if first, second := ck.get(0), ck.get(1); !*first.stopped || *second.stopped || second.d != 26*time.Minute {
		t.Fatalf("first %+v second %+v", first, second)
	}
	if len(ev.started()) != 0 {
		t.Fatal("a timer that has not fired queued the PR")
	}
	ck.get(0).f()
	if len(ev.started()) != 0 {
		t.Fatal("a replaced timer queued the PR")
	}
	ck.get(1).f()
	ev.waitStarted(t, 1)
	if w := ev.works[k][0]; len(w.Sources) != 1 || w.Sources[0] != SourceTimer {
		t.Fatalf("sources %v", w.Sources)
	}
	// A due time already past fires at once.
	q.EnqueueAt(SourceTimer, k.Repo, 2, "x", now.Add(-time.Minute))
	if d := ck.get(2).d; d != 0 {
		t.Fatalf("past due timer waits %v", d)
	}
	close(ev.release)
}

// CancelTimer stops one timer by (repo, PR, id) and leaves the others.
func TestCancelTimerDropsOnlyThatTimer(t *testing.T) {
	ev := newEvals()
	ck := &fakeTimers{}
	now := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	q := &Queue{Run: ev.Run, After: ck.after, Now: func() time.Time { return now }}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); q.Wait() }()
	q.Start(ctx)
	q.EnqueueAt(SourceTimer, "o/one", 1, "a", now.Add(time.Minute))
	q.EnqueueAt(SourceTimer, "o/one", 1, "b", now.Add(time.Minute))
	q.CancelTimer("o/one", 1, "a")
	q.CancelTimer("o/one", 1, "missing")
	if first, second := ck.get(0), ck.get(1); !*first.stopped || *second.stopped {
		t.Fatalf("a stopped=%v b stopped=%v", *first.stopped, *second.stopped)
	}
	ck.get(0).f()
	if len(ev.started()) != 0 {
		t.Fatal("a cancelled timer queued the PR")
	}
	ck.get(1).f()
	ev.waitStarted(t, 1)
}

// (c) A PR that closes or merges is not queued by the timers it left: Forget,
// which the engine calls for a PR it finds closed, and a sweep whose open list
// no longer holds it both drop them.
func TestTimersOfAPRThatLeftTheOpenSetDoNotQueueIt(t *testing.T) {
	ev := newEvals()
	ck := &fakeTimers{}
	now := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	q := &Queue{Run: ev.Run, After: ck.after, Now: func() time.Time { return now }}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); q.Wait() }()
	q.Start(ctx)
	q.SetOpen("o/one", map[int]string{1: "h1", 2: "h2", 3: "h3"})
	q.EnqueueAt(SourceTimer, "o/one", 1, "a", now.Add(time.Minute))
	q.EnqueueAt(SourceTimer, "o/one", 1, "b", now.Add(time.Minute))
	q.EnqueueAt(SourceTimer, "o/one", 2, "a", now.Add(time.Minute))
	q.EnqueueAt(SourceTimer, "o/one", 3, "a", now.Add(time.Minute))
	q.Forget("o/one", 1)
	q.SetOpen("o/one", map[int]string{3: "h3"})
	for i := 0; i < 3; i++ {
		ck.get(i).f()
	}
	if len(ev.started()) != 0 {
		t.Fatalf("a PR that left the open set was queued: %v", ev.started())
	}
	ck.get(3).f()
	ev.waitStarted(t, 1)
}

func TestSetOpenPreservesTimersForPRLearnedDuringList(t *testing.T) {
	q := &Queue{After: (&fakeTimers{}).after}
	q.SetOpen("o/one", map[int]string{2: "listed"})
	clock := &fakeTimers{}
	q.After = clock.after
	q.EnqueueAt(SourceTimer, "o/one", 1, "learned", time.Now().Add(time.Hour))
	q.EnqueueAt(SourceTimer, "o/one", 2, "neither", time.Now().Add(time.Hour))
	q.BeginList("o/one")
	q.Learn("o/one", 1, "new")
	q.SetOpen("o/one", map[int]string{})
	if got := q.TimerIDs("o/one", 1); len(got) != 1 || got[0] != "learned" {
		t.Fatalf("learned PR timers %v, want learned", got)
	}
	if got := q.TimerIDs("o/one", 2); len(got) != 0 {
		t.Fatalf("unlisted PR timers %v, want none", got)
	}
}
