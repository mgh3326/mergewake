package daemon

import (
	"context"
	"testing"
	"time"
)

// The wait after a miss starts when the read returns missing, not when the
// tick began: earlier reads in the tick can take seconds each.
func TestReviewerWatchMissingWaitStartsAtTheMiss(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	start := time.Now()
	clock := start
	w := watchFor(h, &syncBuf{}, &Dirty{}, func() {})
	w.now = func() time.Time { return clock }
	read := h.read
	w.Read = func(ctx context.Context, label string) (JobView, error) {
		clock = clock.Add(6 * time.Minute) // the earlier reads of this tick took this long
		return read(ctx, label)
	}
	w.Tick(context.Background())
	clock = start.Add(6*time.Minute + 10*time.Second)
	w.Tick(context.Background())
	if h.readCount() != 1 {
		t.Fatalf("a record missing at +6m was read again 10s later: reads=%d, want 1", h.readCount())
	}
}

// A record that appears just after its first miss is read on the next tick, so
// the finished job wakes the pr role within one watch interval.
func TestReviewerWatchSeesRecordThatAppearsAfterAMiss(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	wakes := 0
	w := watchFor(h, &syncBuf{}, &Dirty{}, func() { wakes++ })
	clock := time.Now()
	w.now = func() time.Time { return clock }
	w.Tick(context.Background())
	h.set("rev-7", JobView{State: "completed", HasResult: true})
	clock = clock.Add(ReviewerWatchEvery)
	w.Tick(context.Background())
	if wakes != 1 {
		t.Fatalf("a record that appeared after its first miss did not wake the role: wakes=%d", wakes)
	}
}

// A record that stays missing is read rarely: the wait doubles from the first
// miss up to the cap, so 20 ticks of the watch read it at most six times.
func TestReviewerWatchMissingRecordReadsDoubleToCap(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	w := watchFor(h, &syncBuf{}, &Dirty{}, func() {})
	clock := time.Now()
	w.now = func() time.Time { return clock }
	for range 20 {
		w.Tick(context.Background())
		clock = clock.Add(ReviewerWatchEvery)
	}
	if reads := h.readCount(); reads > 6 || reads < 2 {
		t.Fatalf("reads of a missing record over 20 ticks = %d, want 2 to 6", reads)
	}
}
