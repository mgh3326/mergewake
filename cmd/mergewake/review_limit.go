package main

import (
	"context"
	"sync"
)

// reviewLimit admits the reviews of one repository's pass. Without a cap
// every review is admitted at once; with one, at most cap run, and a
// priority-1 review may also take one of urgentExtra slots past it. It keeps
// counts, not a pre-sized buffer, so an explicit cap of any size costs nothing.
type reviewLimit struct {
	mu      sync.Mutex
	cap     int // zero: no cap
	slots   int // reviews running within cap
	extra   int // priority-1 reviews running past cap
	peak    int // most reviews that ran at once
	changed chan struct{}
}

func newReviewLimit(cap int) *reviewLimit {
	return &reviewLimit{cap: cap, changed: make(chan struct{})}
}

// acquire waits for a slot and returns the function that frees it, and
// whether the slot is one past the cap. ok is false if ctx ended first.
func (l *reviewLimit) acquire(ctx context.Context, urgent bool) (release func(), over, ok bool) {
	for {
		l.mu.Lock()
		switch {
		case l.cap == 0 || l.slots < l.cap:
			l.slots++
			over = false
		case urgent && l.extra < urgentExtra:
			l.extra++
			over = true
		default:
			wait := l.changed
			l.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, false, false
			}
		}
		l.peak = max(l.peak, l.slots+l.extra)
		l.mu.Unlock()
		return func() { l.release(over) }, over, true
	}
}

func (l *reviewLimit) release(over bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if over {
		l.extra--
	} else {
		l.slots--
	}
	close(l.changed)
	l.changed = make(chan struct{})
}

// concurrentMax is the most reviews that ran at once.
func (l *reviewLimit) concurrentMax() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.peak
}
