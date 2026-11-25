package daemon

import (
	"testing"
	"time"
)

// Took reads the running phase's time while the phase is still open, and the
// booked time once the phase ends.
func TestTookIncludesRunningPhase(t *testing.T) {
	clock := time.Unix(1000, 0)
	p := newPhases(func() time.Time { return clock })
	p.Enter(PhaseReview)
	clock = clock.Add(4 * time.Second)
	if got := p.Took(PhaseReview); got != 4*time.Second {
		t.Fatalf("mid-phase Took(review) = %v, want 4s", got)
	}
	if got := p.Took(PhaseRead); got != 0 {
		t.Fatalf("Took(read) while in review = %v, want 0", got)
	}
	clock = clock.Add(time.Second)
	p.Enter(PhaseSettle)
	clock = clock.Add(2 * time.Second)
	if got := p.Took(PhaseReview); got != 5*time.Second {
		t.Fatalf("Took(review) after it ended = %v, want 5s", got)
	}
	if got := p.Took(PhaseSettle); got != 2*time.Second {
		t.Fatalf("mid-phase Took(settle) = %v, want 2s", got)
	}
}

// A nil Phases records and reports nothing.
func TestTookNilReceiverIsZero(t *testing.T) {
	var p *Phases
	if got := p.Took(PhaseReview); got != 0 {
		t.Fatalf("nil Took = %v, want 0", got)
	}
}
