package state

import (
	"errors"
	"testing"
)

func advance(t *testing.T, s Snapshot, k EventKind) Snapshot {
	t.Helper()
	r, err := Reduce(s, Event{Kind: k, Head: s.Head, Generation: s.Generation})
	if err != nil {
		t.Fatalf("%s from %s: %v", k, s.Stage, err)
	}
	return r.Next
}
func observed(t *testing.T) Snapshot {
	t.Helper()
	r, err := Reduce(Snapshot{}, Event{Kind: Observe, Head: "head-1", Base: "base"})
	if err != nil {
		t.Fatal(err)
	}
	return r.Next
}
func TestReviewCannotGrantReady(t *testing.T) {
	s := observed(t)
	if _, err := Reduce(s, Event{Kind: GatePassed, Head: s.Head, Generation: s.Generation}); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
	s = advance(t, s, QueueReview)
	s = advance(t, s, StartReview)
	s = advance(t, s, ReviewPassed)
	if s.Stage != GatePending {
		t.Fatal(s)
	}
	s = advance(t, s, GatePassed)
	if s.Stage != Ready {
		t.Fatal(s)
	}
}
func TestTwoFixRoundsCannotBeResetByHeadsOrQueueEvent(t *testing.T) {
	s := observed(t)
	s = advance(t, s, QueueReview)
	s = advance(t, s, StartReview)
	s = advance(t, s, Findings)
	for i := 1; i <= 2; i++ {
		r, err := Reduce(s, Event{Kind: Observe, Head: string(rune('a' + i)), Base: s.Base})
		if err != nil {
			t.Fatal(err)
		}
		s = r.Next
		s = advance(t, s, QueueReview)
		if s.FixRounds != i {
			t.Fatal(s)
		}
		s = advance(t, s, StartReview)
		s = advance(t, s, Findings)
	}
	if s.Stage != BlockedAuthor {
		t.Fatal(s)
	}
	r, err := Reduce(s, Event{Kind: Observe, Head: "another-head", Base: s.Base})
	if err != nil {
		t.Fatal(err)
	}
	s = r.Next
	if s.Stage != BlockedAuthor || s.FixRounds != 2 {
		t.Fatal(s)
	}
	if _, err = Reduce(s, Event{Kind: QueueReview, Head: s.Head, Generation: s.Generation}); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
	s = advance(t, s, Resume)
	if s.FixRounds != 0 || s.Stage != Observing {
		t.Fatal(s)
	}
}
func TestEquivalentPatchAndDuplicateObservation(t *testing.T) {
	s := observed(t)
	r, err := Reduce(s, Event{Kind: Observe, Head: s.Head, Base: s.Base})
	if err != nil || r.Next != s || len(r.Effects) != 0 {
		t.Fatal(r, err)
	}
	s = advance(t, s, QueueReview)
	s = advance(t, s, StartReview)
	s = advance(t, s, Findings)
	s = advance(t, s, EquivalentPatch)
	if s.FixRounds != 0 || s.Stage != GatePending {
		t.Fatal(s)
	}
}
func TestStaleCompletionAndTerminalRefusal(t *testing.T) {
	s := observed(t)
	old := s
	r, _ := Reduce(s, Event{Kind: Observe, Head: "new", Base: s.Base})
	s = r.Next
	if _, err := Reduce(s, Event{Kind: ReviewPassed, Head: old.Head, Generation: old.Generation}); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	s = advance(t, s, ConfirmClosed)
	if _, err := Reduce(s, Event{Kind: Observe, Head: "reopen", Base: s.Base}); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
}
func TestConsecutiveFailureBudget(t *testing.T) {
	s := observed(t)
	s = advance(t, s, Failure)
	s = advance(t, s, Failure)
	s = advance(t, s, Recovered)
	if s.Failures != 0 {
		t.Fatal(s)
	}
	for i := 1; i <= 3; i++ {
		r, err := Reduce(s, Event{Kind: Failure, Head: s.Head, Generation: s.Generation})
		if err != nil {
			t.Fatal(err)
		}
		s = r.Next
		if (len(r.Effects) == 1) != (i == 3) {
			t.Fatal(r)
		}
	}
	if s.Stage != SystemBlocked {
		t.Fatal(s)
	}
	r, err := Reduce(s, Event{Kind: Failure, Head: s.Head, Generation: s.Generation})
	if err != nil || r.Next != s || len(r.Effects) != 0 {
		t.Fatal(r, err)
	}
	s = advance(t, s, Resume)
	if s.Failures != 0 {
		t.Fatal(s)
	}
}
