package state

import (
	"errors"
	"testing"
)

func TestClosedReopensOnVerifiedOpenFact(t *testing.T) {
	s := advance(t, advance(t, advance(t, advance(t, observed(t), QueueReview), StartReview), ReviewPassed), ConfirmClosed)
	r, err := Reduce(s, Event{Kind: Reopen, Head: s.Head, Base: s.Base, Generation: s.Generation})
	if err != nil {
		t.Fatalf("verified reopen refused: %v", err)
	}
	if r.Next.Stage != Observing || r.Next.Generation <= s.Generation || r.Next.Lifecycle <= s.Lifecycle || r.Next.Reviewed {
		t.Fatalf("reopen reused closed lifecycle: %+v", r)
	}
	if _, err = Reduce(r.Next, Event{Kind: ConfirmClosed, Head: s.Head, Generation: s.Generation}); !errors.Is(err, ErrStale) {
		t.Fatalf("old close changed reopened PR: %v", err)
	}
	if _, err = Reduce(r.Next, Event{Kind: EquivalentPatch, Head: r.Next.Head, Generation: r.Next.Generation}); !errors.Is(err, ErrTransition) {
		t.Fatalf("reopen reused old review: %v", err)
	}
	merged := advance(t, r.Next, ConfirmMerged)
	if _, err = Reduce(merged, Event{Kind: Reopen, Head: merged.Head, Base: merged.Base, Generation: merged.Generation}); !errors.Is(err, ErrTransition) {
		t.Fatalf("merged reopened: %v", err)
	}
}

func TestSameHeadConvertToDraft(t *testing.T) {
	for _, stage := range []Stage{Reviewing, Ready} {
		t.Run(string(stage), func(t *testing.T) {
			s := advance(t, advance(t, observed(t), QueueReview), StartReview)
			if stage == Ready {
				s = advance(t, advance(t, s, ReviewPassed), GatePassed)
			}
			r, err := Reduce(s, Event{Kind: MarkDraft, Head: s.Head, Generation: s.Generation})
			if err != nil {
				t.Fatalf("same-head draft conversion from %s refused: %v", stage, err)
			}
			if r.Next.Stage != Draft || r.Next.Generation <= s.Generation || r.Next.Head != s.Head {
				t.Fatalf("draft retained eligibility: %+v", r)
			}
			if _, err = Reduce(r.Next, Event{Kind: GatePassed, Head: s.Head, Generation: s.Generation}); !errors.Is(err, ErrStale) {
				t.Fatalf("old gate survived draft: %v", err)
			}
			draft := r.Next
			duplicate, err := Reduce(draft, Event{Kind: MarkDraft, Head: draft.Head, Generation: draft.Generation})
			if err != nil || duplicate.Next != draft || len(duplicate.Effects) != 0 {
				t.Fatalf("duplicate draft not idempotent: %+v %v", duplicate, err)
			}
			ready := advance(t, draft, ReadyForReview)
			if ready.IsDraft || ready.Stage != Observing || ready.Generation <= draft.Generation {
				t.Fatal(ready)
			}
			if _, err = Reduce(ready, Event{Kind: MarkDraft, Head: draft.Head, Generation: draft.Generation}); !errors.Is(err, ErrStale) {
				t.Fatalf("late draft regressed readiness: %v", err)
			}
			duplicate, err = Reduce(ready, Event{Kind: ReadyForReview, Head: ready.Head, Generation: ready.Generation})
			if err != nil || duplicate.Next != ready || len(duplicate.Effects) != 0 {
				t.Fatal(duplicate, err)
			}
		})
	}
}

func ciFact(t *testing.T, s Snapshot, status CIStatus) Transition {
	t.Helper()
	r, err := Reduce(s, Event{Kind: CIChanged, Head: s.Head, Generation: s.Generation, CI: status})
	if err != nil {
		t.Fatal(status, err)
	}
	return r
}

func TestCIOutcomesAreDomainFacts(t *testing.T) {
	for _, status := range []CIStatus{CIFailure, CICancelled, CITimedOut} {
		t.Run(string(status), func(t *testing.T) {
			s := advance(t, observed(t), WaitCI)
			s.Failures = 2
			failed := ciFact(t, s, status)
			if failed.Next.Stage != AwaitingAuthor || failed.Next.CI != status || failed.Next.Failures != 2 {
				t.Fatal(failed)
			}
			duplicate := ciFact(t, failed.Next, status)
			if duplicate.Next != failed.Next || len(duplicate.Effects) != 0 {
				t.Fatal(duplicate)
			}
			pending := ciFact(t, failed.Next, CIPending)
			if pending.Next.Stage != WaitingCI || pending.Next.Head != s.Head || pending.Next.Failures != 2 {
				t.Fatal(pending)
			}
			if _, err := Reduce(pending.Next, Event{Kind: GatePassed, Head: s.Head, Generation: s.Generation}); !errors.Is(err, ErrStale) {
				t.Fatalf("old gate survived CI rerun: %v", err)
			}
			if _, err := Reduce(pending.Next, Event{Kind: GatePassed, Head: s.Head, Generation: pending.Next.Generation}); !errors.Is(err, ErrTransition) {
				t.Fatalf("pending CI became ready: %v", err)
			}
			green := ciFact(t, pending.Next, CISuccess)
			if green.Next.Stage != Observing || green.Next.Failures != 2 || green.Next.CI != CISuccess {
				t.Fatal(green)
			}
		})
	}
}

func TestCIChangesPreserveIndependentReviewAndHolds(t *testing.T) {
	for _, status := range []CIStatus{CIPending, CIFailure, CICancelled, CITimedOut} {
		s := advance(t, advance(t, observed(t), QueueReview), StartReview)
		changed := ciFact(t, s, status).Next
		if changed.Stage != Reviewing || changed.Generation != s.Generation {
			t.Fatalf("CI cancelled independent review: %+v", changed)
		}
		done := advance(t, changed, ReviewPassed)
		want := AwaitingAuthor
		if status == CIPending {
			want = WaitingCI
		}
		if done.Stage != want {
			t.Fatal(status, done)
		}
	}
	s := observed(t)
	s.Stage = BlockedAuthor
	s.FixRounds = 2
	s.Reviewed = true
	s = advance(t, s, MarkDraft)
	s = advance(t, s, ReadyForReview)
	s = ciFact(t, s, CISuccess).Next
	if s.Stage != BlockedAuthor || s.FixRounds != 2 {
		t.Fatal("eligibility reset exhausted budget", s)
	}
}

func TestReopenWithChangedHeadKeepsBudgetAndOperationalHolds(t *testing.T) {
	s := observed(t)
	s.Reviewed = true
	s.FixRounds = 1
	s = advance(t, s, ConfirmClosed)
	r, err := Reduce(s, Event{Kind: Reopen, Head: "new-head", Base: "new-base", Generation: s.Generation})
	if err != nil || !r.Next.Valid() || r.Next.Head == s.Head || r.Next.FixRounds != 1 || r.Next.Reviewed {
		t.Fatal(r, err)
	}
	s.Failures = 3
	r, err = Reduce(s, Event{Kind: Reopen, Head: s.Head, Base: s.Base, Generation: s.Generation})
	if err != nil || r.Next.Stage != SystemBlocked || r.Next.Failures != 3 {
		t.Fatal(r, err)
	}
}

func TestWaitingCIFailureAndRerun(t *testing.T) {
	s := advance(t, observed(t), WaitCI)
	r, err := Reduce(s, Event{Kind: GateFailed, Head: s.Head, Generation: s.Generation})
	if err != nil {
		t.Fatalf("waiting CI failure refused: %v", err)
	}
	if r.Next.Stage != AwaitingAuthor || r.Next.Failures != 0 {
		t.Fatalf("CI failure became system failure: %+v", r)
	}
	s = advance(t, r.Next, WaitCI)
	if s.Stage != WaitingCI || s.Failures != 0 {
		t.Fatal(s)
	}
}
