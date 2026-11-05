package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mgh3326/mergewake/internal/server/state"
)

func inputFor(t *testing.T, s *Store, k Key, event state.Event) Input {
	t.Helper()
	snap, err := s.Snapshot(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	if event.Head == "" {
		event.Head = snap.Head
	}
	if event.Base == "" {
		event.Base = snap.Base
	}
	if event.Generation == 0 {
		event.Generation = snap.Generation
	}
	return Input{Key: k, Source: "fixture", ID: uuid(), ExpectedVersion: snap.Version, Event: event, Class: Automatic}
}
func fact(t *testing.T, s *Store, f Fence, k Key, event state.Event) Applied {
	t.Helper()
	a, err := s.Apply(context.Background(), f, inputFor(t, s, k, event))
	if err != nil {
		t.Fatal(event.Kind, err)
	}
	return a
}
func activeReviews(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mw_attempts a JOIN mw_jobs j ON j.id=a.job_id WHERE j.kind='review' AND a.status IN ('running','uncertain')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMySQLClosedReopenFencesOldWorkAndObservations(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	queuedReview(t, s, f, k, Automatic)
	old, err := s.Claim(ctx, f, state.Review, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	closed := fact(t, s, f, k, state.Event{Kind: state.ConfirmClosed}).Snapshot
	// A close read started before the verified open read finishes later.
	lateClose := inputFor(t, s, k, state.Event{Kind: state.ConfirmClosed})
	reopen := inputFor(t, s, k, state.Event{Kind: state.Reopen})
	opened, err := s.Apply(ctx, f, reopen)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Snapshot.Generation <= closed.Generation || opened.Snapshot.Lifecycle <= closed.Lifecycle || opened.Snapshot.Reviewed {
		t.Fatal(opened)
	}
	dup, err := s.Apply(ctx, f, reopen)
	if err != nil || !dup.Duplicate || dup.Snapshot != opened.Snapshot {
		t.Fatal(dup, err)
	}
	if _, err = s.Apply(ctx, f, lateClose); !errors.Is(err, ErrConflict) {
		t.Fatalf("late close read regressed reopened PR: %v", err)
	}
	lateClose.ID = uuid()
	lateClose.ExpectedVersion = opened.Snapshot.Version
	if _, err = s.Apply(ctx, f, lateClose); !errors.Is(err, state.ErrStale) {
		t.Fatalf("old close generation accepted: %v", err)
	}
	if activeReviews(t, s) != 1 {
		t.Fatal("reopen released unconfirmed remote reservation")
	}
	if err = s.Complete(ctx, f, old, &state.Event{Kind: state.ReviewPassed, Head: head(1), Generation: old.Generation}); err != nil {
		t.Fatal(err)
	}
	current, err := s.Snapshot(ctx, k)
	if err != nil || current != opened.Snapshot || activeReviews(t, s) != 0 {
		t.Fatal(current, err)
	}
}

func TestMySQLDraftCancellationRetainsCapacityUntilTerminal(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	queuedReview(t, s, f, k, Automatic)
	old, err := s.Claim(ctx, f, state.Review, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	draftInput := inputFor(t, s, k, state.Event{Kind: state.MarkDraft})
	draft, err := s.Apply(ctx, f, draftInput)
	if err != nil {
		t.Fatal(err)
	}
	if !draft.Snapshot.IsDraft || draft.Snapshot.Stage != state.Draft {
		t.Fatal(draft)
	}
	if _, err = s.Claim(ctx, f, state.Review, Automatic); !errors.Is(err, ErrNoJob) {
		t.Fatal("draft review remained claimable", err)
	}
	if activeReviews(t, s) != 1 {
		t.Fatal("draft released unconfirmed remote reservation")
	}
	cancel, err := s.Claim(ctx, f, state.Cancel, Automatic)
	if err != nil {
		t.Fatal("no durable cancellation intent", err)
	}
	if cancel.CancelThrough != old.Generation || cancel.CancelThrough >= draft.Snapshot.Generation {
		t.Fatal("cancellation cutoff includes future work", cancel)
	}
	recovered, err := s.Outstanding(ctx, f, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range recovered {
		if c.ID == cancel.ID {
			found = true
			if c.CancelThrough != cancel.CancelThrough {
				t.Fatal("restart lost cancellation cutoff", c)
			}
		}
	}
	if !found {
		t.Fatal("cancellation missing from recovery")
	}
	if err = s.Complete(ctx, f, cancel, nil); err != nil {
		t.Fatal(err)
	}
	if activeReviews(t, s) != 1 {
		t.Fatal("cancellation request assumed terminal confirmation")
	}
	ready := fact(t, s, f, k, state.Event{Kind: state.ReadyForReview})
	lateCancel, err := s.Claim(ctx, f, state.Cancel, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	if lateCancel.CancelThrough != draft.Snapshot.Generation || lateCancel.CancelThrough >= ready.Snapshot.Generation {
		t.Fatal("late cancellation targets ready-for-review generation", lateCancel)
	}
	if err = s.Complete(ctx, f, lateCancel, nil); err != nil {
		t.Fatal(err)
	}
	dup, err := s.Apply(ctx, f, draftInput)
	if err != nil || !dup.Duplicate || dup.Snapshot != ready.Snapshot {
		t.Fatal("duplicate draft regressed eligibility", dup, err)
	}
	late := draftInput
	late.ID = uuid()
	if _, err = s.Apply(ctx, f, late); !errors.Is(err, ErrConflict) {
		t.Fatal("late draft read regressed eligibility", err)
	}
	if err = s.Complete(ctx, f, old, &state.Event{Kind: state.ReviewPassed, Head: head(1), Generation: old.Generation}); err != nil {
		t.Fatal(err)
	}
	current, err := s.Snapshot(ctx, k)
	if err != nil || current != ready.Snapshot || activeReviews(t, s) != 0 {
		t.Fatal(current, err)
	}
	if _, err = s.Claim(ctx, f, state.Refresh, Automatic); err != nil {
		t.Fatal("cancellation blocked resumed refresh", err)
	}
}

func TestMySQLCIOutcomeAndLateRead(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	apply(t, s, f, k, state.Observe, head(1), Automatic)
	fact(t, s, f, k, state.Event{Kind: state.CIChanged, CI: state.CIPending})
	latePending := inputFor(t, s, k, state.Event{Kind: state.CIChanged, CI: state.CIPending})
	failed := fact(t, s, f, k, state.Event{Kind: state.CIChanged, CI: state.CITimedOut})
	if failed.Snapshot.Stage != state.AwaitingAuthor || failed.Snapshot.Failures != 0 || failed.Snapshot.CI != state.CITimedOut {
		t.Fatal(failed)
	}
	if _, err := s.Apply(ctx, f, latePending); !errors.Is(err, ErrConflict) {
		t.Fatal("late pending read regressed CI", err)
	}
	pending := fact(t, s, f, k, state.Event{Kind: state.CIChanged, CI: state.CIPending})
	if pending.Snapshot.Stage != state.WaitingCI || pending.Snapshot.Failures != 0 {
		t.Fatal(pending)
	}
	green := fact(t, s, f, k, state.Event{Kind: state.CIChanged, CI: state.CISuccess})
	if green.Snapshot.Stage != state.Observing || green.Snapshot.Failures != 0 {
		t.Fatal(green)
	}
	if _, err := s.Claim(ctx, f, state.Refresh, Automatic); err != nil {
		t.Fatal("CI rerun did not resume planning", err)
	}
}
