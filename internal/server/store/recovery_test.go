package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mgh3326/mergewake/internal/server/state"
)

func TestMySQLCancelCompletionCannotHideRefreshFailures(t *testing.T) {
	s := mysqlStore(t)
	f := fence(t, s)
	ctx := context.Background()
	k := Key{"example/project", 1}
	apply(t, s, f, k, state.Observe, head(1), Automatic)
	// A changed head creates cancellation and refresh work in the same generation.
	apply(t, s, f, k, state.Observe, head(2), Automatic)
	cancel, err := s.Claim(ctx, f, state.Cancel, Automatic)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		refresh, err := s.Claim(ctx, f, state.Refresh, Automatic)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.ConfirmAbsent(ctx, f, refresh); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec(`UPDATE mw_jobs SET available_at=UTC_TIMESTAMP(6) WHERE id=?`, refresh.ID); err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			// Persisted job kind, not caller-supplied claim metadata, owns recovery.
			cancel.Kind = state.Refresh
			snap, err := s.Snapshot(ctx, k)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Complete(ctx, f, cancel, &state.Event{Kind: state.Recovered, Head: snap.Head, Generation: snap.Generation}); !errors.Is(err, state.ErrTransition) {
				t.Fatal("cancellation injected a recovery fact", err)
			}
			if err = s.Complete(ctx, f, cancel, nil); err != nil {
				t.Fatal(err)
			}
		}
		snap, err := s.Snapshot(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Failures != i {
			t.Fatalf("refresh failures after attempt %d = %d, want %d", i, snap.Failures, i)
		}
	}
	snap, err := s.Snapshot(ctx, k)
	if err != nil || snap.Stage != state.SystemBlocked {
		t.Fatal("refresh parked without PR escalation", snap, err)
	}
	escalation, err := s.Claim(ctx, f, state.Escalate, Automatic)
	if err != nil {
		t.Fatal("third refresh failure failed to queue claimable escalation", err)
	}
	if err = s.Complete(ctx, f, escalation, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, f, state.Escalate, Automatic); !errors.Is(err, ErrNoJob) {
		t.Fatal("duplicate escalation", err)
	}
}
