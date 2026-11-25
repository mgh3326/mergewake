package pipeline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// landWatchSetup is the landed-revert incident of landedRevertSetup with the
// pending attempt on PR #9 written by role, and the revert PR's head branch set
// to head. The squash of #9 is on main, so Land would settle the attempt as
// landed and hold the PR if it were allowed to.
func landWatchSetup(t *testing.T, role, head string) (*Service, *recoveryGH, *fakeNotifier, config.Repo, string) {
	t.Helper()
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.messages = map[string]string{shaSquashed: "Revert the culprit (#9)"}
		l.parents[shaSquashed] = shaC
		l.head = shaSquashed
		l.checks[shaSquashed] = github.Red
		if head != "" {
			l.revert.Head.Ref = head
		}
	})
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 9), mergeAttempt{PR: 9, Head: revertHead, Base: shaC, At: now, Role: role}); e != nil {
		t.Fatal(e)
	}
	path := s.mergeAttemptFile(r.Name, 9)
	return s, g, n, r, path
}

// Land leaves a pending attempt that watch owns exactly as it is: no error, no
// merge, no merge hook or task event, no notice. The next watch pass settles it
// once, and a later pass does not record it again.
func TestLandLeavesWatchOwnedAttemptToWatch(t *testing.T) {
	for _, c := range []struct{ name, role, head string }{
		{"role watch", mergeRoleWatch, ""},
		{"role-less on the recovery branch", "", github.RecoveryBranch},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, g, n, r, path := landWatchSetup(t, c.role, c.head)
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var out strings.Builder
			s.Out = &out
			hooks := 0
			ctx := WithMergeHook(context.Background(), func(string, int) { hooks++ })
			var waiting *WaitingOnWatchError
			if e := s.Land(ctx, r, 9, Options{}); !errors.As(e, &waiting) {
				t.Fatalf("Land = %v, want a typed wait on watch", e)
			}
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(before, after) {
				t.Fatalf("watch-owned attempt changed or was cleared: %v", e)
			}
			if hooks != 0 || len(n.events) != 0 || g.merges != 0 {
				t.Fatalf("hook calls %d, notices %d, merges %d; want none", hooks, len(n.events), g.merges)
			}
			if !strings.Contains(out.String(), "land-waiting-on-watch") {
				t.Fatalf("Land printed %q, want a waiting-on-watch row", out.String())
			}

			if got := recoveryWatch(t, s, r); got.Action != "recovery-merged" {
				t.Fatalf("watch pass action %q (%s), want recovery-merged", got.Action, got.Reason)
			}
			if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
				t.Fatalf("recovery-merged rows %d, want 1", len(rows))
			}
			if lines := ownerLaneEvents(n); len(lines) != 1 {
				t.Fatalf("design-lane notices %v, want one", lines)
			}
			if got := recoveryWatch(t, s, r); got.Action != "recovery-hold" {
				t.Fatalf("second watch pass action %q (%s), want recovery-hold", got.Action, got.Reason)
			}
			if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
				t.Fatalf("recovery-merged rows %d after a repeat pass, want 1", len(rows))
			}
		})
	}
}

// If watch takes ownership while Land waits for the repository lock, Land
// re-reads the attempt under that lock and leaves it for watch to settle.
func TestLandRechecksWatchOwnershipUnderMergeLock(t *testing.T) {
	s, g, n, r, path := landWatchSetup(t, mergeRolePR, "")
	unlock, err := s.mergeLock(r.Name)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	s.Out = &out
	hooks := 0
	ctx := WithMergeHook(context.Background(), func(string, int) { hooks++ })
	done := make(chan error, 1)
	go func() { done <- s.Land(ctx, r, 9, Options{}) }()
	// The held lock keeps Land between the initial PR-role read and settle.
	select {
	case err := <-done:
		unlock()
		t.Fatalf("Land returned before the lock was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := fsutil.WriteJSON(path, mergeAttempt{PR: 9, Head: revertHead, Base: shaC, At: s.now(), Role: mergeRoleWatch}); err != nil {
		unlock()
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	var waiting *WaitingOnWatchError
	select {
	case err := <-done:
		if !errors.As(err, &waiting) {
			t.Fatalf("Land = %v, want a typed wait on watch", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Land did not return after releasing merge lock")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("watch-owned attempt changed or was cleared: %v", err)
	}
	if hooks != 0 || len(n.events) != 0 || g.merges != 0 {
		t.Fatalf("hook calls %d, notices %d, merges %d; want none", hooks, len(n.events), g.merges)
	}
	if !strings.Contains(out.String(), "land-waiting-on-watch") {
		t.Fatalf("Land printed %q, want a waiting-on-watch row", out.String())
	}
}

// Attempts Land settles as before: a pr-role attempt, and a role-less attempt
// on an ordinary branch, both hold the landed PR (MergeHeldError) and are not
// left to watch.
func TestLandSettlesPRAttemptAsBefore(t *testing.T) {
	for _, c := range []struct{ name, role, head string }{
		{"pr role on the recovery branch", mergeRolePR, github.RecoveryBranch},
		{"role-less on an ordinary branch", "", "feature"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _, n, r, _ := landWatchSetup(t, c.role, c.head)
			var held *MergeHeldError
			if e := s.Land(context.Background(), r, 9, Options{}); !errors.As(e, &held) {
				t.Fatalf("Land = %v, want a MergeHeldError", e)
			}
			if len(n.events) != 1 {
				t.Fatalf("hold notices %d, want 1", len(n.events))
			}
		})
	}
}

// mergePinned, which Land reaches after its gate, refuses a watch-owned attempt
// written after Land's own check: it sends no merge and clears nothing.
func TestMergePinnedRefusesWatchOwnedAttempt(t *testing.T) {
	s, g, n, r, path := landWatchSetup(t, mergeRoleWatch, "")
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e := s.mergePinned(context.Background(), r, g.revert); !errors.Is(e, errWaitingOnWatch) {
		t.Fatalf("mergePinned = %v, want errWaitingOnWatch", e)
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(before, after) || g.merges != 0 || len(n.events) != 0 {
		t.Fatalf("attempt changed %v, merges %d, notices %d", e, g.merges, len(n.events))
	}
}
