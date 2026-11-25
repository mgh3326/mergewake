package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// landedRevertSetup is the red-main incident of recoverySetup with the revert
// PR's squash landed on main and GitHub left PR #9 open. A pending unknown
// outcome for that PR, with base shaC, is on disk, so the next watch pass
// must settle it.
func landedRevertSetup(t *testing.T, mainCI github.CI) (*Service, *recoveryGH, *fakeNotifier, config.Repo, time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.messages = map[string]string{shaSquashed: "Revert the culprit (#9)"}
		l.parents[shaSquashed] = shaC
		l.head = shaSquashed
		l.checks[shaSquashed] = mainCI
	})
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 9), mergeAttempt{PR: 9, Head: revertHead, Base: shaC, At: now}); e != nil {
		t.Fatal(e)
	}
	return s, g, n, r, now
}

// A revert whose unknown merge landed, with main still red and the fix window
// open, is recorded as merged on the watch pass: no blocker, no second revert,
// no merge PUT, and one design-lane notice that the PR was left open.
func TestWatchSettledRevertLandedRedMainRecordsMerged(t *testing.T) {
	s, g, n, r, _ := landedRevertSetup(t, github.Red)

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-merged" {
		t.Fatalf("action %q (%s), want recovery-merged", got.Action, got.Reason)
	}
	if g.merges != 0 || len(g.closed) != 0 || len(g.comments) != 0 {
		t.Fatalf("merges %d closed %v comments %v; want none", g.merges, g.closed, g.comments)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d, want 1", len(rows))
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 || !strings.Contains(lines[0], "already on main") || !strings.Contains(lines[0], "left the PR open") {
		t.Fatalf("design-lane lines %v", lines)
	}

	// The attempt stays pending while the PR is open, so a later pass settles
	// it again: it records and notifies nothing new.
	if got := recoveryWatch(t, s, r); got.Action != "recovery-merged" {
		t.Fatalf("repeat action %q (%s)", got.Action, got.Reason)
	}
	if len(ownerLaneEvents(n)) != 1 || g.merges != 0 {
		t.Fatalf("repeat notified again or merged: lines %v merges %d", ownerLaneEvents(n), g.merges)
	}
}

// The same landed revert with main green is not closed as fix-won, is not
// reverted again, and is still recorded as merged once.
func TestWatchSettledRevertLandedGreenMainNotFixWon(t *testing.T) {
	s, g, n, r, _ := landedRevertSetup(t, github.Green)

	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("action %q (%s), want healthy", got.Action, got.Reason)
	}
	if len(g.closed) != 0 || len(g.comments) != 0 {
		t.Fatalf("closed %v comments %v; a landed revert must not be closed as fix-won", g.closed, g.comments)
	}
	if rows := auditRows(t, s, r, "recovery-fix-won"); len(rows) != 0 {
		t.Fatalf("recovery-fix-won recorded %d times", len(rows))
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d, want 1", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "already on main") {
		t.Fatalf("design-lane lines %v", lines)
	}
	if g.merges != 0 {
		t.Fatalf("revert merged %d times", g.merges)
	}
}
