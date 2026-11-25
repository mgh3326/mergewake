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

// A revert whose unknown merge landed, with main red, is recorded as merged on
// the pass that first sees it: no blocker, no merge PUT, no close, one
// design-lane notice that the PR was left open and main is red. Later red
// passes hold per head rather than passing silently.
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
	if len(lines) != 1 || !strings.Contains(lines[0], "already on main") || !strings.Contains(lines[0], "main red at") || !strings.Contains(lines[0], "left the PR open") {
		t.Fatalf("design-lane lines %v", lines)
	}

	// The attempt stays pending while the PR is open. A later red pass on the
	// same head holds, once, and merges nothing.
	for pass := 0; pass < 2; pass++ {
		if got := recoveryWatch(t, s, r); got.Action != "recovery-hold" {
			t.Fatalf("pass %d action %q (%s), want recovery-hold", pass, got.Action, got.Reason)
		}
	}
	if len(ownerLaneEvents(n)) != 2 || g.merges != 0 || len(g.closed) != 0 {
		t.Fatalf("lines %v merges %d closed %v", ownerLaneEvents(n), g.merges, g.closed)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after repeat passes, want 1", len(rows))
	}
}

// The same landed revert with main green is not closed as fix-won, is not
// reverted again, and is recorded as merged once. A later red at a new head is
// held for that head, not silenced by the landed revert.
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
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "already on main") || !strings.Contains(lines[0], "main green at") {
		t.Fatalf("design-lane lines %v", lines)
	}
	if g.merges != 0 {
		t.Fatalf("revert merged %d times", g.merges)
	}

	// Main breaks again at a new head, with the revert still open: the watch
	// holds for that head and merges nothing.
	g.head, g.latch = advancedHead, true
	g.parents[advancedHead] = shaSquashed
	g.checks[advancedHead] = github.Red
	if got := recoveryWatch(t, s, r); got.Action != "recovery-hold" || !strings.Contains(got.Reason, "left the PR open") {
		t.Fatalf("new red: action %q (%s), want recovery-hold", got.Action, got.Reason)
	}
	if g.merges != 0 || len(g.closed) != 0 {
		t.Fatalf("new red merged %d closed %v", g.merges, g.closed)
	}
	if lines := ownerLaneEvents(n); len(lines) != 2 {
		t.Fatalf("design-lane lines %v; want the landing notice and one hold for the new head", lines)
	}
}

// A landed revert that a person then closes, as the notice asks, ends the
// latch without a fix-won record for a revert that landed on main.
func TestWatchSettledRevertClosedAfterLandingEndsLatch(t *testing.T) {
	s, g, _, r, _ := landedRevertSetup(t, github.Green)
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("landing pass: %+v", got)
	}
	g.revert.State = "closed"
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("closed pass: %+v", got)
	}
	if g.latch {
		t.Fatal("latch still active after the landed revert was closed")
	}
	if rows := auditRows(t, s, r, "recovery-fix-won"); len(rows) != 0 {
		t.Fatalf("recovery-fix-won recorded %d times for a landed revert", len(rows))
	}
	if len(g.comments) != 0 {
		t.Fatalf("comments %v on a landed revert", g.comments)
	}
}
