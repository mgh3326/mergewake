package pipeline

import (
	"context"
	"errors"
	"io"
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

// A stored incident that names another revert PR is not this revert's: the
// landed revert on PR #9 records the culprit of its own body, not the stale
// incident of PR #8. A stored incident that names PR #9 is kept.
func TestWatchSettledRevertIgnoresOtherRevertsIncident(t *testing.T) {
	staleCulprit := strings.Repeat("e", 40)
	cases := []struct {
		name      string
		stored    recoveryIncident
		culprit   string
		culpritPR int
	}{
		{"other revert PR", recoveryIncident{RevertPR: 8, Culprit: staleCulprit, CulpritPR: 6}, shaB, 7},
		{"matching revert PR", recoveryIncident{RevertPR: 9, Culprit: staleCulprit, CulpritPR: 6}, staleCulprit, 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, r, now := landedRevertSetup(t, github.Green)
			st := tc.stored
			st.RedSince, st.Head = now, shaC
			if e := s.saveRecovery(r.Name, st); e != nil {
				t.Fatal(e)
			}
			if got := recoveryWatch(t, s, r); got.Action != "healthy" {
				t.Fatalf("action %q (%s), want healthy", got.Action, got.Reason)
			}
			rows := auditRows(t, s, r, "recovery-merged")
			if len(rows) != 1 {
				t.Fatalf("recovery-merged rows %d, want 1", len(rows))
			}
			if rows[0]["culprit"] != tc.culprit || rows[0]["culprit_pr"] != float64(tc.culpritPR) {
				t.Fatalf("recorded culprit %v #%v, want %s #%d", rows[0]["culprit"], rows[0]["culprit_pr"], tc.culprit, tc.culpritPR)
			}
		})
	}
}

// A merged revert whose notice failed on an earlier green pass is recorded and
// noticed on the next green pass; a third pass adds neither the row nor the
// notice.
func TestWatchMergedRevertNoticeRetriedOnceOnNextGreenPass(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, _, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[shaC] = github.Green
		at := "2026-11-07T11:00:00Z"
		l.revert.State, l.revert.Merged, l.revert.MergedAt = "closed", true, &at
	})
	s.Now = func() time.Time { return now }
	s.Out = io.Discard
	if e := s.saveRecovery(r.Name, recoveryIncident{RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaC, RevertPR: 9, Task: "42"}); e != nil {
		t.Fatal(e)
	}

	n.err = errors.New("design lane unreachable")
	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("notify failure was hidden")
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 || len(ownerLaneEvents(n)) != 0 {
		t.Fatalf("after failed notice: rows %d lines %v", len(rows), ownerLaneEvents(n))
	}

	n.err = nil
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("retry pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after retry, want 1", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "reverted by PR #9") {
		t.Fatalf("design-lane lines after retry: %v", lines)
	}

	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("third pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after third pass, want 1", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines %v after third pass, want 1", lines)
	}
}

// A mismatched incident is rebuilt the way recovery() does: the row carries a
// real red_since, and the notice neither a zero time nor a reopen claim.
func TestWatchMismatchedRevertRebuildsIncident(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, _, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[shaC] = github.Green
		at := "2026-11-07T11:00:00Z"
		l.revert.State, l.revert.Merged, l.revert.MergedAt = "closed", true, &at
	})
	s.Now = func() time.Time { return now }
	stale := strings.Repeat("e", 40)
	if e := s.saveRecovery(r.Name, recoveryIncident{RedSince: now, Culprit: stale, CulpritPR: 6, Head: shaC, RevertPR: 8, Task: "99"}); e != nil {
		t.Fatal(e)
	}
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("action %q (%s), want healthy", got.Action, got.Reason)
	}
	rows := auditRows(t, s, r, "recovery-merged")
	if len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d, want 1", len(rows))
	}
	if red, _ := rows[0]["red_since"].(string); red != now.Format(time.RFC3339) {
		t.Fatalf("red_since %q, want %q", red, now.Format(time.RFC3339))
	}
	if rows[0]["culprit"] != shaB || rows[0]["culprit_pr"] != float64(7) {
		t.Fatalf("recorded culprit %v #%v, want culprit PR #7", rows[0]["culprit"], rows[0]["culprit_pr"])
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 {
		t.Fatalf("design-lane lines %v, want 1", lines)
	}
	if strings.Contains(lines[0], "0001-01-01") || strings.Contains(lines[0], "reopened") {
		t.Fatalf("notice carries a zero time or a reopen claim: %q", lines[0])
	}
	if !strings.Contains(lines[0], "reverted by PR #9") || !strings.Contains(lines[0], "no task to reopen") {
		t.Fatalf("notice missing revert or no-reopen note: %q", lines[0])
	}
}

// A mismatched incident whose first notify fails is recorded and announced
// once on the next green pass.
func TestWatchMismatchedRevertNoticeRetriedOnceOnNextGreenPass(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, _, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[shaC] = github.Green
		at := "2026-11-07T11:00:00Z"
		l.revert.State, l.revert.Merged, l.revert.MergedAt = "closed", true, &at
	})
	s.Now = func() time.Time { return now }
	s.Out = io.Discard
	stale := strings.Repeat("e", 40)
	if e := s.saveRecovery(r.Name, recoveryIncident{RedSince: now, Culprit: stale, CulpritPR: 6, Head: shaC, RevertPR: 8, Task: "99"}); e != nil {
		t.Fatal(e)
	}

	n.err = errors.New("design lane unreachable")
	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("notify failure was hidden")
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 || len(ownerLaneEvents(n)) != 0 {
		t.Fatalf("after failed notice: rows %d lines %v", len(rows), ownerLaneEvents(n))
	}

	n.err = nil
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("retry pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after retry, want 1", len(rows))
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 || !strings.Contains(lines[0], "reverted by PR #9") || strings.Contains(lines[0], "reopened") || strings.Contains(lines[0], "0001-01-01") {
		t.Fatalf("design-lane lines after retry: %v", lines)
	}

	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("third pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after third pass, want 1", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines %v after third pass, want 1", lines)
	}
}

// With no stored incident and nothing pending, a merged revert whose latch
// clear failed on the first pass is not recorded as merged on the retry: no
// incident is rebuilt without a pending attempt or a landed record for this
// revert, so the retry clears the latch and writes no row or notice.
func TestWatchMergedRevertNoStoredIncidentLatchClearRetry(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[shaC] = github.Green
		at := "2026-11-07T11:00:00Z"
		l.revert.State, l.revert.Merged, l.revert.MergedAt = "closed", true, &at
		l.revert.MergeCommitSHA = shaC
		l.clearErr = errors.New("latch clear refused")
	})
	s.Now = func() time.Time { return now }
	s.Out = io.Discard

	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("latch clear failure was hidden")
	}
	if _, found, _ := s.loadRecovery(r.Name); found {
		t.Fatal("a rebuilt incident was saved with no stored incident")
	}

	g.clearErr = nil
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("retry pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if g.latch {
		t.Fatal("latch still active after the retry")
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 0 {
		t.Fatalf("recovery-merged rows %d on retry, want 0", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 0 {
		t.Fatalf("design-lane lines %v on retry, want none", lines)
	}
}

// With no stored incident, a pending merged attempt for this revert rebuilds
// the incident before the settle: the row carries a real red_since, the notice
// claims no reopen, and a notice that failed on the first pass is retried on
// the next pass with the same red_since.
func TestWatchMissingIncidentPendingAttemptRebuildsAndRetriesNotice(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, _, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[shaC] = github.Green
		at := "2026-11-07T11:00:00Z"
		l.revert.State, l.revert.Merged, l.revert.MergedAt = "closed", true, &at
		l.revert.MergeCommitSHA = shaC
	})
	s.Now = func() time.Time { return now }
	s.Out = io.Discard
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 9), mergeAttempt{PR: 9, Head: revertHead, Base: shaC, At: now}); e != nil {
		t.Fatal(e)
	}

	n.err = errors.New("design lane unreachable")
	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("notify failure was hidden")
	}
	rows := auditRows(t, s, r, "recovery-merged")
	if len(rows) != 1 || len(ownerLaneEvents(n)) != 0 {
		t.Fatalf("after failed notice: rows %d lines %v", len(rows), ownerLaneEvents(n))
	}
	red, _ := rows[0]["red_since"].(string)
	if red != now.Format(time.RFC3339) {
		t.Fatalf("red_since %q, want %q", red, now.Format(time.RFC3339))
	}
	st, found := recoveryState(t, s, r)
	if !found || st.RevertPR != 9 || st.TaskReopen != "none" || !st.RedSince.Equal(now) {
		t.Fatalf("saved incident %+v found %v, want PR 9 with TaskReopen none at %v", st, found, now)
	}

	n.err = nil
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("retry pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after retry, want 1", len(rows))
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 || !strings.Contains(lines[0], "reverted by PR #9") || !strings.Contains(lines[0], now.Format(time.RFC3339)) {
		t.Fatalf("design-lane lines after retry: %v", lines)
	}
	if strings.Contains(lines[0], "reopened") || strings.Contains(lines[0], "0001-01-01") {
		t.Fatalf("notice claims a reopen or a zero time: %q", lines[0])
	}
	if !strings.Contains(lines[0], "no task to reopen") {
		t.Fatalf("notice missing no-reopen note: %q", lines[0])
	}

	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("third pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after third pass, want 1", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines %v after third pass, want 1", lines)
	}
}

// With no stored incident, a landed record for this revert rebuilds the
// incident before the record: the row carries a real red_since and a notice
// that failed on the first pass is retried on the next pass without a second
// row.
func TestWatchMissingIncidentLandedRecordRebuildsIncident(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.messages = map[string]string{shaSquashed: "Revert the culprit (#9)"}
		l.parents[shaSquashed] = shaC
		l.head = shaSquashed
		l.checks[shaSquashed] = github.Green
	})
	s.Now = func() time.Time { return now }
	s.Out = io.Discard
	if e := s.writeLanded(r.Name, 9, shaSquashed); e != nil {
		t.Fatal(e)
	}
	n.err = errors.New("design lane unreachable")
	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("notify failure was hidden")
	}
	rows := auditRows(t, s, r, "recovery-merged")
	if len(rows) != 1 || len(ownerLaneEvents(n)) != 0 {
		t.Fatalf("after failed notice: rows %d lines %v", len(rows), ownerLaneEvents(n))
	}
	if red, _ := rows[0]["red_since"].(string); red != now.Format(time.RFC3339) {
		t.Fatalf("red_since %q, want %q", red, now.Format(time.RFC3339))
	}
	st, found := recoveryState(t, s, r)
	if !found || st.RevertPR != 9 || st.TaskReopen != "none" || !st.RedSince.Equal(now) {
		t.Fatalf("saved incident %+v found %v, want PR 9 with TaskReopen none at %v", st, found, now)
	}
	n.err = nil
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("retry pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after retry, want 1", len(rows))
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 || !strings.Contains(lines[0], "already on main") || !strings.Contains(lines[0], "main green at") {
		t.Fatalf("design-lane lines after retry: %v", lines)
	}
	if len(g.closed) != 0 || len(g.comments) != 0 {
		t.Fatalf("closed %v comments %v; a landed revert must not be closed as fix-won", g.closed, g.comments)
	}
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("third pass action %q (%s), want healthy", got.Action, got.Reason)
	}
	if rows := auditRows(t, s, r, "recovery-merged"); len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d after third pass, want 1", len(rows))
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines %v after third pass, want 1", lines)
	}
}
