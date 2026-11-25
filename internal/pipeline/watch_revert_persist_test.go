package pipeline

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// A rebuilt mismatched incident is saved before the merged record is written,
// so a retried notice on a later green pass reuses the same red_since rather
// than the time of the retry.
func TestWatchMismatchedRevertRebuiltIncidentKeepsRedSinceOnRetry(t *testing.T) {
	first := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	retry := first.Add(time.Hour)
	s, _, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[shaC] = github.Green
		at := "2026-11-07T11:00:00Z"
		l.revert.State, l.revert.Merged, l.revert.MergedAt = "closed", true, &at
	})
	now := first
	s.Now = func() time.Time { return now }
	s.Out = io.Discard
	stale := strings.Repeat("e", 40)
	if e := s.saveRecovery(r.Name, recoveryIncident{RedSince: first, Culprit: stale, CulpritPR: 6, Head: shaC, RevertPR: 8, Task: "99"}); e != nil {
		t.Fatal(e)
	}

	n.err = errors.New("design lane unreachable")
	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("notify failure was hidden")
	}

	now = retry
	n.err = nil
	if got := recoveryWatch(t, s, r); got.Action != "healthy" {
		t.Fatalf("retry pass action %q (%s), want healthy", got.Action, got.Reason)
	}

	rows := auditRows(t, s, r, "recovery-merged")
	if len(rows) != 1 {
		t.Fatalf("recovery-merged rows %d, want 1", len(rows))
	}
	rowRed, _ := rows[0]["red_since"].(string)
	if rowRed != first.Format(time.RFC3339) {
		t.Fatalf("row red_since %q, want first pass %q", rowRed, first.Format(time.RFC3339))
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 {
		t.Fatalf("design-lane lines %v, want exactly one", lines)
	}
	if !strings.Contains(lines[0], "red since "+rowRed) {
		t.Fatalf("notice %q does not carry the row's red_since %q", lines[0], rowRed)
	}
	if strings.Contains(lines[0], retry.Format(time.RFC3339)) {
		t.Fatalf("notice %q carries the retry time", lines[0])
	}
}
