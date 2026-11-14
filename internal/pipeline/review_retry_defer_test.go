package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
)

// deferredRetryHead seeds PR 7's retry state as a head with one counted
// failure whose backoff has not elapsed: next is in the future.
func deferredRetryHead(t *testing.T, next time.Time) (*Service, *fakeGH, config.Repo, *clock) {
	t.Helper()
	s, g, _, _, r := reviewSetup(t)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 3}
	st := reviewRetryState{Head: shaA, Failures: 1, Backoffs: 1, Next: next, LastError: "boom"}
	if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), st); err != nil {
		t.Fatal(err)
	}
	return s, g, r, c
}

func deferredRows(t *testing.T, s *Service, r config.Repo) []map[string]any {
	t.Helper()
	return auditRows(t, s, r, "review-retry-deferred")
}

// (a) A backoff that has not elapsed leaves one review-retry-deferred row with
// the PR, head, next, failures and last error, and starts no review.
func TestRetryReviewDeferredWritesOneRow(t *testing.T) {
	next := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	s, g, r, _ := deferredRetryHead(t, next)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatalf("deferred poll: %v", err)
	}
	rows := deferredRows(t, s, r)
	if len(rows) != 1 {
		t.Fatalf("deferred rows=%v, want one", rows)
	}
	row := rows[0]
	if row["pr"] != float64(7) || row["head"] != shaA || row["next"] != next.Format(time.RFC3339) || row["failures"] != float64(1) || row["last_error"] != "boom" {
		t.Fatalf("deferred row=%v", row)
	}
	if g.pullCalls != 0 || g.postCount != 0 {
		t.Fatalf("deferred poll reviewed: pulls=%d posts=%d", g.pullCalls, g.postCount)
	}
}

// (b) The same head and next value on a later pass writes no new row.
func TestRetryReviewDeferredSameNextWritesNoNewRow(t *testing.T) {
	next := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	s, g, r, c := deferredRetryHead(t, next)
	for i := 0; i < 3; i++ {
		c.t = c.t.Add(time.Second)
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
			t.Fatalf("deferred poll %d: %v", i+1, err)
		}
	}
	if rows := deferredRows(t, s, r); len(rows) != 1 {
		t.Fatalf("deferred rows=%d, want one across repeated passes: %v", len(rows), rows)
	}
}

// (c) A new next value on the same head writes a new row.
func TestRetryReviewDeferredNewNextWritesNewRow(t *testing.T) {
	first := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	s, g, r, _ := deferredRetryHead(t, first)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	second := first.Add(time.Hour)
	st := retryStateOf(t, s, r)
	st.Next = second
	if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), st); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := deferredRows(t, s, r)
	if len(rows) != 2 || rows[1]["next"] != second.Format(time.RFC3339) {
		t.Fatalf("deferred rows=%v, want a second row for the new next", rows)
	}
}
