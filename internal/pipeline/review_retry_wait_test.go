package pipeline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// failedRetryHead seeds PR 7's retry state as a head that already counted
// failures attempts whose backoff has elapsed, with a reviewer that fails
// every attempt (an author family nothing resolves).
func failedRetryHead(t *testing.T, failures, threshold int) (*Service, *fakeGH, *fakeNotifier, config.Repo, *clock) {
	t.Helper()
	s, g, _, n, r := reviewSetup(t)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: threshold}
	g.pull.Body = "Mergewake-Author-Profile: unknown@v1"
	s.Config.Review.DefaultFamily = ""
	st := reviewRetryState{Head: shaA, Failures: failures, Backoffs: failures, Next: c.t.Add(-time.Minute)}
	if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), st); err != nil {
		t.Fatal(err)
	}
	return s, g, n, r, c
}

func makeConflicting(g *fakeGH) {
	no := false
	g.pull.Mergeable = &no
	g.pull.MergeableState = "dirty"
}

func redChecks(g *fakeGH, ci github.CI) {
	g.checkNames = map[string]map[string]github.CI{shaA: {"CI": ci}}
}

func waitingRows(t *testing.T, s *Service, r config.Repo) []map[string]any {
	t.Helper()
	return auditRows(t, s, r, "review-retry-waiting")
}

// (a) A failed head that conflicts gets no new attempt and no escalation;
// the wait is audited once per head and reason, not once per poll, and the
// failure count, backoff and last error stay as they were.
func TestRetryReviewWaitsWhileHeadConflicts(t *testing.T) {
	s, g, n, r, c := failedRetryHead(t, 2, 3)
	makeConflicting(g)
	for i := 0; i < 2; i++ {
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
			t.Fatalf("poll %d: %v", i+1, err)
		}
	}
	if g.pullCalls != 2 {
		t.Fatalf("pull calls=%d, want the fresh read only: a review ran", g.pullCalls)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 2 || st.Backoffs != 2 || st.Escalated || !st.Next.Equal(c.t.Add(-time.Minute)) {
		t.Fatalf("state=%+v, want the kept count, backoff and no escalation", st)
	}
	rows := waitingRows(t, s, r)
	if len(rows) != 1 || rows[0]["reason"] != "conflict" || rows[0]["pr"] != float64(7) || rows[0]["head"] != shaA || rows[0]["failures"] != float64(2) {
		t.Fatalf("waiting rows=%v, want one conflict row for the head", rows)
	}
	if len(auditRows(t, s, r, "review-failed")) != 0 || n.count != 0 {
		t.Fatalf("failure or escalation while waiting: %v", n.events)
	}
}

// (b) The same wait applies while a required check under the trusted base
// policy is red.
func TestRetryReviewWaitsWhileChecksRed(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 2, 3)
	redChecks(g, github.Red)
	for i := 0; i < 2; i++ {
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
			t.Fatalf("poll %d: %v", i+1, err)
		}
	}
	if g.pullCalls != 2 {
		t.Fatalf("pull calls=%d, want the fresh read only: a review ran", g.pullCalls)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 2 || st.Escalated {
		t.Fatalf("state=%+v, want the kept count and no escalation", st)
	}
	rows := waitingRows(t, s, r)
	if len(rows) != 1 || rows[0]["reason"] != "ci_red" || rows[0]["head"] != shaA || rows[0]["failures"] != float64(2) {
		t.Fatalf("waiting rows=%v, want one ci_red row for the head", rows)
	}
	if len(auditRows(t, s, r, "review-failed")) != 0 || n.count != 0 {
		t.Fatalf("failure or escalation while waiting: %v", n.events)
	}
}

// (c) Once the conflict clears the kept count resumes: the next due poll
// runs the review again and escalates at the threshold as today.
func TestRetryReviewResumesWhenConflictClears(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 2, 3)
	makeConflicting(g)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	mergeable := true
	g.pull.Mergeable = &mergeable
	g.pull.MergeableState = "clean"
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("want the retried review to fail")
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 3 || !st.Escalated {
		t.Fatalf("state=%+v, want the kept count to reach the threshold and escalate", st)
	}
	if n.count != 1 {
		t.Fatalf("escalations=%d: %v", n.count, n.events)
	}
	if rows := waitingRows(t, s, r); len(rows) != 1 {
		t.Fatalf("waiting rows=%v, want only the earlier wait", rows)
	}
}

// (d) A head whose passes are complete never counts a failure and never
// escalates, conflicting and red or not: it is done, not waiting.
func TestRetryReviewCompleteHeadIsNotWaiting(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 2, 3)
	makeConflicting(g)
	redChecks(g, github.Red)
	g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Family: "anthropic"}}}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.pullCalls != 1 {
		t.Fatalf("pull calls=%d, want no review", g.pullCalls)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 0 {
		t.Fatalf("state=%+v, want the completed head's retry state reset", st)
	}
	if len(waitingRows(t, s, r)) != 0 || len(auditRows(t, s, r, "review-failed")) != 0 || n.count != 0 {
		t.Fatalf("a completed head was treated as failed: %v", n.events)
	}
}

// (e) The first attempt of a head runs as today, conflicting or not.
func TestRetryReviewFirstAttemptRunsOnConflict(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 0, 3)
	makeConflicting(g)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("want the first attempt's failure")
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 1 {
		t.Fatalf("state=%+v, want the first attempt counted", st)
	}
	if len(waitingRows(t, s, r)) != 0 || n.count != 0 {
		t.Fatalf("a first attempt waited or escalated: %v", n.events)
	}
}

// (f) Pending checks are not red: a failed head is retried as today.
func TestRetryReviewPendingChecksRetry(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 2, 3)
	redChecks(g, github.Pending)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("want the retried review to fail")
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 3 || !st.Escalated {
		t.Fatalf("state=%+v, want pending checks retried as today", st)
	}
	if len(waitingRows(t, s, r)) != 0 {
		t.Fatal("pending checks counted as red")
	}
	if n.count != 1 {
		t.Fatalf("escalations=%d: %v", n.count, n.events)
	}
}

// assertEscalationHeld checks a head at the escalation threshold that waits:
// one attempt-free pull, no escalation notice, one wait row, Escalated false.
func assertEscalationHeld(t *testing.T, s *Service, r config.Repo, g *fakeGH, n *fakeNotifier, reason string) {
	t.Helper()
	if g.pullCalls != 1 {
		t.Fatalf("pull calls=%d, want the fresh read only: a review ran", g.pullCalls)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 3 || st.Escalated {
		t.Fatalf("state=%+v, want the kept count and no escalation", st)
	}
	if n.count != 0 {
		t.Fatalf("escalation notices=%d: %v", n.count, n.events)
	}
	rows := waitingRows(t, s, r)
	if len(rows) != 1 || rows[0]["reason"] != reason || rows[0]["head"] != shaA || rows[0]["failures"] != float64(3) {
		t.Fatalf("waiting rows=%v, want one %s row for the head", rows, reason)
	}
}

// At the threshold the escalation waits too: a conflicting head sends no
// escalation notice and keeps Escalated false while the conflict holds.
func TestRetryReviewEscalationWaitsWhileHeadConflicts(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 3, 3)
	makeConflicting(g)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	assertEscalationHeld(t, s, r, g, n, "conflict")
}

// The escalation wait also holds while a required check under the trusted
// base policy is red.
func TestRetryReviewEscalationWaitsWhileChecksRed(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 3, 3)
	redChecks(g, github.Red)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	assertEscalationHeld(t, s, r, g, n, "ci_red")
}

// A wait whose audit row fails is not claimed: the flag and the state file
// stay as they were, and the next evaluation writes the row and sets the flag.
func TestRetryReviewWaitRetriesFailedAudit(t *testing.T) {
	s, g, _, r, _ := failedRetryHead(t, 2, 3)
	makeConflicting(g)
	ctx := context.Background()
	stateFile := filepath.Join(s.state(r.Name), "review-retry-7.json")
	before, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	// A directory under the journal's name makes its append fail.
	journal := filepath.Join(s.state(r.Name), "decisions.jsonl")
	if err := os.Mkdir(journal, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryReview(ctx, r, g.pull, Options{}); err != nil {
		t.Fatalf("poll with a failed audit: %v", err)
	}
	after, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("state file changed on a failed audit:\n%s\n%s", before, after)
	}
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if rows := waitingRows(t, s, r); len(rows) != 0 {
		t.Fatalf("waiting rows=%v after a failed audit, want none", rows)
	}
	if err := s.RetryReview(ctx, r, g.pull, Options{}); err != nil {
		t.Fatalf("poll after the journal recovers: %v", err)
	}
	st := retryStateOf(t, s, r)
	if !st.WaitConflict || st.Failures != 2 || st.Escalated {
		t.Fatalf("state=%+v, want the conflict wait set and the kept count", st)
	}
	rows := waitingRows(t, s, r)
	if len(rows) != 1 || rows[0]["reason"] != "conflict" || rows[0]["failures"] != float64(2) {
		t.Fatalf("waiting rows=%v, want one conflict row", rows)
	}
}

// A poll the GitHub client skips while the hold reads the head's review
// status is no review: the pass returns the skip, with no attempt, no
// escalation and no failure row, and the failure count stays as it was.
func TestRetryReviewSkippedPollInHoldIsWait(t *testing.T) {
	s, g, n, r, _ := failedRetryHead(t, 2, 3)
	makeConflicting(g)
	g.historyErr = github.ErrPollSkipped
	err := s.RetryReview(context.Background(), r, g.pull, Options{})
	if !errors.Is(err, github.ErrPollSkipped) {
		t.Fatalf("skipped poll in the hold: err=%v, want the skip", err)
	}
	if g.pullCalls != 1 {
		t.Fatalf("pull calls=%d, want the fresh read only: a review ran", g.pullCalls)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 2 || st.Escalated {
		t.Fatalf("state=%+v, want the kept count and no escalation", st)
	}
	if n.count != 0 || len(auditRows(t, s, r, "review-failed")) != 0 {
		t.Fatalf("failure or escalation on a skipped poll: %v", n.events)
	}
}

// A head with no counted failure whose review-history read is skipped: the
// pass returns the skip, no review ran, and the retry state file is unchanged.
func TestRetryReviewSkippedPollOnMainPathIsNoReview(t *testing.T) {
	s, g, _, n, r := reviewSetup(t)
	file := filepath.Join(s.state(r.Name), "review-retry-7.json")
	if err := fsutil.WriteJSON(file, reviewRetryState{Head: shaA}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	g.historyErr = github.ErrPollSkipped
	err = s.RetryReview(context.Background(), r, g.pull, Options{})
	if !errors.Is(err, github.ErrPollSkipped) {
		t.Fatalf("skipped poll on the main path: err=%v, want the skip", err)
	}
	after, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("state changed by a skipped poll:\n%s\n%s", before, after)
	}
	if n.count != 0 || len(auditRows(t, s, r, "review-failed")) != 0 {
		t.Fatalf("failure on a skipped poll: %v", n.events)
	}
}
