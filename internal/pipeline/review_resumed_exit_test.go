package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A resumed pass that ends on any exit but the wait timeout is a real failure:
// the reviewer it re-attached to produced no result, so the retry backs off and
// the failure count can escalate. Only the wait timeout is pending.
func TestResumedReviewerRealExitCountsAsFailure(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 3}
	c.t = c.t.Add(2 * time.Second)
	runner.sink.missing = &integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1"}
	runner.exits = []int{1}
	err := s.RetryReview(context.Background(), r, g.pull, Options{})
	if err == nil || errors.Is(err, ErrReviewPending) || errors.Is(err, errPassPending) {
		t.Fatalf("err=%v, want a real review failure", err)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 1 || st.Backoffs != 1 || !st.Next.Equal(c.t.Add(time.Minute)) {
		t.Fatalf("state=%+v, want one failure and the initial backoff", st)
	}
	if len(runner.spawns) != 2 || g.postCount != 0 {
		t.Fatalf("spawns=%d posts=%d: the pass must re-attach once and post nothing", len(runner.spawns), g.postCount)
	}
}

// A resumed pass whose wait ends on the timeout is still pending: the job may
// run on, so nothing failed, backed off or escalated.
func TestResumedReviewerWaitTimeoutStaysPending(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 3}
	c.t = c.t.Add(2 * time.Second)
	runner.sink.missing = &integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1"}
	runner.exits = []int{panewireWaitTimeout}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewPending) {
		t.Fatalf("err=%v, want ErrReviewPending", err)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 0 || st.Backoffs != 0 || !st.Next.IsZero() {
		t.Fatalf("state=%+v, want no failure or backoff", st)
	}
	if len(runner.spawns) != 2 || g.postCount != 0 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
}

// A fresh pass keeps its exit classes: any exit but the wait timeout is a real
// failure, and the wait timeout is pending.
func TestFreshReviewerExitClassesUnchanged(t *testing.T) {
	for name, tc := range map[string]struct {
		exits   []int
		pending bool
	}{
		"non-timeout": {exits: []int{1}},
		"wait":        {exits: []int{panewireWaitTimeout}, pending: true},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai", exits: tc.exits}
			s, g, r := panewireSetup(t, runner)
			s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 3}
			c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
			s.Now = c.now
			err := s.RetryReview(context.Background(), r, g.pull, Options{})
			if tc.pending {
				if !errors.Is(err, ErrReviewPending) {
					t.Fatalf("err=%v, want ErrReviewPending", err)
				}
				if st := retryStateOf(t, s, r); st.Failures != 0 || st.Backoffs != 0 {
					t.Fatalf("state=%+v, want no failure or backoff", st)
				}
				return
			}
			if err == nil || errors.Is(err, ErrReviewPending) {
				t.Fatalf("err=%v, want a real review failure", err)
			}
			st := retryStateOf(t, s, r)
			if st.Failures != 1 || st.Backoffs != 1 || !st.Next.Equal(c.t.Add(time.Minute)) {
				t.Fatalf("state=%+v, want one failure and the initial backoff", st)
			}
			if len(runner.spawns) != 1 || g.postCount != 0 {
				t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
			}
		})
	}
}
