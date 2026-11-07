package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// expireLease makes the pass lease of the head old enough to take.
func expireLease(g *fakeGH) {
	if g.leaseTimes == nil {
		g.leaseTimes = map[string]time.Time{}
	}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
}

func retryStateOf(t *testing.T, s *Service, r config.Repo) reviewRetryState {
	t.Helper()
	var st reviewRetryState
	if ok, err := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), &st); err != nil || !ok {
		t.Fatalf("retry state: ok=%v err=%v", ok, err)
	}
	return st
}

func resultLostNotices(s *Service) (lost, failed int) {
	for _, e := range s.Notifier.(*fakeNotifier).events {
		switch {
		case strings.Contains(e.Text, "result was lost"):
			lost++
		case strings.Contains(e.Text, "review failed"):
			failed++
		}
	}
	return lost, failed
}

// quotaStopped is an interrupted reviewer whose hk job panewire then recorded
// as stopped on a quota pool.
func quotaStopped(t *testing.T, endReason string) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 3}
	runner.sink.missing = &integrations.JobRecord{State: "lost", EndReason: endReason, Host: "node-a", PaneID: "p1"}
	return s, g, r, runner, c
}

// (a) A quota-stopped reviewer is no lost result, spends no replacement and
// counts no failure; the pass runs again at the pool's reset.
func TestRetryQuotaStopWaitsForResetWithoutFailure(t *testing.T) {
	reset := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	s, g, r, runner, c := quotaStopped(t, "quota_stop pool=devin reset_at="+reset.Format(time.RFC3339))
	s.Config.Daemon.PR.ReviewRetry.Maximum = "24h" // above the wait, so the reset is not capped
	old := readReviewerJob(t, s, r, 1)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewQuotaStopped) {
		t.Fatalf("err=%v, want ErrReviewQuotaStopped", err)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 0 || st.Escalated || !st.Next.Equal(reset) {
		t.Fatalf("state=%+v, want no failure and next=%v", st, reset)
	}
	if lost, failed := resultLostNotices(s); lost != 0 || failed != 0 {
		t.Fatalf("notices lost=%d failed=%d: %v", lost, failed, s.Notifier.(*fakeNotifier).events)
	}
	if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 0 {
		t.Fatalf("result-lost audit: %v", rows)
	}
	rows := auditRows(t, s, r, "review-quota-stopped")
	if len(rows) != 1 || rows[0]["pr"] != float64(7) || rows[0]["head"] != shaA || rows[0]["pass"] != float64(1) || rows[0]["label"] != old.Label || rows[0]["pool"] != "devin" || rows[0]["reset_at"] != reset.Format(time.RFC3339) {
		t.Fatalf("quota audit: %v", rows)
	}
	reviewerJobGone(t, s, r, 1)
	if len(runner.spawns) != 1 || len(runner.closes) != 0 {
		t.Fatalf("spawns=%d closes=%v: the stopped reviewer must not be replaced or closed", len(runner.spawns), runner.closes)
	}
	// Before the reset nothing runs; at the reset a fresh reviewer is placed
	// with its one replacement unspent.
	c.t = reset.Add(-time.Second)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewQuotaStopped) || len(runner.spawns) != 1 {
		t.Fatalf("before the reset: err=%v spawns=%d", err, len(runner.spawns))
	}
	c.t = reset
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	runner.exits = []int{panewireWaitTimeout}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
		t.Fatal(err)
	}
	fresh := readReviewerJob(t, s, r, 1)
	if len(runner.spawns) != 2 || fresh.Label == old.Label || fresh.Replacements != 0 {
		t.Fatalf("spawns=%d fresh=%+v", len(runner.spawns), fresh)
	}
}

// (b) A quota stop without a known reset waits the initial backoff; so does a
// reset that is not in the future or does not parse.
func TestRetryQuotaStopWithoutResetWaitsInitial(t *testing.T) {
	for name, reason := range map[string]string{
		"no reset_at":   "quota_stop pool=devin",
		"unparseable":   "quota_stop pool=devin reset_at=soon",
		"already past":  "quota_stop pool=devin reset_at=2026-11-05T07:00:00Z",
		"extra tokens":  "quota_stop pool=devin note=x",
		"bare marker":   "quota_stop",
		"reset == now":  "quota_stop pool=devin reset_at=2026-11-05T08:00:00Z",
		"lost, no pool": "quota_stop reset_at=",
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r, runner, c := quotaStopped(t, reason)
			if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewQuotaStopped) {
				t.Fatalf("err=%v, want ErrReviewQuotaStopped", err)
			}
			st := retryStateOf(t, s, r)
			if st.Failures != 0 || !st.Next.Equal(c.t.Add(time.Minute)) || len(runner.spawns) != 1 {
				t.Fatalf("state=%+v spawns=%d, want next=%v", st, len(runner.spawns), c.t.Add(time.Minute))
			}
		})
	}
}

// (h) A reset far ahead (a weekly quota) is not waited out: the retry waits at
// most the maximum backoff, since panewire skips a pool it measures exhausted.
func TestRetryQuotaStopFarResetWaitsAtMostMaximum(t *testing.T) {
	s, g, r, runner, c := quotaStopped(t, "quota_stop pool=devin reset_at="+time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC).Format(time.RFC3339))
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewQuotaStopped) {
		t.Fatalf("err=%v, want ErrReviewQuotaStopped", err)
	}
	st := retryStateOf(t, s, r)
	if st.Failures != 0 || !st.Next.Equal(c.t.Add(30*time.Minute)) || len(runner.spawns) != 1 {
		t.Fatalf("state=%+v spawns=%d, want next=%v", st, len(runner.spawns), c.t.Add(30*time.Minute))
	}
}

// (c) The next attempt time is in the state file, so a restarted daemon
// keeps waiting for it.
func TestRetryQuotaStopSurvivesRestart(t *testing.T) {
	reset := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	s, g, r, runner, c := quotaStopped(t, "quota_stop pool=devin reset_at="+reset.Format(time.RFC3339))
	s.Config.Daemon.PR.ReviewRetry.Maximum = "24h" // above the wait, so the reset is not capped
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewQuotaStopped) {
		t.Fatalf("err=%v, want ErrReviewQuotaStopped", err)
	}
	restarted := *s
	c.t = reset.Add(-time.Minute)
	if err := restarted.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewQuotaStopped) || len(runner.spawns) != 1 {
		t.Fatalf("restarted before the reset: err=%v spawns=%d", err, len(runner.spawns))
	}
	if st := retryStateOf(t, &restarted, r); !st.Next.Equal(reset) {
		t.Fatalf("next=%v, want %v", st.Next, reset)
	}
	c.t = reset
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	runner.exits = []int{panewireWaitTimeout}
	err := restarted.RetryReview(context.Background(), r, g.pull, Options{})
	if (err != nil && !errors.Is(err, ErrReviewPending)) || len(runner.spawns) != 2 {
		t.Fatalf("restarted at the reset: err=%v spawns=%d", err, len(runner.spawns))
	}
}

// (d) A reviewer whose result mergewake cannot collect backs off but never
// escalates, however often it recurs.
func TestRetryCollectionFailuresBackOffWithoutEscalating(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, runner)
	s.Config.Notifier.Lane = "desk"
	c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "8m", Failures: 3}
	// The job completed with no result and nothing on disk: the collection fails.
	runner.sink.missing = &integrations.JobRecord{State: "completed"}
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 8 * time.Minute, 8 * time.Minute}
	for i, delay := range want {
		expireLease(g)
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
			t.Fatalf("failure %d: want the collection error", i+1)
		}
		st := retryStateOf(t, s, r)
		if st.Failures != 0 || st.Escalated || st.Backoffs != i+1 || !st.Next.Equal(c.t.Add(delay)) {
			t.Fatalf("failure %d: state=%+v, want backoff %v", i+1, st, delay)
		}
		c.t = st.Next
	}
	if _, failed := resultLostNotices(s); failed != 0 {
		t.Fatalf("escalated: %v", s.Notifier.(*fakeNotifier).events)
	}
}

// (e) Real review failures count and escalate as before.
func TestRetryRealFailuresStillEscalate(t *testing.T) {
	s, g, _, n, r := reviewSetup(t)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 3}
	g.pull.Body = "Mergewake-Author-Profile: unknown@v1"
	s.Config.Review.DefaultFamily = ""
	for i := 1; i <= 6; i++ {
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
			t.Fatalf("failure %d: want an error", i)
		}
		st := retryStateOf(t, s, r)
		if st.Failures != i || st.Backoffs != i || st.Escalated != (i >= 3) {
			t.Fatalf("failure %d: state=%+v", i, st)
		}
		c.t = st.Next
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "review failed 3 times") {
		t.Fatalf("want one escalation at 3: %v", n.events)
	}
}

// (f) A reviewer placed in place of an earlier attempt starts the head's retry
// state over, whether it replaces a lost pass or follows an expired lease.
func TestRetryPlacedReviewerResetsState(t *testing.T) {
	stale := func(c *clock, head string) reviewRetryState {
		return reviewRetryState{Head: head, Failures: 5, Escalated: true, Backoffs: 5, Next: c.t.Add(-time.Minute)}
	}
	check := func(t *testing.T, s *Service, r config.Repo) {
		t.Helper()
		if st := retryStateOf(t, s, r); st.Failures != 0 || st.Escalated || st.Head != shaA {
			t.Fatalf("state=%+v, want failures 0 and not escalated", st)
		}
	}
	t.Run("replaced lost pass", func(t *testing.T) {
		s, g, r, runner, c := interruptedReviewer(t)
		s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 3}
		old := readReviewerJob(t, s, r, 1)
		budgetPane(t, s, runner, old, "done")
		runner.exits = []int{panewireWaitTimeout}
		pastBudget(g, c)
		if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), stale(c, shaA)); err != nil {
			t.Fatal(err)
		}
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
			t.Fatal(err)
		}
		if fresh := readReviewerJob(t, s, r, 1); fresh.Label == old.Label || fresh.Replacements != 1 {
			t.Fatalf("fresh=%+v", fresh)
		}
		check(t, s, r)
	})
	t.Run("expired lease, no record", func(t *testing.T) {
		runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
		s, g, r := panewireSetup(t, runner)
		c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
		s.Now = c.now
		runner.sink.missing = &integrations.JobRecord{State: "claimed"}
		expireLease(g)
		if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), stale(c, shaA)); err != nil {
			t.Fatal(err)
		}
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
			t.Fatal(err)
		}
		if len(runner.spawns) != 1 {
			t.Fatalf("spawns=%d", len(runner.spawns))
		}
		check(t, s, r)
	})
	t.Run("refused spawn keeps the count", func(t *testing.T) {
		runner := &panewireRunner{family: "openai", refuse: map[string]int{"reviewer": panewireNoHost}}
		s, g, r := panewireSetup(t, runner)
		c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
		s.Now = c.now
		expireLease(g)
		old := stale(c, shaA)
		old.Escalated = false
		old.Failures, old.Backoffs = 4, 4
		if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), old); err != nil {
			t.Fatal(err)
		}
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
			t.Fatal("want the refused spawn to fail the review")
		}
		if st := retryStateOf(t, s, r); st.Failures != 5 {
			t.Fatalf("state=%+v, want a refused spawn counted", st)
		}
	})
}

// (g) A state file from before Backoffs loads, and its failures set the
// backoff step.
func TestRetryStateFileWithoutBackoffsLoads(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 9}
	g.pull.Body = "Mergewake-Author-Profile: unknown@v1"
	s.Config.Review.DefaultFamily = ""
	old := `{"head":"` + shaA + `","failures":2,"next":"2025-12-31T00:00:00Z","escalated":false}`
	if err := os.MkdirAll(s.state(r.Name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.state(r.Name), "review-retry-7.json"), []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("want an error")
	}
	if st := retryStateOf(t, s, r); st.Failures != 3 || st.Backoffs != 3 || !st.Next.Equal(c.t.Add(4*time.Minute)) {
		t.Fatalf("state=%+v", st)
	}
}

func TestParseQuotaStop(t *testing.T) {
	reset := time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC)
	for name, tc := range map[string]struct {
		job   integrations.JobRecord
		pool  string
		reset time.Time
		ok    bool
	}{
		"full":           {integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=devin reset_at=2026-11-06T01:02:03Z"}, "devin", reset, true},
		"no reset":       {integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=devin"}, "devin", time.Time{}, true},
		"other reason":   {integrations.JobRecord{State: "lost", EndReason: "pane closed"}, "", time.Time{}, false},
		"prefix only":    {integrations.JobRecord{State: "lost", EndReason: "quota_stopped pool=x"}, "", time.Time{}, false},
		"not lost":       {integrations.JobRecord{State: "completed", EndReason: "quota_stop pool=devin"}, "", time.Time{}, false},
		"no end reason":  {integrations.JobRecord{State: "lost"}, "", time.Time{}, false},
		"bad reset":      {integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=a reset_at=x"}, "a", time.Time{}, true},
		"decoded record": decodedQuotaRecord(),
	} {
		t.Run(name, func(t *testing.T) {
			pool, at, _, ok := parseQuotaStop(tc.job)
			if pool != tc.pool || !at.Equal(tc.reset) || ok != tc.ok {
				t.Fatalf("got %q %v %v", pool, at, ok)
			}
		})
	}
}

// decodedQuotaRecord is what hk's JSON becomes: end_reason is read.
func decodedQuotaRecord() (tc struct {
	job   integrations.JobRecord
	pool  string
	reset time.Time
	ok    bool
}) {
	_ = json.Unmarshal([]byte(`{"state":"lost","end_reason":"quota_stop pool=p reset_at=2026-11-06T01:02:03Z"}`), &tc.job)
	tc.pool, tc.reset, tc.ok = "p", time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC), true
	return tc
}

// The second loss of a reviewer (its hk job unreadable or its pane finished
// with no result, twice) is review-unavailable from mergewake's own
// collection: it backs off and counts no failure.
func TestRetrySecondLossIsCollectionFailure(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 1}
	runner.exits = []int{panewireWaitTimeout}
	budgetPane(t, s, runner, readReviewerJob(t, s, r, 1), "done")
	pastBudget(g, c)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
		t.Fatalf("replacement run: %v", err)
	}
	budgetPane(t, s, runner, readReviewerJob(t, s, r, 1), "done")
	pastBudget(g, c)
	st := retryStateOf(t, s, r)
	c.t = st.Next.Add(time.Hour)
	err := s.RetryReview(context.Background(), r, g.pull, Options{})
	if !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want review-unavailable", err)
	}
	st = retryStateOf(t, s, r)
	if st.Failures != 0 || st.Escalated || st.Backoffs != 1 || !st.Next.After(c.t) {
		t.Fatalf("state=%+v", st)
	}
	if _, failed := resultLostNotices(s); failed != 0 {
		t.Fatalf("escalated at threshold 1: %v", s.Notifier.(*fakeNotifier).events)
	}
}
