package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

const heldBody = "hub refused spawn: job_already_claimed (held by request=req-9 job=%s)"

// heldRetrySetup is a panewire review whose reviewer spawns the hub refuses as
// held by holder, on a fake clock, with retry settings 1m, 30m, 3.
func heldRetrySetup(t *testing.T, holder, extra string) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock) {
	t.Helper()
	runner := &panewireRunner{family: "openai", refuse: map[string]int{"reviewer": panewireHeld}, refuseStderr: fmt.Sprintf(heldBody, holder) + extra}
	s, g, r := panewireSetup(t, runner)
	c := &clock{t: time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 3}
	s.Config.Reap.Enabled, s.Config.Reap.Act = true, true
	return s, g, r, runner, c
}

// retryOnce runs one RetryReview attempt with the pass lease long expired.
func retryOnce(t *testing.T, s *Service, g *fakeGH, r config.Repo) error {
	t.Helper()
	if g.leaseTimes == nil {
		g.leaseTimes = map[string]time.Time{}
	}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	return s.RetryReview(context.Background(), r, g.pull, Options{})
}

// stubSweep stands in for the reviewer sweep until the test ends.
func stubSweep(t *testing.T, fn func(context.Context, int) error) {
	t.Helper()
	old := sweepReviewers
	sweepReviewers = func(ctx context.Context, _ *Service, _ config.Repo, n int, _ Options) error { return fn(ctx, n) }
	t.Cleanup(func() { sweepReviewers = old })
}

func noNotices(t *testing.T, s *Service) {
	t.Helper()
	if ev := s.Notifier.(*fakeNotifier).events; len(ev) != 0 {
		t.Fatalf("notices: %v", ev)
	}
}

// otherPRHolder is a reviewer job of another PR: nothing here may close it.
const otherPRHolder = "mwr-8-1111111-p1-1700000000"

// (a) Held refusals in a row count no failure, no backoff and no escalation.
func TestRetryHeldRefusalsNeverFail(t *testing.T) {
	s, g, r, runner, _ := heldRetrySetup(t, otherPRHolder, "")
	for i := 1; i <= 4; i++ {
		if err := retryOnce(t, s, g, r); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		if st := retryStateOf(t, s, r); st.Failures != 0 || st.Backoffs != 0 || st.Escalated || len(runner.spawns) != i {
			t.Fatalf("attempt %d: state=%+v spawns=%d", i, st, len(runner.spawns))
		}
	}
	noNotices(t, s)
	if rows := auditRows(t, s, r, "reviewer-held"); len(rows) != 4 {
		t.Fatalf("each refusal is audited: %v", rows)
	}
}

// (b) A holder that is this PR's reviewer of the same pass at an older head:
// its pane is closed through the reviewer sweep and the spawn is retried in
// the same evaluation.
func TestRetryHeldByOlderHeadReviewerSweepsAndRetries(t *testing.T) {
	holder := "mwr-7-7e8272c-p1-1700000000"
	s, g, r, runner, _ := heldRetrySetup(t, holder, "")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: []byte(`{"findings":[]}`)}
	var swept []int
	stubSweep(t, func(_ context.Context, n int) error {
		swept = append(swept, n)
		runner.refuse = nil // the hub frees the role once the pane is gone
		return nil
	})
	if err := retryOnce(t, s, g, r); err != nil {
		t.Fatal(err)
	}
	if len(swept) != 1 || swept[0] != 7 || len(runner.spawns) != 2 || g.postCount != 1 {
		t.Fatalf("swept=%v spawns=%d posts=%d", swept, len(runner.spawns), g.postCount)
	}
	if st := retryStateOf(t, s, r); st != (reviewRetryState{Head: shaA}) {
		t.Fatalf("state=%+v, want cleared", st)
	}
	noNotices(t, s)
}

// A failed sweep is audited and leaves the evaluation held: no second spawn,
// no failure, and the sweep runs once per run.
func TestRetryHeldByOlderHeadSweepFailsLeavesHold(t *testing.T) {
	s, g, r, runner, _ := heldRetrySetup(t, "mwr-7-7e8272c-p1-1700000000", "")
	sweeps := 0
	stubSweep(t, func(context.Context, int) error { sweeps++; return fmt.Errorf("no hosts") })
	if err := retryOnce(t, s, g, r); err != nil {
		t.Fatal(err)
	}
	if sweeps != 1 || len(runner.spawns) != 1 {
		t.Fatalf("sweeps=%d spawns=%d", sweeps, len(runner.spawns))
	}
	if st := retryStateOf(t, s, r); st.Failures != 0 || st.Backoffs != 0 {
		t.Fatalf("state=%+v", st)
	}
	if rows := auditRows(t, s, r, "reviewer-held-sweep-failed"); len(rows) == 0 {
		t.Fatalf("sweep failure not audited")
	}
}

// Passes of one run share the sweep and its outcome.
func TestReapOlderHeadReviewersSharesOutcome(t *testing.T) {
	s, g, r, _, _ := heldRetrySetup(t, otherPRHolder, "")
	sweeps := 0
	boom := fmt.Errorf("hosts down")
	stubSweep(t, func(context.Context, int) error { sweeps++; return boom })
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, opts: Options{}}
	for i := 0; i < 3; i++ {
		if err := x.reapOlderHeadReviewers(context.Background()); err != boom {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if sweeps != 1 {
		t.Fatalf("sweeps=%d", sweeps)
	}
}

// Where Reap does not act (disabled, or observe-only) the sweep is not tried,
// no second spawn follows, and no failure row is written: the hold waits.
func TestRetryHeldSweepNeedsReapEnabledAndAct(t *testing.T) {
	for name, set := range map[string]func(*config.Reap){
		"disabled":     func(c *config.Reap) { c.Enabled = false },
		"observe-only": func(c *config.Reap) { c.Act = false },
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r, runner, _ := heldRetrySetup(t, "mwr-7-7e8272c-p1-1700000000", "")
			set(&s.Config.Reap)
			stubSweep(t, func(context.Context, int) error {
				t.Fatal("swept where reap cannot act")
				return nil
			})
			if err := retryOnce(t, s, g, r); err != nil {
				t.Fatal(err)
			}
			if st := retryStateOf(t, s, r); st.Failures != 0 || st.Backoffs != 0 || len(runner.spawns) != 1 {
				t.Fatalf("state=%+v spawns=%d", st, len(runner.spawns))
			}
			if rows := auditRows(t, s, r, "reviewer-held-sweep-failed"); len(rows) != 0 {
				t.Fatalf("misleading audit: %v", rows)
			}
		})
	}
}

// flipRunner runs panewireRunner and calls onHeld after a held refusal.
type flipRunner struct {
	*panewireRunner
	onHeld func()
}

func (f *flipRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	out, err := f.panewireRunner.Run(ctx, command, args, input, dir)
	if command == "panewire" && exitStatus(err) == panewireHeld {
		f.onHeld()
	}
	return out, err
}

// A held refusal whose older-head job cannot be re-closed (hk unreadable)
// stays a hold: no failure, no backoff, no escalation.
func TestRetryHeldCloseFailureStaysAHold(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld, panewireHeld}}
	s, g, r, _ := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	s.Runner = &flipRunner{runner, func() { runner.sink.jobErr = errors.New("hk unreachable") }}
	c := &clock{t: time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 1}
	if err := retryOnce(t, s, g, r); err != nil {
		t.Fatalf("err=%v: a held refusal must not fail the review", err)
	}
	if st := retryStateOf(t, s, r); st.Failures != 0 || st.Backoffs != 0 || st.Escalated {
		t.Fatalf("state=%+v", st)
	}
	noNotices(t, s)
}

// (c) A holder of another PR, another pass or the same head is not closed; the
// next attempt waits for retry_after_s.
func TestRetryHeldByOtherHolderWaitsRetryAfter(t *testing.T) {
	for name, holder := range map[string]string{
		"another PR":   otherPRHolder,
		"same head":    "mwr-7-" + shaA[:7] + "-p1-1700000000",
		"another pass": "mwr-7-7e8272c-p2-1700000000",
		"not reviewer": "mw-author-7",
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r, runner, c := heldRetrySetup(t, holder, " retry_after_s=240")
			stubSweep(t, func(context.Context, int) error {
				t.Fatal("swept for a holder that is not an older head of this pass")
				return nil
			})
			if err := retryOnce(t, s, g, r); err != nil {
				t.Fatal(err)
			}
			if st := retryStateOf(t, s, r); st.Failures != 0 || st.Backoffs != 0 || !st.Next.Equal(c.t.Add(240*time.Second)) || len(runner.spawns) != 1 {
				t.Fatalf("state=%+v spawns=%d", st, len(runner.spawns))
			}
			c.t = c.t.Add(239 * time.Second)
			if err := retryOnce(t, s, g, r); err != nil || len(runner.spawns) != 1 {
				t.Fatalf("inside the wait: err=%v spawns=%d", err, len(runner.spawns))
			}
			c.t = c.t.Add(time.Second)
			if err := retryOnce(t, s, g, r); err != nil || len(runner.spawns) != 2 {
				t.Fatalf("after the wait: err=%v spawns=%d", err, len(runner.spawns))
			}
		})
	}
}

// (d) Without retry_after_s the next evaluation retries: no wait, no backoff.
func TestRetryHeldWithoutRetryAfterRetriesNextEvaluation(t *testing.T) {
	s, g, r, runner, c := heldRetrySetup(t, otherPRHolder, "")
	for i := 1; i <= 3; i++ {
		if err := retryOnce(t, s, g, r); err != nil || len(runner.spawns) != i {
			t.Fatalf("evaluation %d: err=%v spawns=%d", i, err, len(runner.spawns))
		}
		if st := retryStateOf(t, s, r); !st.Next.IsZero() || st.Backoffs != 0 {
			t.Fatalf("state=%+v: a hold without retry_after_s must not wait", st)
		}
		c.t = c.t.Add(time.Second)
	}
}

// (c, d) retry_after_s forms and bounds.
func TestHeldRetryAfterOf(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"x retry_after_s=90":                      90 * time.Second,
		`409 {"error":"x","retry_after_s":240}`:   240 * time.Second,
		`{"retry_after_s": "15"}`:                 15 * time.Second,
		"retry_after_s=999999":                    24 * time.Hour,
		"retry_after_s=0":                         0,
		"job_already_claimed (held by request=a)": 0,
		"retry_after_s=99999999999999999999999":   0,
		"":                                        0,
	} {
		if got := heldRetryAfterOf(in); got != want {
			t.Errorf("heldRetryAfterOf(%q)=%v, want %v", in, got, want)
		}
	}
}

// (e) The next attempt time is in the state file: a restarted daemon keeps
// waiting for it.
func TestRetryHeldSurvivesRestart(t *testing.T) {
	s, g, r, runner, c := heldRetrySetup(t, otherPRHolder, " retry_after_s=600")
	if err := retryOnce(t, s, g, r); err != nil {
		t.Fatal(err)
	}
	refused := c.t
	restarted := *s
	c.t = refused.Add(599 * time.Second)
	if err := retryOnce(t, &restarted, g, r); err != nil || len(runner.spawns) != 1 {
		t.Fatalf("restarted inside the wait: err=%v spawns=%d", err, len(runner.spawns))
	}
	if st := retryStateOf(t, &restarted, r); !st.Next.Equal(refused.Add(600 * time.Second)) {
		t.Fatalf("next=%v, want %v", st.Next, refused.Add(600*time.Second))
	}
	c.t = refused.Add(600 * time.Second)
	if err := retryOnce(t, &restarted, g, r); err != nil || len(runner.spawns) != 2 {
		t.Fatalf("restarted after the wait: err=%v spawns=%d", err, len(runner.spawns))
	}
}

// A held refusal mixed with a real failure: only the real one counts.
func TestRetryHeldMixedWithRealFailure(t *testing.T) {
	s, g, r, runner, c := heldRetrySetup(t, otherPRHolder, "")
	if err := retryOnce(t, s, g, r); err != nil {
		t.Fatal(err)
	}
	held := runner.refuse
	runner.refuse = map[string]int{"reviewer": panewireRefused, "worker": panewireRefused}
	if err := retryOnce(t, s, g, r); err == nil {
		t.Fatal("a refused spawn is a real failure")
	}
	if st := retryStateOf(t, s, r); st.Failures != 1 || st.Backoffs != 1 || !st.Next.Equal(c.t.Add(time.Minute)) {
		t.Fatalf("after the real failure: %+v", st)
	}
	runner.refuse = held
	c.t = c.t.Add(time.Minute)
	if err := retryOnce(t, s, g, r); err != nil {
		t.Fatal(err)
	}
	if st := retryStateOf(t, s, r); st.Failures != 1 || st.Backoffs != 1 || st.Escalated {
		t.Fatalf("after the held refusal: %+v", st)
	}
	noNotices(t, s)
}
