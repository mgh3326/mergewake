package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// longReviewerRunner is a panewire whose reviewer runs until released: a spawn
// that waits (--wait) does not return before then, one that only places the
// reviewer returns at once.
type longReviewerRunner struct {
	*panewireRunner
	release chan struct{}
}

func (r *longReviewerRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" && slices.Contains(args, "--wait") {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

// revisits records what the pass asked the queue for.
type revisits struct{ got []revisit }

type revisit struct {
	pr    int
	label string
	at    time.Time
}

func (v *revisits) fn(_ string, pr int, label string, at time.Time) {
	v.got = append(v.got, revisit{pr, label, at})
}

type asyncFixture struct {
	s      *Service
	g      *fakeGH
	r      config.Repo
	runner *longReviewerRunner
	c      *clock
	v      *revisits
	out    *bytes.Buffer
	ctx    context.Context
}

// asyncReview is a review of PR 7 under a context that carries a revisit, with
// a 25 minute review.timeout and a reviewer that has not finished.
func asyncReview(t *testing.T) *asyncFixture {
	t.Helper()
	inner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, inner)
	runner := &longReviewerRunner{panewireRunner: inner, release: make(chan struct{})}
	t.Cleanup(func() { close(runner.release) })
	s.Runner = runner
	s.Config.Notifier.Lane = "desk"
	s.Config.Review.Timeout = "25m"
	c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	out := &bytes.Buffer{}
	s.Out = out
	v := &revisits{}
	inner.sink.missing = &integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"}
	return &asyncFixture{s, g, r, runner, c, v, out, WithRevisit(context.Background(), v.fn)}
}

// review runs one evaluation of PR 7 and fails if it waits for the reviewer.
func (f *asyncFixture) review(t *testing.T) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.s.Review(f.ctx, f.r, 7, "", Options{}) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the review waited for its reviewer")
		return nil
	}
}

func (f *asyncFixture) finish(label string) {
	f.runner.sink.jobs[label] = integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", Result: json.RawMessage(`{"findings":[]}`)}
}

// (a) A fresh spawn returns pending at once: the item ends while the reviewer
// runs, the reviewer's record and checkout stay, and a timer is asked for.
func TestAsyncSpawnReturnsPendingAtOnce(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(f.runner.spawns) != 1 || slices.Contains(f.runner.spawns[0], "--wait") || slices.Contains(f.runner.spawns[0], "--wait-timeout") {
		t.Fatalf("spawns %v", f.runner.spawns)
	}
	rj := readReviewerJob(t, f.s, f.r, 1)
	if rj.Label != arg(f.runner.spawns[0], "-l") || !slices.Contains(rj.Args, "--wait") || !strings.Contains(rj.Spawned, "OK pane=p1") {
		t.Fatalf("record %+v", rj)
	}
	if f.g.postCount != 0 || f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("posts=%d", f.g.postCount)
	}
	if slices.ContainsFunc(gitCalls(f.runner.panewireRunner, f.r.Root), func(c string) bool { return strings.HasPrefix(c, "worktree remove") }) {
		t.Fatal("the checkout of a running reviewer was removed")
	}
	if want := []revisit{{7, rj.Label, rj.Recorded.Add(25*time.Minute + revisitMargin)}}; !slices.Equal(f.v.got, want) {
		t.Fatalf("revisits %v, want %v", f.v.got, want)
	}
	if !strings.Contains(f.out.String(), `"state":"started"`) || !strings.Contains(f.out.String(), `"reviewer":"`+rj.Label+`"`) {
		t.Fatalf("output %s", f.out)
	}
}

// (b) The reviewer-finished event is an ordinary evaluation: the resumed pass
// collects the result, posts and settles, without another spawn.
func TestAsyncReviewerFinishedEventCollectsAndSettles(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	label := readReviewerJob(t, f.s, f.r, 1).Label
	f.c.t = f.c.t.Add(7 * time.Minute)
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	f.finish(label)
	f.out.Reset()
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(f.runner.spawns) != 1 || f.g.postCount != 1 || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d posts=%d", len(f.runner.spawns), f.g.postCount)
	}
	reviewerJobGone(t, f.s, f.r, 1)
	if !strings.Contains(f.out.String(), `"state":"collected"`) || !strings.Contains(f.out.String(), `"elapsed_ms":420000`) {
		t.Fatalf("output %s", f.out)
	}
	// The pane is closed from the placement the spawn reported.
	if !slices.ContainsFunc(gitCalls(f.runner.panewireRunner, f.r.Root), func(c string) bool { return strings.HasPrefix(c, "worktree remove") }) {
		t.Fatal("the finished reviewer's checkout was kept")
	}
}

// (c) A reviewer that never finishes is handled when its timer fires: the
// resumed pass sees the record past review.timeout and replaces it.
func TestAsyncTimeoutTimerReplacesLostReviewer(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	timer := f.v.got[0].at
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	// Before the timer the record is inside its budget: still pending.
	f.c.t = old.Recorded.Add(24 * time.Minute)
	if err := f.review(t); err != nil || len(f.runner.spawns) != 1 {
		t.Fatalf("err=%v spawns=%d", err, len(f.runner.spawns))
	}
	f.c.t = timer
	f.out.Reset()
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(auditRows(t, f.s, f.r, "review-result-lost")) != 1 || len(f.runner.spawns) != 2 || arg(f.runner.spawns[1], "-l") == old.Label {
		t.Fatalf("lost rows=%d spawns=%v", len(auditRows(t, f.s, f.r, "review-result-lost")), f.runner.spawns)
	}
	fresh := readReviewerJob(t, f.s, f.r, 1)
	if fresh.Label == old.Label || fresh.Replacements != 1 || !f.v.got[len(f.v.got)-1].at.After(timer) {
		t.Fatalf("record %+v revisits %v", fresh, f.v.got)
	}
	if !strings.Contains(f.out.String(), `"state":"lost"`) {
		t.Fatalf("output %s", f.out)
	}
}

// (e) Without a revisit (a manual review, or a daemon that cannot read hk job
// records) the pass waits a bounded time, so a reviewer that finishes within it
// is collected in the same run.
func TestWithoutRevisitBoundedWaitCollectsInSameRun(t *testing.T) {
	inner := &panewireRunner{family: "openai", findings: `{"findings":[]}`}
	s, g, r := panewireSetup(t, inner)
	s.Config.Review.Timeout = "25m"
	inner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	sp := inner.spawns[0]
	if !slices.Contains(sp, "--wait") || arg(sp, "--wait-timeout") != "30" || len(inner.spawns) != 1 || g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%v posts=%d", inner.spawns, g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
}

// (f) A resumed record whose reviewer still runs returns pending without a
// spawn and without waiting, and asks for the timer again.
func TestAsyncResumedRunningReturnsPendingWithoutWaiting(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	f.c.t = f.c.t.Add(3 * time.Minute)
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	f.out.Reset()
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(f.runner.spawns) != 1 || f.g.postCount != 0 || readReviewerJob(t, f.s, f.r, 1).Label != old.Label {
		t.Fatalf("spawns=%d posts=%d", len(f.runner.spawns), f.g.postCount)
	}
	if len(f.v.got) != 2 || f.v.got[1] != f.v.got[0] || !strings.Contains(f.out.String(), `"state":"pending"`) {
		t.Fatalf("revisits %v output %s", f.v.got, f.out)
	}
}

// A record with no job behind it, as when the daemon stopped between recording
// and spawning, replays its spawn once, still without waiting.
func TestAsyncResumedWithoutJobReplaysSpawnWithoutWaiting(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	f.runner.sink.missing = nil
	f.c.t = f.c.t.Add(time.Minute)
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(f.runner.spawns) != 2 || slices.Contains(f.runner.spawns[1], "--wait") || arg(f.runner.spawns[1], "-l") != arg(f.runner.spawns[0], "-l") {
		t.Fatalf("spawns %v", f.runner.spawns)
	}
}
