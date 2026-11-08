package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// The fix window runs from the moment watch records the incident (red_since),
// so the time spent confirming counts against it. docs/config.md must say so.
func TestDocsFixWindowCountsFromIncidentRecording(t *testing.T) {
	b, e := os.ReadFile("../../docs/config.md")
	if e != nil {
		t.Fatal(e)
	}
	doc := strings.Join(strings.Fields(string(b)), " ")
	want := "The 30-minute fix window counts from the moment watch records the incident (`red_since` in `recovery.json`), not from confirmation"
	if !strings.Contains(doc, want) {
		t.Fatalf("docs/config.md does not state where the fix window starts: want %q", want)
	}
}

// runBoundGH is a client that learns the run id of each dispatch and reads a
// status only when it names that run, like the live client.
type runBoundGH struct {
	*recoveryGH
	nextRun     int64
	runOf       map[string]int64
	runStatuses map[runStatusKey]statusRecord
}

type runStatusKey struct {
	sha, name string
	run       int64
}

// DispatchWorkflow starts a run GitHub gives an id, whether or not the caller
// asks to learn it.
func (g *runBoundGH) DispatchWorkflow(ctx context.Context, repo, workflow, ref string, inputs map[string]string) error {
	_, e := g.DispatchWorkflowRun(ctx, repo, workflow, ref, inputs)
	return e
}

func (g *runBoundGH) DispatchWorkflowRun(ctx context.Context, repo, workflow, ref string, inputs map[string]string) (int64, error) {
	if e := g.recoveryGH.DispatchWorkflow(ctx, repo, workflow, ref, inputs); e != nil {
		return 0, e
	}
	g.nextRun++
	if g.runOf == nil {
		g.runOf = map[string]int64{}
	}
	g.runOf[inputs["ref"]] = g.nextRun
	return g.nextRun, nil
}

func (g *runBoundGH) LatestRunStatus(_ context.Context, _, sha, name string, run int64) (github.CI, time.Time, bool, error) {
	v, ok := g.runStatuses[runStatusKey{sha, name, run}]
	return v.ci, v.at, ok, nil
}

// A status that a run other than the dispatched one posted after the dispatch
// (a late result of an earlier dispatch, a manual run of the same sha) is not
// the re-run's result.
func TestConfirmationReadsOnlyTheDispatchedRunsStatus(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, l, _, _, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
	})
	g := &runBoundGH{recoveryGH: l, runStatuses: map[runStatusKey]statusRecord{}}
	s.GH = g
	s.Now = func() time.Time { return now }

	if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" {
		t.Fatalf("%+v", got)
	}
	runA, runB := g.runOf[shaA], g.runOf[shaB]
	if runA == 0 || runB == 0 {
		t.Fatalf("dispatched runs %v", g.runOf)
	}
	now = now.Add(time.Minute)
	// Another run's green status lands on the culprit after the dispatch.
	g.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Green, now}, {shaA, "CI"}: {github.Green, now}}
	got := recoveryWatch(t, s, r)
	if st, _ := recoveryState(t, s, r); st.Flaky != "" || st.Confirmed {
		t.Fatalf("a foreign run's status counted: %+v / %+v", got, st)
	}
	// The dispatched runs report: baseline passes, culprit fails.
	now = now.Add(time.Minute)
	g.runStatuses[runStatusKey{shaA, "CI", runA}] = statusRecord{github.Green, now}
	g.runStatuses[runStatusKey{shaB, "CI", runB}] = statusRecord{github.Red, now}
	if got = recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	if st, _ := recoveryState(t, s, r); !st.Confirmed || st.Flaky != "" {
		t.Fatalf("not confirmed: %+v", st)
	}
}

// A fix PR opened while the revert still waits out the initial fix window is
// reviewed first from the poll that sees it, not only once the window ends.
func TestFixPRIsUrgentDuringInitialWindow(t *testing.T) {
	s, g, _, _, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	g.openPulls = []github.Pull{fixPull(shaB)}

	now = now.Add(5 * time.Minute)
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-fix-window" || g.merges != 0 {
		t.Fatalf("%+v merges=%d", got, g.merges)
	}
	if p := s.ReviewPriority(context.Background(), r, fixPull(shaB)); p.Value != 1 || p.Source != "recovery-fix" {
		t.Fatalf("fix PR priority inside the window: %+v", p)
	}
}

// A failed write of recovery-urgent.json is retried by the next poll: the fix
// PR number was already recorded in recovery.json, which must not count as the
// priority having been published.
func TestUrgentMarkIsRetriedAfterWriteFailure(t *testing.T) {
	s, g, _, _, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	g.openPulls = []github.Pull{fixPull(shaB)}

	// A directory where the file belongs makes the write fail.
	if e := os.Mkdir(s.urgentFile(r.Name), 0o755); e != nil {
		t.Fatal(e)
	}
	now = now.Add(5 * time.Minute)
	if e := s.Watch(context.Background(), r, Options{}); e == nil {
		t.Fatal("the failed write was not reported")
	}
	if e := os.Remove(s.urgentFile(r.Name)); e != nil {
		t.Fatal(e)
	}

	now = now.Add(time.Minute)
	recoveryWatch(t, s, r)
	if p := s.ReviewPriority(context.Background(), r, fixPull(shaB)); p.Value != 1 || p.Source != "recovery-fix" {
		t.Fatalf("fix PR priority after the retry: %+v", p)
	}
}

// reopenFailSink is a sink whose task reopen is refused.
type reopenFailSink struct{ *fakeSink }

func (reopenFailSink) Reopen(context.Context, string, string) error {
	return errors.New("hk unreachable")
}

// The merged-revert notice says the task was reopened only when the reopen
// that happened when the revert PR opened succeeded; the outcome is recorded
// with the incident before the notice reads it.
func TestMergedNoticeReportsTheTaskReopenOutcome(t *testing.T) {
	s, g, sink, n, r := recoverySetup(t, nil)
	s.Sink = reopenFailSink{sink}
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	// The failed reopen is returned with the pass; the revert PR is open.
	_ = s.Watch(context.Background(), r, Options{})
	if g.revertCount != 1 {
		t.Fatalf("reverts %d", g.revertCount)
	}
	if st, _ := recoveryState(t, s, r); st.TaskReopen != "failed" {
		t.Fatalf("recorded reopen outcome %q", st.TaskReopen)
	}
	now = now.Add(fixWindow)
	if got := recoveryWatch(t, s, r); got.Action != "recovery-merged" {
		t.Fatalf("%+v", got)
	}
	lines := ownerLaneEvents(n)
	if len(lines) != 1 || strings.Contains(lines[0], "task 42 reopened") || !strings.Contains(lines[0], "task 42 reopen failed") {
		t.Fatalf("notice: %v", lines)
	}
}

// noRevertGH is a repository whose latch no revert PR can be proven to belong
// to: the latch was replaced, or the recovery branch was force-pushed.
type noRevertGH struct{ *recoveryGH }

func (noRevertGH) RecoveryRevert(context.Context, string) (*github.Pull, error) { return nil, nil }

// A green main with a latch no revert PR provably belongs to leaves the latch
// and the PR recovery.json names alone, for an operator.
func TestFixWonLeavesAnUnverifiedLatch(t *testing.T) {
	s, l, _, _, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[l.head] = github.Green
	})
	g := noRevertGH{l}
	s.GH = g
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	if e := s.saveRecovery(r.Name, recoveryIncident{RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaC, RevertPR: 9, Task: "42"}); e != nil {
		t.Fatal(e)
	}

	recoveryWatch(t, s, r)
	if len(l.closed) != 0 || len(l.comments) != 0 || l.clears != 0 || !l.latch || l.revert.State != "open" {
		t.Fatalf("an unverified latch was handled: closed=%v comments=%v clears=%d latch=%t", l.closed, l.comments, l.clears, l.latch)
	}
}

// Marking a fix PR urgent tells the daemon, which re-ranks the PR if its review
// already waits in the queue under an older priority.
func TestUrgentMarkTellsTheQueue(t *testing.T) {
	s, g, _, _, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	g.openPulls = []github.Pull{fixPull(shaB)}

	type told struct {
		repo string
		pr   int
	}
	var calls []told
	ctx := WithUrgentHook(context.Background(), func(repo string, pr int) { calls = append(calls, told{repo, pr}) })
	now = now.Add(5 * time.Minute)
	for range 2 {
		if e := s.Watch(ctx, r, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if len(calls) != 1 || calls[0] != (told{r.Name, 11}) {
		t.Fatalf("hook calls %v, want one for PR 11", calls)
	}
}

// A confirmation hold that needs the desk is reported once per incident, by
// the incident's key: the first poll, which records the incident, and the polls
// after it, which read the record, must agree on the key or one hold is
// announced twice.
func TestConfirmationHoldIsReportedOncePerIncident(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, _, _, n, r := recoverySetup(t, nil) // no any_ref_workflow: the hold needs the desk
	s.Now = func() time.Time { return now }
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "no any_ref_workflow") {
		t.Fatalf("first pass %+v", got)
	}
	now = now.Add(time.Minute)
	if got = recoveryWatch(t, s, r); got.Action != "recovery-confirming" {
		t.Fatalf("second pass %+v", got)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("one hold was reported %d times: %v", len(lines), lines)
	}
}
