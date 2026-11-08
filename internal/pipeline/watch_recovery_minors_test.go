package pipeline

import (
	"context"
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
