package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// Check answers the named sha's first reads as readResult and every read from
// ambiguousFrom on as the same-sha check/status disagreement a re-run that
// passed beside the head's own failed run leaves behind. It lets one pass see a
// re-run land between its diagnosis and a later fresh read.
func (g *recoveryGH) Check(ctx context.Context, repo, sha string, names []string) (github.CI, error) {
	if g.ambiguousFrom > 0 && sha == g.ambiguousSHA {
		g.headReads++
		if g.headReads >= g.ambiguousFrom {
			return github.Pending, fmt.Errorf("%w named %q", github.ErrAmbiguousCheck, "CI")
		}
		return g.readResult, nil
	}
	return g.fakeGH.Check(ctx, repo, sha, names)
}

// seedHeadConfirmed records the 2/2 evidence an earlier confirmation produced
// for an incident whose culprit is also the head.
func seedHeadConfirmed(t *testing.T, s *Service, r config.Repo, now time.Time) {
	t.Helper()
	seedConfirmedChecks(t, s, r, now, "CI")
}

// seedConfirmedChecks is seedHeadConfirmed for an incident confirmed under the
// named check set.
func seedConfirmedChecks(t *testing.T, s *Service, r config.Repo, now time.Time, checks string) {
	t.Helper()
	st := recoveryIncident{
		RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaB, Baseline: shaA, Task: "42", Checks: checks,
		Runs:      map[string][]bool{shaA: {true, true}, shaB: {false, false}},
		Confirmed: true,
	}
	if e := s.saveRecovery(r.Name, st); e != nil {
		t.Fatal(e)
	}
}

// headCulpritRerun is the red main of a head-culprit incident: the head is the
// culprit, its own run failed, and a re-run posted a passing status beside it.
func headCulpritRerun(t *testing.T) (*Service, *recoveryGH, *fakeNotifier, config.Repo, time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
		l.head = shaB
		l.parents = map[string]string{shaB: shaA}
		l.pull.MergeCommitSHA = shaB
		l.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Green, now}}
	})
	s.Now = func() time.Time { return now }
	seedHeadConfirmed(t, s, r, now)
	return s, g, n, r, now
}

// completeRerun posts the statuses a complete re-run of the named checks leaves
// at the culprit one minute after the dispatch, and returns that time.
func completeRerun(g *recoveryGH, now time.Time, state github.CI, names ...string) time.Time {
	at := now.Add(time.Minute)
	g.statuses = map[[2]string]statusRecord{}
	for _, name := range names {
		g.statuses[[2]string{shaB, name}] = statusRecord{state, at}
	}
	return at
}

// (a) A confirmed incident whose culprit is the head keeps its confirmation
// when a passing re-run lands: the pass does not fail on the ambiguous read,
// the conflict starts one complete any-ref run instead of reading the statuses
// already on the head, and only that run's result marks the incident flaky and
// stops the automatic revert - once.
func TestWatchConfirmedHeadCulpritRerunHolds(t *testing.T) {
	s, g, n, r, now := headCulpritRerun(t)

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "re-running the culprit") {
		t.Fatalf("%+v", got)
	}
	if g.dispatched[shaB] != 1 || g.revertCount != 0 {
		t.Fatalf("dispatches=%v reverts=%d", g.dispatched, g.revertCount)
	}
	if st, found := recoveryState(t, s, r); !found || st.Flaky != "" || !st.Confirmed {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if lines := designLaneEvents(n); len(lines) != 0 {
		t.Fatalf("told the design lane before the run: %v", lines)
	}

	now = completeRerun(g, now, github.Green, "CI")
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("a mixed result was blamed: reverts=%d merges=%d", g.revertCount, g.merges)
	}
	st, found := recoveryState(t, s, r)
	if !found || !st.Confirmed || st.Culprit != shaB || st.Flaky == "" || st.RevertPR != 0 {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 1 {
		t.Fatalf("decision rows %v", rows)
	}
	if lines := designLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "passed a re-run after confirmation") {
		t.Fatalf("design-lane lines: %v", lines)
	}

	now = now.Add(time.Minute)
	if got = recoveryWatch(t, s, r); got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("repeat: %+v", got)
	}
	if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 1 {
		t.Fatalf("decision recorded again: %v", rows)
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("repeat told the design lane: %v", lines)
	}
	if g.revertCount != 0 || g.dispatched[shaB] != 1 {
		t.Fatalf("repeat acted: reverts=%d dispatches=%v", g.revertCount, g.dispatched)
	}
}

// (b) The conflict can appear between the diagnosis and openRevert's fresh
// read. The revert is not opened on it either: the same complete run is
// started, and only its passing result is the one decision.
func TestWatchConfirmedHeadRerunBeforeRevertHolds(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
		l.head = shaB
		l.parents = map[string]string{shaB: shaA}
		l.pull.MergeCommitSHA = shaB
		l.ambiguousSHA, l.readResult, l.ambiguousFrom = shaB, github.Red, 2
	})
	s.Now = func() time.Time { return now }
	seedHeadConfirmed(t, s, r, now)

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "re-running the culprit") {
		t.Fatalf("%+v", got)
	}
	if g.headReads != 2 {
		t.Fatalf("head reads = %d, want the diagnosis and openRevert's fresh read", g.headReads)
	}
	if g.revertCount != 0 || g.dispatched[shaB] != 1 {
		t.Fatalf("reverts=%d dispatches=%v", g.revertCount, g.dispatched)
	}
	if st, found := recoveryState(t, s, r); !found || st.RevertPR != 0 || st.Flaky != "" {
		t.Fatalf("incident %+v found=%t", st, found)
	}

	now = completeRerun(g, now, github.Green, "CI")
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 {
		t.Fatalf("opened a revert on a mixed result: %d", g.revertCount)
	}
	if st, found := recoveryState(t, s, r); !found || st.Flaky == "" || st.RevertPR != 0 {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 1 {
		t.Fatalf("decision rows %v", rows)
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (c) The deploy stream takes the same decision for its own incident: a
// confirmed deploy incident whose culprit is the deploy head starts a deploy
// re-run instead of failing the pass or opening a revert.
func TestWatchDeployConfirmedHeadCulpritRerunHolds(t *testing.T) {
	s, g, n, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	st.Head = shaB
	seedDeployIncident(t, s, r, st)
	g.head = shaB
	g.parents = map[string]string{shaB: shaA}
	g.statuses = map[[2]string]statusRecord{{shaB, "deploy-ok"}: {github.Green, now}}

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "re-running the culprit") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("deploy mixed result blamed: reverts=%d merges=%d", g.revertCount, g.merges)
	}
	dispatched := 0
	for _, input := range g.dispatchInputs {
		if input["ref"] == shaB && input["suite"] == "deploy" {
			dispatched++
		}
	}
	if dispatched != 1 {
		t.Fatalf("dispatches=%v", g.dispatchInputs)
	}
	if after, found := readDeployIncident(t, s, r); !found || after.Flaky != "" || !after.Confirmed {
		t.Fatalf("deploy incident %+v found=%t", after, found)
	}

	completeRerun(g.recoveryGH, now, github.Green, "deploy-ok")
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	after, found := readDeployIncident(t, s, r)
	if !found || after.Flaky == "" || !after.Confirmed || after.Culprit != shaB || after.RevertPR != 0 {
		t.Fatalf("deploy incident %+v found=%t", after, found)
	}
	if g.revertCount != 0 {
		t.Fatalf("opened a deploy revert on a mixed result: %d", g.revertCount)
	}
	if lines := designLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "deploy watch") {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (d) An unconfirmed incident is unchanged: a mixed head is still resolved by
// the recorded evidence (#214), never by the confirmed-incident rule.
func TestWatchUnconfirmedHeadCulpritMixedUnchanged(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, _, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
		l.head = shaB
		l.parents = map[string]string{shaB: shaA}
		l.pull.MergeCommitSHA = shaB
		l.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Green, now.Add(-time.Minute)}}
	})
	s.Now = func() time.Time { return now }
	if e := s.saveRecovery(r.Name, recoveryIncident{
		RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaB, Baseline: shaA, Task: "42", Checks: "CI",
		Runs: map[string][]bool{shaA: {true}, shaB: {false}},
	}); e != nil {
		t.Fatal(e)
	}

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "confirming the failure") {
		t.Fatalf("%+v", got)
	}
	if g.dispatched[shaA] != 1 || g.dispatched[shaB] != 1 {
		t.Fatalf("dispatches %v", g.dispatched)
	}
	st, found := recoveryState(t, s, r)
	if !found || st.Flaky != "" || st.Confirmed {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 0 {
		t.Fatalf("confirmed-incident rule applied to an unconfirmed incident: %v", rows)
	}
}

// (e) A restart reads the recorded decision: the incident stays flaky, the
// pass holds, and nothing is recorded or told a second time.
func TestWatchConfirmedHeadRerunRestartKeepsDecision(t *testing.T) {
	s, g, n, r, now := headCulpritRerun(t)
	if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" {
		t.Fatalf("%+v", got)
	}
	now = completeRerun(g, now, github.Green, "CI")
	if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" {
		t.Fatalf("%+v", got)
	}
	fresh := &Service{Config: s.Config, GH: g, Runner: s.Runner, Sink: s.Sink, Notifier: s.Notifier, Now: func() time.Time { return now.Add(time.Minute) }}
	got := recoveryWatch(t, fresh, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("restart: %+v", got)
	}
	if rows := auditRows(t, fresh, r, "recovery-mixed-head"); len(rows) != 1 {
		t.Fatalf("decision rows after restart %v", rows)
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("restart told the design lane again: %v", lines)
	}
	if g.revertCount != 0 {
		t.Fatalf("restart opened a revert: %d", g.revertCount)
	}
}

// (f) The recorded decision also stops a revert that is already open: an
// incident holding the mixed result does not merge it when a later pass reads
// the head red again.
func TestWatchConfirmedHeadRerunStopsRevertMerge(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.head = shaB
		l.parents = map[string]string{shaB: shaA}
	})
	s.Now = func() time.Time { return now }
	st := recoveryIncident{
		RedSince: now.Add(-fixWindow), Culprit: shaB, CulpritPR: 7, Head: shaB, Baseline: shaA, Task: "42",
		Checks: "CI", Runs: map[string][]bool{shaA: {true, true}, shaB: {false, false}},
		Confirmed: true, Flaky: "the culprit passed a re-run after confirmation", RevertPR: 9,
	}
	if e := s.saveRecovery(r.Name, st); e != nil {
		t.Fatal(e)
	}

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	if g.merges != 0 {
		t.Fatalf("merged the revert of a flaky incident: %d", g.merges)
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (g) The statuses already on the head mix results from different runs, so none
// of them is a complete re-run: statuses that predate the dispatch, and a run
// that posted only one of its checks, leave the incident unresolved rather than
// marked flaky. A run that fails makes the head plainly red again, so the next
// pass takes the normal recovery path.
func TestWatchConfirmedHeadPartialRerunHolds(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	for name, statuses := range map[string]map[[2]string]statusRecord{
		"statuses predate the run": {
			{shaB, "CI"}:   {github.Green, now},
			{shaB, "LINT"}: {github.Green, now},
		},
		"one check of the run": {
			{shaB, "CI"}:   {github.Green, now},
			{shaB, "LINT"}: {github.Green, now.Add(time.Minute)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
				l.policyExtra = "any_ref_workflow: ci-any-ref.yml\nwatch_checks: [CI, LINT]\nany_ref_checks: [CI, LINT]\n"
				l.head = shaB
				l.parents = map[string]string{shaB: shaA}
				l.pull.MergeCommitSHA = shaB
				l.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Green, now}}
			})
			s.Now = func() time.Time { return now }
			seedConfirmedChecks(t, s, r, now, "CI,LINT")
			if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" {
				t.Fatalf("%+v", got)
			}

			g.statuses = statuses
			s.Now = func() time.Time { return now.Add(time.Minute) }
			got := recoveryWatch(t, s, r)
			if !strings.Contains(got.Reason, "waiting for a complete re-run") {
				t.Fatalf("%+v", got)
			}
			st, found := recoveryState(t, s, r)
			if !found || st.Flaky != "" || !st.Confirmed {
				t.Fatalf("incident %+v found=%t", st, found)
			}
			if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 0 {
				t.Fatalf("marked the incident flaky without one complete run: %v", rows)
			}
			if lines := designLaneEvents(n); len(lines) != 0 {
				t.Fatalf("design-lane lines: %v", lines)
			}
			if g.revertCount != 0 {
				t.Fatalf("opened a revert without a complete pass: %d", g.revertCount)
			}
		})
	}
}

// (g-bis) A re-run that fails leaves the head's own run and its newest status
// agreeing on red, so the next pass recovers normally instead of holding.
func TestWatchConfirmedHeadFailedRerunResumesRecovery(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
		l.head = shaB
		l.parents = map[string]string{shaB: shaA}
		l.pull.MergeCommitSHA = shaB
		l.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Green, now}}
	})
	s.Now = func() time.Time { return now }
	seedHeadConfirmed(t, s, r, now)

	if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" || g.dispatched[shaB] != 1 {
		t.Fatalf("%+v dispatches=%v", got, g.dispatched)
	}
	now = completeRerun(g, now, github.Red, "CI")
	got := recoveryWatch(t, s, r)
	if got.Action != "opened-revert-pr" || g.revertCount != 1 {
		t.Fatalf("recovery did not resume: %+v reverts=%d", got, g.revertCount)
	}
	if st, found := recoveryState(t, s, r); !found || st.Flaky != "" {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 0 {
		t.Fatalf("a failed re-run was recorded as a mixed result: %v", rows)
	}
	if lines := designLaneEvents(n); len(lines) != 0 {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (h) A confirmation under several checks is a mixed result only when one
// complete run passes the whole set.
func TestWatchConfirmedHeadFullCheckSetRerunHolds(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\nwatch_checks: [CI, LINT]\nany_ref_checks: [CI, LINT]\n"
		l.head = shaB
		l.parents = map[string]string{shaB: shaA}
		l.pull.MergeCommitSHA = shaB
		l.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Green, now}}
	})
	s.Now = func() time.Time { return now }
	seedConfirmedChecks(t, s, r, now, "CI,LINT")

	if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" {
		t.Fatalf("%+v", got)
	}
	now = completeRerun(g, now, github.Green, "CI", "LINT")
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 {
		t.Fatalf("opened a revert on a mixed result: %d", g.revertCount)
	}
	if st, found := recoveryState(t, s, r); !found || st.Flaky == "" {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-mixed-head"); len(rows) != 1 {
		t.Fatalf("decision rows %v", rows)
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines: %v", lines)
	}
}
