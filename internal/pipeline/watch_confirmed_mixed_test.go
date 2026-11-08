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
	st := recoveryIncident{
		RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaB, Baseline: shaA, Task: "42", Checks: "CI",
		Runs:      map[string][]bool{shaA: {true, true}, shaB: {false, false}},
		Confirmed: true,
	}
	if e := s.saveRecovery(r.Name, st); e != nil {
		t.Fatal(e)
	}
}

// headlineSuite is the red main of a head-culprit incident: the head is the
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

// (a) A confirmed incident whose culprit is the head decides a passing re-run
// once: the pass does not fail on the ambiguous read, the incident keeps its
// confirmation and holds as flaky instead of reverting, and a second pass does
// not re-record the decision.
func TestWatchConfirmedHeadCulpritRerunHolds(t *testing.T) {
	s, g, n, r, now := headCulpritRerun(t)

	got := recoveryWatch(t, s, r)
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
	if g.revertCount != 0 {
		t.Fatalf("repeat opened a revert: %d", g.revertCount)
	}
}

// (b) The re-run can land between the diagnosis and openRevert's fresh read.
// The revert is not opened on that mixed result either: the pass records the
// same decision instead of failing on the ambiguous read.
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
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	if g.headReads != 2 {
		t.Fatalf("head reads = %d, want the diagnosis and openRevert's fresh read", g.headReads)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("revert opened on a mixed result: reverts=%d merges=%d", g.revertCount, g.merges)
	}
	st, found := recoveryState(t, s, r)
	if !found || st.Flaky == "" || st.RevertPR != 0 || !st.Confirmed {
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
// confirmed deploy incident whose culprit is the deploy head is not stalled by
// the ambiguous read and opens no revert.
func TestWatchDeployConfirmedHeadCulpritRerunHolds(t *testing.T) {
	s, g, n, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	st.Head = shaB
	seedDeployIncident(t, s, r, st)
	g.head = shaB
	g.parents = map[string]string{shaB: shaA}
	g.statuses = map[[2]string]statusRecord{{shaB, "deploy-ok"}: {github.Green, now}}

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "passed a re-run after confirmation") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("deploy mixed result blamed: reverts=%d merges=%d", g.revertCount, g.merges)
	}
	after, found := readDeployIncident(t, s, r)
	if !found || after.Flaky == "" || !after.Confirmed || after.Culprit != shaB || after.RevertPR != 0 {
		t.Fatalf("deploy incident %+v found=%t", after, found)
	}
	if lines := designLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "deploy watch recovery-confirming") {
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
