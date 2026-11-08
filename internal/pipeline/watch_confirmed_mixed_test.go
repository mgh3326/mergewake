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

// headCulpritRerun is the red main of a head-culprit incident: the head is the
// culprit, its own run failed, and a re-run posted a passing status beside it.
func headCulpritRerun(t *testing.T) (*Service, *recoveryGH, *fakeNotifier, config.Repo) {
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
	return s, g, n, r
}

// (a) A confirmed incident whose culprit is the head does not fail the pass on
// the ambiguous read, keeps its confirmation and holds the automatic revert
// instead of reverting or re-deciding the verdict. The conflict is recorded
// once and the design lane hears it once.
func TestWatchConfirmedHeadConflictHolds(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, n, r := headCulpritRerun(t)

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "checks conflict") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("a conflicting read was acted on: reverts=%d merges=%d", g.revertCount, g.merges)
	}
	st, found := recoveryState(t, s, r)
	if !found || !st.Confirmed || st.Culprit != shaB || st.Baseline != shaA || st.Flaky != "" || st.RevertPR != 0 {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-conflict-head"); len(rows) != 1 {
		t.Fatalf("decision rows %v", rows)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "checks conflict") {
		t.Fatalf("design-lane lines: %v", lines)
	}

	now = now.Add(time.Minute)
	if got = recoveryWatch(t, s, r); got.Action != "recovery-hold" || !strings.Contains(got.Reason, "checks conflict") {
		t.Fatalf("repeat: %+v", got)
	}
	if rows := auditRows(t, s, r, "recovery-conflict-head"); len(rows) != 1 {
		t.Fatalf("decision recorded again: %v", rows)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("repeat told the design lane: %v", lines)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("repeat acted: reverts=%d merges=%d", g.revertCount, g.merges)
	}
}

// (b) The conflict can appear between the diagnosis and openRevert's fresh
// read. The revert is not opened on it either: the same hold and the same one
// record.
func TestWatchConfirmedHeadConflictBeforeRevertHolds(t *testing.T) {
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
	if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "checks conflict") {
		t.Fatalf("%+v", got)
	}
	if g.headReads != 2 {
		t.Fatalf("head reads = %d, want the diagnosis and openRevert's fresh read", g.headReads)
	}
	if g.revertCount != 0 || g.dispatched[shaB] != 0 {
		t.Fatalf("opened a revert or a re-run: reverts=%d dispatches=%v", g.revertCount, g.dispatched)
	}
	st, found := recoveryState(t, s, r)
	if !found || st.RevertPR != 0 || st.Flaky != "" || !st.Confirmed {
		t.Fatalf("incident %+v found=%t", st, found)
	}
	if rows := auditRows(t, s, r, "recovery-conflict-head"); len(rows) != 1 {
		t.Fatalf("decision rows %v", rows)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (c) The deploy stream takes the same decision for its own incident: a
// confirmed deploy incident whose culprit is the deploy head holds instead of
// failing the pass or opening a revert.
func TestWatchDeployConfirmedHeadConflictHolds(t *testing.T) {
	s, g, n, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	st.Head = shaB
	seedDeployIncident(t, s, r, st)
	g.head = shaB
	g.parents = map[string]string{shaB: shaA}
	g.statuses = map[[2]string]statusRecord{{shaB, "deploy-ok"}: {github.Green, now}}

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "checks conflict") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("deploy conflict acted on: reverts=%d merges=%d", g.revertCount, g.merges)
	}
	after, found := readDeployIncident(t, s, r)
	if !found || after.Flaky != "" || !after.Confirmed || after.Culprit != shaB || after.RevertPR != 0 {
		t.Fatalf("deploy incident %+v found=%t", after, found)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "deploy watch") {
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
	if rows := auditRows(t, s, r, "recovery-conflict-head"); len(rows) != 0 {
		t.Fatalf("confirmed-incident rule applied to an unconfirmed incident: %v", rows)
	}
}

// (e) A restart reads the recorded decision: the same hold, nothing recorded or
// told a second time.
func TestWatchConfirmedHeadConflictRestartKeepsDecision(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, n, r := headCulpritRerun(t)
	if got := recoveryWatch(t, s, r); got.Action != "recovery-hold" {
		t.Fatalf("%+v", got)
	}
	fresh := &Service{Config: s.Config, GH: g, Runner: s.Runner, Sink: s.Sink, Notifier: s.Notifier, Now: func() time.Time { return now.Add(time.Minute) }}
	got := recoveryWatch(t, fresh, r)
	if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "checks conflict") {
		t.Fatalf("restart: %+v", got)
	}
	if rows := auditRows(t, fresh, r, "recovery-conflict-head"); len(rows) != 1 {
		t.Fatalf("decision rows after restart %v", rows)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 {
		t.Fatalf("restart told the design lane again: %v", lines)
	}
	if g.revertCount != 0 {
		t.Fatalf("restart opened a revert: %d", g.revertCount)
	}
}

// (f) The hold ends by itself: once the head's checks read decisively again the
// next pass takes the normal recovery path, with no re-decision to undo.
func TestWatchConfirmedHeadConflictClearsWhenChecksAgree(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s, g, _, r := headCulpritRerun(t)
	if got := recoveryWatch(t, s, r); got.Action != "recovery-hold" {
		t.Fatalf("%+v", got)
	}
	g.statuses = map[[2]string]statusRecord{{shaB, "CI"}: {github.Red, now.Add(time.Minute)}}
	s.Now = func() time.Time { return now.Add(time.Minute) }
	got := recoveryWatch(t, s, r)
	if got.Action != "opened-revert-pr" || g.revertCount != 1 {
		t.Fatalf("recovery did not resume: %+v reverts=%d", got, g.revertCount)
	}
	if st, found := recoveryState(t, s, r); !found || st.Flaky != "" || st.RevertPR == 0 {
		t.Fatalf("incident %+v found=%t", st, found)
	}
}
