package pipeline

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// depG is the commit whose any-ref deploy run posts ci-ok-deploy: it never
// had a real deploy run, as 4b0a25a on 2026-11-12.
var depG = strings.Repeat("7", 40)

const anyRefDeployCheck = "ci-ok-deploy"

type revertCall struct {
	culprit string
	pr      int
}

// anyRefRevertGH records which commit and PR a revert is opened for.
type anyRefRevertGH struct {
	*deployTestGH
	reverts []revertCall
}

func (g *anyRefRevertGH) Revert(ctx context.Context, repo, branch, head, culprit string, pr int) (string, error) {
	g.reverts = append(g.reverts, revertCall{culprit, pr})
	return g.deployTestGH.Revert(ctx, repo, branch, head, culprit, pr)
}

func (g *anyRefRevertGH) RevertWithTrailers(ctx context.Context, repo, branch, head, culprit string, pr int, trailers string) (string, error) {
	g.reverts = append(g.reverts, revertCall{culprit, pr})
	return g.deployTestGH.RevertWithTrailers(ctx, repo, branch, head, culprit, pr, trailers)
}

// depEnv is the 48d47133 shape on the deploy tier: main head shaC is red on
// deploy-ok, depG and shaB never ran a deploy and only an any-ref deploy run
// can judge them, and shaA is the last deploy-ok checkpoint. shaB is the first
// bad commit (PR #7); the any-ref run posts ci-ok-deploy.
type depEnv struct {
	t   *testing.T
	s   *Service
	g   *anyRefRevertGH
	r   config.Repo
	now *time.Time
}

func newDepEnv(t *testing.T, field bool) *depEnv {
	t.Helper()
	s, base, _, r, start := deploySetup(t)
	base.policyExtra = "deploy_checks: [ci-ok, deploy-ok]\nany_ref_workflow: ci-any-ref.yml\n"
	if field {
		base.policyExtra += "any_ref_deploy_checks: [" + anyRefDeployCheck + "]\n"
	}
	base.parents = map[string]string{shaC: shaB, shaB: depG, depG: shaA}
	base.checkNames = map[string]map[string]github.CI{
		shaA:       {"CI": github.Green, "ci-ok": github.Green, "deploy-ok": github.Green},
		depG:       {"CI": github.Green},
		shaB:       {"CI": github.Green},
		shaC:       {"CI": github.Green, "ci-ok": github.Green, "deploy-ok": github.Red},
		revertHead: {"CI": github.Green},
	}
	base.entries[depG] = map[string]github.TreeEntry{"tool": blob("pre")}
	base.statuses = map[[2]string]statusRecord{}
	g := &anyRefRevertGH{deployTestGH: base}
	s.GH = g
	now := start
	s.Now = func() time.Time { return now }
	return &depEnv{t: t, s: s, g: g, r: r, now: &now}
}

func (e *depEnv) advance(d time.Duration) { *e.now = e.now.Add(d) }

// post records the any-ref deploy run's verdict for sha, newer than any
// dispatch made so far.
func (e *depEnv) post(sha string, ci github.CI) {
	e.g.statuses[[2]string{sha, anyRefDeployCheck}] = statusRecord{ci, e.now.Add(time.Second)}
}

func (e *depEnv) pass() watchResult {
	e.t.Helper()
	return recoveryWatch(e.t, e.s, e.r)
}

func (e *depEnv) dispatchedSHAs() []string {
	var out []string
	for _, in := range e.g.dispatchInputs {
		if in["suite"] != "deploy" {
			e.t.Fatalf("dispatch is not the deploy suite: %v", in)
		}
		out = append(out, in["ref"])
	}
	return out
}

// toRevert drives the incident until a revert PR opens: depG green and shaB red
// on the any-ref run, re-run for confirmation as the main stream does.
func (e *depEnv) toRevert() {
	e.t.Helper()
	first := e.pass()
	if first.Reason != "waiting for any-ref CI on "+short7(depG) {
		e.t.Fatalf("first pass = %+v", first)
	}
	e.advance(time.Minute)
	e.post(depG, github.Green)
	second := e.pass()
	if second.Reason != "waiting for any-ref CI on "+short7(shaB) {
		e.t.Fatalf("second pass = %+v (a green candidate must let the walk continue)", second)
	}
	for i := 0; i < 12 && len(e.g.reverts) == 0; i++ {
		e.advance(time.Minute)
		e.post(depG, github.Green)
		e.post(shaB, github.Red)
		got := e.pass()
		if strings.Contains(got.Reason, "timed out") {
			e.t.Fatalf("pass %d timed out: %+v", i, got)
		}
	}
	if len(e.g.reverts) == 0 {
		e.t.Fatal("no revert opened")
	}
}

// (a) A candidate whose any-ref deploy run posts ci-ok-deploy is judged by it:
// the walk continues, the first bad commit is found and exactly one revert
// opens, for that commit and its PR.
func TestWatchDeployAnyRefDeployChecksFindsFirstBadAndRevertsOnce(t *testing.T) {
	e := newDepEnv(t, true)
	e.toRevert()
	if len(e.g.reverts) != 1 || e.g.reverts[0] != (revertCall{shaB, 7}) || e.g.revertCount != 1 {
		t.Fatalf("reverts = %v count=%d", e.g.reverts, e.g.revertCount)
	}
	st, found := readDeployIncident(t, e.s, e.r)
	if !found || st.Culprit != shaB || st.CulpritPR != 7 || st.Baseline != depG || st.RevertPR != 9 || st.Checks != anyRefDeployCheck || !st.Confirmed {
		t.Fatalf("incident = %+v found=%v", st, found)
	}
	// Every dispatch ran the deploy suite on a bisect or confirmation commit.
	for _, sha := range e.dispatchedSHAs() {
		if sha != depG && sha != shaB {
			t.Fatalf("dispatched %s outside the bracket", sha)
		}
	}
}

// (d) Passes after the revert opened, and after it merged, open nothing new.
func TestWatchDeployAnyRefDeployChecksOpensNoSecondRevert(t *testing.T) {
	e := newDepEnv(t, true)
	e.toRevert()
	dispatches := len(e.g.dispatchInputs)
	for range 3 {
		e.advance(time.Minute)
		e.pass()
	}
	if len(e.g.reverts) != 1 || e.g.merges != 0 {
		t.Fatalf("after the revert opened: reverts=%v merges=%d", e.g.reverts, e.g.merges)
	}
	mergedAt := "2026-11-07T11:00:00Z"
	e.g.revert.Merged, e.g.revert.State, e.g.revert.MergedAt, e.g.revert.MergeCommitSHA = true, "closed", &mergedAt, advancedHead
	e.g.head = advancedHead
	e.g.parents[advancedHead] = shaC
	e.g.checkNames[advancedHead] = map[string]github.CI{"CI": github.Green, "deploy-ok": github.Pending}
	for range 3 {
		e.advance(time.Minute)
		e.pass()
	}
	if _, found := readDeployIncident(t, e.s, e.r); found || len(e.g.reverts) != 1 || len(e.g.dispatchInputs) != dispatches {
		t.Fatalf("after the merge: incident=%v reverts=%v dispatches=%d want %d", found, e.g.reverts, len(e.g.dispatchInputs), dispatches)
	}
}

// (b) Without the field nothing changes: the candidate's ci-ok-deploy is not
// read, so the hold waits for the timeout and no revert opens.
func TestWatchDeployWithoutAnyRefDeployChecksKeepsTimeoutHold(t *testing.T) {
	e := newDepEnv(t, false)
	if got := e.pass(); got.Reason != "waiting for any-ref CI on "+short7(depG) {
		t.Fatalf("first pass = %+v", got)
	}
	e.advance(time.Minute)
	e.post(depG, github.Green)
	if got := e.pass(); got.Reason != "waiting for any-ref CI on "+short7(depG) {
		t.Fatalf("a posted ci-ok-deploy must not be read without the field: %+v", got)
	}
	e.advance(46 * time.Minute)
	got := e.pass()
	if got.Reason != "any-ref CI timed out on "+short7(depG) || len(e.g.reverts) != 0 || len(e.g.dispatchInputs) != 1 {
		t.Fatalf("timeout pass = %+v reverts=%v dispatches=%d", got, e.g.reverts, len(e.g.dispatchInputs))
	}
	if _, found := readDeployIncident(t, e.s, e.r); found {
		t.Fatal("an incident was recorded on unjudged evidence")
	}
}

// (c) A pending ci-ok-deploy keeps the hold and dispatches nothing further; a
// red one marks the candidate bad.
func TestWatchDeployAnyRefDeployChecksPendingHoldsRedBlames(t *testing.T) {
	e := newDepEnv(t, true)
	e.pass()
	e.advance(time.Minute)
	e.post(depG, github.Pending)
	got := e.pass()
	if got.Reason != "waiting for any-ref CI on "+short7(depG) || len(e.g.dispatchInputs) != 1 || len(e.g.reverts) != 0 || got.Culprit != "" {
		t.Fatalf("pending pass = %+v dispatches=%d reverts=%v", got, len(e.g.dispatchInputs), e.g.reverts)
	}
	e.advance(time.Minute)
	e.post(depG, github.Red)
	got = e.pass()
	if got.Culprit != depG {
		t.Fatalf("red candidate pass = %+v, want culprit %s", got, short7(depG))
	}
}

// A commit whose own deploy run is decisive keeps that verdict with the field
// set: only a commit with no decisive deploy result is dispatched for.
func TestWatchDeployAnyRefDeployChecksKeepsRealDeployVerdict(t *testing.T) {
	e := newDepEnv(t, true)
	e.g.checkNames[shaB]["deploy-ok"] = github.Red
	e.post(depG, github.Green)
	e.pass()
	st, found := readDeployIncident(t, e.s, e.r)
	if !found || st.Culprit != shaB || st.Baseline != depG {
		t.Fatalf("incident = %+v found=%v", st, found)
	}
	var dispatch anyRefIncident
	if found, err := fsutil.ReadJSON(filepath.Join(e.s.state(e.r.Name), "any-ref-deploy-dispatch.json"), &dispatch); err != nil || found {
		t.Fatalf("bisect dispatched for a commit with a real deploy verdict: found=%v err=%v %+v", found, err, dispatch)
	}
}

// Adding the key while a deploy incident is recorded must hold the incident's
// confirmation, not let the new check set read an intervening commit as a fix
// and discard the recorded culprit.
func TestWatchDeployAnyRefDeployChecksAddedMidIncidentHolds(t *testing.T) {
	e := newDepEnv(t, true)
	depX := strings.Repeat("8", 40)
	old := confirmedDeploy(*e.now)
	old.Baseline, old.Checks = shaA, "ci-ok,deploy-ok"
	seedDeployIncident(t, e.s, e.r, old)
	e.g.head = advancedHead
	e.g.parents[advancedHead] = depX
	e.g.parents[depX] = shaC
	e.g.checkNames[advancedHead] = map[string]github.CI{"CI": github.Green, "ci-ok": github.Green, "deploy-ok": github.Red}
	e.g.checkNames[depX] = map[string]github.CI{"CI": github.Green}
	e.post(depX, github.Green)
	got := e.pass()
	st, found := readDeployIncident(t, e.s, e.r)
	if !found || st.Culprit != shaB || !st.Confirmed || len(e.g.reverts) != 0 || !strings.Contains(got.Reason, "the confirmation check set changed") {
		t.Fatalf("incident=%+v found=%v reverts=%v result=%+v", st, found, e.g.reverts, got)
	}
}
