package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestWatchDeployCheckpoint(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = "deploy_checks: [deploy-ok]\n"
	g.head = shaB
	g.parents = map[string]string{shaB: shaA}
	g.checkNames = map[string]map[string]github.CI{shaB: {"CI": github.Green, "deploy-ok": github.Green}}
	if err := s.Watch(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(s.state(r.Name), "last-deploy-ok"))
	if err != nil || string(b) != shaB {
		t.Fatalf("deploy checkpoint = %q, %v", b, err)
	}
}

func TestWatchDeployPendingDoesNotAdvance(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = "deploy_checks: [deploy-ok]\n"
	g.head = shaB
	g.parents = map[string]string{shaB: shaA}
	g.checkNames = map[string]map[string]github.CI{shaB: {"CI": github.Green, "deploy-ok": github.Pending}, shaA: {"deploy-ok": github.Green}}
	if err := os.MkdirAll(s.state(r.Name), 0700); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(s.state(r.Name), "last-deploy-ok")
	if err := os.WriteFile(checkpoint, []byte(shaA), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Watch(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(checkpoint)
	if err != nil || string(b) != shaA {
		t.Fatalf("deploy checkpoint = %q, %v", b, err)
	}
}

func TestWatchWithoutDeployLeavesNoCheckpoint(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.ci = github.Green
	if err := s.Watch(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.state(r.Name), "last-deploy-ok")); !os.IsNotExist(err) {
		t.Fatalf("unexpected deploy checkpoint: %v", err)
	}
}

type deployTestGH struct {
	*recoveryGH
	suite bool
}

func (g *deployTestGH) ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
	if name == ".github/workflows/ci-any-ref.yml" {
		if g.suite {
			return []byte("on:\n  workflow_dispatch:\n    inputs:\n      ref:\n        type: string\n      suite:\n        type: choice\n"), nil
		}
		return []byte("on:\n  workflow_dispatch:\n    inputs:\n      ref:\n        type: string\n"), nil
	}
	return g.recoveryGH.ReadFile(ctx, repo, sha, name)
}

func deploySetup(t *testing.T) (*Service, *deployTestGH, *fakeNotifier, config.Repo, time.Time) {
	t.Helper()
	s, base, _, n, r := recoverySetup(t, nil)
	base.policyExtra = "deploy_checks: [deploy-ok]\nany_ref_workflow: ci-any-ref.yml\n"
	base.checks = nil
	base.ci = ""
	base.checkNames = map[string]map[string]github.CI{
		shaA:       {"CI": github.Green, "deploy-ok": github.Green},
		shaB:       {"CI": github.Green, "deploy-ok": github.Red},
		shaC:       {"CI": github.Green, "deploy-ok": github.Red},
		revertHead: {"CI": github.Green},
	}
	base.commitPRs = map[string][]github.Pull{shaB: {base.pull}}
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	g := &deployTestGH{recoveryGH: base, suite: true}
	s.GH = g
	if err := os.MkdirAll(s.state(r.Name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.state(r.Name), "last-deploy-ok"), []byte(shaA), 0600); err != nil {
		t.Fatal(err)
	}
	return s, g, n, r, now
}

func readDeployIncident(t *testing.T, s *Service, r config.Repo) (recoveryIncident, bool) {
	t.Helper()
	var st recoveryIncident
	found, err := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "recovery-deploy.json"), &st)
	if err != nil {
		t.Fatal(err)
	}
	return st, found
}
func seedDeployIncident(t *testing.T, s *Service, r config.Repo, st recoveryIncident) {
	t.Helper()
	if err := fsutil.WriteJSON(filepath.Join(s.state(r.Name), "recovery-deploy.json"), st); err != nil {
		t.Fatal(err)
	}
}
func confirmedDeploy(now time.Time) recoveryIncident {
	return recoveryIncident{RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaC, Baseline: shaA, Task: "42", Checks: "deploy-ok", Runs: map[string][]bool{shaA: {true, true}, shaB: {false, false}}, Confirmed: true}
}

func TestWatchDeployRedDispatchAndRestart(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	recoveryWatch(t, s, r)
	st, found := readDeployIncident(t, s, r)
	if !found || st.Baseline != shaA || st.Culprit != shaB || !st.RedSince.Equal(now) {
		t.Fatalf("incident = %+v found=%v", st, found)
	}
	if len(g.dispatchInputs) != 2 {
		t.Fatalf("dispatches = %v", g.dispatchInputs)
	}
	for _, input := range g.dispatchInputs {
		if input["suite"] != "deploy" {
			t.Fatalf("dispatch = %v", input)
		}
	}
	fresh := &Service{Config: s.Config, GH: g, Runner: s.Runner, Sink: s.Sink, Notifier: s.Notifier, Now: func() time.Time { return now.Add(time.Minute) }}
	recoveryWatch(t, fresh, r)
	after, found := readDeployIncident(t, fresh, r)
	if !found || !after.RedSince.Equal(now) || after.Culprit != st.Culprit {
		t.Fatalf("restart incident = %+v", after)
	}
}

func TestWatchDeployUnknownKeepsIncident(t *testing.T) {
	for _, state := range []github.CI{"", github.Pending} {
		t.Run(string(state), func(t *testing.T) {
			s, g, _, r, now := deploySetup(t)
			old := confirmedDeploy(now)
			seedDeployIncident(t, s, r, old)
			g.head = advancedHead
			g.parents[advancedHead] = shaC
			g.checkNames[advancedHead] = map[string]github.CI{"CI": github.Green, "deploy-ok": state}
			recoveryWatch(t, s, r)
			got, found := readDeployIncident(t, s, r)
			b, err := os.ReadFile(filepath.Join(s.state(r.Name), "last-deploy-ok"))
			if !found || got.Culprit != old.Culprit || !got.RedSince.Equal(old.RedSince) || err != nil || string(b) != shaA {
				t.Fatalf("incident=%+v checkpoint=%s err=%v", got, b, err)
			}
		})
	}
}

func TestWatchDeployFixClosesRevert(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	st.RevertPR = 9
	st.RevertURL = "https://github.com/example/project/pull/9"
	seedDeployIncident(t, s, r, st)
	g.latch = true
	g.head = advancedHead
	g.parents[advancedHead] = shaC
	g.checkNames[advancedHead] = map[string]github.CI{"CI": github.Green, "deploy-ok": github.Green}
	recoveryWatch(t, s, r)
	_, found := readDeployIncident(t, s, r)
	b, err := os.ReadFile(filepath.Join(s.state(r.Name), "last-deploy-ok"))
	if found || err != nil || string(b) != advancedHead || len(g.closed) != 1 || len(g.comments) != 1 || g.latch {
		t.Fatalf("found=%v checkpoint=%s err=%v closed=%v comments=%v latch=%v", found, b, err, g.closed, g.comments, g.latch)
	}
}

func TestWatchDeployWaitsForMainRecovery(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	main := confirmedDeploy(now)
	main.Checks = "CI"
	if err := s.saveRecovery(r.Name, main); err != nil {
		t.Fatal(err)
	}
	g.checkNames[shaC]["CI"] = github.Red
	seedDeployIncident(t, s, r, confirmedDeploy(now))
	recoveryWatch(t, s, r)
	st, found := readDeployIncident(t, s, r)
	if !found || st.Culprit != shaB || st.RevertPR != 0 || g.revertCount != 1 || g.merges != 0 {
		t.Fatalf("deploy=%+v found=%v reverts=%d merges=%d", st, found, g.revertCount, g.merges)
	}
	// The one revert is main's; deploy may act only after main has recovered.
	g.latch = false
	g.checkNames[shaC]["CI"] = github.Green
	recoveryWatch(t, s, r)
	if g.revertCount != 2 {
		t.Fatalf("deploy did not resume after main recovered: reverts=%d", g.revertCount)
	}
}

func TestWatchDeployRevertUsesPRChecks(t *testing.T) {
	s, g, n, r, now := deploySetup(t)
	st := confirmedDeploy(now.Add(-fixWindow))
	st.RevertPR = 9
	seedDeployIncident(t, s, r, st)
	g.latch = true
	recoveryWatch(t, s, r)
	lines := designLaneEvents(n)
	if g.merges != 1 || len(lines) != 1 || !strings.Contains(lines[0], "deploy red since") {
		t.Fatalf("merges=%d notices=%v", g.merges, lines)
	}
}

func TestWatchDeployWorkflowWithoutSuite(t *testing.T) {
	s, g, _, r, _ := deploySetup(t)
	g.suite = false
	recoveryWatch(t, s, r)
	if len(g.dispatchInputs) != 2 {
		t.Fatalf("dispatches=%v", g.dispatchInputs)
	}
	for _, input := range g.dispatchInputs {
		if _, ok := input["suite"]; ok {
			t.Fatalf("unexpected suite: %v", input)
		}
	}
}

func TestWatchDeployRevertYieldsToLaterMainRed(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	st.RevertPR = 9
	seedDeployIncident(t, s, r, st)
	main := confirmedDeploy(now)
	main.Checks = "CI"
	if err := s.saveRecovery(r.Name, main); err != nil {
		t.Fatal(err)
	}
	g.latch = true
	g.checkNames[shaC]["CI"] = github.Red
	recoveryWatch(t, s, r)
	got, found := readDeployIncident(t, s, r)
	if !found || got.Culprit != shaB || got.RevertPR != 0 || len(g.closed) != 1 || g.closed[0] != 9 || g.revertCount != 1 {
		t.Fatalf("incident=%+v closed=%v new reverts=%d", got, g.closed, g.revertCount)
	}
}

func TestWatchDeploySameCommitGreenDoesNotWin(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	seedDeployIncident(t, s, r, st)
	g.checkNames[shaC]["deploy-ok"] = github.Green
	recoveryWatch(t, s, r)
	got, found := readDeployIncident(t, s, r)
	b, _ := os.ReadFile(filepath.Join(s.state(r.Name), "last-deploy-ok"))
	if !found || got.Culprit != st.Culprit || string(b) != shaA {
		t.Fatalf("incident=%+v found=%v checkpoint=%s", got, found, b)
	}
}

type deployCheckSpy struct {
	*fakeGH
	calls [][]string
}

func (g *deployCheckSpy) Check(ctx context.Context, repo, sha string, names []string) (github.CI, error) {
	g.calls = append(g.calls, append([]string(nil), names...))
	return g.fakeGH.Check(ctx, repo, sha, names)
}
func TestWatchWithoutDeployMakesNoDeployCheck(t *testing.T) {
	s, base, _, _, r := setup(t)
	base.enabled = true
	base.ci = github.Green
	spy := &deployCheckSpy{fakeGH: base}
	s.GH = spy
	recoveryWatch(t, s, r)
	for _, names := range spy.calls {
		for _, name := range names {
			if name == "deploy-ok" {
				t.Fatalf("unexpected deploy check: %v", spy.calls)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(s.state(r.Name), "recovery-deploy.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected incident: %v", err)
	}
}

func TestWatchDeployMergedRevertEndsIncident(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	st := confirmedDeploy(now)
	st.RevertPR = 9
	seedDeployIncident(t, s, r, st)
	g.latch = true
	mergedAt := "2026-11-07T11:00:00Z"
	g.revert.Merged = true
	g.revert.MergedAt = &mergedAt
	g.revert.MergeCommitSHA = advancedHead
	g.head = advancedHead
	g.parents[advancedHead] = shaC
	g.checkNames[advancedHead] = map[string]github.CI{"CI": github.Green, "deploy-ok": github.Pending}
	recoveryWatch(t, s, r)
	if _, found := readDeployIncident(t, s, r); found || g.latch {
		t.Fatalf("merged incident persists: found=%v latch=%v", found, g.latch)
	}
	recoveryWatch(t, s, r)
	if _, found := readDeployIncident(t, s, r); found {
		t.Fatal("old red restarted after revert merged")
	}
}

func TestWatchDeployBothSuitesDispatch(t *testing.T) {
	s, g, _, r, _ := deploySetup(t)
	g.checkNames[shaB]["CI"] = github.Red
	g.checkNames[shaC]["CI"] = github.Red
	recoveryWatch(t, s, r)
	counts := map[string]int{}
	for _, input := range g.dispatchInputs {
		counts[input["suite"]]++
	}
	if counts["main"] != 2 || counts["deploy"] != 2 {
		t.Fatalf("dispatches=%v", g.dispatchInputs)
	}
}

func TestWatchDeployRecordsDuringMainRecovery(t *testing.T) {
	s, g, _, r, now := deploySetup(t)
	main := confirmedDeploy(now)
	main.Checks = "CI"
	main.RevertPR = 9
	if err := s.saveRecovery(r.Name, main); err != nil {
		t.Fatal(err)
	}
	g.latch = true
	g.checkNames[shaC]["CI"] = github.Red
	recoveryWatch(t, s, r)
	st, found := readDeployIncident(t, s, r)
	if !found || st.Culprit != shaB || st.Baseline != shaA || st.RevertPR != 0 || g.revertCount != 0 || g.merges != 0 {
		t.Fatalf("deploy=%+v found=%v reverts=%d merges=%d", st, found, g.revertCount, g.merges)
	}
}
