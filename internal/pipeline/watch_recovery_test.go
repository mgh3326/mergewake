package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

const revertHead = "dddddddddddddddddddddddddddddddddddddddd"

// recoveryGH is a repository with an open revert PR watch opened, plus the
// trees the exact-inverse proof reads.
type recoveryGH struct {
	*fakeGH
	revert  github.Pull
	merges  int
	clears  int
	entries map[string]map[string]github.TreeEntry
}

func (g *recoveryGH) Pull(ctx context.Context, repo string, n int) (github.Pull, error) {
	if n == g.revert.Number {
		return g.revert, nil
	}
	return g.fakeGH.Pull(ctx, repo, n)
}
func (g *recoveryGH) Merge(_ context.Context, _ string, n int, sha string) error {
	if n == g.revert.Number {
		if sha != g.revert.Head.SHA {
			return errors.New("unbound revert head")
		}
		g.merges++
		at := "2026-11-07T11:00:00Z"
		g.revert.State, g.revert.Merged, g.revert.MergedAt = "closed", true, &at
		return nil
	}
	return g.fakeGH.Merge(context.Background(), "", n, sha)
}
func (g *recoveryGH) RecoveryRevert(context.Context, string) (*github.Pull, error) {
	p := g.revert
	return &p, nil
}
func (g *recoveryGH) ClearRecovery(context.Context, string) error {
	g.clears++
	g.latch = false
	return nil
}
func (g *recoveryGH) Commit(ctx context.Context, repo, sha string) (github.Commit, error) {
	c, e := g.fakeGH.Commit(ctx, repo, sha)
	c.Commit.Tree.SHA = sha
	return c, e
}
func (g *recoveryGH) Entry(_ context.Context, _, tree, path string) (github.TreeEntry, error) {
	return g.entries[tree][path], nil
}

func blob(sha string) github.TreeEntry {
	return github.TreeEntry{Path: "tool", Mode: "100644", Type: "blob", SHA: sha}
}

// recoverySetup builds red main after the culprit PR #7 and the open revert PR
// #9 watch opened for it: culprit shaB, its parent shaA, red head shaC.
func recoverySetup(t *testing.T, mutate func(*recoveryGH)) (*Service, *recoveryGH, *fakeSink, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, sink, n, r := setup(t)
	g.enabled = true
	g.head = shaC
	g.parents = map[string]string{shaC: shaB, shaB: shaA}
	g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green, revertHead: github.Green}
	merged := "2026-01-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.MergeCommitSHA = shaB
	g.pull.Body = "Mergewake-Task: 42"
	s.Config.Sink.Type = "handoffkeep"
	r.Lane = "design-lane"
	mergeable := true
	revert := github.Pull{
		Number: 9, State: "open", ChangedFiles: 1, Mergeable: &mergeable,
		MergeableState: "clean", Body: "Reverts " + shaB + " from PR #7. Review and merge this PR.",
		Head: github.Ref{SHA: revertHead, Ref: github.RecoveryBranch},
		Base: github.Ref{Ref: r.Branch},
	}
	revert.Head.Repo.FullName = r.Name
	l := &recoveryGH{
		fakeGH: g,
		revert: revert,
		entries: map[string]map[string]github.TreeEntry{
			shaA:      {"tool": blob("pre")},
			shaB:      {"tool": blob("post")},
			shaC:      {"tool": blob("post")},
			revertHead: {"tool": blob("pre")},
		},
	}
	s.GH = l
	if mutate != nil {
		mutate(l)
	}
	return s, l, sink, n, r
}

// recoveryWatch runs one pass and returns its final watch action and reason.
func recoveryWatch(t *testing.T, s *Service, r config.Repo) watchResult {
	t.Helper()
	var out strings.Builder
	s.Out = &out
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	var got watchResult
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec struct {
			Command string          `json:"command"`
			Value   json.RawMessage `json:"value"`
		}
		if e := json.Unmarshal([]byte(line), &rec); e != nil {
			t.Fatal(e)
		}
		if rec.Command == "watch" {
			got = watchResult{}
			if e := json.Unmarshal(rec.Value, &got); e != nil {
				t.Fatal(e)
			}
		}
	}
	return got
}

func designLaneEvents(n *fakeNotifier) []string {
	var out []string
	for _, e := range n.events {
		if e.Lane == "design-lane" {
			out = append(out, e.Text)
		}
	}
	return out
}

func recoveryState(t *testing.T, s *Service, r config.Repo) (recoveryIncident, bool) {
	t.Helper()
	st, found, e := s.loadRecovery(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	return st, found
}

// seedConfirmed records the 2/2 evidence an earlier confirmation already
// produced, so the next pass opens the revert.
func seedConfirmed(t *testing.T, s *Service, r config.Repo, now time.Time) {
	t.Helper()
	st := recoveryIncident{
		RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaC, Baseline: shaA, Task: "42",
		Runs:      map[string][]bool{shaA: {true, true}, shaB: {false, false}},
		Confirmed: true,
	}
	if e := s.saveRecovery(r.Name, st); e != nil {
		t.Fatal(e)
	}
}

// (a) A red main with an identified culprit opens the revert PR at once and
// does not merge it before the 30-minute fix window has passed.
func TestWatchRevertFixWindow(t *testing.T) {
	s, g, sink, _, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)

	got := recoveryWatch(t, s, r)
	if got.Action != "opened-revert-pr" || g.revertCount != 1 || g.merges != 0 || sink.reopens != 1 {
		t.Fatalf("%+v reverts=%d merges=%d reopens=%d", got, g.revertCount, g.merges, sink.reopens)
	}
	st, found := recoveryState(t, s, r)
	if !found || !st.RedSince.Equal(now) || st.RevertPR != 9 || st.Culprit != shaB {
		t.Fatalf("state %+v found=%t", st, found)
	}

	now = now.Add(29 * time.Minute)
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-fix-window" || g.merges != 0 {
		t.Fatalf("merged before the window: %+v merges=%d", got, g.merges)
	}
	if !strings.Contains(got.Reason, "1m0s remaining") {
		t.Fatalf("reason %q", got.Reason)
	}
}

// (b) A fix that turns main green inside the window closes the revert PR with
// a comment and clears the latch.
func TestWatchFixWinsInsideWindow(t *testing.T) {
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.latch = true
		l.checks[l.head] = github.Green
	})
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	if e := s.saveRecovery(r.Name, recoveryIncident{RedSince: now, Culprit: shaB, CulpritPR: 7, Head: shaC, RevertPR: 9, Task: "42"}); e != nil {
		t.Fatal(e)
	}

	got := recoveryWatch(t, s, r)
	if got.Action != "healthy" || g.merges != 0 || g.revertCount != 0 {
		t.Fatalf("%+v merges=%d reverts=%d", got, g.merges, g.revertCount)
	}
	if len(g.comments) != 1 || !strings.Contains(g.comments[0], "main is green again at "+shaC+"; revert not needed") {
		t.Fatalf("comments %v", g.comments)
	}
	if len(g.closed) != 1 || g.closed[0] != 9 || g.latch || g.clears != 1 {
		t.Fatalf("closed=%v latch=%t clears=%d", g.closed, g.latch, g.clears)
	}
	if _, e := os.Stat(s.recoveryFile(r.Name)); !os.IsNotExist(e) {
		t.Fatalf("recovery state kept: %v", e)
	}
	if got := designLaneEvents(n); len(got) != 0 {
		t.Fatalf("fix-won notified the design lane: %v", got)
	}
}

// (c) After the window, with main still red, the revert CI green and the diff
// the exact inverse, watch merges the revert, keeps the task reopened once and
// sends one design-lane line.
func TestWatchRevertMergesAfterWindow(t *testing.T) {
	s, g, sink, n, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)

	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	now = now.Add(fixWindow)
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-merged" || g.merges != 1 {
		t.Fatalf("%+v merges=%d", got, g.merges)
	}
	if sink.reopens != 1 {
		t.Fatalf("task reopened %d times", sink.reopens)
	}
	lines := designLaneEvents(n)
	if len(lines) != 1 {
		t.Fatalf("%d design-lane lines: %v", len(lines), lines)
	}
	want := "example/project main red since 2026-11-07T10:00:00Z by PR #7; reverted by PR #9 after 30m; task 42 reopened"
	if lines[0] != want {
		t.Fatalf("line %q, want %q", lines[0], want)
	}
	// A repeated pass sends nothing further.
	now = now.Add(time.Minute)
	if got := recoveryWatch(t, s, r); got.Action != "operator-required-recovery-latch" {
		t.Fatalf("repeat: %+v", got)
	}
	if len(designLaneEvents(n)) != 1 || g.merges != 1 {
		t.Fatalf("repeat merged or announced again: %d", g.merges)
	}
}

// (d) Red or pending CI on the revert PR holds it and tells the design lane
// once.
func TestWatchRevertCIBlocksMerge(t *testing.T) {
	for _, ci := range []github.CI{github.Red, github.Pending} {
		t.Run(string(ci), func(t *testing.T) {
			s, g, _, n, r := recoverySetup(t, nil)
			now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
			s.Now = func() time.Time { return now }
			seedConfirmed(t, s, r, now)
			if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
				t.Fatalf("%+v", got)
			}
			g.checks[revertHead] = ci
			now = now.Add(fixWindow)
			for i := 0; i < 2; i++ {
				got := recoveryWatch(t, s, r)
				if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "revert PR CI is "+string(ci)) {
					t.Fatalf("%+v", got)
				}
			}
			if g.merges != 0 {
				t.Fatalf("merged with %s revert CI", ci)
			}
			if lines := designLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "revert PR CI is "+string(ci)) {
				t.Fatalf("%d design-lane lines: %v", len(lines), lines)
			}
		})
	}
}

// (e) A revert whose diff is not the exact inverse is refused and held.
func TestWatchRevertNotExactInverse(t *testing.T) {
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.entries[revertHead] = map[string]github.TreeEntry{"tool": blob("post")}
	})
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	now = now.Add(fixWindow)
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "does not restore tool") {
		t.Fatalf("%+v", got)
	}
	if g.merges != 0 {
		t.Fatal("merged a revert that is not the exact inverse")
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("%v", lines)
	}
}

// A culprit path changed again on main is not an exact revert either.
func TestWatchRevertCulpritPathChanged(t *testing.T) {
	s, g, _, _, r := recoverySetup(t, func(l *recoveryGH) {
		l.entries[shaC] = map[string]github.TreeEntry{"tool": blob("edited")}
	})
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	recoveryWatch(t, s, r)
	now = now.Add(fixWindow)
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-hold" || !strings.Contains(got.Reason, "main changed tool after the culprit") {
		t.Fatalf("%+v", got)
	}
	if g.merges != 0 {
		t.Fatal("merged over a later edit")
	}
}

// (f) A culprit that touched a blocking path is only reported; no revert PR
// and no incident are created.
func TestWatchBlockingCulpritReportOnly(t *testing.T) {
	s, g, _, n, r := setup(t)
	g.enabled = true
	g.blocking = true
	g.head = shaC
	g.parents = map[string]string{shaC: shaB, shaB: shaA}
	g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green}
	merged := "2026-01-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.MergeCommitSHA = shaB
	g.pull.Body = "Mergewake-Task: 42"
	s.Config.Sink.Type = "handoffkeep"
	r.Lane = "design-lane"

	var out strings.Builder
	s.Out = &out
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), `"action":"report-culprit"`) {
		t.Fatal(out.String())
	}
	if g.revertCount != 0 {
		t.Fatalf("reverts=%d", g.revertCount)
	}
	if _, e := os.Stat(s.recoveryFile(r.Name)); !os.IsNotExist(e) {
		t.Fatalf("recovery state written: %v", e)
	}
	if lines := designLaneEvents(n); len(lines) != 1 {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (h) A re-run that passes at the culprit is a flaky failure: no revert.
func TestWatchFlakyFailureNoRevert(t *testing.T) {
	s, g, _, n, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
	})
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }

	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || g.revertCount != 0 {
		t.Fatalf("%+v reverts=%d", got, g.revertCount)
	}
	if g.dispatched[shaA] != 1 || g.dispatched[shaB] != 1 {
		t.Fatalf("dispatches %v", g.dispatched)
	}
	now = now.Add(time.Minute)
	g.statuses = map[[2]string]statusRecord{
		{shaA, "CI"}: {github.Green, now},
		{shaB, "CI"}: {github.Green, now},
	}
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "culprit passed a re-run") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 || g.latch {
		t.Fatal("a flaky failure was blamed")
	}
	if lines := designLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "flaky") && !strings.Contains(lines[0], "passed a re-run") {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (i) Only 1/2 at the culprit is not confirmation: nothing is reverted.
func TestWatchOneOfTwoAtCulpritHolds(t *testing.T) {
	s, g, _, _, r := recoverySetup(t, func(l *recoveryGH) {
		l.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
	})
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }

	if got := recoveryWatch(t, s, r); got.Action != "recovery-confirming" || g.revertCount != 0 {
		t.Fatalf("%+v reverts=%d", got, g.revertCount)
	}
	now = now.Add(time.Minute)
	g.statuses = map[[2]string]statusRecord{{shaA, "CI"}: {github.Green, now}}
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "1/2 at "+shaB[:7]) {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 {
		t.Fatal("reverted on 1/2 at the culprit")
	}
	now = now.Add(46 * time.Minute)
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-confirming" || !strings.Contains(got.Reason, "timed out") {
		t.Fatalf("%+v", got)
	}
	if g.revertCount != 0 {
		t.Fatal("reverted without a second culprit run")
	}
}

func fixPull(sha string) github.Pull {
	return github.Pull{
		Number: 11, State: "open", Head: github.Ref{SHA: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
		Body: "Fixes-Red-Main: " + sha + "\n",
		Base: github.Ref{Ref: "main"},
	}
}

// (j) An open Fixes-Red-Main PR holds the revert past the window; at 2x the
// window, with main still red, the revert lands.
func TestWatchFixPRHoldsRevert(t *testing.T) {
	s, g, _, n, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	g.openPulls = []github.Pull{fixPull(shaB)}

	now = now.Add(fixWindow)
	got := recoveryWatch(t, s, r)
	if got.Action != "recovery-fix-pr" || !strings.Contains(got.Reason, "#11") || g.merges != 0 {
		t.Fatalf("%+v merges=%d", got, g.merges)
	}
	if priority := s.ReviewPriority(context.Background(), r, fixPull(shaB)); priority.Value != 1 || priority.Source != "recovery-fix" {
		t.Fatalf("fix PR priority %+v", priority)
	}
	now = now.Add(31 * time.Minute)
	got = recoveryWatch(t, s, r)
	if got.Action != "recovery-merged" || g.merges != 1 {
		t.Fatalf("%+v merges=%d", got, g.merges)
	}
	lines := designLaneEvents(n)
	if len(lines) != 2 || !strings.Contains(lines[0], "#11") || !strings.Contains(lines[1], "reverted by PR #9") {
		t.Fatalf("design-lane lines: %v", lines)
	}
}

// (k) The fix PR merging green closes the revert instead.
func TestWatchFixPRMergesGreenClosesRevert(t *testing.T) {
	s, g, _, n, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	g.openPulls = []github.Pull{fixPull(shaB)}
	g.checks[shaC] = github.Green

	got := recoveryWatch(t, s, r)
	if got.Action != "healthy" || g.merges != 0 || g.revertCount != 1 {
		t.Fatalf("%+v merges=%d reverts=%d", got, g.merges, g.revertCount)
	}
	if len(g.closed) != 1 || g.closed[0] != 9 || g.clears != 1 {
		t.Fatalf("closed=%v clears=%d", g.closed, g.clears)
	}
	if lines := designLaneEvents(n); len(lines) != 0 {
		t.Fatalf("fix-won notified the design lane: %v", lines)
	}
}

// A repo whose any-ref workflow accepts packages is re-run for the culprit's
// changed Go packages; one that does not gets the ref only.
type packagesGH struct{ *fakeGH }

func (g *packagesGH) ReadFile(_ context.Context, _, _, name string) ([]byte, error) {
	if name == ".github/workflows/ci-any-ref.yml" {
		return []byte("on:\n  workflow_dispatch:\n    inputs:\n      packages:\n        type: string\n"), nil
	}
	return g.fakeGH.ReadFile(context.Background(), "", "", name)
}

func TestRecoveryConfirmInputsPackages(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.files = []policy.File{
		{Path: "internal/mcp/a.go", Kind: "modified"},
		{Path: "cmd/tool/b.go", Kind: "added"},
		{Path: "docs/a.md", Kind: "modified"},
	}
	p := policy.Policy{AnyRefWorkflow: "ci-any-ref.yml"}
	st := recoveryIncident{CulpritPR: 7}

	if got := s.confirmInputs(context.Background(), r, p, st, shaB); got["packages"] != "" || got["ref"] != shaB {
		t.Fatalf("no workflow input: %v", got)
	}
	s.GH = &packagesGH{fakeGH: g}
	got := s.confirmInputs(context.Background(), r, p, st, shaB)
	if got["packages"] != "./cmd/tool/ ./internal/mcp/" || got["ref"] != shaB {
		t.Fatalf("packages %v", got)
	}
}

// (g) A restart keeps red_since: the window is not restarted by a new process.
func TestWatchRevertRestartKeepsWindow(t *testing.T) {
	s, g, _, _, r := recoverySetup(t, nil)
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	seedConfirmed(t, s, r, now)
	if got := recoveryWatch(t, s, r); got.Action != "opened-revert-pr" {
		t.Fatalf("%+v", got)
	}
	fresh := &Service{Config: s.Config, GH: g, Runner: s.Runner, Sink: s.Sink, Notifier: s.Notifier, Now: s.Now}
	now = now.Add(29 * time.Minute)
	if got := recoveryWatch(t, fresh, r); got.Action != "recovery-fix-window" {
		t.Fatalf("restart restarted the window: %+v", got)
	}
	st, _ := recoveryState(t, s, r)
	if !st.RedSince.Equal(time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("red_since moved: %s", st.RedSince)
	}
	now = now.Add(time.Minute)
	if got := recoveryWatch(t, fresh, r); got.Action != "recovery-merged" {
		t.Fatalf("%+v", got)
	}
	if g.merges != 1 {
		t.Fatalf("merges=%d", g.merges)
	}
}
