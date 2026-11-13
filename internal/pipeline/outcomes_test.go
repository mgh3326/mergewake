package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

type outcomeGH struct {
	*fakeGH
	recovery github.Pull
	first    bool
	calls    int
	pullIDs  []int
	pulls    map[int]github.Pull
	firstCI  map[int]*bool
}

func (g *outcomeGH) FirstPRCI(_ context.Context, _ string, p github.Pull, _ []string) (*bool, error) {
	g.calls++
	if v, ok := g.firstCI[p.Number]; ok {
		return v, nil
	}
	v := g.first
	return &v, nil
}
func (g *outcomeGH) Pull(_ context.Context, _ string, n int) (github.Pull, error) {
	g.pullIDs = append(g.pullIDs, n)
	if n == 9 {
		return g.recovery, nil
	}
	if p, ok := g.pulls[n]; ok {
		return p, nil
	}
	return g.pull, nil
}
func TestShadowOutcomesAndSevenDayFollowup(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Now().UTC()
	opened := now.Add(-10 * 24 * time.Hour)
	merged := now.Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	g.pull.CreatedAt = opened.Format(time.RFC3339)
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = "Mergewake-Author-Profile: example@high\nMergewake-Author-Model: concrete-model\nMergewake-Experiment: effort-test/control\nMergewake-Grade: A\nMergewake-Tier: T1\n"
	source := &outcomeGH{fakeGH: g, first: true}
	s.GH = source
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.recordReview(repo.Name, 7, g.pull.Head.SHA, []github.Finding{}, github.PassReceipt{}, false); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 7)]
	if v.Evidence() != "clean_pass" || v.TimeToMerge == nil || v.Experiment != "effort-test" || !v.Shadow {
		t.Fatalf("%+v", v)
	}
	source.first = false
	if err = s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 {
		t.Fatal("overwrote initial CI")
	}
	// An actual watch recovery merge, rather than merely opening it, fails the PR.
	merged = now.Add(-24 * time.Hour).Format(time.RFC3339)
	g.pull.MergedAt = &merged
	recovered := now.Add(-time.Hour).Format(time.RFC3339)
	source.recovery.MergedAt = &recovered
	if err = s.ledger().Update(repo.Name, 7, func(v *outcomes.Outcome) { v.Reverted = nil; v.RevertPR = 9 }); err != nil {
		t.Fatal(err)
	}
	if err = s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	state, _ = s.ledger().Read()
	v = state.Latest[outcomes.Key(repo.Name, 7)]
	if v.Evidence() != "failure" || v.Reverted == nil || !*v.Reverted {
		t.Fatal(v)
	}
	if g.mergeCount != 0 || g.revertCount != 0 || g.claimCount != 0 {
		t.Fatal("shadow observation mutated GitHub")
	}
}

func TestAuthorProfile(t *testing.T) {
	for _, tt := range []struct {
		value, profile, effort string
	}{
		{"opus", "opus", ""},
		{"opus@high", "opus", "high"},
		{"codex-sol@xhigh", "codex-sol", "xhigh"},
		{"codex@bogus", "codex", ""},
		{"a@b@c", "", ""},
		{"bad name", "", ""},
		{"", "", ""},
	} {
		profile, effort := authorProfile(tt.value)
		if profile != tt.profile || effort != tt.effort {
			t.Errorf("authorProfile(%q) = %q, %q; want %q, %q", tt.value, profile, effort, tt.profile, tt.effort)
		}
	}
}

// fleetPRBody keeps the trailer block of mgh3326/panewire#143 (merged
// 2026-11-01), written to the rstack wake brief without an @effort suffix.
const fleetPRBody = "Synthetic summary of the change.\n" +
	"\n" +
	"Mergewake-Author-Family: anthropic\n" +
	"Mergewake-Author-Profile: opus\n" +
	"\n" +
	"🤖 Generated with [Claude Code](https://claude.com/claude-code)"

func TestObservePRRecordsProfileWithoutEffort(t *testing.T) {
	for _, tt := range []struct {
		name, body, model string
	}{
		{"derived from profile", fleetPRBody, "opus-model"},
		{"explicit model wins", fleetPRBody + "\nMergewake-Author-Model: concrete-model\n", "concrete-model"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, repo := setup(t)
			s.Config.Review.Profiles["opus"] = config.Reviewer{Family: "anthropic", Command: "claude", Args: []string{"--model", "opus-model", "--effort", "high"}}
			g.pull.Body = tt.body
			s.GH = &outcomeGH{fakeGH: g, first: true}
			if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
				t.Fatal(err)
			}
			state, err := s.ledger().Read()
			if err != nil {
				t.Fatal(err)
			}
			v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
			if v.Profile != "opus" || v.Effort != "" || v.Model != tt.model || v.Family != "anthropic" {
				t.Fatalf("profile=%q effort=%q model=%q family=%q", v.Profile, v.Effort, v.Model, v.Family)
			}
		})
	}
}

// Rows written before profile-only trailers were accepted stay unattributed,
// so ReconcileOutcomes revisits them and records the profile on its next pass.
func TestReconcileBackfillsProfileWithoutEffort(t *testing.T) {
	s, g, _, _, repo := setup(t)
	s.Config.Review.Profiles["opus"] = config.Reviewer{Family: "anthropic", Command: "claude", Args: []string{"--model", "opus-model"}}
	merged := time.Now().UTC().Format(time.RFC3339)
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = fleetPRBody
	s.GH = &outcomeGH{fakeGH: g, first: true}
	if err := s.ledger().Update(repo.Name, g.pull.Number, func(v *outcomes.Outcome) { v.Final = true; v.Decision = "landed" }); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileOutcomes(context.Background(), repo, noneOpen, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	if v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]; v.Profile != "opus" || v.Model != "opus-model" {
		t.Fatalf("profile=%q model=%q", v.Profile, v.Model)
	}
}

func TestReconcileClosedRowsAreThrottledAndFinished(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	mergedAt := now.Add(-24 * time.Hour)
	merged := mergedAt.Format(time.RFC3339)
	makePull := func(number int, mergeAt *string) github.Pull {
		p := g.pull
		p.Number, p.State, p.MergedAt = number, "closed", mergeAt
		p.CreatedAt = now.Add(-48 * time.Hour).Format(time.RFC3339)
		p.Body = "Mergewake-Author-Profile: claude\nMergewake-Author-Model: model\nMergewake-Grade: B\nMergewake-Tier: T1\n"
		return p
	}
	mergedPull := makePull(1, &merged)
	noCIPull := makePull(3, nil)
	unattributedPull := makePull(2, nil)
	unattributedPull.Body = ""
	newlyClosedPull := makePull(4, nil)
	source := &outcomeGH{fakeGH: g, first: true, pulls: map[int]github.Pull{
		1: mergedPull, 2: unattributedPull, 3: noCIPull, 4: newlyClosedPull,
	}, firstCI: map[int]*bool{3: nil}}
	s.GH = source
	green := true
	for _, row := range []struct {
		pr      int
		merged  *time.Time
		ci      *bool
		profile string
	}{
		{1, &mergedAt, &green, "claude"},
		{2, nil, &green, ""},
		{3, nil, nil, "claude"},
		{4, nil, &green, "claude"},
	} {
		if err := s.ledger().Update(repo.Name, row.pr, func(v *outcomes.Outcome) {
			v.Final, v.Decision, v.Profile, v.Model, v.Grade, v.Tier = row.pr != 4, "landed", row.profile, "model", "B", "T1"
			v.MergedAt, v.FirstCIGreen = row.merged, row.ci
		}); err != nil {
			t.Fatal(err)
		}
	}

	reconcile(t, s, repo, Options{Shadow: true})
	if got := source.pullIDs; len(got) != 4 {
		t.Fatalf("first sweep Pull calls = %v, want one per closed row", got)
	}
	source.pullIDs = nil
	now = now.Add(time.Minute)
	reconcile(t, s, repo, Options{Shadow: true})
	if len(source.pullIDs) != 0 {
		t.Errorf("second sweep read closed rows: %v", source.pullIDs)
	}

	source.pullIDs = nil
	now = now.Add(time.Hour)
	reconcile(t, s, repo, Options{Shadow: true})
	if len(source.pullIDs) != 1 || source.pullIDs[0] != 1 {
		t.Errorf("hourly sweep Pull calls = %v, want only pending merged row 1", source.pullIDs)
	}

	source.pullIDs = nil
	now = mergedAt.Add(7*24*time.Hour + time.Minute)
	reconcile(t, s, repo, Options{Shadow: true})
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 1)]
	if v.Reverted == nil || *v.Reverted {
		t.Fatalf("seven-day verdict = %v, want false", v.Reverted)
	}
	source.pullIDs = nil
	reconcile(t, s, repo, Options{Shadow: true})
	if len(source.pullIDs) != 0 {
		t.Errorf("settled rows read again: %v", source.pullIDs)
	}
}

func TestReconcileRecordsRevertInsideWindow(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	mergedAt := now.Add(-2 * 24 * time.Hour)
	revertedAt := now.Add(-time.Hour).Format(time.RFC3339)
	merged := mergedAt.Format(time.RFC3339)
	p := g.pull
	p.Number, p.State, p.MergedAt, p.MergeCommitSHA = 8, "closed", &merged, "merge-sha"
	source := &outcomeGH{fakeGH: g, recovery: github.Pull{Number: 9, State: "closed", MergedAt: &revertedAt}, pulls: map[int]github.Pull{8: p}}
	s.GH = source
	green := true
	if err := s.ledger().Update(repo.Name, 8, func(v *outcomes.Outcome) {
		v.Final, v.Decision, v.Profile, v.Model, v.Grade, v.Tier = true, "landed", "claude", "model", "B", "T1"
		v.MergedAt, v.FirstCIGreen, v.RevertPR = &mergedAt, &green, 9
	}); err != nil {
		t.Fatal(err)
	}
	reconcile(t, s, repo, Options{Shadow: true})
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	if v := state.Latest[outcomes.Key(repo.Name, 8)]; v.Reverted == nil || !*v.Reverted {
		t.Fatalf("inside-window revert verdict = %v, want true", v.Reverted)
	}
}

func TestObserveReopenedPRReopensOutcomeReconciliation(t *testing.T) {
	s, g, _, _, repo := setup(t)
	green := true
	due := time.Now().UTC().Add(time.Hour)
	if err := s.ledger().Update(repo.Name, g.pull.Number, func(v *outcomes.Outcome) {
		v.Final, v.ReconcileFinished, v.ReconcileDueAt, v.FirstCIGreen = true, true, &due, &green
	}); err != nil {
		t.Fatal(err)
	}
	s.GH = &outcomeGH{fakeGH: g, first: true}
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
	if v.Final || v.ReconcileFinished || v.ReconcileDueAt != nil {
		t.Fatalf("reopened PR reconciliation state = final:%t finished:%t due:%v", v.Final, v.ReconcileFinished, v.ReconcileDueAt)
	}
}

func TestReconcileMergedRowWithoutCIOrAttributionKeepsRevertCheck(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	mergedAt := now.Add(-24 * time.Hour)
	merged := mergedAt.Format(time.RFC3339)
	p := g.pull
	p.Number, p.State, p.MergedAt = 5, "closed", &merged
	p.Body = ""
	source := &outcomeGH{fakeGH: g, pulls: map[int]github.Pull{5: p}, firstCI: map[int]*bool{5: nil}}
	s.GH = source
	if err := s.ledger().Update(repo.Name, 5, func(v *outcomes.Outcome) {
		v.Final, v.Decision, v.MergedAt = true, "landed", &mergedAt
	}); err != nil {
		t.Fatal(err)
	}
	reconcile(t, s, repo, Options{Shadow: true})
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 5)]
	if v.Attributed() || v.FirstCIGreen != nil || v.ReconcileFinished || v.ReconcileDueAt == nil {
		t.Fatalf("merged pending row state: attributed:%t ci:%v finished:%t due:%v", v.Attributed(), v.FirstCIGreen, v.ReconcileFinished, v.ReconcileDueAt)
	}
	source.pullIDs = nil
	now = now.Add(time.Minute)
	reconcile(t, s, repo, Options{Shadow: true})
	if len(source.pullIDs) != 0 {
		t.Fatalf("not-due merged row read: %v", source.pullIDs)
	}
	now = now.Add(time.Hour)
	reconcile(t, s, repo, Options{Shadow: true})
	if len(source.pullIDs) != 1 || source.pullIDs[0] != 5 {
		t.Fatalf("hourly merged-row Pull calls = %v, want [5]", source.pullIDs)
	}
	source.pullIDs = nil
	now = mergedAt.Add(7*24*time.Hour + time.Minute)
	reconcile(t, s, repo, Options{Shadow: true})
	state, err = s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v = state.Latest[outcomes.Key(repo.Name, 5)]
	if v.Reverted == nil || *v.Reverted || !v.ReconcileFinished || v.ReconcileDueAt != nil {
		t.Fatalf("expired no-evidence row state: reverted:%v finished:%t due:%v", v.Reverted, v.ReconcileFinished, v.ReconcileDueAt)
	}
}

// A PR closed without merging is re-read daily for a week after ClosedAt, so
// a reopen that was closed or merged before any evaluation saw it open is
// still observed.
func TestReconcileClosedUnmergedRowRevisitedDaily(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	closedAt := now.Add(-time.Hour)
	closed := closedAt.Format(time.RFC3339)
	pull := g.pull
	pull.Number, pull.State, pull.ClosedAt = 6, "closed", &closed
	pull.CreatedAt = now.Add(-48 * time.Hour).Format(time.RFC3339)
	pull.Body = "Mergewake-Author-Profile: claude\nMergewake-Author-Model: model\nMergewake-Grade: B\nMergewake-Tier: T1\n"
	source := &outcomeGH{fakeGH: g, first: true, pulls: map[int]github.Pull{6: pull}}
	s.GH = source
	if err := s.ledger().Update(repo.Name, 6, func(v *outcomes.Outcome) {
		v.Final, v.Decision, v.ClosedAt = true, "blocked", &closedAt
	}); err != nil {
		t.Fatal(err)
	}
	read := func() outcomes.Outcome {
		t.Helper()
		reconcile(t, s, repo, Options{Shadow: true})
		state, err := s.ledger().Read()
		if err != nil {
			t.Fatal(err)
		}
		return state.Latest[outcomes.Key(repo.Name, 6)]
	}

	v := read()
	if got := source.pullIDs; len(got) != 1 || got[0] != 6 {
		t.Fatalf("first sweep Pull calls = %v, want [6]", got)
	}
	if v.ReconcileFinished || v.ReconcileDueAt == nil || !v.ReconcileDueAt.Equal(now.Add(24*time.Hour)) {
		t.Fatalf("after first read: finished=%t due=%v, want unfinished due in 24h", v.ReconcileFinished, v.ReconcileDueAt)
	}

	source.pullIDs = nil
	now = now.Add(time.Hour)
	read()
	if len(source.pullIDs) != 0 {
		t.Fatalf("hourly sweep read the closed row: %v", source.pullIDs)
	}

	// By the next daily read the PR was reopened and merged unseen: the row
	// lands and keeps the hourly revert cadence.
	merged := now.Format(time.RFC3339)
	mergedPull := pull
	mergedPull.MergedAt = &merged
	source.pulls[6] = mergedPull
	source.pullIDs = nil
	now = now.Add(23 * time.Hour)
	v = read()
	if got := source.pullIDs; len(got) != 1 || got[0] != 6 {
		t.Fatalf("daily sweep Pull calls = %v, want [6]", got)
	}
	if v.Decision != "landed" || v.MergedAt == nil || v.ReconcileFinished || v.ReconcileDueAt == nil || !v.ReconcileDueAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("merged row: decision=%q merged=%v finished=%t due=%v", v.Decision, v.MergedAt, v.ReconcileFinished, v.ReconcileDueAt)
	}
	source.pullIDs = nil
	now = now.Add(time.Hour)
	read()
	if got := source.pullIDs; len(got) != 1 || got[0] != 6 {
		t.Fatalf("hourly revert-cadence Pull calls = %v, want [6]", got)
	}
}

// A week after ClosedAt the closed-unmerged row is finished without another
// read.
func TestReconcileClosedUnmergedRowFinishedAfterWindow(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	closedAt := now.Add(-7*24*time.Hour - time.Minute)
	closed := closedAt.Format(time.RFC3339)
	pull := g.pull
	pull.Number, pull.State, pull.ClosedAt = 7, "closed", &closed
	source := &outcomeGH{fakeGH: g, first: true, pulls: map[int]github.Pull{7: pull}}
	s.GH = source
	if err := s.ledger().Update(repo.Name, 7, func(v *outcomes.Outcome) {
		v.Final, v.Decision, v.ClosedAt = true, "blocked", &closedAt
	}); err != nil {
		t.Fatal(err)
	}
	reconcile(t, s, repo, Options{Shadow: true})
	if got := source.pullIDs; len(got) != 0 {
		t.Fatalf("past-window row read again: %v", got)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 7)]
	if !v.ReconcileFinished || v.ReconcileDueAt != nil {
		t.Fatalf("past-window row: finished=%t due=%v", v.ReconcileFinished, v.ReconcileDueAt)
	}
}

// A closed-unmerged row without ClosedAt keeps today's rule: finished at the
// first read.
func TestReconcileClosedUnmergedRowWithoutClosedAtFinishes(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	pull := g.pull
	pull.Number, pull.State = 8, "closed"
	source := &outcomeGH{fakeGH: g, first: true, pulls: map[int]github.Pull{8: pull}}
	s.GH = source
	if err := s.ledger().Update(repo.Name, 8, func(v *outcomes.Outcome) {
		v.Final, v.Decision = true, "blocked"
	}); err != nil {
		t.Fatal(err)
	}
	reconcile(t, s, repo, Options{Shadow: true})
	if got := source.pullIDs; len(got) != 1 || got[0] != 8 {
		t.Fatalf("first sweep Pull calls = %v, want [8]", got)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 8)]
	if !v.ReconcileFinished || v.ReconcileDueAt != nil {
		t.Fatalf("no-ClosedAt row: finished=%t due=%v", v.ReconcileFinished, v.ReconcileDueAt)
	}
}

// A row the previous logic finished with no due time, whose ClosedAt is still
// inside the week, is read again so a reopen it missed is recorded. A row
// closed more than a week ago stays finished and is not read.
func TestReconcileFinishedClosedRowInWindowIsReadAgain(t *testing.T) {
	s, g, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	reopened := g.pull
	reopened.Number, reopened.State = 12, "open"
	closedPull := g.pull
	closedPull.Number, closedPull.State = 13, "closed"
	source := &outcomeGH{fakeGH: g, first: true, pulls: map[int]github.Pull{12: reopened, 13: closedPull}}
	s.GH = source
	for _, row := range []struct {
		pr       int
		closedAt time.Time
	}{{12, now.Add(-48 * time.Hour)}, {13, now.Add(-8 * 24 * time.Hour)}} {
		closed := row.closedAt
		if err := s.ledger().Update(repo.Name, row.pr, func(v *outcomes.Outcome) {
			v.Final, v.Decision, v.ReconcileFinished, v.ClosedAt = true, "blocked", true, &closed
		}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, s, repo, Options{Shadow: true})
	if got := source.pullIDs; len(got) != 1 || got[0] != 12 {
		t.Fatalf("Pull calls = %v, want [12]", got)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	if v := state.Latest[outcomes.Key(repo.Name, 12)]; v.Final || v.ReconcileFinished {
		t.Fatalf("reopen not recorded: final=%t finished=%t", v.Final, v.ReconcileFinished)
	}
	if v := state.Latest[outcomes.Key(repo.Name, 13)]; !v.ReconcileFinished || v.ReconcileDueAt != nil {
		t.Fatalf("past-window row: finished=%t due=%v", v.ReconcileFinished, v.ReconcileDueAt)
	}
}

// A flag write computed from a ClosedAt that a later evaluation replaced (the
// PR reclosed) leaves the row unfinished and undated.
func TestReconcileFlagWriteKeepsRowWhenReclosed(t *testing.T) {
	s, _, _, _, repo := setup(t)
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-8 * 24 * time.Hour)
	reclosed := now.Add(-time.Hour)
	if err := s.ledger().Update(repo.Name, 14, func(v *outcomes.Outcome) {
		v.Final, v.ClosedAt = true, &reclosed
	}); err != nil {
		t.Fatal(err)
	}
	seen := outcomes.Outcome{Repo: repo.Name, PR: 14, Final: true, ClosedAt: &stale}
	if err := s.writeReconcileFlags(repo.Name, 14, seen, true, nil); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, 14)]
	if v.ReconcileFinished || v.ReconcileDueAt != nil {
		t.Fatalf("reclosed row: finished=%t due=%v", v.ReconcileFinished, v.ReconcileDueAt)
	}
}

// The flag write applies only while the row still has the Final and MergedAt
// the reconcile computed from. An evaluation that recorded the PR open between
// the reconcile's ledger re-read and its write leaves the row unfinished and
// undated, so the next reconcile reads it again.
func TestReconcileFlagWriteKeepsRowAnEvaluationChanged(t *testing.T) {
	s, _, _, _, repo := setup(t)
	closedAt := time.Now().UTC().Add(-8 * 24 * time.Hour)
	// PR 9: the evaluation saw the PR reopen after the reconcile read the row
	// closed and computed finished=true for it.
	if err := s.ledger().Update(repo.Name, 9, func(v *outcomes.Outcome) {
		v.Final, v.ReconcileFinished = false, false
	}); err != nil {
		t.Fatal(err)
	}
	seen := outcomes.Outcome{Repo: repo.Name, PR: 9, Final: true, ClosedAt: &closedAt}
	if err := s.writeReconcileFlags(repo.Name, 9, seen, true, nil); err != nil {
		t.Fatal(err)
	}
	// PR 10: unchanged since the read, so the computed flags land.
	if err := s.ledger().Update(repo.Name, 10, func(v *outcomes.Outcome) {
		v.Final = true
	}); err != nil {
		t.Fatal(err)
	}
	seen10 := outcomes.Outcome{Repo: repo.Name, PR: 10, Final: true, MergedAt: nil}
	if err := s.writeReconcileFlags(repo.Name, 10, seen10, true, nil); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	changed := state.Latest[outcomes.Key(repo.Name, 9)]
	if changed.Final || changed.ReconcileFinished || changed.ReconcileDueAt != nil {
		t.Fatalf("evaluation-changed row: final=%t finished=%t due=%v", changed.Final, changed.ReconcileFinished, changed.ReconcileDueAt)
	}
	same := state.Latest[outcomes.Key(repo.Name, 10)]
	if !same.ReconcileFinished || same.ReconcileDueAt != nil {
		t.Fatalf("unchanged row: finished=%t due=%v", same.ReconcileFinished, same.ReconcileDueAt)
	}
}

func TestReconcilePullThatIsOpenDoesNotFinishRow(t *testing.T) {
	s, g, _, _, repo := setup(t)
	source := &outcomeGH{fakeGH: g, first: true}
	s.GH = source
	if err := s.ledger().Update(repo.Name, g.pull.Number, func(v *outcomes.Outcome) {
		v.Final, v.Decision, v.ReconcileFinished = true, "blocked", false
	}); err != nil {
		t.Fatal(err)
	}
	reconcile(t, s, repo, Options{Shadow: true})
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
	if v.Final || v.ReconcileFinished {
		t.Fatalf("open PR was finalized by reconciliation: final=%t finished=%t", v.Final, v.ReconcileFinished)
	}
}
