package pipeline

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// A replacement of a lost reviewer leaves out the quota pool that reviewer ran
// on. The first spawn of the pass carries no exclusion.
func TestReplacementAfterLostDevinExcludesDevinPool(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner", Pool: "devin"})
	if slices.Contains(runner.spawns[0], "--exclude-pool") {
		t.Fatalf("first spawn excludes a pool: %v", runner.spawns[0])
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	if got := arg(runner.spawns[1], "--exclude-pool"); got != "devin" {
		t.Fatalf("replacement --exclude-pool = %q, spawn %v", got, runner.spawns[1])
	}
	rows := auditRows(t, s, r, "review-replacement-pools")
	if len(rows) != 1 || fmt.Sprint(rows[0]["excluded_pools"]) != "[devin]" || fmt.Sprint(rows[0]["unknown_pools"]) != "[]" {
		t.Fatalf("replacement pools audit = %v", rows)
	}
}

// A lost reviewer whose hk record names no pool excludes nothing: the
// replacement spawns as it always did, and the audit names the unknown pool.
func TestReplacementWithUnknownPoolSpawnsWithoutExclusion(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	if slices.Contains(runner.spawns[1], "--exclude-pool") {
		t.Fatalf("replacement excludes a pool it could not read: %v", runner.spawns[1])
	}
	rows := auditRows(t, s, r, "review-replacement-pools")
	if len(rows) != 1 || fmt.Sprint(rows[0]["unknown_pools"]) != "["+old.Label+"]" || fmt.Sprint(rows[0]["excluded_pools"]) != "[]" {
		t.Fatalf("replacement pools audit = %v", rows)
	}
}

// When the excluded pool leaves panewire no candidate, the replacement is
// placed once more without the exclusion, and that retry is audited.
func TestReplacementNoCandidateRetriesOnceWithoutExclusion(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner", Pool: "devin"})
	runner.noCandidateExcluded = true
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 3 {
		t.Fatalf("spawns = %d, want first, refused replacement, retry: %v", len(runner.spawns), runner.spawns)
	}
	if got := arg(runner.spawns[1], "--exclude-pool"); got != "devin" {
		t.Fatalf("refused replacement --exclude-pool = %q", got)
	}
	if slices.Contains(runner.spawns[2], "--exclude-pool") || arg(runner.spawns[2], "-l") != arg(runner.spawns[1], "-l") {
		t.Fatalf("retry must keep the label and drop the exclusion: %v", runner.spawns[2])
	}
	if arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
		t.Fatalf("replacement did not finish: posts=%d spawns=%v", g.postCount, runner.spawns)
	}
	if rows := auditRows(t, s, r, "review-replacement-pool-fallback"); len(rows) != 1 || fmt.Sprint(rows[0]["excluded_pools"]) != "devin" {
		t.Fatalf("fallback audit = %v", rows)
	}
}

func TestValidPoolAcceptsOnlyPanewireShapes(t *testing.T) {
	for in, want := range map[string]string{"devin": "devin", "devin-free": "devin-free", "copilot.v2": "copilot.v2", "": "", "devin paid": "", "-devin": "", "a,b": ""} {
		if got := validPool(in); got != want {
			t.Errorf("validPool(%q) = %q, want %q", in, got, want)
		}
	}
}

// replacedPools reads only the reviewers the pass replaced on this head: each
// pool once, sorted, and the unknown ones named apart.
func TestReplacedPoolsOnHead(t *testing.T) {
	replaced := []replacedReviewer{
		{Label: "a", Head: shaA, Pool: "devin"},
		{Label: "b", Head: shaA, Pool: "codex"},
		{Label: "c", Head: shaA, Pool: "devin"},
		{Label: "d", Head: shaA},
		{Label: "e", Head: shaB, Pool: "claude"},
	}
	pools, unknown := replacedPools(replaced, shaA)
	if !slices.Equal(pools, []string{"codex", "devin"}) || !slices.Equal(unknown, []string{"d"}) {
		t.Fatalf("pools=%v unknown=%v", pools, unknown)
	}
}

func TestNoCandidateRefusalKeysOnExitAndText(t *testing.T) {
	refusal := &integrations.CommandError{Command: "panewire", Err: panewireExit(81), Stderr: "no placement candidate accepted spawn; no local fallback"}
	if !noCandidateRefusal(nil, refusal) {
		t.Fatal("exit 81 with the placement refusal text is no candidate")
	}
	if noCandidateRefusal(nil, panewireExit(81)) {
		t.Fatal("exit 81 without its text is not known to be no candidate")
	}
	if noCandidateRefusal([]byte("no placement candidate accepted"), panewireExit(77)) {
		t.Fatal("another exit is not no candidate")
	}
}
