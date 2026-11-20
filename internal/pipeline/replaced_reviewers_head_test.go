package pipeline

import (
	"context"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// A durable replaced list kept under an earlier head names reviewers of a head
// that moved. A fresh spawn of the pass on the current head must not carry them:
// no pool is excluded and no replacement-pools row is audited for them.
func TestStaleHeadReplacedReviewersAreNotCarried(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
	s, _, r := panewireSetup(t, runner)
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	stale := []replacedReviewer{{Label: "old-reviewer", Head: shaB, Pool: "devin"}}
	if err := s.saveReplacedReviewers(r.Name, 7, 1, stale); err != nil {
		t.Fatal(err)
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 || slices.Contains(runner.spawns[0], "--exclude-pool") {
		t.Fatalf("fresh spawn after a moved head excludes a stale pool: %v", runner.spawns)
	}
	if rows := auditRows(t, s, r, "review-replacement-pools"); len(rows) != 0 {
		t.Fatalf("stale-head replacement audited on the new head: %v", rows)
	}
}
