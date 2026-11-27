package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// pullLog records every PR a pass asks GitHub for by number. It answers with
// an error, like failPullGH, so a pass that refetches simply loses that PR.
type pullLog struct {
	reapGH
	prs []int
}

func (g *pullLog) Pull(_ context.Context, _ string, pr int) (github.Pull, error) {
	g.prs = append(g.prs, pr)
	return github.Pull{}, fmt.Errorf("fetch %d failed", pr)
}

func (g *pullLog) fetched(pr int) bool {
	for _, n := range g.prs {
		if n == pr {
			return true
		}
	}
	return false
}

// Reviewer sweep rows hold their own PRs' checkouts. A probe-cap hold whose PR
// the merged listing missed must not queue a GitHub Pull: the sweep covers
// its own rows whatever the PR's state.
func TestReapReviewerHeldRowsDoNotRefetchTheirPRs(t *testing.T) {
	gh := &pullLog{}
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, gh, r)
	var rows []reapReceipt
	for pr := 90; pr <= 93; pr++ {
		label := fmt.Sprintf("rv-%d", pr)
		rows = append(rows, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: pr,
			Host: "node-a", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
			Result: "pending-cleanup", Reason: reviewerLegacyProbeHold,
			Time: time.Now().Add(-25 * time.Hour).UTC()})
	}
	seedReapRows(t, s, testRepo.Name, rows...)
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	for pr := 90; pr <= 93; pr++ {
		if gh.fetched(pr) {
			t.Fatalf("held reviewer row refetched PR %d; fetched %v", pr, gh.prs)
		}
	}
}

// A worker row naming a PR the merged listing missed still refetches it once
// the day's backoff has passed.
func TestReapWorkerRowUnlistedPRIsRefetched(t *testing.T) {
	gh := &pullLog{}
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, gh, r)
	seedReapRows(t, s, testRepo.Name,
		reapReceipt{Action: "close-pane", Repo: testRepo.Name, PR: 77, Host: "local", Worktree: "/stale.wt", Result: "closed", Time: time.Now().Add(-25 * time.Hour).UTC()},
	)
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if !gh.fetched(77) {
		t.Fatalf("worker row's unlisted PR 77 was not refetched; fetched %v", gh.prs)
	}
}
