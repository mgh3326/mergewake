package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
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
	return g.count(pr) > 0
}

func (g *pullLog) count(pr int) int {
	n := 0
	for _, p := range g.prs {
		if p == pr {
			n++
		}
	}
	return n
}

// Reviewer sweep rows hold their own PRs' checkouts. A probe-cap hold whose PR
// the merged listing missed queues at most one GitHub Pull over its life: the
// stamp that refetch writes keeps every later pass from asking again. A PR
// already stamped by an old check-pr row is never asked.
func TestReapReviewerHeldRowsRefetchAtMostOnce(t *testing.T) {
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
	// The base wrote a check-pr stamp per refetch: an old one left by a
	// reviewer hold must not queue the PR either.
	rows = append(rows, reapReceipt{Action: "check-pr", Repo: testRepo.Name, PR: 90, Result: "checked",
		Time: time.Now().Add(-25 * time.Hour).UTC()})
	seedReapRows(t, s, testRepo.Name, rows...)
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if gh.fetched(90) {
		t.Fatalf("stamped reviewer PR 90 was refetched; fetched %v", gh.prs)
	}
	for pr := 91; pr <= 93; pr++ {
		if n := gh.count(pr); n != 1 {
			t.Fatalf("reviewer PR %d fetched %d times over two passes; fetched %v", pr, n, gh.prs)
		}
	}
}

// unlistedPullGH answers Pull for one merged PR, and the merged listing never
// names it: only the receipt that names it can lead reap to the PR.
type unlistedPullGH struct {
	reapGH
	pullLog
}

func (g *unlistedPullGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return nil, nil
}

func (g *unlistedPullGH) Pull(ctx context.Context, repo string, pr int) (github.Pull, error) {
	if pr == jobRecordPR.Number {
		g.prs = append(g.prs, pr)
		return jobRecordPR, nil
	}
	return g.pullLog.Pull(ctx, repo, pr)
}

// Acceptance 1 (hk 6987): a merged PR beyond the listing horizon, named only by
// a reviewer-cleanup row, is fetched once so its bound job's worktree is offered
// and removed. A later pass does not fetch it again.
func TestReapReviewerOnlyPRFetchesBoundWorktree(t *testing.T) {
	s, _, run, _ := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	gh := &unlistedPullGH{}
	s.GH = gh
	seedReapRows(t, s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name,
		PR: jobRecordPR.Number, Host: "local", Label: "rv-12", Worktree: "/work/rv-12" + reviewerCheckoutSuffix,
		Result: "pending-cleanup", Reason: reviewerLegacyProbeHold, Time: time.Now().Add(-25 * time.Hour).UTC()})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !gh.fetched(jobRecordPR.Number) {
		t.Fatalf("reviewer-only merged PR was not fetched; fetched %v", gh.prs)
	}
	if !run.called("git\x00-C\x00/repo\x00worktree\x00remove\x00/repo.wt") {
		t.Fatalf("bound job's worktree never offered for removal: %q", run.calls)
	}
	gh.prs = nil
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if gh.fetched(jobRecordPR.Number) {
		t.Fatalf("reviewer-only PR refetched again on a later pass; fetched %v", gh.prs)
	}
}

// Acceptance 2 (hk 6987): a PR named only by reviewer rows, with an old
// check-pr stamp, is never fetched on any pass. The stamp is a refetch record,
// not a refetch source.
func TestReapReviewerOnlyPRWithOldStampIsNotRefetched(t *testing.T) {
	s, _, _, _ := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	gh := &unlistedPullGH{}
	s.GH = gh
	old := time.Now().Add(-25 * time.Hour).UTC()
	seedReapRows(t, s, testRepo.Name,
		reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: jobRecordPR.Number, Host: "local",
			Label: "rv-12", Worktree: "/work/rv-12" + reviewerCheckoutSuffix,
			Result: "pending-cleanup", Reason: reviewerLegacyProbeHold, Time: old},
		reapReceipt{Action: "check-pr", Repo: testRepo.Name, PR: jobRecordPR.Number, Result: "checked", Time: old})
	for pass := 0; pass < 2; pass++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if gh.fetched(jobRecordPR.Number) {
		t.Fatalf("reviewer-only PR with an old check-pr stamp was fetched; fetched %v", gh.prs)
	}
}

// A worker row naming a PR the merged listing missed still refetches it once
// the day's backoff has passed, even beside an old check-pr stamp.
func TestReapWorkerRowUnlistedPRIsRefetched(t *testing.T) {
	gh := &pullLog{}
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, gh, r)
	seedReapRows(t, s, testRepo.Name,
		reapReceipt{Action: "close-pane", Repo: testRepo.Name, PR: 77, Host: "local", Worktree: "/stale.wt", Result: "closed", Time: time.Now().Add(-25 * time.Hour).UTC()},
		reapReceipt{Action: "check-pr", Repo: testRepo.Name, PR: 77, Result: "checked", Time: time.Now().Add(-25 * time.Hour).UTC()},
	)
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if !gh.fetched(77) {
		t.Fatalf("worker row's unlisted PR 77 was not refetched; fetched %v", gh.prs)
	}
}
