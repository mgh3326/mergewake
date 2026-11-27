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
	for _, n := range g.prs {
		if n == pr {
			return true
		}
	}
	return false
}

// Hold rows of the reviewer sweep (probe-cap holds, unresolved host checkouts,
// drain and budget holds; hk 6864, hk 6993) never queue a GitHub Pull, whatever
// their PR's state, and neither does a reviewer-reap row: the sweep covers its
// own rows.
func TestReapReviewerHeldRowsDoNotRefetchTheirPRs(t *testing.T) {
	gh := &pullLog{}
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, gh, r)
	holds := []string{
		reviewerLegacyProbeHold,
		fmt.Sprintf(reviewerHostProbeHold, "node-b"),
		reviewerHostsPendingPrefix + "node-a",
		reviewerHostsSeveralPrefix + "node-a, node-b",
		reviewerDrainHold,
		reviewerBudgetHold,
	}
	var rows []reapReceipt
	for i, reason := range holds {
		pr := 90 + i
		label := fmt.Sprintf("rv-%d", pr)
		rows = append(rows, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: pr,
			Host: "node-a", Label: label, Worktree: "/work/" + label + reviewerCheckoutSuffix,
			Result: "pending-cleanup", Reason: reason,
			Time: time.Now().Add(-25 * time.Hour).UTC()})
	}
	rows = append(rows, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 96,
		Host: "node-a", Label: "rv-96", Worktree: "/work/rv-96" + reviewerCheckoutSuffix,
		Result: "removed", Time: time.Now().Add(-25 * time.Hour).UTC()})
	// The base wrote a check-pr stamp per refetch: an old one left beside a
	// hold must not queue the PR either.
	rows = append(rows, reapReceipt{Action: "check-pr", Repo: testRepo.Name, PR: 90, Result: "checked",
		Time: time.Now().Add(-25 * time.Hour).UTC()})
	// Control: an ordinary reviewer-cleanup row still queues its PR's refetch.
	rows = append(rows, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 97,
		Host: "node-a", Label: "rv-97", Worktree: "/work/rv-97" + reviewerCheckoutSuffix,
		Result: "removed", Reason: "pane gone", Time: time.Now().Add(-25 * time.Hour).UTC()})
	seedReapRows(t, s, testRepo.Name, rows...)
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	for pr := 90; pr <= 96; pr++ {
		if gh.fetched(pr) {
			t.Errorf("hold or reviewer-reap row refetched PR %d; fetched %v", pr, gh.prs)
		}
	}
	if !gh.fetched(97) {
		t.Fatalf("ordinary reviewer-cleanup row did not refetch PR 97; fetched %v", gh.prs)
	}
}

// A check-pr stamp is a refetch record, never a refetch source on its own.
func TestReapCheckPRStampAloneNeverRefetches(t *testing.T) {
	gh := &pullLog{}
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, gh, r)
	seedReapRows(t, s, testRepo.Name, reapReceipt{Action: "check-pr", Repo: testRepo.Name, PR: 96,
		Result: "checked", Time: time.Now().Add(-25 * time.Hour).UTC()})
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if gh.fetched(96) {
		t.Fatalf("check-pr stamp alone refetched PR 96; fetched %v", gh.prs)
	}
}

// unlistedPullGH answers Pull for one merged PR; the merged listing never names
// it, so only the receipt naming it can lead reap to the PR.
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

// Acceptance (hk 6987 rule 2): a merged PR beyond the listing horizon, named by
// an ordinary reviewer-cleanup row (not a hold), is fetched on the first pass
// so its bound job's worktree is offered and removed. The stamp it writes keeps
// the next pass inside the backoff.
func TestReapReviewerOrdinaryRowFetchesBoundWorktree(t *testing.T) {
	s, _, run, _ := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	gh := &unlistedPullGH{}
	s.GH = gh
	seedReapRows(t, s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name,
		PR: jobRecordPR.Number, Host: "local", Label: "rv-12", Worktree: "/work/rv-12" + reviewerCheckoutSuffix,
		Result: "removed", Reason: "pane gone", Time: time.Now().Add(-25 * time.Hour).UTC()})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !gh.fetched(jobRecordPR.Number) {
		t.Fatalf("ordinary reviewer row's merged PR was not fetched; fetched %v", gh.prs)
	}
	if !run.called("git\x00-C\x00/repo\x00worktree\x00remove\x00/repo.wt") {
		t.Fatalf("bound job's worktree never offered for removal: %q", run.calls)
	}
	gh.prs = nil
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if gh.fetched(jobRecordPR.Number) {
		t.Fatalf("PR refetched again inside the backoff; fetched %v", gh.prs)
	}
}

// A failed Pull is retried once refetchAfter has passed: the attempt stamp
// holds the day's backoff, and nothing else keeps the PR out of the next day.
func TestReapReviewerFailedPullRetriedAfterBackoff(t *testing.T) {
	gh := &pullLog{}
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, gh, r)
	seedReapRows(t, s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name,
		PR: 97, Host: "node-a", Label: "rv-97", Worktree: "/work/rv-97" + reviewerCheckoutSuffix,
		Result: "removed", Reason: "pane gone", Time: time.Now().Add(-25 * time.Hour).UTC()})
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if n := len(gh.prs); n != 1 {
		t.Fatalf("first pass fetched %d times, want 1: %v", n, gh.prs)
	}
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if n := len(gh.prs); n != 1 {
		t.Fatalf("second pass inside the backoff fetched again: %v", gh.prs)
	}
	s.Now = func() time.Time { return time.Now().Add(25 * time.Hour) }
	_ = s.Reap(context.Background(), testRepo, 0, Options{})
	if n := len(gh.prs); n != 2 {
		t.Fatalf("failed PR not retried after the backoff: %v", gh.prs)
	}
}

// Worker rows naming an unlisted PR still refetch once the day's backoff has
// passed, even beside an old check-pr stamp.
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
