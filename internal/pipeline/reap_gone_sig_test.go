package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A worktree a prior pass proved gone is recorded as skipped|worktree-gone.
// Every candidate source must treat that row as terminal, or the gone worktree
// is inspected again on every pass.

// The receipt source (reap.go, the candidate loop over receipts): PR 12 has a
// gone worktree and a live one. The gone one must not be inspected again; the
// live one still is, so the PR stays a real candidate.
func TestReapSkipsGoneWorktreeReceiptCandidate(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &reapGH{}, r)
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 12, Host: "local", Worktree: "/gone.wt", Cwd: "/gone.wt", Result: "skipped", Reason: "worktree-gone"},
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 12, Host: "local", Worktree: "/live.wt", Cwd: "/live.wt", Result: "skipped", Reason: "pane-in-use"},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if r.called("/gone.wt") {
		t.Fatalf("gone worktree inspected again: %q", r.calls)
	}
	if !r.called("/live.wt") {
		t.Fatalf("live receipt worktree was not offered: %q", r.calls)
	}
}

// The job-record source: the task's job record names /repo.wt, which a prior
// pass already proved gone. It must not be inspected again.
func TestReapSkipsGoneWorktreeJobRecordCandidate(t *testing.T) {
	s, _, run, _ := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 12, Host: "local", Worktree: "/repo.wt", Cwd: "/repo.wt", Result: "skipped", Reason: "worktree-gone"},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if run.called("/repo.wt") {
		t.Fatalf("job-record gone worktree inspected again: %q", run.calls)
	}
}

// prDone: a PR whose only named worktree is proved gone is terminal, so the
// pass must not refetch it from GitHub (the refetch would fail here).
func TestReapSkipsGoneWorktreeTerminalPR(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &failPullGH{}, r)
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 77, Host: "local", Worktree: "/gone.wt", Cwd: "/gone.wt", Result: "skipped", Reason: "worktree-gone", Time: time.Now().Add(-25 * time.Hour).UTC()},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatalf("gone-worktree PR was refetched: %v", err)
	}
	if strings.Contains(strings.Join(r.calls, "\n"), "/gone.wt") {
		t.Fatalf("gone worktree inspected: %q", r.calls)
	}
}

// settledLowPR is a merged PR of the same task numbered below jobRecordPR, so
// reap resolves the shared job for it first. It is on another branch.
var settledLowPR = github.Pull{
	Number: 11, State: "closed", Merged: true,
	Body:     "Earlier PR of the same task.\n\nMergewake-Task: 4669\n",
	Head:     github.Ref{Ref: "wk/y", SHA: "feedfeed"},
	Base:     github.Ref{Ref: "main"},
	MergedAt: rfc(time.Now().Add(-8 * 24 * time.Hour)), MergeCommitSHA: "def456",
}

// settledLowGH lists settledLowPR beside the job's own PR.
type settledLowGH struct{ siblingGH }

func (g *settledLowGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return []github.Pull{jobRecordPR, settledLowPR}, nil
}

// A terminal row is per PR. PR 11 already settled /repo.wt (and still has a
// live receipt, so it is evaluated), but PR 12, whose branch the worktree is
// on, has no such row: PR 11's settled result must not be reused for PR 12,
// which still inspects the worktree and is offered it.
func TestReapSettledJobWorktreeIsPerPR(t *testing.T) {
	s, _, run, _ := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	gh := &settledLowGH{siblingGH{reverts: map[int]int{}}}
	s.GH = gh
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 11, Host: "local", Worktree: "/repo.wt", Cwd: "/repo.wt", Result: "skipped", Reason: "worktree-gone"},
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 11, Host: "local", Worktree: "/live.wt", Cwd: "/live.wt", Result: "skipped", Reason: "pane-in-use"},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if !run.called("/live.wt") {
		t.Fatalf("PR 11 was not evaluated, so the test proves nothing: %q", run.calls)
	}
	if !run.called("/repo.wt") {
		t.Fatalf("PR 12 reused PR 11's settled result: %q", run.calls)
	}
	if gh.reverts[12] != 1 {
		t.Fatalf("owner PR 12 revert checks = %d, want 1", gh.reverts[12])
	}
}
