package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

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
