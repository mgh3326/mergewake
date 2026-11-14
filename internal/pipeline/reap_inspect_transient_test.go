package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// sshTransport is what the runner returns when ssh itself fails (exit 255): git
// never answered, so nothing proves the worktree is gone.
var sshTransport = errors.New("ssh failed: exit status 255")

// revParseOf is the rev-parse key Exec.Inspect issues for cwd.
func revParseOf(cwd string) string {
	return key("git", "-C", cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")
}

// countCalls counts runner calls whose key is exactly k.
func countCalls(r *scriptRunner, k string) int {
	n := 0
	for _, c := range r.calls {
		if c == k {
			n++
		}
	}
	return n
}

// A transient inspect failure in a reap pass is recorded as
// worktree-inspect-error, never worktree-gone, and the next pass inspects the
// worktree again.
func TestReapTransientInspectIsRetriedNotGone(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo"), errs: map[string]error{revParseOf("/live.wt"): sshTransport}}
	s := svc(t, &reapGH{}, r)
	seedReapRows(t, s, "example/project",
		reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 12, Host: "local", Worktree: "/live.wt", Cwd: "/live.wt", Result: "skipped", Reason: "pane-in-use"},
	)
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err == nil || !strings.Contains(err.Error(), "inspect /live.wt") {
		t.Fatalf("reap pass error = %v, want the inspect failure reported", err)
	}
	var reasons []string
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["worktree"] == "/live.wt" && row["action"] == "remove-worktree" {
			reasons = append(reasons, row["reason"].(string))
		}
	}
	if strings.Join(reasons, ",") != "pane-in-use,worktree-inspect-error" {
		t.Fatalf("live worktree rows = %q, want the transient failure recorded as worktree-inspect-error", reasons)
	}
	first := countCalls(r, revParseOf("/live.wt"))

	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err == nil {
		t.Fatal("second pass swallowed the inspect failure")
	}
	if countCalls(r, revParseOf("/live.wt")) <= first {
		t.Fatalf("worktree not inspected again on the next pass: %q", r.calls)
	}
}

// A pane scan whose inspect fails must not cache a nil that the job-record
// source reads as job-worktree-gone: the job's worktree is still unknown, so
// the pass reports the failure instead of treating the worktree as gone.
func TestJobRecordAfterFailedScanIsNotGone(t *testing.T) {
	s, _, run, _ := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	_, panes := paneJSON("/repo")
	run.outs[key("herdr", "pane", "list")] = panes
	run.errs = map[string]error{revParseOf("/repo.wt"): sshTransport}
	err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true})
	if err == nil || !strings.Contains(err.Error(), "worktree /repo.wt") {
		t.Fatalf("job worktree after a failed scan = %v, want a job-worktree error", err)
	}
}
