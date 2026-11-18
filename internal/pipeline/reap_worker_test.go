package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// workerCwd is the worker worktree the hub fixture lists: a wk/<job> branch
// under the host's work root, named by no pane, receipt or job record.
const workerCwd = "/work/project.job"

// workerGH is the closed/open inventory a worker pass reads, plus the revert
// watch the merged rule consults.
type workerGH struct {
	legacyGH
	recovery *github.Pull
}

func (g *workerGH) WatchRevert(context.Context, string, int, string) (*github.Pull, error) {
	return g.recovery, nil
}

// workerPR is a PR of the worker branch wk/<job>.
func workerPR(number int, merged bool, mergeSHA string) github.Pull {
	p := github.Pull{
		Number: number, State: "closed", Body: "Work.\n\nMergewake-Task: 3074\n",
		Head: github.Ref{Ref: "wk/job", SHA: headSHA}, Base: github.Ref{Ref: "main"},
		MergeCommitSHA: mergeSHA,
	}
	p.Head.Repo.FullName = "example/project"
	if merged {
		p.Merged, p.MergedAt = true, rfc(time.Now().Add(-time.Hour))
	}
	return p
}

// workerFixture is one hub host whose repository clone holds the main checkout
// and the worker's linked worktree, with i<0 panes anywhere.
func workerFixture(t *testing.T, prs ...github.Pull) (*Service, *fakeReapHub, *workerGH) {
	t.Helper()
	hub := newFakeReapHub()
	hub.agents["agents"], hub.agents["panes"] = []any{}, []any{}
	hub.wtHead = headSHA
	hub.wtPaths = map[string]bool{workerCwd: true}
	hub.branches = map[string]string{workerCwd: "wk/job"}
	hub.lists = map[string][]map[string]any{"/work/project": {
		listedRow("/work/project", "main"),
		listedRow(workerCwd, "wk/job"),
	}}
	s, _ := hubSvc(t, hub, &scriptRunner{})
	s.Config.Hosts[0].WorkRoot = "/work"
	gh := &workerGH{legacyGH: legacyGH{closed: prs}}
	s.GH = gh
	s.Sink = &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
	return s, hub, gh
}

func workerLogRows(t *testing.T, s *Service) []map[string]any {
	t.Helper()
	var out []map[string]any
	if _, err := os.Stat(s.reapLog("example/project")); err != nil {
		return nil
	}
	for _, r := range reapLogRows(t, s, "example/project") {
		if r["action"] == "remove-worktree" && r["source"] == workerSource {
			out = append(out, r)
		}
	}
	return out
}

func jobState(s *Service, job, state string) {
	s.Sink.(*jobSink).recs[job] = integrations.JobRecord{State: state, Host: "node-a", Cwd: workerCwd, Label: job,
		Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/project.git", Branch: new("wk/job")}}
}

// A worktree on a wk/ branch is discovered from the host's own worktree list,
// with no pane, receipt or job record naming it, and removed once its merge has
// a healthy watch verdict.
func TestWorkerWorktreeDiscoveryNeedsNoPaneReceiptOrJob(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != workerCwd || removes[0]["expect_head"] != headSHA {
		t.Fatalf("removes: %#v", removes)
	}
	rows := workerLogRows(t, s)
	if len(rows) != 1 || rows[0]["result"] != "removed" || rows[0]["source"] != workerSource {
		t.Fatalf("rows: %v", rows)
	}
}

// The watch verdict decides, not the clock: a merge with no green main yet
// keeps its worktree.
func TestWorkerWorktreeWithoutWatchVerdictStays(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	if err := os.Remove(filepath.Join(s.state("example/project"), "last-green")); err != nil {
		t.Fatal(err)
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed without a verdict: %d", n)
	}
	rows := workerLogRows(t, s)
	if len(rows) != 1 || rows[0]["result"] != "skipped" || rows[0]["reason"] != "watch-pending" {
		t.Fatalf("rows: %v", rows)
	}
}

// A healthy verdict on main after the merge is what releases the worktree.
func TestWorkerWorktreeHealthyVerdictRemoves(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	greenHead(t, s, testRepo, "later-green")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("removes: %d", n)
	}
}

// A reverted PR keeps its worktree while its recovery PR is open, and until
// the revert's own merge has a healthy verdict; then it may go.
func TestWorkerWorktreeRevertedWaitsForFollowUp(t *testing.T) {
	recoveryLive := func() *github.Pull {
		return &github.Pull{Number: 99, State: "open"}
	}
	recoveryMerged := func() *github.Pull {
		return &github.Pull{Number: 99, State: "closed", Merged: true,
			MergedAt: rfc(time.Now().Add(-time.Hour)), MergeCommitSHA: "rev99"}
	}
	t.Run("recovery open", func(t *testing.T) {
		s, hub, gh := workerFixture(t, workerPR(40, true, "abc123"))
		jobState(s, "job", "completed")
		gh.recovery = recoveryLive()
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if n := len(hub.ops("worktree.remove")); n != 0 {
			t.Fatalf("removes: %d", n)
		}
		if rows := workerLogRows(t, s); len(rows) != 1 || rows[0]["reason"] != "recovery-open" {
			t.Fatalf("rows: %v", rows)
		}
	})
	t.Run("reverted, revert not green", func(t *testing.T) {
		s, hub, gh := workerFixture(t, workerPR(40, true, "abc123"))
		jobState(s, "job", "completed")
		gh.recovery = recoveryMerged()
		gh.notAncestor = map[[2]string]bool{{"rev99", mergedPull.MergeCommitSHA}: true}
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if n := len(hub.ops("worktree.remove")); n != 0 {
			t.Fatalf("removes: %d", n)
		}
		if rows := workerLogRows(t, s); len(rows) != 1 || rows[0]["reason"] != "reverted" {
			t.Fatalf("rows: %v", rows)
		}
	})
	t.Run("reverted, follow-up green", func(t *testing.T) {
		s, hub, gh := workerFixture(t, workerPR(40, true, "abc123"))
		jobState(s, "job", "completed")
		gh.recovery = recoveryMerged()
		greenHead(t, s, testRepo, "rev99")
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if n := len(hub.ops("worktree.remove")); n != 1 {
			t.Fatalf("removes: %d", n)
		}
	})
}

// Unmerged work goes when its PR closed unmerged (or never existed) and the hk
// job is terminal; a live job keeps it.
func TestWorkerWorktreeUnmergedNeedsTerminalJob(t *testing.T) {
	t.Run("closed unmerged, job completed", func(t *testing.T) {
		s, hub, _ := workerFixture(t, workerPR(40, false, ""))
		jobState(s, "job", "completed")
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if n := len(hub.ops("worktree.remove")); n != 1 {
			t.Fatalf("removes: %d", n)
		}
	})
	t.Run("no PR, job lost", func(t *testing.T) {
		s, hub, _ := workerFixture(t)
		jobState(s, "job", "lost")
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if n := len(hub.ops("worktree.remove")); n != 1 {
			t.Fatalf("removes: %d", n)
		}
	})
	t.Run("no PR, live job", func(t *testing.T) {
		s, hub, _ := workerFixture(t)
		jobState(s, "job", "working")
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if n := len(hub.ops("worktree.remove")); n != 0 {
			t.Fatalf("removes: %d", n)
		}
		if rows := workerLogRows(t, s); len(rows) != 1 || rows[0]["reason"] != "job-active:working" {
			t.Fatalf("rows: %v", rows)
		}
	})
}

// A merge whose list body carries no merge_commit_sha resolves it through Pull
// and still removes: the merge-commit-unknown skip had no live cause left.
func TestWorkerWorktreeMergeCommitResolvedThroughPull(t *testing.T) {
	s, hub, gh := workerFixture(t, workerPR(40, true, ""))
	jobState(s, "job", "completed")
	gh.closed[0].MergeCommitSHA = "abc123"
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("removes: %d", n)
	}
	for _, row := range workerLogRows(t, s) {
		if row["reason"] == "merge-commit-unknown" {
			t.Fatalf("merge commit stayed unknown: %v", row)
		}
	}
}

// Each guard failure keeps the worktree and names its reason; the unpushed
// commit is the case that must never leave.
func TestWorkerWorktreeGuardKeepsWithReason(t *testing.T) {
	for _, tc := range []struct{ name, outcome, want string }{
		{"unpushed", "unmerged-or-unpushed", "unpushed"},
		{"dirty", "dirty", "dirty"},
		{"locked", "locked", "locked"},
		{"ignored", "ignored-files", "ignored-files"},
		{"busy", "busy", "busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
			jobState(s, "job", "completed")
			hub.removeOutcome = tc.outcome
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			rows := workerLogRows(t, s)
			if len(rows) != 1 || rows[0]["result"] != "skipped" {
				t.Fatalf("rows: %v", rows)
			}
			if reason, _ := rows[0]["reason"].(string); !strings.HasPrefix(reason, tc.want) {
				t.Fatalf("reason %q, want %q", reason, tc.want)
			}
		})
	}
}

// A pane in the worktree keeps it, before any hk read or removal.
func TestWorkerWorktreePaneInUseStays(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.agents["panes"] = []any{map[string]any{"pane_id": "w3:p1", "agent_status": "idle", "cwd": workerCwd}}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removes: %d", n)
	}
	if rows := workerLogRows(t, s); len(rows) != 1 || rows[0]["reason"] != "pane-in-use" {
		t.Fatalf("rows: %v", rows)
	}
}

// The disposable list is passed to the guarded removal, so a node that takes
// it may remove a checkout holding only the report draft.
func TestWorkerWorktreeDisposablePassedToGuard(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 {
		t.Fatalf("removes: %d", len(removes))
	}
	if got := removes[0]["disposable"]; got == nil || !strings.Contains(strings.Join(toStrings(got), ","), "report.draft.md") {
		t.Fatalf("disposable arg: %#v", removes[0])
	}
}

func toStrings(v any) []string {
	rows, _ := v.([]any)
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// The per-host cap holds: one host's backlog drains in bounded steps.
func TestWorkerWorktreePerHostCap(t *testing.T) {
	hub := newFakeReapHub()
	hub.agents["agents"], hub.agents["panes"] = []any{}, []any{}
	hub.wtHead = headSHA
	hub.wtPaths = map[string]bool{"/work/a.wt": true, "/work/b.wt": true, "/work/c.wt": true}
	hub.branches = map[string]string{"/work/a.wt": "wk/a", "/work/b.wt": "wk/b", "/work/c.wt": "wk/c"}
	hub.lists = map[string][]map[string]any{"/work/project": {
		listedRow("/work/project", "main"),
		listedRow("/work/a.wt", "wk/a"),
		listedRow("/work/b.wt", "wk/b"),
		listedRow("/work/c.wt", "wk/c"),
	}}
	s, out := hubSvc(t, hub, &scriptRunner{})
	s.Config.Hosts[0].WorkRoot = "/work"
	s.Config.Reap.WorktreeDrain = config.ReviewerDrain{PerHost: 1}
	s.GH = &workerGH{legacyGH: legacyGH{}}
	s.Sink = &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
	for _, job := range []string{"a", "b", "c"} {
		s.Sink.(*jobSink).recs[job] = integrations.JobRecord{State: "completed", Host: "node-a", Cwd: "/work/" + job + ".wt"}
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("removes %d, want the per-host cap: %s", n, out.String())
	}
	// The next pass takes the next one.
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 2 {
		t.Fatalf("removes after the second pass %d", n)
	}
}

// The guarded removal's own disposable list allows an untracked report draft
// and still refuses any other untracked file.
func TestExecWorktreeRemoveDisposableList(t *testing.T) {
	outs := func(remoteHead, untracked string) map[string]string {
		return map[string]string{
			key("git", "-C", "/repo.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.wt\n/repo/.git\n/repo.wt\n",
			key("git", "-C", "/repo.wt", "branch", "--show-current"):                                               "wk/x\n",
			key("git", "-C", "/repo.wt", "rev-parse", "HEAD"):                                                      headSHA + "\n",
			key("git", "-C", "/repo.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
			key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                           "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.wt\nHEAD " + headSHA + "\nbranch refs/heads/wk/x\n",
			key("git", "-C", "/repo.wt", "status", "--porcelain", "--untracked-files=normal"):                      untracked,
			key("git", "-C", "/repo.wt", "status", "--porcelain", "--untracked-files=no"):                          "",
			key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored"):                                     "",
			key("rm", "-rf", "/repo.wt/report.draft.md"):                                                           "",
			key("git", "-C", "/repo", "ls-remote", "--heads", "origin", "refs/heads/wk/x"):                         remoteHead + "\trefs/heads/wk/x\n",
			key("git", "-C", "/repo", "worktree", "remove", "/repo.wt"):                                            "",
		}
	}
	run := func(remoteHead, untracked string) (*scriptRunner, reap.Exec) {
		r := &scriptRunner{outs: outs(remoteHead, untracked)}
		return r, reap.Exec{Runner: r, Host: reap.Host{Name: "local"}}
	}
	t.Run("disposable draft allowed", func(t *testing.T) {
		r, e := run(headSHA, "?? report.draft.md\n")
		got, err := e.WorktreeRemoveDisposable(context.Background(), "/repo.wt", headSHA, "main", nil, []string{"report.draft.md"}, false)
		if err != nil || got.Outcome != "removed" {
			t.Fatalf("%v %v", got, err)
		}
		if !r.called("worktree\x00remove\x00/repo.wt") {
			t.Fatal("worktree not removed")
		}
	})
	t.Run("other untracked blocks", func(t *testing.T) {
		r, e := run(headSHA, "?? notes.md\n")
		got, err := e.WorktreeRemoveDisposable(context.Background(), "/repo.wt", headSHA, "main", nil, []string{"report.draft.md"}, false)
		if err != nil || got.Outcome != "dirty" {
			t.Fatalf("%v %v", got, err)
		}
		if r.called("worktree\x00remove") || r.called("rm\x00-rf") {
			t.Fatal("blocked worktree was touched")
		}
	})
	t.Run("unpushed head blocks", func(t *testing.T) {
		_, e := run("ffff0000", "")
		got, err := e.WorktreeRemoveDisposable(context.Background(), "/repo.wt", headSHA, "main", nil, nil, false)
		if err != nil || got.Outcome != "unpushed" {
			t.Fatalf("%v %v", got, err)
		}
	})
}

// A refusal keeps the draft: disposable files are deleted only immediately
// before the removal, so ignored-files and unpushed checkouts keep them.
func TestExecWorktreeRemoveDisposableKeepsDraftOnRefusal(t *testing.T) {
	base := func(remoteHead, ignored, untracked string) map[string]string {
		return map[string]string{
			key("git", "-C", "/repo.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.wt\n/repo/.git\n/repo.wt\n",
			key("git", "-C", "/repo.wt", "branch", "--show-current"):                                               "wk/x\n",
			key("git", "-C", "/repo.wt", "rev-parse", "HEAD"):                                                      headSHA + "\n",
			key("git", "-C", "/repo.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
			key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                           "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.wt\nHEAD " + headSHA + "\nbranch refs/heads/wk/x\n",
			key("git", "-C", "/repo.wt", "status", "--porcelain", "--untracked-files=normal"):                      untracked,
			key("git", "-C", "/repo.wt", "status", "--porcelain", "--untracked-files=no"):                          "",
			key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored"):                                     ignored,
			key("rm", "-rf", "/repo.wt/report.draft.md"):                                                           "",
			key("git", "-C", "/repo", "ls-remote", "--heads", "origin", "refs/heads/wk/x"):                         remoteHead + "\trefs/heads/wk/x\n",
			key("git", "-C", "/repo", "merge-base", "--is-ancestor", headSHA, "origin/main"):                       "",
			key("git", "-C", "/repo", "worktree", "remove", "/repo.wt"):                                            "",
		}
	}
	t.Run("ignored files keep the draft", func(t *testing.T) {
		r := &scriptRunner{outs: base(headSHA, "!! .env\n", "?? report.draft.md\n")}
		e := reap.Exec{Runner: r, Host: reap.Host{Name: "local"}}
		got, err := e.WorktreeRemoveDisposable(context.Background(), "/repo.wt", headSHA, "main", nil, []string{"report.draft.md"}, false)
		if err != nil || got.Outcome != "ignored-files" {
			t.Fatalf("%v %v", got, err)
		}
		if r.called("rm\x00-rf") {
			t.Fatal("draft deleted from a kept worktree")
		}
	})
	t.Run("unpushed keeps the draft", func(t *testing.T) {
		outs := base("ffff0000", "", "?? report.draft.md\n")
		delete(outs, key("git", "-C", "/repo", "merge-base", "--is-ancestor", headSHA, "origin/main"))
		r := &scriptRunner{outs: outs}
		e := reap.Exec{Runner: r, Host: reap.Host{Name: "local"}}
		got, err := e.WorktreeRemoveDisposable(context.Background(), "/repo.wt", headSHA, "main", nil, []string{"report.draft.md"}, false)
		if err != nil || got.Outcome != "unpushed" {
			t.Fatalf("%v %v", got, err)
		}
		if r.called("rm\x00-rf") {
			t.Fatal("draft deleted from a kept worktree")
		}
	})
}

// A merged branch whose remote ref was deleted (delete-branch-on-merge) passes
// on base ancestry instead of being kept as unpushed for ever.
func TestExecWorktreeRemoveUsesBaseWhenRemoteBranchIsGone(t *testing.T) {
	r := &scriptRunner{outs: map[string]string{
		key("git", "-C", "/repo.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/repo.wt\n/repo/.git\n/repo.wt\n",
		key("git", "-C", "/repo.wt", "branch", "--show-current"):                                               "wk/x\n",
		key("git", "-C", "/repo.wt", "rev-parse", "HEAD"):                                                      headSHA + "\n",
		key("git", "-C", "/repo.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", "/repo", "worktree", "list", "--porcelain"):                                           "worktree /repo\nHEAD bbbb\nbranch refs/heads/main\n\nworktree /repo.wt\nHEAD " + headSHA + "\nbranch refs/heads/wk/x\n",
		key("git", "-C", "/repo.wt", "status", "--porcelain", "--untracked-files=normal"):                      "",
		key("git", "-C", "/repo.wt", "status", "--porcelain", "--untracked-files=no"):                          "",
		key("git", "-C", "/repo.wt", "status", "--porcelain", "--ignored"):                                     "",
		key("git", "-C", "/repo", "ls-remote", "--heads", "origin", "refs/heads/wk/x"):                         "",
		key("git", "-C", "/repo", "merge-base", "--is-ancestor", headSHA, "origin/main"):                       "",
		key("git", "-C", "/repo", "worktree", "remove", "/repo.wt"):                                            "",
	}}
	e := reap.Exec{Runner: r, Host: reap.Host{Name: "local"}}
	got, err := e.WorktreeRemoveDisposable(context.Background(), "/repo.wt", headSHA, "main", nil, nil, false)
	if err != nil || got.Outcome != "removed" {
		t.Fatalf("%v %v", got, err)
	}
}

// A refused local branch delete is a row, not a pass error.
type branchDeleteFake struct {
	reap.Ops
	delErr error
	path   string
}

func (f *branchDeleteFake) AllPanes(context.Context) ([]reap.Pane, error) { return nil, nil }
func (f *branchDeleteFake) Inspect(context.Context, string) (*reap.Worktree, error) {
	return &reap.Worktree{Path: f.path, Common: "/repo/.git", Branch: "wk/job", Head: headSHA, Repo: "example/project"}, nil
}
func (f *branchDeleteFake) WorktreeRemoveDisposable(context.Context, string, string, string, []string, []string, bool) (reap.WorktreeRemoveResult, error) {
	return reap.WorktreeRemoveResult{Outcome: "removed"}, nil
}
func (f *branchDeleteFake) DeleteBranch(context.Context, string, string) error { return f.delErr }

func TestWorkerBranchDeleteRefusalIsARow(t *testing.T) {
	var rows []reapReceipt
	f := &branchDeleteFake{delErr: errors.New("exit status 1"), path: "/work/project.job"}
	w := &workerPass{s: &Service{Config: repoConfig(t.TempDir())}, r: testRepo, acting: true,
		note: func(v reapReceipt) { rows = append(rows, v) }}
	hs := &reapHost{host: reap.Host{Name: "node", SSH: "ops@node.example"}, ops: f, trees: map[string]*reap.Worktree{}}
	rec := &reapReceipt{Action: "remove-worktree", Repo: "example/project", Host: "node", Worktree: f.path, Branch: "wk/job"}
	var errs []error
	got := w.remove(context.Background(), hs, workerCand{path: f.path, branch: "wk/job", head: headSHA, common: "/repo"}, rec, &errs)
	if got.Result != "removed" || len(errs) != 0 {
		t.Fatalf("%v %v", got, errs)
	}
	if len(rows) != 1 || rows[0].Action != "delete-branch" || rows[0].Result != "skipped" || !strings.HasPrefix(rows[0].Reason, "refused: ") {
		t.Fatalf("rows: %#v", rows)
	}
}
