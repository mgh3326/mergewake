package pipeline

import (
	"context"
	"encoding/json"
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

// workerCommon is the git common dir the fixture's clone is listed by.
const workerCommon = "/work/project/.git"

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
// and the worker's linked worktree.
func workerFixture(t *testing.T, prs ...github.Pull) (*Service, *fakeReapHub, *workerGH) {
	t.Helper()
	hub := newFakeReapHub()
	hub.agents["agents"], hub.agents["panes"] = []any{}, []any{}
	hub.wtHead = headSHA
	hub.wtPaths = map[string]bool{workerCwd: true}
	hub.branches = map[string]string{workerCwd: "wk/job"}
	hub.commons = map[string]string{workerCwd: workerCommon}
	hub.lists = map[string][]map[string]any{workerCommon: {
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
// a healthy watch verdict. The clone is listed by its git common dir.
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
	lists := hub.ops("worktree.list")
	if len(lists) == 0 || lists[0]["common"] != workerCommon {
		t.Fatalf("worktree.list args: %#v", lists)
	}
	rows := workerLogRows(t, s)
	if len(rows) != 1 || rows[0]["result"] != "removed" || rows[0]["source"] != workerSource {
		t.Fatalf("rows: %v", rows)
	}
}

// A host with no node mapping never falls back to ssh or local commands: its
// checkouts stay and one row names the refusal.
func TestWorkerWorktreeNonHubHostIsRefused(t *testing.T) {
	outs := worktreeOuts("/repo")
	outs[key("git", "-C", "/repo.wt", "branch", "--show-current")] = "wk/x\n"
	r := &scriptRunner{outs: outs}
	s := svc(t, &workerGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "hub-required" && row["host"] == "local" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("hub-required row missing")
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

// Unmerged work goes when its PR closed unmerged (or never existed) and the job
// encoded by wk/<job> is terminal; a live job keeps it.
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

// A closed PR that binds another job never stands in for the branch's own job:
// the checkout is kept while its own job is live.
func TestWorkerWorktreeConflictingJobBindingKeeps(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, false, ""))
	s.Sink.(*jobSink).jobs["3074"] = "old"
	s.Sink.(*jobSink).recs["old"] = integrations.JobRecord{State: "completed", Host: "node-a", Label: "old"}
	jobState(s, "job", "working")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removes: %d", n)
	}
	if rows := workerLogRows(t, s); len(rows) != 1 || rows[0]["reason"] != "job-conflict" {
		t.Fatalf("rows: %v", rows)
	}
}

// A merge whose list body carries no merge_commit_sha resolves it through Pull
// and still removes.
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

// Each guard failure keeps the worktree and names its reason.
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

// A pane in the worktree keeps it.
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

// The disposable list is passed to the guarded removal, so the node that takes
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
	common := "/work/project/.git"
	hub.wtPaths = map[string]bool{"/work/a.wt": true, "/work/b.wt": true, "/work/c.wt": true}
	hub.branches = map[string]string{"/work/a.wt": "wk/a", "/work/b.wt": "wk/b", "/work/c.wt": "wk/c"}
	hub.commons = map[string]string{"/work/a.wt": common, "/work/b.wt": common, "/work/c.wt": common}
	hub.lists = map[string][]map[string]any{common: {
		listedRow("/work/project", "main"),
		listedRow("/work/a.wt", "wk/a"),
		listedRow("/work/b.wt", "wk/b"),
		listedRow("/work/c.wt", "wk/c"),
	}}
	s, _ := hubSvc(t, hub, &scriptRunner{})
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
		t.Fatalf("removes %d, want the per-host cap", n)
	}
	// The next pass takes the next one.
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 2 {
		t.Fatalf("removes after the second pass %d", n)
	}
}

// A clone guessed from <work_root>/<repo> belongs to this repository only when
// its worktrees' origin says so; another owner's clone is left alone, and the
// same-named clone is never listed as this repo.
func TestWorkerWorktreeForeignCloneIsNotTouched(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.foreign = map[string]bool{workerCwd: true}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removes: %d", n)
	}
	if rows := workerLogRows(t, s); len(rows) != 0 {
		t.Fatalf("rows: %v", rows)
	}
}

// A stale first registration is inconclusive: the clone's identity is proven by
// the next registered candidate, which is then removed.
func TestWorkerWorktreeStaleFirstRegistrationDoesNotDropTheClone(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.lists[workerCommon] = []map[string]any{
		listedRow("/work/project", "main"),
		listedRow("/work/gone.wt", "wk/gone"),
		listedRow(workerCwd, "wk/job"),
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != workerCwd {
		t.Fatalf("removes: %#v", removes)
	}
}

// A clone whose every registration is stale (its directory is gone) is skipped
// for the pass: no pass error, nothing removed, and one skipped row per UTC day.
func TestWorkerWorktreeStaleOnlyCloneIsSkippedNotAnError(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.lists[workerCommon] = []map[string]any{
		listedRow("/work/project", "main"),
		listedRow("/work/gone.wt", "wk/gone"),
	}
	for pass := 1; pass <= 2; pass++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatalf("pass %d error: %v", pass, err)
		}
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removes: %d", n)
	}
	var skips []map[string]any
	for _, r := range reapLogRows(t, s, "example/project") {
		if r["reason"] == "repository-identity-inconclusive" {
			skips = append(skips, r)
		}
	}
	if len(skips) != 1 || skips[0]["result"] != "skipped" {
		t.Fatalf("skipped rows %d, want one per UTC day: %v", len(skips), skips)
	}
}

// The stale-only skip is one row per clone per UTC day, not one per pass: the
// second UTC day gets its own row, and a pass within a day writes nothing.
func TestWorkerWorktreeStaleOnlyCloneWritesOneRowPerUTCDay(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.lists[workerCommon] = []map[string]any{
		listedRow("/work/project", "main"),
		listedRow("/work/gone.wt", "wk/gone"),
	}
	skipRows := func() int {
		n := 0
		for _, r := range reapLogRows(t, s, "example/project") {
			if r["reason"] == "repository-identity-inconclusive" {
				n++
			}
		}
		return n
	}
	day1 := time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	passes := []struct {
		at   time.Time
		want int
	}{
		{day1, 1},
		{day1.Add(time.Hour), 1},
		{day2, 2},
		{day2.Add(time.Hour), 2},
	}
	for i, p := range passes {
		s.Now = func() time.Time { return p.at }
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatalf("pass %d error: %v", i+1, err)
		}
		if n := skipRows(); n != p.want {
			t.Fatalf("pass %d at %s: skipped rows %d, want %d", i+1, p.at.Format(time.DateOnly), n, p.want)
		}
	}
}

// The stale-only skip is its own action, so no worker-worktree or author-sweep
// tally counts the main clone as a made worktree.
func TestWorkerWorktreeStaleOnlyCloneIsOutsideWorktreeTallies(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.lists[workerCommon] = []map[string]any{
		listedRow("/work/project", "main"),
		listedRow("/work/gone.wt", "wk/gone"),
	}
	day := time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return day }
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(reapLogRows(t, s, "example/project")); n != 1 {
		t.Fatalf("reap rows %d, want the one skipped row", n)
	}
	var scanned int
	if err := s.workerWorktreeScan(s.Config.Repos, func(workerWorktreeRow) { scanned++ }); err != nil {
		t.Fatal(err)
	}
	if scanned != 0 {
		t.Fatalf("worker worktree rows %d, want 0", scanned)
	}
	res, err := s.livenessForDay(s.Config.Repos, day.Format(reviewerSummaryDay))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range res.Mechanisms {
		if m.Mechanism == MechAuthorWorktreeSweep && m.Made != 0 {
			t.Fatalf("author-worktree-sweep made %d, want 0", m.Made)
		}
	}
}

// A stale registration beside one whose inspect errors still reports the error:
// an unproven clone is never admitted, and the failure is not hidden as stale.
func TestWorkerWorktreeErroredInspectStillReported(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	hub.lists[workerCommon] = []map[string]any{
		listedRow("/work/project", "main"),
		listedRow("/work/gone.wt", "wk/gone"),
		listedRow("/work/err.wt", "wk/err"),
	}
	hub.inspectErr = map[string]string{"/work/err.wt": "worktree_unavailable"}
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "/work/err.wt") {
		t.Fatalf("error %v, want the inspect failure of /work/err.wt", err)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removes: %d", n)
	}
}

// A past removed row does not hide a worktree that is checked out again.
func TestWorkerWorktreeRecreatedAtSamePathIsEvaluated(t *testing.T) {
	s, hub, _ := workerFixture(t, workerPR(40, true, "abc123"))
	jobState(s, "job", "completed")
	row, _ := json.Marshal(reapReceipt{Action: "remove-worktree", Repo: "example/project", PR: 40, Host: "node",
		Worktree: workerCwd, Branch: "wk/job", Head: headSHA, Result: "removed"})
	if err := os.MkdirAll(filepath.Dir(s.reapLog("example/project")), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.reapLog("example/project"), append(row, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("removes: %d", n)
	}
}

// guardFake is a hub host whose inspected identity a test controls.
type guardFake struct {
	reap.Ops
	head    string
	removes int
}

func (f *guardFake) AllPanes(context.Context) ([]reap.Pane, error) { return nil, nil }
func (f *guardFake) Inspect(_ context.Context, path string) (*reap.Worktree, error) {
	return &reap.Worktree{Path: path, Common: workerCommon, Branch: "wk/job", Head: f.head, Repo: "example/project"}, nil
}
func (f *guardFake) WorktreeRemoveDisposable(context.Context, string, string, string, []string, []string, bool) (reap.WorktreeRemoveResult, error) {
	f.removes++
	return reap.WorktreeRemoveResult{Outcome: "removed"}, nil
}

// The candidate tuple is revalidated immediately before the guarded removal:
// a checkout whose head moved on is kept.
func TestWorkerWorktreeIdentityRevalidatedBeforeRemoval(t *testing.T) {
	f := &guardFake{head: "moved"}
	w := &workerPass{s: &Service{Config: repoConfig(t.TempDir())}, r: testRepo, acting: true}
	hs := &reapHost{host: reap.Host{Name: "node", Machine: "node-a"}, ops: f, viaHub: true, trees: map[string]*reap.Worktree{}}
	cand := workerCand{path: workerCwd, branch: "wk/job", head: headSHA, common: workerCommon}
	rec := &reapReceipt{Action: "remove-worktree", Repo: "example/project", Host: "node", Worktree: cand.path, Branch: cand.branch}
	var errs []error
	if got := w.remove(context.Background(), hs, cand, rec, &errs); got.Result != "skipped" || got.Reason != "worktree-changed" {
		t.Fatalf("%v %v", got, errs)
	}
	if f.removes != 0 {
		t.Fatal("removal dispatched for a changed checkout")
	}
	f.head = headSHA
	if got := w.remove(context.Background(), hs, cand, rec, &errs); got.Result != "removed" {
		t.Fatalf("%v %v", got, errs)
	}
	if f.removes != 1 {
		t.Fatalf("removes %d", f.removes)
	}
}

// The older merged-author path never removes a wk/ checkout: the worker pass
// owns it, in whole-repo and PR-scoped passes alike.
func TestNoExecRemovalForWorkerWorktree(t *testing.T) {
	for _, pr := range []int{0, 12} {
		r := &scriptRunner{outs: panegoneWorktreeOuts("/repo")}
		s := svc(t, &workerBranchGH{}, r)
		seedWorktreeReceipt(t, s, "example/project", "/repo.wt")
		if err := s.Reap(context.Background(), testRepo, pr, Options{}); err != nil {
			t.Fatalf("pr %d: %v", pr, err)
		}
		if r.called("worktree\x00remove") || r.called("branch\x00-d") {
			t.Fatalf("pr %d removed a wk/ checkout: %q", pr, r.calls)
		}
	}
}

// workerBranchGH is a merged pull on the worktree's own wk/ branch, so the
// older path is offered the checkout the worker pass owns.
type workerBranchGH struct{ reapGH }

func (workerBranchGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	p := mergedPull
	p.Head.Ref = "wk/x"
	return []github.Pull{p}, nil
}

func (workerBranchGH) Pull(context.Context, string, int) (github.Pull, error) {
	p := mergedPull
	p.Head.Ref = "wk/x"
	return p, nil
}

// The older merged-author path checks processes before its next removal.
func TestMergedPathProcessCheck(t *testing.T) {
	orig := mergedProcessEvidence
	t.Cleanup(func() { mergedProcessEvidence = orig })
	mergedProcessEvidence = func(context.Context, *Service) (procEvidence, error) {
		return procEvidence{cwds: []string{"/repo.wt/sub"}}, nil
	}
	outs := panegoneWorktreeOuts("/repo")
	keyBranch := key("git", "-C", "/repo.wt", "branch", "--show-current")
	outs[keyBranch] = "feat/x\n"
	outs[key("git", "-C", "/repo", "merge-base", "--is-ancestor", "feat/x", "origin/main")] = ""
	outs[key("git", "-C", "/repo", "branch", "-d", "feat/x")] = ""
	delete(outs, key("git", "-C", "/repo", "worktree", "remove", "/repo.wt"))
	r := &scriptRunner{outs: outs}
	s := svc(t, &featGH{}, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("removed a checkout a process holds")
	}
	var saw bool
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "process-in-use" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("process-in-use not recorded")
	}
}

// featGH is the merged pull on a non-worker branch the older path cleans.
type featGH struct{ reapGH }

func (featGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	p := mergedPull
	p.Head.Ref = "feat/x"
	return []github.Pull{p}, nil
}
