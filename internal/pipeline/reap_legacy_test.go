package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// legacyGH lists every closed and open PR for the legacy sweep, as the real
// client does without a recency window.
type legacyGH struct {
	reapGH
	closed []github.Pull
	open   []github.Pull
	// closedCalls counts the AllClosedPulls reads: a schedule test tells a
	// sweep that ran from one the interval skipped by it.
	closedCalls int
	// closedErr fails AllClosedPulls for the named repo.
	closedErr map[string]error
	// openByCall, when set, answers the nth OpenPulls call (the last entry for
	// later calls); onOpen runs at the start of each call.
	openByCall [][]github.Pull
	openCalls  int
	onOpen     func(call int)
}

func (g *legacyGH) AllClosedPulls(_ context.Context, repo string) ([]github.Pull, error) {
	g.closedCalls++
	if e := g.closedErr[repo]; e != nil {
		return nil, e
	}
	return g.closed, nil
}

func (g *legacyGH) OpenPulls(_ context.Context, _, branch string) ([]github.Pull, error) {
	if branch != "" {
		return nil, context.Canceled
	}
	g.openCalls++
	if g.onOpen != nil {
		g.onOpen(g.openCalls)
	}
	if n := len(g.openByCall); n > 0 {
		return g.openByCall[min(g.openCalls, n)-1], nil
	}
	return g.open, nil
}

func (g *legacyGH) Pull(_ context.Context, _ string, n int) (github.Pull, error) {
	for _, p := range g.closed {
		if p.Number == n {
			return p, nil
		}
	}
	return github.Pull{}, context.Canceled
}

const legacyCwd = "/old.wt"

// legacyPR is a PR of the repo whose branch wk/x was merged days ago, with the
// task trailer that binds it to job job-a.
func legacyPR(number int, daysAgo int, merged bool) github.Pull {
	p := github.Pull{
		Number: number, State: "closed", Body: "Work.\n\nMergewake-Task: 3074\n",
		Head: github.Ref{Ref: "wk/x", SHA: headSHA}, Base: github.Ref{Ref: "main"},
		MergeCommitSHA: "abc123",
	}
	p.Head.Repo.FullName = "example/project"
	if merged {
		p.Merged, p.MergedAt = true, rfc(time.Now().Add(-time.Duration(daysAgo)*24*time.Hour))
	}
	return p
}

// legacyFixture is a hub host node-a holding one linked worktree of the repo at
// legacyCwd, on branch wk/x at headSHA, with no pane in it, and PR 40 merged
// 20 days ago; hk binds task 3074 to job job-a, which ran in that worktree.
func legacyFixture(t *testing.T) (*Service, *fakeReapHub, *legacyGH) {
	t.Helper()
	hub := newFakeReapHub()
	hub.agents = map[string]any{"machine_id": "node-a", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true}
	hub.wtPaths = map[string]bool{legacyCwd: true}
	s, _ := hubSvc(t, hub, &scriptRunner{})
	gh := &legacyGH{closed: []github.Pull{legacyPR(40, 20, true)}}
	s.GH = gh
	s.Sink = &jobSink{
		jobs: map[string]string{"3074": "job-a"},
		recs: map[string]integrations.JobRecord{"job-a": {State: "completed", Host: "node-a", Cwd: legacyCwd, Label: "job-a"}},
	}
	return s, hub, gh
}

// legacyRows are the legacy-sweep rows of reap.jsonl.
func legacyRows(t *testing.T, s *Service) []map[string]any {
	t.Helper()
	var out []map[string]any
	if _, err := os.Stat(s.reapLog("example/project")); err != nil {
		return nil // nothing was written
	}
	for _, r := range reapLogRows(t, s, "example/project") {
		if r["action"] == legacyAction {
			out = append(out, r)
		}
	}
	return out
}

func legacyOnly(t *testing.T, s *Service) map[string]any {
	t.Helper()
	rows := legacyRows(t, s)
	if len(rows) != 1 {
		t.Fatalf("want one legacy-sweep row, got %v", rows)
	}
	return rows[0]
}

func apply() Options { return Options{} }
func plan() Options  { return Options{DryRun: true} }

func wantRow(t *testing.T, row map[string]any, result, reason string) {
	t.Helper()
	if row["result"] != result || row["reason"] != reason && !(reason == "" && row["reason"] == nil) {
		t.Fatalf("want %s/%q, got %v", result, reason, row)
	}
}

// (a) A clean worktree of a PR merged 20 days ago: the plan says would-remove
// and touches nothing; the apply removes it through worktree.remove.
func TestLegacySweepRemovesCleanOldMergedWorktree(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	row := legacyOnly(t, s)
	wantRow(t, row, "would-remove", legacyGuardNote)
	if row["host"] != "node" || row["worktree"] != legacyCwd || row["branch"] != "wk/x" || row["head"] != headSHA || row["pr"] != float64(40) {
		t.Fatalf("receipt shape: %v", row)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("plan sent worktree.remove %d times", n)
	}

	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["detached"] != false || removes[0]["cwd"] != legacyCwd || removes[0]["expect_head"] != headSHA || removes[0]["base"] != "main" {
		t.Fatalf("remove args: %#v", removes)
	}
	rows := legacyRows(t, s)
	if len(rows) != 2 {
		t.Fatalf("rows: %v", rows)
	}
	wantRow(t, rows[1], "removed", "")
}

// (b) The same worktree merged 10 days ago is too young.
func TestLegacySweepKeepsYoungMerge(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	gh.closed = []github.Pull{legacyPR(40, 10, true)}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "too-young")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a young worktree %d times", n)
	}
}

// (c) Cleanliness is the node guard's: a plan never probes it, and when the
// node answers dirty the worktree is refused, not removed.
func TestLegacySweepDirtyWorktreeIsTheNodesRefusal(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	hub.removeOutcome = "dirty"
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "would-remove", legacyGuardNote)
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("plan probed the node guard %d times", n)
	}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	rows := legacyRows(t, s)
	wantRow(t, rows[len(rows)-1], "refused", "dirty")
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove calls: %d", n)
	}
}

// (d) A PR closed without merge keeps its worktree while its task cannot be
// shown dropped or duplicate: with no task record the row is no-task.
func TestLegacySweepKeepsClosedUnmergedPR(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	gh.closed = []github.Pull{legacyPR(40, 20, false)}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "no-task")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a closed-unmerged worktree %d times", n)
	}
}

// (e) A worktree on a branch no PR of its task used has no PR; its task comes
// from the job record, and an unreadable one keeps it.
func TestLegacySweepKeepsWorktreeWithNoPR(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	p := legacyPR(40, 20, true)
	p.Head.Ref = "wk/other"
	gh.closed = []github.Pull{p}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "no-task")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a worktree with no PR %d times", n)
	}
}

// (f) A pane sitting in the worktree keeps it.
func TestLegacySweepKeepsWorktreeWithPane(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	hub.agents["panes"] = []any{map[string]any{"pane_id": "w3:p1", "agent_status": "idle", "cwd": legacyCwd}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "pane-in-use")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a worktree with a pane %d times", n)
	}
}

// (g) A node refusal is recorded with the node's reason, and a rerun does not
// ask the node again.
func TestLegacySweepRefusalIsFinal(t *testing.T) {
	for _, outcome := range []string{"ignored-files", "worktree-changed"} {
		t.Run(outcome, func(t *testing.T) {
			s, hub, _ := legacyFixture(t)
			hub.removeOutcome = outcome
			for i := 0; i < 2; i++ {
				if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
					t.Fatal(err)
				}
			}
			row := legacyOnly(t, s)
			if row["result"] != "refused" || !strings.HasPrefix(row["reason"].(string), outcome) {
				t.Fatalf("row: %v", row)
			}
			if n := len(hub.ops("worktree.remove")); n != 1 {
				t.Fatalf("rerun retried a refusal: %d calls", n)
			}
		})
	}
}

// (h) A head that is neither the merged head nor in the merged work keeps the
// worktree.
func TestLegacySweepKeepsMovedHead(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	hub.wtHead = "ffff9999"
	s.GH = &movedHeadGH{legacyGH: gh}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "head-moved")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a moved head %d times", n)
	}
}

type movedHeadGH struct{ *legacyGH }

func (*movedHeadGH) Ancestor(context.Context, string, string, string) (bool, error) {
	return false, nil
}

// (i) A rerun after the apply removes nothing and writes no second receipt.
func TestLegacySweepRerunIsIdempotent(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	for i := 0; i < 3; i++ {
		if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove calls: %d", n)
	}
	rows := legacyRows(t, s)
	if len(rows) != 1 {
		t.Fatalf("rows after reruns: %v", rows)
	}
	wantRow(t, rows[0], "removed", "")
	// A repeated plan writes its would-remove once as well.
	s2, _, _ := legacyFixture(t)
	for i := 0; i < 2; i++ {
		if err := s2.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
			t.Fatal(err)
		}
	}
	if rows := legacyRows(t, s2); len(rows) != 1 {
		t.Fatalf("plan rows: %v", rows)
	}
}

// A terminal receipt binds the checkout it names: the same branch and head
// stays suppressed, while a replacement checkout at the same path — another
// branch, or the same branch at another head — is evaluated again and its
// removal is recorded as a new receipt.
func TestLegacySweepTerminalReceiptBindsCheckoutIdentity(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "removed", "")
	// The same checkout stays suppressed on the next sweep.
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	if rows := legacyRows(t, s); len(rows) != 1 {
		t.Fatalf("same checkout re-evaluated: %v", rows)
	}
	// A replacement on another branch at the same path is evaluated: its row
	// says kept/no-task, not already-removed.
	hub.branches = map[string]string{legacyCwd: "wk/y"}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	rows := legacyRows(t, s)
	if len(rows) != 2 {
		t.Fatalf("replacement checkout not evaluated: %v", rows)
	}
	wantRow(t, rows[1], "kept", "no-task")
	if rows[1]["branch"] != "wk/y" {
		t.Fatalf("row: %v", rows[1])
	}

	// Same branch at another head is a different checkout too: evaluated and
	// removed again through the guard, and its removal is its own receipt, so
	// the journal's newest row names the head that was removed.
	s2, hub2, _ := legacyFixture(t)
	if err := s2.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	hub2.wtHead = "ffff9999"
	if err := s2.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	removes := hub2.ops("worktree.remove")
	if len(removes) != 2 || removes[1]["expect_head"] != "ffff9999" {
		t.Fatalf("moved head not re-evaluated: %#v", removes)
	}
	rows = legacyRows(t, s2)
	if len(rows) != 2 {
		t.Fatalf("second removal wrote no receipt: %v", rows)
	}
	wantRow(t, rows[0], "removed", "")
	wantRow(t, rows[1], "removed", "")
	if rows[0]["head"] != headSHA || rows[1]["head"] != "ffff9999" || rows[1]["branch"] != "wk/x" {
		t.Fatalf("removal receipts: %v", rows)
	}
}

// A terminal receipt written before heads were recorded has no head: it still
// suppresses the same branch, so old rows do not all re-evaluate once.
func TestLegacySweepTerminalReceiptWithoutHeadStillMatches(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	writeFile(t, s.reapLog("example/project"), `{"time":"2026-01-01T00:00:00Z","action":"legacy-sweep","repo":"example/project","host":"node","worktree":"/old.wt","branch":"wk/x","result":"removed"}`+"\n")
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	if rows := legacyRows(t, s); len(rows) != 1 {
		t.Fatalf("headless receipt re-evaluated: %v", rows)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("worktree.remove calls: %d", n)
	}
}

// A path an earlier sweep removed is quiet on later sweeps even though a bound
// job's cwd still reaches it: the worktree is no longer listed and inspection
// reports it gone, so there is nothing new to journal.
func TestLegacySweepGonePathReachedByJobCwdWritesNoRow(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	// A pane outside the worktree makes its common discoverable, so the first
	// sweep lists the worktree; the later ones find it gone and unlisted.
	hub.wtPaths["/repo.wt"] = true
	pane := []any{map[string]any{"pane_id": "w3:p1", "agent_status": "idle", "cwd": "/repo.wt"}}
	hub.agents["panes"], hub.agents["agents"] = pane, pane
	hub.lists = map[string][]map[string]any{listCommon: {listedRow(legacyCwd, "wk/x")}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "removed", "")
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove calls: %d", n)
	}
	// The node removed it: it is no longer listed and no longer inspects,
	// while the finished job's cwd still names the path.
	delete(hub.wtPaths, legacyCwd)
	hub.lists[listCommon] = nil
	for i := 0; i < 2; i++ {
		var out strings.Builder
		s.Out = &out
		if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
			t.Fatal(err)
		}
		if rows := legacyRows(t, s); len(rows) != 1 {
			t.Fatalf("sweep %d wrote a row for a removed path: %v", i+2, rows)
		}
		results, _ := summaryOf(t, out.String())["results"].(map[string]any)
		if _, ok := results["already-removed"].(map[string]any); !ok {
			t.Fatalf("sweep %d did not tally the gone path as already-removed: %s", i+2, out.String())
		}
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove calls after reruns: %d", n)
	}
}

// Worktrees of another repository and detached ones are listed, never removed;
// reviewer checkouts belong to the reviewer sweep and get no row.
func TestLegacySweepKeepsForeignDetachedAndSkipsReviewerCheckouts(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(*fakeReapHub)
		cwd   string
		want  string
	}{
		"other repo": {func(h *fakeReapHub) { h.foreign = map[string]bool{legacyCwd: true} }, legacyCwd, "other-repo"},
		"detached":   {func(h *fakeReapHub) { h.detached = map[string]bool{legacyCwd: true} }, legacyCwd, "detached"},
		"reviewer":   {func(h *fakeReapHub) { h.detached = map[string]bool{"/mwr-40-aaaa111-p1-1": true} }, "/mwr-40-aaaa111-p1-1", ""},
		// An attached mwr-* checkout (on the PR's own branch) is the reviewer
		// sweep's too, even once its PR is old enough.
		"attached reviewer": {func(h *fakeReapHub) { h.wtPaths["/mwr-40-aaaa111-p1-1"] = true }, "/mwr-40-aaaa111-p1-1", ""},
	} {
		t.Run(name, func(t *testing.T) {
			s, hub, _ := legacyFixture(t)
			tc.setup(hub)
			s.Sink.(*jobSink).recs["job-a"] = integrations.JobRecord{Host: "node-a", Cwd: tc.cwd}
			if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
				t.Fatal(err)
			}
			if n := len(hub.ops("worktree.remove")); n != 0 {
				t.Fatalf("removed %d", n)
			}
			rows := legacyRows(t, s)
			if tc.want == "" {
				if len(rows) != 0 {
					t.Fatalf("reviewer checkout got rows: %v", rows)
				}
				return
			}
			wantRow(t, legacyOnly(t, s), "kept", tc.want)
		})
	}
}

// A PR with no task trailer is not reachable: it is counted, not guessed at.
func TestLegacySweepCountsUnreachablePRs(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	loose := legacyPR(41, 30, true)
	loose.Body = "No trailer."
	gh.closed = append(gh.closed, loose)
	s.GH = &noTrailerGH{legacyGH: gh}
	var out strings.Builder
	s.Out = &out
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"no-task-trailer":1`) || !strings.Contains(out.String(), `"action":"legacy-sweep-summary"`) {
		t.Fatalf("output:\n%s", out.String())
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed %d", n)
	}
}

type noTrailerGH struct{ *legacyGH }

func (*noTrailerGH) Commit(context.Context, string, string) (github.Commit, error) {
	return github.Commit{}, nil
}

// (j) The daemon loop ignores legacy-sweep rows: a plan's receipt for an old PR
// neither refetches that PR nor adopts its worktree for removal.
func TestReapIgnoresLegacySweepRows(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	s.Now = func() time.Time { return time.Now().Add(72 * time.Hour) }
	s.GH.(*legacyGH).reapGH = reapGH{}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("daemon reap acted on a legacy-sweep row: %v", hub.ops("worktree.remove"))
	}
}

func forkPR(number int, ref string) github.Pull {
	p := github.Pull{Number: number, State: "open", Head: github.Ref{Ref: ref, SHA: "f0f0f0f0"}, Base: github.Ref{Ref: "main"}}
	p.Head.Repo.FullName = "someone/project"
	return p
}

// An open PR that appears on the branch while the sweep runs keeps the
// worktree: open PRs are re-read right before each removal.
func TestLegacySweepRereadsOpenPRsBeforeRemoval(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	opened := legacyPR(50, 0, false)
	opened.State = "open"
	gh.openByCall = [][]github.Pull{nil, {opened}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "open-pr")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a branch with an open PR %d times", n)
	}
	if gh.openCalls != 2 {
		t.Fatalf("open PR reads: %d", gh.openCalls)
	}
}

// Open PRs are matched by source repository: a fork's branch of the same name
// does not hold the worktree, the repo's own open PR does.
func TestLegacySweepOpenPRsMatchBySourceRepository(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	gh.open = []github.Pull{forkPR(51, "wk/x")}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("a fork's open PR blocked the removal: %d calls", n)
	}
	wantRow(t, legacyOnly(t, s), "removed", "")

	s2, hub2, gh2 := legacyFixture(t)
	own := legacyPR(52, 0, false)
	own.State = "open"
	gh2.open = []github.Pull{own}
	if err := s2.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s2), "kept", "open-pr")
	if n := len(hub2.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a branch with an open PR %d times", n)
	}
}

// A job whose cwd is inside the worktree finds its own refusal: rows are keyed
// by the worktree root the node reported.
func TestLegacySweepRefusalFoundByWorktreeRoot(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	sub := legacyCwd + "/sub"
	hub.wtPaths[sub] = true
	hub.tops = map[string]string{sub: legacyCwd}
	hub.removeOutcome = "ignored-files"
	s.Sink.(*jobSink).recs["job-a"] = integrations.JobRecord{Host: "node-a", Cwd: sub}
	for i := 0; i < 2; i++ {
		if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("rerun retried a refusal found under the root: %d calls", n)
	}
	row := legacyOnly(t, s)
	if row["result"] != "refused" || row["worktree"] != legacyCwd {
		t.Fatalf("row: %v", row)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }

// The receipt is appended before the row is printed: a failing stdout after a
// removal still leaves the removed row.
func TestLegacySweepReceiptIsWrittenBeforePrinting(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	s.Out = failingWriter{}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err == nil {
		t.Fatal("a failing stdout went unreported")
	}
	if n := len(hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove calls: %d", n)
	}
	wantRow(t, legacyOnly(t, s), "removed", "")
}

// The newest closed PR of the branch decides, even when it is bound to no job:
// a later closed-unmerged PR keeps the worktree of an older merged one, and
// with no dropped task behind it the row is no-task.
func TestLegacySweepNewestClosedPRDecidesEvenWithoutJob(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	later := legacyPR(41, 20, false)
	later.Body = "No trailer."
	gh.closed = append(gh.closed, later)
	s.GH = &noTrailerGH{legacyGH: gh}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	row := legacyOnly(t, s)
	wantRow(t, row, "kept", "no-task")
	if row["pr"] != float64(41) {
		t.Fatalf("row: %v", row)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed %d", n)
	}
}

// A PR with no head repository (a deleted fork) is not this repository's: it
// neither selects the worktree nor decides its age.
func TestLegacySweepIgnoresPRWithoutHeadRepository(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	orphan := legacyPR(41, 1, true)
	orphan.Head.Repo.FullName = ""
	gh.closed = append(gh.closed, orphan)
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	row := legacyOnly(t, s)
	wantRow(t, row, "would-remove", legacyGuardNote)
	if row["pr"] != float64(40) {
		t.Fatalf("selected the deleted fork's PR: %v", row)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("plan removed %d", n)
	}
}

// A checkout that left the matched branch between the first inspection and the
// removal keeps its worktree: the request carries only the head.
func TestLegacySweepReinspectsBeforeRemoval(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	gh.onOpen = func(call int) {
		if call == 2 { // the re-read right before the removal
			hub.mu.Lock()
			hub.detached = map[string]bool{legacyCwd: true}
			hub.mu.Unlock()
		}
	}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "worktree-changed")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a switched checkout %d times", n)
	}
}

// droppedFixture is legacyFixture with PR 40 closed unmerged and task 3074 in
// the given state, last updated updatedAgo ago (hk's close time for a task
// that ended dropped or duplicate).
func droppedFixture(t *testing.T, state string, updatedAgo time.Duration, closure string) (*Service, *fakeReapHub, *legacyGH) {
	t.Helper()
	s, hub, gh := legacyFixture(t)
	gh.closed = []github.Pull{legacyPR(40, 20, false)}
	task := client.Task{State: state, UpdatedAt: time.Now().Add(-updatedAgo)}
	if closure != "" {
		task.Refs.Closure = &client.TaskClosure{Type: closure, Evidence: "test"}
	}
	s.Sink.(*jobSink).tasks = map[string]client.Task{"3074": task}
	return s, hub, gh
}

// A worktree whose newest PR closed unmerged is removed once hk shows its task
// dropped or duplicate and closed long enough ago: the plan says would-remove
// and the apply goes through the guarded worktree.remove only.
func TestLegacySweepRemovesDroppedTaskWorktree(t *testing.T) {
	for _, tc := range []struct{ name, state, closure string }{
		{"dropped", "dropped", "obsolete"},
		{"dropped superseded", "dropped", "superseded"},
		{"duplicate", "duplicate", "superseded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, hub, _ := droppedFixture(t, tc.state, 20*24*time.Hour, tc.closure)
			if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
				t.Fatal(err)
			}
			row := legacyOnly(t, s)
			wantRow(t, row, "would-remove", legacyGuardNote)
			if row["task"] != "3074" || row["pr"] != float64(40) {
				t.Fatalf("row: %v", row)
			}
			if n := len(hub.ops("worktree.remove")); n != 0 {
				t.Fatalf("plan sent worktree.remove %d times", n)
			}
			if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
				t.Fatal(err)
			}
			removes := hub.ops("worktree.remove")
			if len(removes) != 1 || removes[0]["cwd"] != legacyCwd || removes[0]["expect_head"] != headSHA || removes[0]["base"] != "main" || removes[0]["detached"] != false {
				t.Fatalf("remove args: %#v", removes)
			}
			rows := legacyRows(t, s)
			wantRow(t, rows[len(rows)-1], "removed", "")
		})
	}
}

// A branch with no PR at all resolves its task through the job record whose
// cwd is the worktree, and a dropped task removes it the same way.
func TestLegacySweepRemovesNoPRDroppedTaskWorktree(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	p := legacyPR(40, 20, true)
	p.Head.Ref = "wk/other"
	gh.closed = []github.Pull{p}
	s.Sink.(*jobSink).tasks = map[string]client.Task{"3074": {State: "dropped", UpdatedAt: time.Now().Add(-20 * 24 * time.Hour)}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	row := legacyOnly(t, s)
	if row["result"] != "removed" || row["pr"] != float64(0) {
		t.Fatalf("row: %v", row)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != legacyCwd {
		t.Fatalf("remove args: %#v", removes)
	}
}

// A task that is not dropped or duplicate keeps its worktree, named by state.
func TestLegacySweepKeepsNonDroppedTask(t *testing.T) {
	for _, state := range []string{"reviewing", "developing", "merged", "backlog"} {
		t.Run(state, func(t *testing.T) {
			s, hub, _ := droppedFixture(t, state, 20*24*time.Hour, "")
			if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
				t.Fatal(err)
			}
			wantRow(t, legacyOnly(t, s), "kept", "task-"+state)
			if n := len(hub.ops("worktree.remove")); n != 0 {
				t.Fatalf("removed a %s task's worktree %d times", state, n)
			}
		})
	}
}

// A dropped task closed inside legacyMinAge keeps its worktree.
func TestLegacySweepKeepsYoungTaskClose(t *testing.T) {
	s, hub, _ := droppedFixture(t, "dropped", 10*24*time.Hour, "obsolete")
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "task-too-young")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a recently closed task's worktree %d times", n)
	}
}

// The same guards as the merged path apply: a dirty listed status or a head
// not on the remote keeps the worktree as local-only.
func TestLegacySweepDroppedKeepsLocalOnly(t *testing.T) {
	t.Run("dirty", func(t *testing.T) {
		s, hub, _ := droppedFixture(t, "dropped", 20*24*time.Hour, "obsolete")
		// A pane in another repo worktree seeds the common so worktree.list
		// answers the dirty row for the candidate.
		hub.agents["agents"] = []any{map[string]any{"pane_id": "w1:p9", "agent_status": "idle", "cwd": "/repo.wt"}}
		hub.agents["panes"] = []any{map[string]any{"pane_id": "w1:p9", "agent_status": "idle", "cwd": "/repo.wt"}}
		hub.lists = map[string][]map[string]any{listCommon: {
			withStatus(listedRow(legacyCwd, "wk/x"), map[string]any{"dirty": true, "untracked": 0, "ignored": 0}),
		}}
		if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
			t.Fatal(err)
		}
		wantRow(t, legacyOnly(t, s), "kept", "local-only")
		if n := len(hub.ops("worktree.remove")); n != 0 {
			t.Fatalf("removed a dirty worktree %d times", n)
		}
	})
	t.Run("unpushed", func(t *testing.T) {
		s, hub, gh := droppedFixture(t, "dropped", 20*24*time.Hour, "obsolete")
		hub.wtHead = "ffff9999"
		s.GH = &movedHeadGH{legacyGH: gh}
		if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
			t.Fatal(err)
		}
		wantRow(t, legacyOnly(t, s), "kept", "local-only")
		if n := len(hub.ops("worktree.remove")); n != 0 {
			t.Fatalf("removed an unpushed worktree %d times", n)
		}
	})
}

// A pane in the worktree keeps it even when the task is dropped.
func TestLegacySweepDroppedKeepsPaneInUse(t *testing.T) {
	s, hub, _ := droppedFixture(t, "dropped", 20*24*time.Hour, "obsolete")
	hub.agents["panes"] = []any{map[string]any{"pane_id": "w3:p1", "agent_status": "idle", "cwd": legacyCwd}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "pane-in-use")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a pane's worktree %d times", n)
	}
}

// A task read that fails is kept, not guessed at; the failure is reported
// like any other per-candidate error.
func TestLegacySweepKeepsTaskReadError(t *testing.T) {
	s, hub, _ := droppedFixture(t, "dropped", 20*24*time.Hour, "obsolete")
	s.Sink.(*jobSink).taskEr = map[string]error{"3074": errors.New("hk down")}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err == nil || !strings.Contains(err.Error(), "task 3074") {
		t.Fatalf("want the task read error reported, got %v", err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "task-read-error")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed on a failed task read %d times", n)
	}
}

// Two job records naming the same worktree for different tasks is ambiguous:
// the worktree is kept rather than guessed at. Both tasks are bound through
// merged PRs on other branches, so the worktree's own branch has no PR.
func TestLegacySweepKeepsAmbiguousTaskMapping(t *testing.T) {
	s, hub, gh := legacyFixture(t)
	p1 := legacyPR(40, 20, true)
	p1.Head.Ref = "wk/a1"
	p2 := legacyPR(41, 20, true)
	p2.Head.Ref = "wk/a2"
	p2.Body = "Work.\n\nMergewake-Task: 9999\n"
	gh.closed = []github.Pull{p1, p2}
	s.Sink.(*jobSink).jobs["9999"] = "job-b"
	s.Sink.(*jobSink).recs["job-b"] = integrations.JobRecord{State: "completed", Host: "node-a", Cwd: legacyCwd, Label: "job-b"}
	s.Sink.(*jobSink).tasks = map[string]client.Task{
		"3074": {State: "dropped", UpdatedAt: time.Now().Add(-20 * 24 * time.Hour)},
		"9999": {State: "dropped", UpdatedAt: time.Now().Add(-20 * 24 * time.Hour)},
	}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "no-task")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed an ambiguous worktree %d times", n)
	}
}

// A job record on another host names the same absolute path but not this
// worktree: the foreign record cannot identify the task, so the row stays
// no-task even though its task is dropped.
func TestLegacySweepForeignHostJobRecordKeepsNoTask(t *testing.T) {
	pr := listPR(41, "wk/other", 20)
	pr.Body = "Work.\n\nMergewake-Task: 9999\n"
	s, hub := listFixture(t, []map[string]any{listedRow("/b.wt", "wk/b")}, pr)
	sink := s.Sink.(*jobSink)
	sink.jobs["9999"] = "job-b"
	sink.recs["job-b"] = integrations.JobRecord{State: "completed", Host: "node-b", Cwd: "/b.wt", Label: "job-b"}
	sink.tasks = map[string]client.Task{"9999": {State: "dropped", UpdatedAt: time.Now().Add(-20 * 24 * time.Hour)}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, rowFor(t, s, "/b.wt"), "kept", "no-task")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed on a foreign host's record %d times", n)
	}
}

// The newest closed PR's binding failed this pass, so its task is unknown:
// the older job record bound to the same worktree cannot stand in, and the
// row stays no-task even though that task is dropped.
func TestLegacySweepKeepsWhenNewestPRBindingFails(t *testing.T) {
	s, hub, gh := droppedFixture(t, "dropped", 20*24*time.Hour, "obsolete")
	newer := legacyPR(41, 20, false)
	newer.Body = "No trailer."
	gh.closed = append(gh.closed, newer)
	gh.commitErr = map[string]error{headSHA: errors.New("commit read failed")}
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err == nil || !strings.Contains(err.Error(), "PR 41 job binding") {
		t.Fatalf("want the binding error reported, got %v", err)
	}
	wantRow(t, legacyOnly(t, s), "kept", "no-task")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed on an unknown newest-PR task %d times", n)
	}
}

// jobRecordNames matches a record to a worktree only on the host the job ran
// on; a recorded worktree that disagrees on branch or repo vetoes the cwd
// match (paths get reused), and an origin that does not parse to owner/name
// never matches — head and branch alone are not enough across repos.
func TestJobRecordNamesHostAndOrigin(t *testing.T) {
	host := reap.Host{Name: "node", Machine: "node-a"}
	branch := "wk/b"
	oldBranch := "wk/old"
	wt := &reap.Worktree{Path: "/b.wt", Branch: "wk/b", Head: headSHA, Repo: "example/project"}
	for _, tc := range []struct {
		name string
		rec  *integrations.JobRecord
		want bool
	}{
		{"cwd on machine id", &integrations.JobRecord{Host: "node-a", Cwd: "/b.wt"}, true},
		{"cwd on host name", &integrations.JobRecord{Host: "node", Cwd: "/b.wt"}, true},
		{"cwd in a subdirectory", &integrations.JobRecord{Host: "node-a", Cwd: "/b.wt/sub"}, true},
		{"cwd foreign host", &integrations.JobRecord{Host: "node-b", Cwd: "/b.wt"}, false},
		{"cwd no host", &integrations.JobRecord{Cwd: "/b.wt"}, false},
		{"worktree same repo", &integrations.JobRecord{Host: "node-a", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/project.git", Branch: &branch, Head: headSHA}}, true},
		{"worktree https origin", &integrations.JobRecord{Host: "node-a", Worktree: &integrations.JobWorktree{Origin: "https://github.com/example/project", Branch: &branch, Head: headSHA}}, true},
		{"worktree local origin", &integrations.JobRecord{Host: "node-a", Worktree: &integrations.JobWorktree{Origin: "/src/project", Branch: &branch, Head: headSHA}}, false},
		{"worktree other repo", &integrations.JobRecord{Host: "node-a", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/other.git", Branch: &branch, Head: headSHA}}, false},
		{"worktree detached record", &integrations.JobRecord{Host: "node-a", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/project.git", Head: headSHA}}, false},
		{"worktree foreign host", &integrations.JobRecord{Host: "node-b", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/project.git", Branch: &branch, Head: headSHA}}, false},
		{"reused path other branch", &integrations.JobRecord{Host: "node-a", Cwd: "/b.wt", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/project.git", Branch: &oldBranch, Head: "cccc3333"}}, false},
		{"reused path other repo", &integrations.JobRecord{Host: "node-a", Cwd: "/b.wt", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/other.git", Branch: &branch, Head: headSHA}}, false},
		{"advanced head same branch", &integrations.JobRecord{Host: "node-a", Cwd: "/b.wt", Worktree: &integrations.JobWorktree{Origin: "git@github.com:example/project.git", Branch: &branch, Head: "spawn1111"}}, true},
	} {
		if got := jobRecordNames(tc.rec, wt, host); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
