package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

const listCommon = "/repo/.git"

// listedRow is one worktree.list row of a clean worktree.
func listedRow(path, branch string) map[string]any {
	return map[string]any{"path": path, "branch": branch, "head": headSHA, "detached": false,
		"status": map[string]any{"dirty": false, "untracked": 0, "ignored": 0}}
}

func withStatus(row map[string]any, status map[string]any) map[string]any {
	row["status"] = status
	return row
}

// listPR is a PR of the repo whose branch was merged daysAgo days ago.
func listPR(number int, branch string, daysAgo int) github.Pull {
	p := legacyPR(number, daysAgo, true)
	p.Head.Ref = branch
	return p
}

// listFixture is a hub host whose only pane sits in /repo.wt, a worktree of the
// configured repo, so /repo/.git is the one common reap sees. No job is bound to
// any PR: every candidate comes from worktree.list. The common lists rows
// two at a time; each row's path is a linked worktree on its own branch.
func listFixture(t *testing.T, rows []map[string]any, prs ...github.Pull) (*Service, *fakeReapHub) {
	t.Helper()
	hub := newFakeReapHub()
	hub.wtPaths = map[string]bool{}
	hub.branches = map[string]string{}
	hub.lists = map[string][]map[string]any{listCommon: rows}
	hub.listPage = 2
	for _, r := range rows {
		if path := r["path"].(string); path != "/repo" {
			hub.wtPaths[path] = true
			hub.branches[path] = r["branch"].(string)
		}
	}
	s, _ := hubSvc(t, hub, &scriptRunner{})
	s.GH = &legacyGH{closed: prs}
	s.Sink = &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
	return s, hub
}

func rowFor(t *testing.T, s *Service, path string) map[string]any {
	t.Helper()
	for _, r := range legacyRows(t, s) {
		if r["worktree"] == path {
			return r
		}
	}
	t.Fatalf("no legacy-sweep row for %s in %v", path, legacyRows(t, s))
	return nil
}

// summaryOf is the host's legacy-sweep-summary line from the printed output.
func summaryOf(t *testing.T, out string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		var v map[string]any
		if json.Unmarshal([]byte(line), &v) == nil && v["action"] == legacyAction+"-summary" {
			return v
		}
	}
	t.Fatalf("no summary in:\n%s", out)
	return nil
}

// (a) A common listed over two pages: a clean branch merged 20 days ago is
// would-remove, with no job binding at all; the plan removes nothing.
func TestLegacyListEnumeratesEveryPage(t *testing.T) {
	s, hub := listFixture(t, []map[string]any{
		listedRow("/repo", "main"),
		listedRow("/a.wt", "wk/a"),
		listedRow("/b.wt", "wk/b"),
	}, listPR(40, "wk/a", 20))
	var out strings.Builder
	s.Out = &out
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	lists := hub.ops("worktree.list")
	if len(lists) != 2 || lists[0]["common"] != listCommon || lists[0]["cursor"] != nil || lists[1]["cursor"] != "2" {
		t.Fatalf("worktree.list calls: %v", lists)
	}
	a := rowFor(t, s, "/a.wt")
	wantRow(t, a, "would-remove", legacyGuardNote)
	if a["pr"] != float64(40) || a["branch"] != "wk/a" {
		t.Fatalf("receipt shape: %v", a)
	}
	wantRow(t, rowFor(t, s, "/b.wt"), "kept", "no-pr")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("plan sent worktree.remove %d times", n)
	}
	sum := summaryOf(t, out.String())
	if sum["listing"] != "hub" || sum["commons"] != float64(1) || sum["commons_not_found"] != float64(0) ||
		sum["listed"] != float64(3) || sum["candidates"] != float64(2) || sum["would_remove"] != float64(1) {
		t.Fatalf("summary: %v", sum)
	}
	if kept, _ := sum["kept_by_reason"].(map[string]any); kept["no-pr"] != float64(1) || len(kept) != 1 {
		t.Fatalf("kept_by_reason: %v", sum["kept_by_reason"])
	}
}

// (b) The node's listed status keeps a worktree it already shows as dirty,
// untracked or unreadable, and the guard is never asked.
func TestLegacyListKeepsDirtyUntrackedAndUnreadable(t *testing.T) {
	s, hub := listFixture(t, []map[string]any{
		withStatus(listedRow("/d.wt", "wk/d"), map[string]any{"dirty": true, "untracked": 0, "ignored": 0}),
		withStatus(listedRow("/u.wt", "wk/u"), map[string]any{"dirty": false, "untracked": 2, "ignored": 0}),
		withStatus(listedRow("/e.wt", "wk/e"), map[string]any{"dirty": false, "untracked": 0, "ignored": 0, "error": "git status failed"}),
	}, listPR(41, "wk/d", 20), listPR(42, "wk/u", 20), listPR(43, "wk/e", 20))
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, rowFor(t, s, "/d.wt"), "kept", "dirty")
	wantRow(t, rowFor(t, s, "/u.wt"), "kept", "dirty")
	wantRow(t, rowFor(t, s, "/e.wt"), "kept", "status-error")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a dirty or unreadable worktree %d times", n)
	}
}

// (c) A listed worktree on a branch no PR of the repo used is kept.
func TestLegacyListKeepsBranchWithNoPR(t *testing.T) {
	s, hub := listFixture(t, []map[string]any{listedRow("/n.wt", "wk/none")}, listPR(40, "wk/a", 20))
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, rowFor(t, s, "/n.wt"), "kept", "no-pr")
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed a worktree with no PR %d times", n)
	}
}

// (d) The main checkout and reviewer checkouts are listed but never candidates:
// no row, no inspection of the main checkout, no removal.
func TestLegacyListSkipsMainCheckoutAndReviewerCheckouts(t *testing.T) {
	const mwr = "/mwr-40-aaaa111-p1-1"
	s, hub := listFixture(t, []map[string]any{
		listedRow("/repo", "wk/a"), // the main checkout, on a branch with a merged PR
		listedRow(mwr, "wk/a"),
	}, listPR(40, "wk/a", 20))
	var out strings.Builder
	s.Out = &out
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	if rows := legacyRows(t, s); len(rows) != 0 {
		t.Fatalf("main or reviewer checkout got rows: %v", rows)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed %d", n)
	}
	sum := summaryOf(t, out.String())
	if sum["listed"] != float64(2) || sum["candidates"] != float64(0) {
		t.Fatalf("summary: %v", sum)
	}
}

// (e) A common whose origin is another repository is never listed.
func TestLegacyListIgnoresCommonOfAnotherRepo(t *testing.T) {
	s, hub := listFixture(t, nil, listPR(40, "wk/a", 20))
	hub.agents["panes"] = []any{
		map[string]any{"pane_id": "w1:p1", "agent_status": "idle", "cwd": "/repo.wt"},
		map[string]any{"pane_id": "w2:p1", "agent_status": "idle", "cwd": "/other.wt"},
	}
	hub.wtPaths["/other.wt"] = true
	hub.foreign = map[string]bool{"/other.wt": true}
	hub.commons = map[string]string{"/other.wt": "/other/.git"}
	hub.wtPaths["/foreign.wt"] = true
	hub.branches["/foreign.wt"] = "wk/a"
	hub.lists["/other/.git"] = []map[string]any{listedRow("/foreign.wt", "wk/a")}
	var out strings.Builder
	s.Out = &out
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	for _, l := range hub.ops("worktree.list") {
		if l["common"] == "/other/.git" {
			t.Fatalf("listed another repo's common: %v", l)
		}
	}
	if rows := legacyRows(t, s); len(rows) != 0 {
		t.Fatalf("rows for another repo's worktree: %v", rows)
	}
	if n := len(hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("removed %d", n)
	}
	if sum := summaryOf(t, out.String()); sum["commons"] != float64(1) {
		t.Fatalf("summary: %v", sum)
	}
}

// (f) The apply removes through worktree.remove only: the one clean old merged
// worktree, with the guarded request; the sweep sends no other acting op.
func TestLegacyListApplyRemovesOnlyThroughGuardedRemove(t *testing.T) {
	s, hub := listFixture(t, []map[string]any{
		listedRow("/repo", "main"),
		listedRow("/a.wt", "wk/a"),
		withStatus(listedRow("/d.wt", "wk/d"), map[string]any{"dirty": true}),
	}, listPR(40, "wk/a", 20), listPR(41, "wk/d", 20))
	if err := s.ReapLegacy(context.Background(), testRepo, "", apply()); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != "/a.wt" || removes[0]["expect_head"] != headSHA || removes[0]["base"] != "main" || removes[0]["detached"] != false {
		t.Fatalf("remove args: %#v", removes)
	}
	wantRow(t, rowFor(t, s, "/a.wt"), "removed", "")
	for _, c := range hub.commands {
		switch c["op"] {
		case "worktree.inspect", "worktree.list", "worktree.remove":
		default:
			t.Fatalf("legacy sweep sent %v", c["op"])
		}
	}
}

// A worktree both source reach is one candidate: the listed row and the job
// binding merge by path, and the receipt keeps the job.
func TestLegacyListMergesWithJobBoundCandidate(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	hub.lists = map[string][]map[string]any{listCommon: {listedRow(legacyCwd, "wk/x")}}
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	row := legacyOnly(t, s)
	wantRow(t, row, "would-remove", legacyGuardNote)
	if row["job"] != "job-a" || row["task"] != "3074" {
		t.Fatalf("lost the job binding: %v", row)
	}
}

// A common the node does not accept is counted, not an error, and the job
// binding still finds its worktree.
func TestLegacyListCountsCommonNotFound(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	hub.listErr = map[string]string{listCommon: "invalid_args"}
	var out strings.Builder
	s.Out = &out
	if err := s.ReapLegacy(context.Background(), testRepo, "", plan()); err != nil {
		t.Fatal(err)
	}
	wantRow(t, legacyOnly(t, s), "would-remove", legacyGuardNote)
	if sum := summaryOf(t, out.String()); sum["commons"] != float64(1) || sum["commons_not_found"] != float64(1) || sum["listed"] != float64(0) {
		t.Fatalf("summary: %v", sum)
	}
}
