package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// parentsGH answers Commit with a fixed parent list and counts the reads.
type parentsGH struct {
	reapGH
	parents     []string
	commitCalls int
	commitErr   error
}

func (g *parentsGH) Commit(_ context.Context, _, sha string) (github.Commit, error) {
	g.commitCalls++
	if g.commitErr != nil {
		return github.Commit{}, g.commitErr
	}
	c := github.Commit{SHA: sha}
	for _, p := range g.parents {
		c.Parents = append(c.Parents, github.Ref{SHA: p})
	}
	return c, nil
}

const (
	workerHead = "bbbb2222" // the worker's checkout, first parent of the update merge
	baseTip    = "cccc3333" // main when mergewake updated the branch, second parent
)

// updateReceipt writes the base-update receipt through updateStaleBranch, the
// receipt's only writer, so a drift in its key fails these tests. "ok" is a
// successful update; anything else is the GitHub status the update returned.
func updateReceipt(t *testing.T, s *Service, gh *parentsGH, base, status string) {
	t.Helper()
	switch status {
	case "ok":
		gh.updateErr = nil
	case "unknown":
		gh.updateErr = errors.New("connection reset")
	default:
		gh.updateErr = &github.APIError{Code: 409}
	}
	if got, _, err := s.updateStaleBranch(context.Background(), testRepo, mergedPull, base); err != nil || (status == "ok" && got != "ok") {
		t.Fatalf("receipt %q err=%v", got, err)
	}
}

// ownUpdateService is a reap service whose worktree is checked out at checkout
// while the merged PR's head is a merge commit with the given parents.
func ownUpdateService(t *testing.T, checkout string, parents ...string) (*Service, *parentsGH, *scriptRunner) {
	t.Helper()
	outs := worktreeOuts("/repo")
	outs[key("git", "-C", "/repo.wt", "rev-parse", "HEAD")] = checkout + "\n"
	r := &scriptRunner{outs: outs}
	gh := &parentsGH{parents: parents}
	return svc(t, gh, r), gh, r
}

func noRows(t *testing.T, s *Service) {
	t.Helper()
	if _, err := os.Stat(s.reapLog(testRepo.Name)); err == nil {
		t.Fatalf("rows written: %v", reapLogRows(t, s, testRepo.Name))
	}
}

// (a) mergewake's own update merge: the worker's pane closes and its worktree
// is removed after worktree_after, shadow and acting.
func TestReapMatchesOwnBaseUpdateMerge(t *testing.T) {
	for name, o := range map[string]Options{"shadow": {Shadow: true}, "apply": {}} {
		t.Run(name, func(t *testing.T) {
			s, gh, r := ownUpdateService(t, workerHead, workerHead, baseTip)
			updateReceipt(t, s, gh, baseTip, "ok")
			if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
				t.Fatal(err)
			}
			got := map[string]bool{}
			for _, row := range reapLogRows(t, s, testRepo.Name) {
				got[row["action"].(string)+"/"+row["result"].(string)] = true
				if row["reason"] == "worktree-changed" {
					t.Fatalf("candidate rejected on removal: %v", row)
				}
			}
			want := []string{"close-pane/would-close", "remove-worktree/would-remove", "delete-branch/would-delete"}
			if name == "apply" {
				want = []string{"close-pane/closed", "remove-worktree/removed", "delete-branch/deleted"}
				if !r.called("worktree\x00remove\x00/repo.wt") || !r.called("pane\x00close\x00w1:p1") {
					t.Fatalf("calls %q", r.calls)
				}
			}
			for _, w := range want {
				if !got[w] {
					t.Fatalf("missing %s in %v", w, got)
				}
			}
			if gh.commitCalls != 1 {
				t.Fatalf("head commit read %d times in one pass", gh.commitCalls)
			}
		})
	}
}

// (b)-(e) and the neighbours: nothing matches unless every part of the claim
// holds, so a worktree with its own commits or an unproven merge stays.
func TestReapOwnBaseUpdateNeedsEveryPart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		parents []string
		receipt string // "" writes none
		base    string // receipt's base; baseTip when empty
	}{
		{"no receipt", []string{workerHead, baseTip}, "", ""},
		{"receipt not ok", []string{workerHead, baseTip}, "409", ""},
		{"receipt outcome unknown", []string{workerHead, baseTip}, "unknown", ""},
		{"receipt for another base", []string{workerHead, baseTip}, "ok", "dddd4444"},
		{"first parent is not the checkout", []string{"eeee5555", baseTip}, "ok", ""},
		{"checkout is the second parent", []string{baseTip, workerHead}, "ok", workerHead},
		{"single parent", []string{"eeee5555"}, "ok", "eeee5555"},
		{"three parents", []string{workerHead, baseTip, "ffff6666"}, "ok", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, gh, r := ownUpdateService(t, workerHead, tc.parents...)
			if tc.receipt != "" {
				base := tc.base
				if base == "" {
					base = baseTip
				}
				updateReceipt(t, s, gh, base, tc.receipt)
			}
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			noRows(t, s)
			if r.called("pane\x00close") || r.called("worktree\x00remove") {
				t.Fatalf("acted: %q", r.calls)
			}
		})
	}
}

// A head commit that cannot be read matches nothing, reports one error and is
// not asked for again in the same pass.
func TestReapOwnBaseUpdateCommitErrorMatchesNothing(t *testing.T) {
	s, gh, r := ownUpdateService(t, workerHead)
	gh.commitErr = errors.New("github down")
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || strings.Count(err.Error(), "head parents") != 1 {
		t.Fatalf("err=%v", err)
	}
	noRows(t, s)
	if r.called("pane\x00close") {
		t.Fatal("closed on an unreadable head")
	}
}

// (h) The head commit is read once per PR, by the task lookup that already
// read it before this change, whether the worktree matches exactly or only
// through the update merge, and however many passes follow.
func TestReapHeadCommitReadOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		checkout string
		parents  []string
	}{
		"exact head":   {headSHA, nil},
		"update merge": {workerHead, []string{workerHead, baseTip}},
		"unrelated":    {workerHead, []string{"eeee5555", baseTip}},
	} {
		t.Run(name, func(t *testing.T) {
			s, gh, _ := ownUpdateService(t, tc.checkout, tc.parents...)
			for i := 0; i < 3; i++ {
				if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
					t.Fatal(err)
				}
			}
			if gh.commitCalls != 1 || gh.pullCalls != 0 {
				t.Fatalf("over 3 passes: commit reads %d, pull reads %d", gh.commitCalls, gh.pullCalls)
			}
		})
	}
}

// jobParentsGH lists PRs like jobGH and answers Commit with parents.
type jobParentsGH struct {
	jobGH
	parents []string
}

func (g *jobParentsGH) Commit(_ context.Context, _, sha string) (github.Commit, error) {
	c := github.Commit{SHA: sha}
	for _, p := range g.parents {
		c.Parents = append(c.Parents, github.Ref{SHA: p})
	}
	return c, nil
}

// The job path records the worktree of a lead sitting on the first parent of
// mergewake's update merge, which later cleanup reads.
func TestReapJobBoundCloseRowNamesOwnUpdateWorktree(t *testing.T) {
	s, gh, _, run := leadFixture(t, leadPane(""))
	s.GH = &jobParentsGH{jobGH: *gh, parents: []string{workerHead, baseTip}}
	run.outs[key("git", "-C", "/other.wt", "branch", "--show-current")] = "wk/lead\n"
	run.outs[key("git", "-C", "/other.wt", "rev-parse", "HEAD")] = workerHead + "\n"
	run.outs[key("git", "-C", "/other.wt", "remote", "get-url", "origin")] = "git@github.com:example/project.git\n"
	if _, _, err := s.updateStaleBranch(context.Background(), testRepo, leadPR, baseTip); err != nil {
		t.Fatal(err)
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	row := leadRow(t, s, "close-pane")
	if row == nil || row["result"] != "would-close" || row["worktree"] != "/other.wt" || row["branch"] != "wk/lead" {
		t.Fatalf("close row: %v", reapLogRows(t, s, "example/project"))
	}
}

// The hub's worktree.remove is told the checkout it will remove, not the PR
// head it no longer equals.
func TestReapPanewireRemovesOwnUpdateWorktree(t *testing.T) {
	hub := newFakeReapHub()
	hub.wtHead = workerHead
	s, _ := hubSvc(t, hub, &scriptRunner{})
	gh := &parentsGH{parents: []string{workerHead, baseTip}}
	s.GH = gh
	updateReceipt(t, s, gh, baseTip, "ok")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["expect_head"] != workerHead {
		t.Fatalf("remove args: %#v rows: %v", removes, reapLogRows(t, s, testRepo.Name))
	}
}

// (f) A bound job with no pane wearing its label on its host leaves one row
// naming why; the same outcome on the next pass writes nothing.
func TestReapRecordsBoundJobWithoutPane(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	run.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	for pass := 1; pass <= 2; pass++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		var rows []map[string]any
		for _, row := range reapLogRows(t, s, "example/project") {
			if row["reason"] == "job-pane-missing" {
				rows = append(rows, row)
			}
		}
		if len(rows) != 1 {
			t.Fatalf("pass %d: %d job-pane-missing rows: %v", pass, len(rows), reapLogRows(t, s, "example/project"))
		}
		row := rows[0]
		if row["action"] != "close-pane" || row["result"] != "skipped" || row["task"] != "2497" || row["job"] != "lead-a" || row["host"] != "local" || row["pr"] != float64(12) {
			t.Fatalf("row %v", row)
		}
	}
	if run.called("pane\x00close") || run.called("panewire") {
		t.Fatalf("acted: %q", run.calls)
	}
}

// A host whose panes could not be read says nothing about the label.
func TestReapBoundJobOnUnreadableHostWritesNoMissingRow(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	delete(run.outs, key("herdr", "agent", "list"))
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil {
		t.Fatal("unreadable host not reported")
	}
	if _, e := os.Stat(s.reapLog("example/project")); e == nil {
		for _, row := range reapLogRows(t, s, "example/project") {
			if row["reason"] == "job-pane-missing" {
				t.Fatalf("row %v", row)
			}
		}
	}
}

// (g) A close whose done failed still gets its done once the pane is gone, and
// a pane gone because reap closed it is never reported missing, before or
// after the done.
func TestReapClosedJobIsNotMissing(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	doneKey := key("panewire", "done", "lead-a", "--report", "/jobs/lead-a/pane-screen.txt")
	run.errs = map[string]error{doneKey: errors.New("hk unreachable")}
	agents := run.outs[key("herdr", "agent", "list")]
	run.queue = map[string][]string{key("herdr", "agent", "list"): {agents, agents, `{"result":{"agents":[]}}`}}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err == nil || !strings.Contains(err.Error(), "hk unreachable") {
		t.Fatalf("err=%v", err)
	}
	delete(run.errs, doneKey)
	run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	for pass := 0; pass < 2; pass++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if row := leadRow(t, s, "job-done"); row["result"] != "done" {
		t.Fatalf("job-done row %v", row)
	}
	if n := callCount(run, doneKey); n != 2 {
		t.Fatalf("done called %d times", n)
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["reason"] == "job-pane-missing" {
			t.Fatalf("closed job reported missing: %v", row)
		}
	}
}

// The job path's pane guards keep their reasons: the status, jobs-dir, job
// name and pane id checks that the other job tests do not name.
func TestReapJobPathKeepsPaneGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pane   string
		setup  func(*Service, *jobSink)
		reason string
	}{
		{"working", strings.Replace(leadPane(""), `"idle"`, `"working"`, 1), nil, "status:working"},
		{"no pane id", strings.Replace(leadPane(""), `"pane_id":"w2:p1"`, `"pane_id":""`, 1), nil, "pane-id-missing"},
		{"jobs dir unset", leadPane(""), func(s *Service, _ *jobSink) { s.Config.Hosts[0].Jobs = "" }, "jobs-dir-unset"},
		{"job name invalid", leadPane(""), func(_ *Service, sink *jobSink) {
			sink.jobs["2497"] = "lead a"
			sink.recs["lead a"] = sink.recs["lead-a"]
		}, "job-name-invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, sink, run := leadFixture(t, tc.pane)
			if tc.setup != nil {
				tc.setup(s, sink)
			}
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if run.called("pane\x00close") || run.called("panewire") {
				t.Fatalf("closed: %q", run.calls)
			}
			if row := leadRow(t, s, "close-pane"); row == nil || row["result"] != "skipped" || row["reason"] != tc.reason {
				t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
			}
		})
	}
}
