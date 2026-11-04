package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The worktree heads these tests put on the merged PR's branch. headSHA, the
// PR head, is shared with every reap test.
const (
	behindHead   = "bbbb2222" // an ancestor of headSHA: every commit in it is in the merged PR
	divergedHead = "eeee5555" // a sibling of headSHA: each side has commits the other lacks
	unknownHead  = "dddd4444" // a commit GitHub does not have
)

// compareReply is what the compare endpoint answers for one pair: a compare
// status with its merge base, or an HTTP error code.
type compareReply struct {
	status, base string
	code         int
}

// compareClient is the production client pointed at a server that answers
// GitHub's compare endpoint from table. A pair missing from the table is a 404,
// which is what GitHub answers for a commit it does not have.
func compareClient(t *testing.T, table map[[2]string]compareReply) *github.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pair := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/repos/example/project/compare/"), "...", 2)
		var reply compareReply
		ok := false
		if len(pair) == 2 {
			reply, ok = table[[2]string{pair[0], pair[1]}]
		}
		switch {
		case !ok:
			w.WriteHeader(http.StatusNotFound)
		case reply.code != 0:
			w.WriteHeader(reply.code)
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": reply.status, "merge_base_commit": map[string]string{"sha": reply.base}})
		}
	}))
	t.Cleanup(server.Close)
	c := github.New("placeholder")
	c.BaseURL, c.UploadURL, c.HTTP = server.URL, server.URL, server.Client()
	return c
}

// compareGH is the reap fake GitHub with Ancestor answered by the production
// client, so the status mapping and the 404 are the real ones. calls records
// each ancestor question a pass asked.
type compareGH struct {
	GitHub
	client *github.Client
	calls  [][2]string
}

func (g *compareGH) Ancestor(ctx context.Context, repo, older, newer string) (bool, error) {
	g.calls = append(g.calls, [2]string{older, newer})
	return g.client.Ancestor(ctx, repo, older, newer)
}

// WatchRevert and OpenPulls are forwarded to the inner fake: reap finds them by
// type assertion on the client, which the embedded interface hides.
func (g *compareGH) WatchRevert(ctx context.Context, repo string, pr int, sha string) (*github.Pull, error) {
	return g.GitHub.(interface {
		WatchRevert(context.Context, string, int, string) (*github.Pull, error)
	}).WatchRevert(ctx, repo, pr, sha)
}

func (g *compareGH) OpenPulls(ctx context.Context, repo, branch string) ([]github.Pull, error) {
	return g.GitHub.(interface {
		OpenPulls(context.Context, string, string) ([]github.Pull, error)
	}).OpenPulls(ctx, repo, branch)
}

// ancestryService is a reap service whose worktree sits at head on the merged
// PR's branch, with the compare answers in table.
func ancestryService(t *testing.T, head string, table map[[2]string]compareReply) (*Service, *compareGH, *scriptRunner) {
	t.Helper()
	outs := worktreeOuts("/repo")
	outs[key("git", "-C", "/repo.wt", "rev-parse", "HEAD")] = head + "\n"
	run := &scriptRunner{outs: outs}
	gh := &compareGH{GitHub: &reapGH{}, client: compareClient(t, table)}
	return svc(t, gh, run), gh, run
}

func noRows(t *testing.T, s *Service) {
	t.Helper()
	if _, err := os.Stat(s.reapLog(testRepo.Name)); err == nil {
		t.Fatalf("rows written: %v", reapLogRows(t, s, testRepo.Name))
	}
}

// requireRows fails unless the receipt log holds every action/result pair.
func requireRows(t *testing.T, s *Service, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, row := range reapLogRows(t, s, testRepo.Name) {
		got[row["action"].(string)+"/"+row["result"].(string)] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("missing %s in %v", w, got)
		}
	}
}

// requireNothing fails if the pass wrote a row or closed a pane or removed a
// worktree.
func requireNothing(t *testing.T, s *Service, run *scriptRunner) {
	t.Helper()
	noRows(t, s)
	if run.called("pane\x00close") || run.called("worktree\x00remove") {
		t.Fatalf("acted: %q", run.calls)
	}
}

// (a) On the PR head the worktree is the PR's own, and no compare is asked.
func TestReapAncestorExactHeadNeedsNoCompare(t *testing.T) {
	s, gh, _ := ancestryService(t, headSHA, nil)
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if len(gh.calls) != 0 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireRows(t, s, "close-pane/would-close", "remove-worktree/would-remove", "delete-branch/would-delete")
}

// (b) Two commits behind the merged head: an ancestor, so every commit in the
// worktree is in the merged PR. It is the PR's own and is closed and removed
// like an exact match, after one compare.
func TestReapAncestorBehindHeadQualifies(t *testing.T) {
	table := map[[2]string]compareReply{{behindHead, headSHA}: {status: "ahead", base: behindHead}}
	for name, o := range map[string]Options{"shadow": {Shadow: true}, "apply": {}} {
		t.Run(name, func(t *testing.T) {
			s, gh, run := ancestryService(t, behindHead, table)
			if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
				t.Fatal(err)
			}
			if len(gh.calls) != 1 || gh.calls[0] != [2]string{behindHead, headSHA} {
				t.Fatalf("compares %v", gh.calls)
			}
			if name == "shadow" {
				requireRows(t, s, "close-pane/would-close", "remove-worktree/would-remove", "delete-branch/would-delete")
				return
			}
			requireRows(t, s, "close-pane/closed", "remove-worktree/removed", "delete-branch/deleted")
			if !run.called("worktree\x00remove\x00/repo.wt") {
				t.Fatalf("calls %q", run.calls)
			}
		})
	}
}

// (c) A head GitHub does not know (compare 404) is not the PR's own and is not
// an error: nothing is recorded, closed or removed.
func TestReapAncestorUnknownHeadDoesNotQualify(t *testing.T) {
	table := map[[2]string]compareReply{{unknownHead, headSHA}: {code: http.StatusNotFound}}
	s, gh, run := ancestryService(t, unknownHead, table)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatalf("unknown head reported: %v", err)
	}
	if len(gh.calls) != 1 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireNothing(t, s, run)
}

// (d) A diverged head: the compare says diverged, so the worktree holds commits
// the merged PR lacks. It does not qualify.
func TestReapAncestorDivergedHeadDoesNotQualify(t *testing.T) {
	table := map[[2]string]compareReply{{divergedHead, headSHA}: {status: "diverged", base: "ffff0000"}}
	s, gh, run := ancestryService(t, divergedHead, table)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatalf("diverged head reported: %v", err)
	}
	if len(gh.calls) != 1 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireNothing(t, s, run)
}

// (e) Qualifying by ancestry does not lift the dirty guard: the pane closes, the
// worktree is refused as dirty and is never removed.
func TestReapAncestorDirtyWorktreeKeptByGuard(t *testing.T) {
	table := map[[2]string]compareReply{{behindHead, headSHA}: {status: "ahead", base: behindHead}}
	s, gh, run := ancestryService(t, behindHead, table)
	run.outs[key("git", "-C", "/repo.wt", "status", "--porcelain")] = " M tool.go\n"
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(gh.calls) != 1 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireRows(t, s, "close-pane/closed", "remove-worktree/skipped")
	reason := ""
	for _, row := range reapLogRows(t, s, testRepo.Name) {
		if row["action"] == "remove-worktree" {
			reason, _ = row["reason"].(string)
		}
	}
	if reason != "dirty" {
		t.Fatalf("remove-worktree reason %q: %v", reason, reapLogRows(t, s, testRepo.Name))
	}
	if run.called("worktree\x00remove") || run.called("branch\x00-d") {
		t.Fatalf("dirty worktree removed: %q", run.calls)
	}
}

// A compare that fails with anything but a 404 matches nothing: the failure is
// reported once, and the compare is not asked again in that pass.
func TestReapAncestorReadErrorMatchesNothing(t *testing.T) {
	table := map[[2]string]compareReply{{behindHead, headSHA}: {code: http.StatusInternalServerError}}
	s, gh, run := ancestryService(t, behindHead, table)
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || strings.Count(err.Error(), "head ancestry") != 1 {
		t.Fatalf("err=%v", err)
	}
	if len(gh.calls) != 1 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireNothing(t, s, run)
}

// The compare is asked once per PR head per pass, however many panes and
// removal re-checks ask within that pass: three passes ask it three times.
func TestReapAncestryReadOncePerPass(t *testing.T) {
	table := map[[2]string]compareReply{{behindHead, headSHA}: {status: "ahead", base: behindHead}}
	s, gh, _ := ancestryService(t, behindHead, table)
	for pass := 1; pass <= 3; pass++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
			t.Fatal(err)
		}
		if len(gh.calls) != pass {
			t.Fatalf("after pass %d: %d compares", pass, len(gh.calls))
		}
	}
}

// The job path records the worktree of a lead sitting on an ancestor of the
// merged head, which later cleanup reads.
func TestReapJobBoundCloseRowNamesAncestorWorktree(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	table := map[[2]string]compareReply{{behindHead, leadPR.Head.SHA}: {status: "ahead", base: behindHead}}
	s.GH = &compareGH{GitHub: s.GH, client: compareClient(t, table)}
	run.outs[key("git", "-C", "/other.wt", "branch", "--show-current")] = "wk/lead\n"
	run.outs[key("git", "-C", "/other.wt", "rev-parse", "HEAD")] = behindHead + "\n"
	run.outs[key("git", "-C", "/other.wt", "remote", "get-url", "origin")] = "git@github.com:example/project.git\n"
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	row := leadRow(t, s, "close-pane")
	if row == nil || row["result"] != "would-close" || row["worktree"] != "/other.wt" || row["branch"] != "wk/lead" {
		t.Fatalf("close row: %v", reapLogRows(t, s, "example/project"))
	}
}

// The hub's worktree.remove is told the checkout it will remove, which is the
// ancestor head, not the PR head it no longer equals.
func TestReapPanewireRemovesAncestorWorktree(t *testing.T) {
	hub := newFakeReapHub()
	hub.wtHead = behindHead
	s, _ := hubSvc(t, hub, &scriptRunner{})
	table := map[[2]string]compareReply{{behindHead, headSHA}: {status: "ahead", base: behindHead}}
	s.GH = &compareGH{GitHub: s.GH, client: compareClient(t, table)}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["expect_head"] != behindHead {
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

// A second host that also answers to the job's recorded host and cannot be
// read: the pane found on the readable host is still closed there, and when
// no pane is found nothing is reported missing, because the unread host may
// hold it.
func TestReapJobPaneWithUnreadableSiblingHost(t *testing.T) {
	sibling := func(s *Service) {
		s.Config.Hosts = append(s.Config.Hosts, reapHostSibling())
	}
	t.Run("pane found keeps its host", func(t *testing.T) {
		s, _, _, run := leadFixture(t, leadPane(""))
		sibling(s)
		_ = s.Reap(context.Background(), testRepo, 0, Options{Shadow: true})
		row := leadRow(t, s, "close-pane")
		if row == nil || row["result"] != "would-close" || row["host"] != "local" {
			t.Fatalf("row %v", row)
		}
		_ = run
	})
	t.Run("no pane and sibling unread", func(t *testing.T) {
		s, _, _, run := leadFixture(t, leadPane(""))
		sibling(s)
		run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
		run.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
		_ = s.Reap(context.Background(), testRepo, 0, Options{})
		if _, err := os.Stat(s.reapLog("example/project")); err == nil {
			for _, row := range reapLogRows(t, s, "example/project") {
				if row["reason"] == "job-pane-missing" {
					t.Fatalf("row %v", row)
				}
			}
		}
	})
}

func reapHostSibling() reap.Host {
	return reap.Host{Name: "remote", SSH: "ops@unreachable.example", Machine: "local"}
}
