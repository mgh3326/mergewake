package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
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
// While its pane lists, removal waits as pane-in-use; once panewire's node
// closes the pane, the shadow plan names the removal.
func TestReapAncestorExactHeadNeedsNoCompare(t *testing.T) {
	s, gh, run := ancestryService(t, headSHA, nil)
	agents, panes := run.outs[key("herdr", "agent", "list")], run.outs[key("herdr", "pane", "list")]
	emptyPanes := `{"result":{"panes":[]}}`
	run.queue = map[string][]string{
		key("herdr", "agent", "list"): {agents, agents},
		key("herdr", "pane", "list"):  {panes, emptyPanes, emptyPanes},
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if len(gh.calls) != 0 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireRows(t, s, "remove-worktree/skipped")
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
	}
	if run.called("pane\x00close") {
		t.Fatalf("pane closed: %q", run.calls)
	}
	// The pane is gone from the pane list: the shadow plan names the removal
	// with no compare.
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if len(gh.calls) != 0 {
		t.Fatalf("compares %v", gh.calls)
	}
	requireRows(t, s, "remove-worktree/would-remove", "delete-branch/would-delete")
}

// (b) Two commits behind the merged head: an ancestor, so every commit in the
// worktree is in the merged PR. It is the PR's own and is removed like an
// exact match, after one compare per pass. Nothing closes the pane.
func TestReapAncestorBehindHeadQualifies(t *testing.T) {
	table := map[[2]string]compareReply{{behindHead, headSHA}: {status: "ahead", base: behindHead}}
	for name, o := range map[string]Options{"shadow": {Shadow: true}, "apply": {}} {
		t.Run(name, func(t *testing.T) {
			s, gh, run := ancestryService(t, behindHead, table)
			agents, panes := run.outs[key("herdr", "agent", "list")], run.outs[key("herdr", "pane", "list")]
			emptyPanes := `{"result":{"panes":[]}}`
			run.queue = map[string][]string{
				key("herdr", "agent", "list"): {agents, agents},
				key("herdr", "pane", "list"):  {panes, emptyPanes, emptyPanes},
			}
			if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
				t.Fatal(err)
			}
			if len(gh.calls) != 1 || gh.calls[0] != [2]string{behindHead, headSHA} {
				t.Fatalf("compares %v", gh.calls)
			}
			requireRows(t, s, "remove-worktree/skipped")
			// The pane is gone from the pane list: the worktree goes, after
			// one compare per pass.
			if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
				t.Fatal(err)
			}
			if len(gh.calls) != 2 || gh.calls[1] != [2]string{behindHead, headSHA} {
				t.Fatalf("compares %v", gh.calls)
			}
			if name == "shadow" {
				requireRows(t, s, "remove-worktree/would-remove", "delete-branch/would-delete")
			} else {
				requireRows(t, s, "remove-worktree/removed", "delete-branch/deleted")
				if !run.called("worktree\x00remove\x00/repo.wt") {
					t.Fatalf("calls %q", run.calls)
				}
			}
			if row := leadRow(t, s, "close-pane"); row != nil {
				t.Fatalf("close-pane row written: %v", row)
			}
			if run.called("pane\x00close") {
				t.Fatalf("pane closed: %q", run.calls)
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

// (e) Qualifying by ancestry does not lift the dirty guard: the worktree is
// refused as dirty and is never removed. Nothing closes the pane either.
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
	requireRows(t, s, "remove-worktree/skipped")
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
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
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

// The hub's worktree.remove is told the checkout it will remove, which is the
// ancestor head, not the PR head it no longer equals. The pane still listed
// waits first; once panewire's node closes it, the removal runs.
func TestReapPanewireRemovesAncestorWorktree(t *testing.T) {
	hub := newFakeReapHub()
	hub.wtHead = behindHead
	s, _ := hubSvc(t, hub, &scriptRunner{})
	table := map[[2]string]compareReply{{behindHead, headSHA}: {status: "ahead", base: behindHead}}
	s.GH = &compareGH{GitHub: s.GH, client: compareClient(t, table)}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(hub.ops("worktree.remove")) != 0 {
		t.Fatalf("removed with the pane still listed: %v", hub.ops("worktree.remove"))
	}
	hub.agents["agents"], hub.agents["panes"] = []any{}, []any{}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["expect_head"] != behindHead {
		t.Fatalf("remove args: %#v rows: %v", removes, reapLogRows(t, s, testRepo.Name))
	}
}

// A host whose panes could not be read fails the pass with no rows about it.
func TestReapBoundJobOnUnreadableHostReportsError(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	delete(run.outs, key("herdr", "agent", "list"))
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil {
		t.Fatal("unreadable host not reported")
	}
}
