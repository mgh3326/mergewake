package pipeline

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// jobGH lists merged and open PRs per repo for the job-bound reap tests.
type jobGH struct {
	reapGH
	merged map[string][]github.Pull
	open   map[string][]github.Pull
}

func (g *jobGH) MergedPulls(_ context.Context, repo, _ string, _ time.Time) ([]github.Pull, error) {
	return g.merged[repo], nil
}

func (g *jobGH) OpenPulls(_ context.Context, repo, branch string) ([]github.Pull, error) {
	if branch != "" {
		return nil, errors.New("open PRs must be listed against every base")
	}
	return g.open[repo], nil
}

const leadScreen = `herdr pane read "$2" > "$1/pane-screen.txt"`

// leadPR is a merged PR of example/project whose lead pane works from a
// worktree of example/other.
var leadPR = github.Pull{
	Number: 12, State: "closed", Merged: true, Body: "Lead work.\n\nMergewake-Task: 2497\n",
	Head: github.Ref{Ref: "feat/lead", SHA: "cccc3333"}, Base: github.Ref{Ref: "main"},
	MergedAt: rfc(time.Now().Add(-time.Hour)), MergeCommitSHA: "def456",
}

// leadPane is the job's pane: its name is the job label and its cwd a linked
// worktree of another repository.
func leadPane(fields string) string {
	return `{"pane_id":"w2:p1","name":"lead-a","agent_status":"idle","agent":"claude","cwd":"/other.wt"` + fields + `}`
}

// leadFixture is a service whose hk binds task 2497 to job lead-a on host
// local; panewire done flips the record to completed, as its hk PUT does.
func leadFixture(t *testing.T, pane string) (*Service, *jobGH, *jobSink, *scriptRunner) {
	t.Helper()
	sink := &jobSink{
		jobs: map[string]string{"2497": "lead-a"},
		recs: map[string]integrations.JobRecord{"lead-a": {State: "spawned", Role: "lead", Label: "lead-a", Host: "local", PaneID: "w2:p1"}},
	}
	run := &scriptRunner{outs: map[string]string{
		key("herdr", "agent", "list"):                                `{"result":{"agents":[` + pane + `]}}`,
		key("herdr", "pane", "list"):                                 `{"result":{"panes":[{"pane_id":"w2:p1","cwd":"/other.wt"}]}}`,
		key("herdr", "pane", "read", "w2:p1", "--source", "visible"): emptyComposer,
		key("herdr", "pane", "close", "w2:p1"):                       "",
		key("git", "-C", "/other.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/other/.git/worktrees/other.wt\n/other/.git\n/other.wt\n",
		key("git", "-C", "/other.wt", "branch", "--show-current"):                                               "feat/other\n",
		key("git", "-C", "/other.wt", "rev-parse", "HEAD"):                                                      "dddd4444\n",
		key("git", "-C", "/other.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/other.git\n",
		key("sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", "/jobs/lead-a"):                            "yes\n",
		key("sh", "-c", `test -f "$1" && echo yes || echo no`, "sh", "/jobs/lead-a/report.md"):                  "no\n",
		key("sh", "-c", leadScreen, "sh", "/jobs/lead-a", "w2:p1"):                                              "",
	}}
	run.fn = func(k string) (string, bool) {
		if k == key("panewire", "done", "lead-a", "--report", "/jobs/lead-a/pane-screen.txt") {
			rec := sink.recs["lead-a"]
			rec.State = "completed"
			sink.recs["lead-a"] = rec
			return "", true
		}
		return "", false
	}
	gh := &jobGH{merged: map[string][]github.Pull{"example/project": {leadPR}}, open: map[string][]github.Pull{}}
	s := svc(t, gh, run)
	s.Sink = sink
	s.Config.Repos = append(s.Config.Repos, config.Repo{Name: "example/other", Branch: "main"})
	return s, gh, sink, run
}

func leadRow(t *testing.T, s *Service, action string) map[string]any {
	t.Helper()
	// A reap that closes nothing may leave no log at all.
	if _, err := os.Stat(s.reapLog("example/project")); err != nil {
		return nil
	}
	var last map[string]any
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["action"] == action {
			last = row
		}
	}
	return last
}

func callCount(run *scriptRunner, k string) int {
	n := 0
	for _, c := range run.calls {
		if c == k {
			n++
		}
	}
	return n
}

// Acceptance 1: the lead pane is found through the job, but it is never
// closed and never marked done: panewire's node owns the pane and the job's
// own harness ends it. Shadow and acting passes alike write no close-pane or
// job-done rows and run no close or panewire command.
func TestReapLeadPaneNeverCloses(t *testing.T) {
	for _, shadow := range []bool{true, false} {
		s, _, _, run := leadFixture(t, leadPane(""))
		o := Options{}
		if shadow {
			o = Options{Shadow: true}
		}
		if err := s.Reap(context.Background(), testRepo, 0, o); err != nil {
			t.Fatal(err)
		}
		if row := leadRow(t, s, "close-pane"); row != nil {
			t.Fatalf("close-pane row written: %v", row)
		}
		if row := leadRow(t, s, "job-done"); row != nil {
			t.Fatalf("job-done row written: %v", row)
		}
		if run.called("herdr\x00pane\x00close") || run.called("panewire") {
			t.Fatalf("shadow=%t acted: %q", shadow, run.calls)
		}
	}
}

// Acceptance 2: a merged PR without the trailer is matched by its worktree
// exactly as before, even with an hk jobs reader configured — except nothing
// closes the pane. While the pane uses the worktree, removal waits as
// pane-in-use; once panewire's node has closed the pane, the worktree and its
// branch go.
func TestReapWithoutTrailerKeepsWorktreeMatch(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	agents, panes := r.outs[key("herdr", "agent", "list")], r.outs[key("herdr", "pane", "list")]
	empty := `{"result":{"agents":[],"panes":[]}}`
	r.queue = map[string][]string{
		key("herdr", "agent", "list"): {agents, agents, empty},
		key("herdr", "pane", "list"):  {panes, empty, empty},
	}
	s := svc(t, &reapGH{}, r)
	s.Sink = &jobSink{jobs: map[string]string{"2497": "lead-a"}}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
	}
	if row := leadRow(t, s, "remove-worktree"); row == nil || row["result"] != "skipped" || row["reason"] != "pane-in-use" {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	for _, k := range []string{"herdr\x00pane\x00close", "herdr\x00pane\x00read", "panewire"} {
		if r.called(k) {
			t.Fatalf("pane touched while in use: %q", r.calls)
		}
	}
	// The pane is gone: panewire closed it, so the worktree and branch go.
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if row := leadRow(t, s, "remove-worktree"); row == nil || row["result"] != "removed" {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	if row := leadRow(t, s, "delete-branch"); row == nil || row["result"] != "deleted" {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
	}
	if r.called("herdr\x00pane\x00close") || r.called("panewire") {
		t.Fatalf("pane closed by reap: %q", r.calls)
	}
}

// A lead bound to a merged PR's worktree through its task still gets that
// worktree cleaned once the merge ages out and the pane is gone — without
// closing anything.
func TestReapJobBoundLeadKeepsWorktreeCleanup(t *testing.T) {
	outs := worktreeOuts("/repo")
	r := &scriptRunner{outs: outs}
	agents, panes := outs[key("herdr", "agent", "list")], outs[key("herdr", "pane", "list")]
	empty := `{"result":{"agents":[],"panes":[]}}`
	r.queue = map[string][]string{
		key("herdr", "agent", "list"): {agents, agents, empty},
		key("herdr", "pane", "list"):  {panes, empty, empty},
	}
	pr := mergedPull
	pr.Body = "Mergewake-Task: 2497\n"
	gh := &jobGH{merged: map[string][]github.Pull{"example/project": {pr}}}
	s := svc(t, gh, r)
	s.Sink = &jobSink{
		jobs: map[string]string{"2497": "wk-7"},
		recs: map[string]integrations.JobRecord{"wk-7": {State: "spawned", Label: "wk-7", Host: "local", PaneID: "w1:p1", Cwd: "/repo.wt"}},
	}
	merged := *parsedTime(*pr.MergedAt)
	s.Now = func() time.Time { return merged.Add(time.Hour) }
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
	}
	if row := leadRow(t, s, "job-done"); row != nil {
		t.Fatalf("job-done row written: %v", row)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("removed before worktree_after")
	}
	if r.called("herdr\x00pane\x00close") || r.called("panewire") {
		t.Fatalf("pane closed by reap: %q", r.calls)
	}
	s.Now = nil
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called(key("git", "-C", "/repo", "worktree", "remove", "/repo.wt")) {
		t.Fatalf("worktree never removed: %v", reapLogRows(t, s, "example/project"))
	}
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
	}
}

// countingSink counts TaskJob reads.
type countingSink struct {
	jobSink
	taskReads int
}

func (s *countingSink) TaskJob(ctx context.Context, task string) (string, string, error) {
	s.taskReads++
	return s.jobSink.TaskJob(ctx, task)
}

// A task with no bound job yet is asked again each pass; its pane still waits
// out panewire's close as pane-in-use, and nothing closes it meanwhile.
func TestReapTaskWithoutJobKeepsWorktreeWaiting(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	pr := mergedPull
	pr.Body = "Mergewake-Task: 2497\n"
	gh := &jobGH{merged: map[string][]github.Pull{"example/project": {pr}}}
	s := svc(t, gh, r)
	sink := &countingSink{jobSink: jobSink{jobs: map[string]string{}}}
	s.Sink = sink
	for range 2 {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if sink.taskReads != 2 {
		t.Fatalf("task read %d times", sink.taskReads)
	}
	if row := leadRow(t, s, "close-pane"); row != nil {
		t.Fatalf("close-pane row written: %v", row)
	}
	if row := leadRow(t, s, "remove-worktree"); row == nil || row["result"] != "skipped" || row["reason"] != "pane-in-use" {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	if r.called("herdr\x00pane\x00close") || r.called("panewire") {
		t.Fatalf("pane closed by reap: %q", r.calls)
	}
}
