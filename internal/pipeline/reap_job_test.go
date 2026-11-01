package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
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
	Head: github.Ref{Ref: "wk/lead", SHA: "cccc3333"}, Base: github.Ref{Ref: "main"},
	MergedAt: rfc(time.Now().Add(-time.Hour)), MergeCommitSHA: "def456",
}

// leadPane is the job's pane: its name is the job label and its cwd a linked
// worktree of another repository.
func leadPane(fields string) string {
	return `{"pane_id":"w2:p1","name":"lead-a","agent_status":"idle","agent":"claude","cwd":"/other.wt"` + fields + `}`
}

// leadFixture is a service whose hk binds task 2497 to job lead-a on host
// local; runtug done flips the record to completed, as its hk PUT does.
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
		key("git", "-C", "/other.wt", "branch", "--show-current"):                                               "wk/other\n",
		key("git", "-C", "/other.wt", "rev-parse", "HEAD"):                                                      "dddd4444\n",
		key("git", "-C", "/other.wt", "remote", "get-url", "origin"):                                            "git@github.com:example/other.git\n",
		key("sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", "/jobs/lead-a"):                            "yes\n",
		key("sh", "-c", `test -f "$1" && echo yes || echo no`, "sh", "/jobs/lead-a/report.md"):                  "no\n",
		key("sh", "-c", leadScreen, "sh", "/jobs/lead-a", "w2:p1"):                                              "",
	}}
	run.fn = func(k string) (string, bool) {
		if k == key("runtug", "done", "lead-a", "--report", "/jobs/lead-a/pane-screen.txt") {
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

// Acceptance 1: the lead pane sits in another repo's worktree and is still
// found for the merged PR through Mergewake-Task → job → pane.
func TestReapFindsLeadPaneThroughJob(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	row := leadRow(t, s, "close-pane")
	if row == nil || row["result"] != "would-close" || row["pane"] != "w2:p1" || row["host"] != "local" || row["job"] != "lead-a" || row["task"] != "2497" || row["saved"] != "/jobs/lead-a/pane-screen.txt" || row["cwd"] != nil || row["worktree"] != nil {
		t.Fatalf("close row: %v", reapLogRows(t, s, "example/project"))
	}
	if done := leadRow(t, s, "job-done"); done == nil || done["result"] != "would-done" {
		t.Fatalf("job-done row: %v", done)
	}
	if run.called("herdr\x00pane\x00close") || run.called("runtug") {
		t.Fatalf("shadow acted: %q", run.calls)
	}
}

// Acceptance 2: one open PR of the job keeps the pane; once it merges the
// close is planned.
func TestReapKeepsLeadWhileJobHasOpenPR(t *testing.T) {
	s, gh, _, run := leadFixture(t, leadPane(""))
	// The open PR names the task only in its head commit.
	open := github.Pull{Number: 31, State: "open", Head: github.Ref{Ref: "wk/other", SHA: "eeee5555"}, Base: github.Ref{Ref: "release"}}
	gh.open["example/other"] = []github.Pull{open}
	gh.messages = map[string]string{"eeee5555": "fix: other half\n\nMergewake-Task: 2497\n"}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	row := leadRow(t, s, "close-pane")
	if row == nil || row["result"] != "skipped" || row["reason"] != "job_has_open_pr" || !slices.Equal(anyStrings(row["open"]), []string{"example/other#31"}) {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	if run.called("herdr\x00pane\x00close") || run.called("runtug") || leadRow(t, s, "job-done") != nil {
		t.Fatalf("pane closed with an open PR: %q", run.calls)
	}

	gh.open["example/other"] = nil
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if row := leadRow(t, s, "close-pane"); row["result"] != "would-close" || row["reason"] != nil {
		t.Fatalf("after merge: %v", reapLogRows(t, s, "example/project"))
	}
}

func anyStrings(v any) []string {
	var out []string
	list, _ := v.([]any)
	for _, x := range list {
		out = append(out, x.(string))
	}
	return out
}

// Acceptance 3: the screen is saved to the job dir before the close, and the
// job is marked done exactly once, also across a later pass.
func TestReapLeadSavesScreenClosesThenMarksDone(t *testing.T) {
	s, _, sink, run := leadFixture(t, leadPane(""))
	agents := run.outs[key("herdr", "agent", "list")]
	run.queue = map[string][]string{key("herdr", "agent", "list"): {agents, agents, `{"result":{"agents":[]}}`, `{"result":{"agents":[]}}`}}
	for i := 0; i < 2; i++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	screen := slices.Index(run.calls, key("sh", "-c", leadScreen, "sh", "/jobs/lead-a", "w2:p1"))
	closed := slices.Index(run.calls, key("herdr", "pane", "close", "w2:p1"))
	doneKey := key("runtug", "done", "lead-a", "--report", "/jobs/lead-a/pane-screen.txt")
	done := slices.Index(run.calls, doneKey)
	if screen < 0 || closed < screen || done < closed {
		t.Fatalf("want screen, close, done in order: %q", run.calls)
	}
	if n := callCount(run, doneKey); n != 1 {
		t.Fatalf("runtug done ran %d times", n)
	}
	if sink.recs["lead-a"].State != "completed" {
		t.Fatal("job not completed")
	}
	if row := leadRow(t, s, "close-pane"); row["result"] != "closed" || row["saved"] != "/jobs/lead-a/pane-screen.txt" {
		t.Fatalf("close row %v", row)
	}
	if row := leadRow(t, s, "job-done"); row["result"] != "done" || row["job"] != "lead-a" {
		t.Fatalf("job-done row %v", row)
	}
}

// A done that failed after the close is retried on a later pass, once the
// pane is gone.
func TestReapRetriesJobDoneAfterClose(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	doneKey := key("runtug", "done", "lead-a", "--report", "/jobs/lead-a/pane-screen.txt")
	run.errs = map[string]error{doneKey: errors.New("hk unreachable")}
	agents := run.outs[key("herdr", "agent", "list")]
	run.queue = map[string][]string{key("herdr", "agent", "list"): {agents, agents, `{"result":{"agents":[]}}`}}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err == nil || !strings.Contains(err.Error(), "hk unreachable") {
		t.Fatalf("err=%v", err)
	}
	delete(run.errs, doneKey)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := callCount(run, doneKey); n != 2 || callCount(run, key("herdr", "pane", "close", "w2:p1")) != 1 {
		t.Fatalf("calls %q", run.calls)
	}
	if row := leadRow(t, s, "job-done"); row["result"] != "done" {
		t.Fatalf("job-done row %v", row)
	}
}

// Acceptance 4: protected labels, operator-owned panes, panes without a job
// dir and panes with unsent composer text are never closed.
func TestReapLeadProtections(t *testing.T) {
	for _, tt := range []struct {
		name   string
		pane   string
		setup  func(*Service, *jobSink, *scriptRunner)
		reason string
	}{
		{"protected label", `{"pane_id":"w2:p1","name":"repo-integrator","agent_status":"idle","agent":"claude","cwd":"/other.wt"}`, func(_ *Service, sink *jobSink, _ *scriptRunner) {
			rec := sink.recs["lead-a"]
			rec.Label = "repo-integrator"
			sink.recs["lead-a"] = rec
		}, "protected:repo-integrator"},
		{"operator focused", leadPane(`,"focused":true`), nil, "operator-owned"},
		{"pane not the job's", leadPane(""), func(_ *Service, sink *jobSink, _ *scriptRunner) {
			rec := sink.recs["lead-a"]
			rec.PaneID = "w9:p9"
			sink.recs["lead-a"] = rec
		}, "operator-owned"},
		{"not a job", leadPane(""), func(_ *Service, _ *jobSink, run *scriptRunner) {
			run.outs[key("sh", "-c", `test -d "$1" && echo yes || echo no`, "sh", "/jobs/lead-a")] = "no\n"
		}, "not-a-job"},
		{"composer text", leadPane(""), func(_ *Service, _ *jobSink, run *scriptRunner) {
			run.outs[key("herdr", "pane", "read", "w2:p1", "--source", "visible")] = draftComposer
		}, "composer-text"},
		{"composer unreadable", leadPane(`,"agent":"other"`), nil, "composer-unknown"},
		{"pane id unrecorded", leadPane(""), func(_ *Service, sink *jobSink, _ *scriptRunner) {
			rec := sink.recs["lead-a"]
			rec.PaneID, rec.Host = "", ""
			sink.recs["lead-a"] = rec
		}, "pane-id-unrecorded"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _, sink, run := leadFixture(t, tt.pane)
			s.Config.Reap.Protect = append(s.Config.Reap.Protect, "director-*", "fleet-tooling-*", "consult-*")
			if tt.setup != nil {
				tt.setup(s, sink, run)
			}
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if run.called("herdr\x00pane\x00close") || run.called("runtug") {
				t.Fatalf("closed: %q", run.calls)
			}
			if row := leadRow(t, s, "close-pane"); row == nil || row["result"] != "skipped" || row["reason"] != tt.reason {
				t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
			}
		})
	}
}

// The worktree-matched path keeps the same operator and composer guards.
func TestReapWorktreePaneGuards(t *testing.T) {
	for _, tt := range []struct {
		name, agents, screen, reason string
	}{
		{"operator focused", `{"pane_id":"w1:p1","name":"wk-7","agent_status":"idle","agent":"claude","focused":true,"cwd":"/repo.wt"}`, emptyComposer, "operator-owned"},
		{"composer text", `{"pane_id":"w1:p1","name":"wk-7","agent_status":"idle","agent":"claude","cwd":"/repo.wt"}`, draftComposer, "composer-text"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outs := worktreeOuts("/repo")
			outs[key("herdr", "agent", "list")] = `{"result":{"agents":[` + tt.agents + `]}}`
			outs[key("herdr", "pane", "read", "w1:p1", "--source", "visible")] = tt.screen
			r := &scriptRunner{outs: outs}
			s := svc(t, &reapGH{}, r)
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if r.called("herdr\x00pane\x00close") {
				t.Fatal("pane closed")
			}
			if row := leadRow(t, s, "close-pane"); row == nil || row["reason"] != tt.reason {
				t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
			}
		})
	}
}

// Acceptance 5: a merged PR without the trailer is matched by its worktree
// exactly as before, even with an hk jobs reader configured.
func TestReapWithoutTrailerKeepsWorktreeMatch(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	s := svc(t, &reapGH{}, r)
	s.Sink = &jobSink{jobs: map[string]string{"2497": "lead-a"}}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	row := leadRow(t, s, "close-pane")
	if row == nil || row["result"] != "closed" || row["pane"] != "w1:p1" || row["cwd"] != "/repo.wt" || row["worktree"] != "/repo.wt" || row["branch"] != "wk/x" || row["job"] != "wk-7" || row["saved"] != "/jobs/wk-7/report.md" || row["task"] != nil {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	if r.called("runtug") || leadRow(t, s, "job-done") != nil {
		t.Fatalf("worktree match marked a job done: %q", r.calls)
	}
	if leadRow(t, s, "remove-worktree")["result"] != "removed" {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
}

// hk may never show the job terminal; reap's own done receipt still ends the
// retries.
func TestReapJobDoneRunsOnceWhenHKLags(t *testing.T) {
	s, _, _, run := leadFixture(t, leadPane(""))
	doneKey := key("runtug", "done", "lead-a", "--report", "/jobs/lead-a/pane-screen.txt")
	run.fn = func(k string) (string, bool) { return "", k == doneKey }
	agents := run.outs[key("herdr", "agent", "list")]
	run.queue = map[string][]string{key("herdr", "agent", "list"): {agents, agents}}
	run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	for i := 0; i < 3; i++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if n := callCount(run, doneKey); n != 1 {
		t.Fatalf("runtug done ran %d times", n)
	}
}

// A lead sitting in its merged PR's own worktree records that worktree, so
// the worktree is still removed once the merge ages out and the pane is gone.
func TestReapJobBoundLeadKeepsWorktreeCleanup(t *testing.T) {
	outs := worktreeOuts("/repo")
	outs[key("herdr", "pane", "read", "w1:p1", "--source", "visible")] = emptyComposer
	outs[key("runtug", "done", "wk-7", "--report", "/jobs/wk-7/report.md")] = ""
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
		recs: map[string]integrations.JobRecord{"wk-7": {State: "spawned", Label: "wk-7", Host: "local", PaneID: "w1:p1"}},
	}
	merged := *parsedTime(*pr.MergedAt)
	s.Now = func() time.Time { return merged.Add(time.Hour) }
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if row := leadRow(t, s, "close-pane"); row["result"] != "closed" || row["task"] != "2497" || row["worktree"] != "/repo.wt" {
		t.Fatalf("close row %v", row)
	}
	if r.called("worktree\x00remove") {
		t.Fatal("removed before worktree_after")
	}
	s.Now = nil
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !r.called(key("git", "-C", "/repo", "worktree", "remove", "/repo.wt")) {
		t.Fatalf("worktree never removed: %v", reapLogRows(t, s, "example/project"))
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

// A task with no bound job yet is asked again each pass, and its worktree
// match still waits for the task's other open PRs.
func TestReapTaskWithoutJobWaitsForOpenPRs(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo")}
	pr := mergedPull
	pr.Body = "Mergewake-Task: 2497\n"
	gh := &jobGH{merged: map[string][]github.Pull{"example/project": {pr}}, open: map[string][]github.Pull{
		"example/project": {{Number: 40, State: "open", Body: "Mergewake-Task: 2497\n", Base: github.Ref{Ref: "main"}}},
	}}
	s := svc(t, gh, r)
	sink := &countingSink{jobSink: jobSink{jobs: map[string]string{}}}
	s.Sink = sink
	for i := 0; i < 2; i++ {
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if sink.taskReads != 2 {
		t.Fatalf("task read %d times", sink.taskReads)
	}
	if row := leadRow(t, s, "close-pane"); row["reason"] != "job_has_open_pr" || row["cwd"] != "/repo.wt" || r.called("herdr\x00pane\x00close") {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
}
