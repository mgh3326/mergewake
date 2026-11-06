package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

const projectOrigin = "git@github.com:example/project.git"

// aPane is one agent pane in a linked worktree on branch of origin.
type aPane struct{ id, name, cwd, branch, origin string }

// authorHost scripts the host's agent list, pane list and per-cwd worktree
// inspection for panes.
func authorHost(panes ...aPane) *scriptRunner {
	var agents, all []string
	outs := map[string]string{}
	for _, p := range panes {
		agents = append(agents, fmt.Sprintf(`{"pane_id":%q,"name":%q,"agent_status":"working","agent":"claude","cwd":%q}`, p.id, p.name, p.cwd))
		all = append(all, fmt.Sprintf(`{"pane_id":%q,"cwd":%q}`, p.id, p.cwd))
		outs[key("git", "-C", p.cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")] = "/repo/.git/worktrees/" + p.id + "\n/repo/.git\n" + p.cwd + "\n"
		outs[key("git", "-C", p.cwd, "branch", "--show-current")] = p.branch + "\n"
		outs[key("git", "-C", p.cwd, "rev-parse", "HEAD")] = "aaaa\n"
		outs[key("git", "-C", p.cwd, "remote", "get-url", "origin")] = p.origin + "\n"
	}
	outs[key("herdr", "agent", "list")] = `{"result":{"agents":[` + strings.Join(agents, ",") + `]}}`
	outs[key("herdr", "pane", "list")] = `{"result":{"panes":[` + strings.Join(all, ",") + `]}}`
	return &scriptRunner{outs: outs}
}

// authorPaneSetup is an open PR on branch "feature" whose task 2481 has no
// bound job and whose body names a Notify-Lane trailer, on one configured
// host holding one worker pane on that branch. The worker's job record names
// lane w-9-lane.
func authorPaneSetup(t *testing.T, panes ...aPane) (*Service, *jobSink, *scriptRunner) {
	t.Helper()
	s, g, _, _, _, _ := deliverSetup(t)
	sink := s.Sink.(*jobSink)
	delete(sink.jobs, "2481")
	sink.lanes["2481"] = "task-lane"
	sink.recs["w-9"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-9", HubLane: "w-9-lane"}
	if len(panes) == 0 {
		panes = []aPane{{"w1:p1", "w-9", "/wt/a", "feature", projectOrigin}}
	}
	run := authorHost(panes...)
	s.Runner = run
	s.Config.Hosts = []reap.Host{{Name: "local"}}
	g.pull.Body = "Summary\n\nMergewake-Task: 2481\nMergewake-Notify-Lane: trailer-lane\n"
	return s, sink, run
}

// (a) A task with a bound job routes to the job's lane as before, without
// reading any pane.
func TestAuthorPaneJobBindingNeedsNoPaneLookup(t *testing.T) {
	s, sink, run := authorPaneSetup(t)
	sink.jobs["2481"] = "w-7"
	sink.recs["w-7"] = integrations.JobRecord{State: "spawned", Label: "w-7", OwnerLane: "integrator"}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7" || rt.via != "task 2481 job w-7" || len(run.calls) != 0 {
		t.Fatalf("route %q via %q, runner calls %v", rt.lane, rt.via, run.calls)
	}
}

// (b) No job binding, one pane on the PR's branch in this repository: its
// job's lane, and the event says which pane chose it.
func TestAuthorPaneFindingsRoute(t *testing.T) {
	s, _, _ := authorPaneSetup(t)
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-9-lane" || rt.via != "author pane w1:p1 job w-9" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (c) READY reaches the same worker, and shares FINDINGS' host scan when the
// evaluation shares a context.
func TestAuthorPaneReadyRouteSharesScan(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	ctx := WithAuthorScan(context.Background())
	pr := s.GH.(*fakeGH).pull
	rt, e := s.findingsRoute(ctx, testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	lane, via, e := s.notifyLane(ctx, testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	if lane != "w-9-lane" || via != "author pane w1:p1 job w-9" || rt.lane != lane {
		t.Fatalf("ready %q via %q, findings %q", lane, via, rt.lane)
	}
	lists := 0
	for _, c := range run.calls {
		if strings.Contains(c, "herdr\x00agent\x00list") {
			lists++
		}
	}
	if lists != 1 {
		t.Fatalf("agent list read %d times for one evaluation: %q", lists, run.calls)
	}
}

// (d) Two panes on the PR's branch are ambiguous: the Notify-Lane trailer.
func TestAuthorPaneAmbiguousFallsThrough(t *testing.T) {
	s, _, _ := authorPaneSetup(t,
		aPane{"w1:p1", "w-9", "/wt/a", "feature", projectOrigin},
		aPane{"w1:p2", "w-8", "/wt/b", "feature", projectOrigin})
	pr := s.GH.(*fakeGH).pull
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	lane, via, e := s.notifyLane(context.Background(), testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "trailer-lane" || rt.via != "notify-lane trailer" || lane != "task-lane" || via != "task 2481" {
		t.Fatalf("findings %q via %q; ready %q via %q", rt.lane, rt.via, lane, via)
	}
}

// (e) No pane on the branch: today's order — trailer, then task lane, then desk.
func TestAuthorPaneNoMatchKeepsOrder(t *testing.T) {
	s, sink, _ := authorPaneSetup(t, aPane{"w1:p1", "w-9", "/wt/a", "other-branch", projectOrigin})
	pr := s.GH.(*fakeGH).pull
	r := testRepoFor(s)
	rt, e := s.findingsRoute(context.Background(), r, pr)
	if e != nil || rt.lane != "trailer-lane" || rt.via != "notify-lane trailer" {
		t.Fatalf("trailer: %+v, %v", rt, e)
	}
	s.GH.(*fakeGH).pull.Body = "Mergewake-Task: 2481\n"
	pr = s.GH.(*fakeGH).pull
	rt, e = s.findingsRoute(context.Background(), r, pr)
	if e != nil || rt.lane != "task-lane" || rt.via != "task 2481" {
		t.Fatalf("task lane: %+v, %v", rt, e)
	}
	sink.lanes["2481"] = "operator-desk"
	rt, e = s.findingsRoute(context.Background(), r, pr)
	if e != nil || rt.lane != "operator-desk" || rt.via != "unknown author; desk" {
		t.Fatalf("desk: %+v, %v", rt, e)
	}
}

// (f) A pane on the same branch name in another repository is not this PR's author.
func TestAuthorPaneOtherRepositoryNotMatched(t *testing.T) {
	s, _, _ := authorPaneSetup(t, aPane{"w1:p1", "w-9", "/wt/a", "feature", "git@github.com:example/other.git"})
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "trailer-lane" || rt.via != "notify-lane trailer" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (g) A host that cannot list its panes is an error to retry, in both routes,
// not a reroute to the trailer.
func TestAuthorPaneHostErrorRetries(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	run.errs = map[string]error{key("herdr", "agent", "list"): errors.New("herdr unreachable")}
	pr := s.GH.(*fakeGH).pull
	if rt, e := s.findingsRoute(context.Background(), testRepoFor(s), pr); e == nil {
		t.Fatalf("findings rerouted to %q via %q", rt.lane, rt.via)
	}
	if lane, via, e := s.notifyLane(context.Background(), testRepoFor(s), pr); e == nil {
		t.Fatalf("ready rerouted to %q via %q", lane, via)
	}
}

// (h) The author-scope FINDINGS text tells the author how to acknowledge, by
// the lane the pane resolved.
func TestAuthorPaneFindingsTextCarriesLaneInAck(t *testing.T) {
	s, _, _ := authorPaneSetup(t)
	pr := s.GH.(*fakeGH).pull
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	text := findingsText(testRepoFor(s), pr, "author", rt.lane, []pendingFinding{{thread: "T1", severity: "high", path: "tool", line: 3}})
	if !strings.Contains(text, "ack: panewire inbox list --lane w-9-lane --unread") || !strings.Contains(text, "for lane w-9-lane") {
		t.Fatalf("text %q", text)
	}
}

func testRepoFor(*Service) config.Repo { return config.Repo{Name: "example/project", Branch: "main"} }
