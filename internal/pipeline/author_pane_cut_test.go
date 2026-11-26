package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// scanTimeout is a worktree inspect that did not answer in time: the cut that
// fails a host partway through its scan.
type scanTimeout struct{}

func (scanTimeout) Error() string { return "i/o timeout" }
func (scanTimeout) Timeout() bool { return true }

// hangInspect makes the worktree inspect of cwd time out on run.
func hangInspect(run *scriptRunner, cwd string) {
	if run.errs == nil {
		run.errs = map[string]error{}
	}
	run.errs[key("git", "-C", cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")] = scanTimeout{}
}

// sshGitOuts rewrites a local scripted git probe into the ssh-wrapped form a
// remote host runs it under.
func sshGitOuts(h reap.Host, outs map[string]string) map[string]string {
	res := map[string]string{}
	for k, v := range outs {
		parts := strings.Split(k, "\x00")
		if parts[0] != "git" {
			continue
		}
		res[key("ssh", "-o", "BatchMode=yes", "-T", "-n", h.SSH, reap.Join(parts))] = v
	}
	return res
}

// scriptRemoteHost scripts remote host b: its agent panes, then its full pane
// list (the agents plus the non-agent extra panes). Every agent cwd is
// inspected except timeoutCwd, whose inspect times out.
func scriptRemoteHost(run *scriptRunner, b reap.Host, agents, extra []aPane, timeoutCwd string) {
	ssh := func(args ...string) string {
		return key("ssh", "-o", "BatchMode=yes", "-T", "-n", b.SSH, reap.Join(args))
	}
	for k, v := range hostPaneOuts(b, agents...) {
		run.outs[k] = v
	}
	var all []string
	for _, p := range append(append([]aPane{}, agents...), extra...) {
		all = append(all, fmt.Sprintf(`{"pane_id":%q,"cwd":%q}`, p.id, p.cwd))
	}
	run.outs[ssh("herdr", "pane", "list")] = `{"result":{"panes":[` + strings.Join(all, ",") + `]}}`
	for _, p := range agents {
		for k, v := range sshGitOuts(b, authorHost(p).outs) {
			run.outs[k] = v
		}
	}
	if run.errs == nil {
		run.errs = map[string]error{}
	}
	run.errs[ssh("git", "-C", timeoutCwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")] = scanTimeout{}
}

// A host whose pane list was read in full, then cut by one cwd's inspect,
// still proves a job live by a name or pane id its list holds. Without one, the
// cut hides an agent pane whose cwd was never inspected, so liveness is unknown.
func TestAuthorFinishedCutHostPaneListStillAnswers(t *testing.T) {
	hostA := reap.Host{Name: "node-a"} // local
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	done := integrations.JobRecord{State: "completed", Host: "node-a", Label: "w-7", PaneID: "w1:p1"}
	for _, tt := range []struct {
		name     string
		panesA   []aPane
		finished bool
		unknown  bool
	}{
		{"cut host holds another job's pane", []aPane{{"w1:p9", "w-9", "/wt/hang", "other", projectOrigin}}, false, true},
		{"cut host holds the job's pane", []aPane{{"w1:p1", "w-7", "/wt/hang", "feature", projectOrigin}}, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := &scriptRunner{outs: map[string]string{}}
			for k, v := range hostPaneOuts(hostA, tt.panesA...) {
				run.outs[k] = v
			}
			for k, v := range hostPaneOuts(hostB) {
				run.outs[k] = v
			}
			hangInspect(run, "/wt/hang")
			s := &Service{Config: config.Config{Hosts: []reap.Host{hostA, hostB}}, Runner: run}
			finished, unknown, err := s.authorFinished(context.Background(), "w-7", done)
			if err != nil || unknown != tt.unknown || finished != tt.finished {
				t.Fatalf("authorFinished = finished %v, unknown %v, err %v; want finished %v, unknown %v", finished, unknown, err, tt.finished, tt.unknown)
			}
		})
	}
}

// A host cut only on a non-agent cwd, after every agent cwd was inspected, has
// its agent trees read: the terminal job is proven finished.
func TestAuthorFinishedShellCutStaysFinished(t *testing.T) {
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	done := integrations.JobRecord{State: "completed", Label: "w-7"}
	run := &scriptRunner{outs: map[string]string{}}
	scriptRemoteHost(run, hostB, []aPane{{"w1:p6", "w-6", "/wt/x", "other", projectOrigin}}, []aPane{{"w2:p9", "", "/wt/shell", "", ""}}, "/wt/shell")
	s := &Service{Config: config.Config{Hosts: []reap.Host{hostB}}, Runner: run}
	finished, unknown, err := s.authorFinished(context.Background(), "w-7", done)
	if err != nil || unknown || !finished {
		t.Fatalf("authorFinished = finished %v, unknown %v, err %v; want finished", finished, unknown, err)
	}
}

// A host whose pane list was never read stays unknown, even beside a host
// that answered.
func TestAuthorFinishedUnreadPaneListIsUnknown(t *testing.T) {
	hostA := reap.Host{Name: "node-a"} // local
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	done := integrations.JobRecord{State: "completed", Host: "node-a", Label: "w-7", PaneID: "w1:p1"}
	run := &scriptRunner{outs: map[string]string{}}
	for k, v := range hostPaneOuts(hostB) {
		run.outs[k] = v
	}
	run.errs = map[string]error{key("herdr", "agent", "list"): errors.New("pane list failed")}
	s := &Service{Config: config.Config{Hosts: []reap.Host{hostA, hostB}}, Runner: run}
	finished, unknown, err := s.authorFinished(context.Background(), "w-7", done)
	if finished || !unknown {
		t.Fatalf("authorFinished = finished %v, unknown %v, err %v; want unknown", finished, unknown, err)
	}
}

// A host cut inside its agent loop has lost its agent trees: its candidates
// are not judged, and the scan error stays returned.
func TestTakeoverSkipsHostCutInAgentLoop(t *testing.T) {
	s, _, run := takeoverSetup(t, paneB, aPane{"w1:p4", "w-5", "/wt/hang", "other", projectOrigin})
	hangInspect(run, "/wt/hang")
	names, err := s.takeoverPanes(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull, "w-7", "")
	if err == nil {
		t.Fatal("takeoverPanes dropped the partial scan error")
	}
	if len(names) != 0 {
		t.Fatalf("takeoverPanes = %v, want none from the host cut in its agent loop", names)
	}
}

// A terminal bound job; a head-branch candidate sits on host B, whose
// non-agent pane's cwd times out after every agent cwd was inspected. B's
// agent trees are read, so the candidate is the unique takeover: finished.
func TestBoundLaneTakesOverCandidateOnShellCutHost(t *testing.T) {
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	s, sink, run := takeoverSetup(t, aPane{"w1:p7", "w-6", "/wt/q", "other", projectOrigin})
	s.Config.Hosts = []reap.Host{{Name: "local"}, hostB}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	scriptRemoteHost(run, hostB, []aPane{paneB}, []aPane{{"w2:p9", "", "/wt/shell", "", ""}}, "/wt/shell")
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-9-lane" || rt.finished != "w-7-lane" || rt.unknown {
		t.Fatalf("route lane %q finished %q unknown %v; want takeover w-9-lane, finished w-7-lane", rt.lane, rt.finished, rt.unknown)
	}
}

// A terminal bound job with two head-branch candidates, one on host B whose
// non-agent cwd times out after its agent trees were read. Neither candidate
// is picked: the route goes to the explicit lane, not to the one on A.
func TestBoundLaneDoesNotPickOneOfTwoOnShellCutHost(t *testing.T) {
	hostA := reap.Host{Name: "local"}
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	s, sink, run := takeoverSetup(t, paneB)
	s.Config.Hosts = []reap.Host{hostA, hostB}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	scriptRemoteHost(run, hostB, []aPane{paneC}, []aPane{{"w2:p9", "", "/wt/shell", "", ""}}, "/wt/shell")
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "trailer-lane" || rt.finished != "w-7-lane" {
		t.Fatalf("route lane %q finished %q; want explicit trailer-lane, finished w-7-lane", rt.lane, rt.finished)
	}
}

// A terminal bound job; host B's agent loop is cut at a hung cwd before the
// candidate is inspected. The bound lane stands, unknown and not finished.
func TestBoundLaneStandsWhenAgentLoopCut(t *testing.T) {
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	hang := aPane{"w1:p4", "w-5", "/wt/hang", "other", projectOrigin}
	s, sink, run := takeoverSetup(t, paneB)
	s.Config.Hosts = []reap.Host{hostB}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	scriptRemoteHost(run, hostB, []aPane{hang, paneB}, nil, "/wt/hang")
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || !rt.unknown || rt.finished != "" {
		t.Fatalf("route lane %q unknown %v finished %q; want bound lane w-7-lane, unknown, not finished", rt.lane, rt.unknown, rt.finished)
	}
}

// The same as TestBoundLaneStandsWhenAgentLoopCut, cut by a spent reap budget
// inside the agent loop rather than by a hung inspect.
func TestBoundLaneStandsWhenBudgetCutsAgentLoop(t *testing.T) {
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	s, sink, run := takeoverSetup(t, paneB)
	s.Config.Hosts = []reap.Host{hostB}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	scriptRemoteHost(run, hostB, []aPane{paneB}, nil, "/wt/none")
	delete(run.errs, key("ssh", "-o", "BatchMode=yes", "-T", "-n", hostB.SSH, reap.Join([]string{"git", "-C", "/wt/none", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"})))
	ctx := context.WithValue(context.Background(), reapBudgetKey{}, reapBudget{deadline: time.Now().Add(-time.Second)})
	rt, e := s.findingsRoute(ctx, testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || !rt.unknown || rt.finished != "" {
		t.Fatalf("route lane %q unknown %v finished %q; want bound lane w-7-lane, unknown, not finished", rt.lane, rt.unknown, rt.finished)
	}
}

// An agent cwd whose inspect fails with a tolerated, non-timeout error leaves
// the host without that tree: the host is not complete, so the terminal job is
// unknown, and the bound lane stands.
func TestBoundLaneStandsWhenAgentInspectErrorIsTolerated(t *testing.T) {
	errPane := aPane{"w1:p8", "w-5", "/wt/err", "other", projectOrigin}
	s, sink, run := takeoverSetup(t, errPane)
	run.errs = map[string]error{key("git", "-C", "/wt/err", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): errors.New("signal: terminated")}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || !rt.unknown || rt.finished != "" {
		t.Fatalf("route lane %q unknown %v finished %q; want bound lane w-7-lane, unknown, not finished", rt.lane, rt.unknown, rt.finished)
	}
}

// The same tolerated inspect error, with a later head-branch candidate on that
// host: the candidate is not judged, so there is no takeover and no lane is
// cleared.
func TestBoundLaneNoTakeoverAfterTolerated(t *testing.T) {
	errPane := aPane{"w1:p8", "w-5", "/wt/err", "other", projectOrigin}
	s, sink, run := takeoverSetup(t, errPane, paneB)
	run.errs = map[string]error{key("git", "-C", "/wt/err", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): errors.New("signal: terminated")}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || !rt.unknown || rt.finished != "" {
		t.Fatalf("route lane %q unknown %v finished %q; want bound lane w-7-lane, unknown, not finished", rt.lane, rt.unknown, rt.finished)
	}
}
