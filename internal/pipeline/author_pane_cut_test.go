package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// scanTimeout is a worktree inspect that did not answer in time: the cut that
// fails a host after its pane list was already read.
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

// A live takeover pane on the PR's head branch, read from a host that was cut
// after its pane list was read, is still named. The cut stays a returned error.
func TestTakeoverNamesPaneOnHostCutAfterPaneListRead(t *testing.T) {
	s, _, run := takeoverSetup(t, paneB, aPane{"w1:p4", "w-5", "/wt/hang", "other", projectOrigin})
	hangInspect(run, "/wt/hang")
	names, err := s.takeoverPanes(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull, "w-7", "")
	if err == nil {
		t.Fatal("takeoverPanes dropped the partial scan error")
	}
	if !slices.Equal(names, []string{"w-9"}) {
		t.Fatalf("takeoverPanes = %v, want [w-9]", names)
	}
}

// sshGitOuts rewrites a local scripted git probe for panes into the ssh-wrapped
// form a remote host runs it under.
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

// (a) A terminal bound job, a cut host whose list holds an uninspected head-
// branch pane after the hung cwd: the bound lane stands and liveness is unknown.
func TestBoundLaneStandsWhenCutHostHidesHeadBranchPane(t *testing.T) {
	hang := aPane{"w1:p4", "w-5", "/wt/hang", "other", projectOrigin}
	s, sink, run := takeoverSetup(t, hang, paneB)
	hangInspect(run, "/wt/hang")
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || !rt.unknown || rt.finished != "" {
		t.Fatalf("route lane %q unknown %v finished %q; want bound lane w-7-lane, unknown, not finished", rt.lane, rt.unknown, rt.finished)
	}
}

// (b) A terminal bound job with both head-branch candidates seen, one on a cut
// host: the candidates are kept and no single takeover is chosen.
func TestBoundLaneKeepsCandidatesWhenCutHostIsSeen(t *testing.T) {
	hostA := reap.Host{Name: "local"}
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	hang := aPane{"w1:p5", "w-5", "/wt/hang", "other", projectOrigin}
	s, sink, run := takeoverSetup(t, paneB)
	s.Config.Hosts = []reap.Host{hostA, hostB}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	for k, v := range hostPaneOuts(hostB, paneC, hang) {
		run.outs[k] = v
	}
	for k, v := range sshGitOuts(hostB, authorHost(paneC).outs) {
		run.outs[k] = v
	}
	run.errs = map[string]error{key("ssh", "-o", "BatchMode=yes", "-T", "-n", hostB.SSH, reap.Join([]string{"git", "-C", hang.cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"})): scanTimeout{}}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || rt.finished != "" || !slices.Equal(rt.ambiguous, []string{"w-9", "w-8"}) {
		t.Fatalf("route lane %q finished %q ambiguous %v; want bound lane kept with candidates w-9, w-8", rt.lane, rt.finished, rt.ambiguous)
	}
}

// A terminal bound job; host B's agent panes are all inspected, but a
// non-agent pane's cwd times out, so B is failed with its pane list read. One
// head-branch candidate on healthy host A. The bound lane stands, named
// finished, and the candidate is kept.
func TestBoundLaneFinishedKeptOnPartialScanWithCandidate(t *testing.T) {
	hostA := reap.Host{Name: "local"}
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	agentB := aPane{"w1:p6", "w-6", "/wt/x", "other", projectOrigin}
	s, sink, run := takeoverSetup(t, paneB)
	s.Config.Hosts = []reap.Host{hostA, hostB}
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", HubLane: "w-7-lane"}
	for k, v := range hostPaneOuts(hostB, agentB) {
		run.outs[k] = v
	}
	// The full pane list also holds a shell pane that is not an agent.
	sshHerdr := func(args ...string) string {
		return key("ssh", "-o", "BatchMode=yes", "-T", "-n", hostB.SSH, reap.Join(args))
	}
	run.outs[sshHerdr("herdr", "pane", "list")] = `{"result":{"panes":[{"pane_id":"w1:p6","cwd":"/wt/x"},{"pane_id":"w2:p9","cwd":"/wt/shell"}]}}`
	for k, v := range sshGitOuts(hostB, authorHost(agentB).outs) {
		run.outs[k] = v
	}
	run.errs = map[string]error{sshHerdr("git", "-C", "/wt/shell", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): scanTimeout{}}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7-lane" || rt.finished != "w-7-lane" || !slices.Equal(rt.ambiguous, []string{"w-9"}) {
		t.Fatalf("route lane %q finished %q ambiguous %v; want bound lane kept, finished, candidates [w-9]", rt.lane, rt.finished, rt.ambiguous)
	}
}
