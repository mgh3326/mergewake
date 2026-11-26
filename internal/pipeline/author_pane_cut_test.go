package pipeline

import (
	"context"
	"errors"
	"slices"
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
// still answers for the pane names and ids its list holds. The other healthy
// host is read as well, so the job's liveness is decided, not unknown.
func TestAuthorFinishedCutHostPaneListStillAnswers(t *testing.T) {
	hostA := reap.Host{Name: "node-a"} // local
	hostB := reap.Host{Name: "node-b", SSH: "ops@node-b", Machine: "mb-b"}
	done := integrations.JobRecord{State: "completed", Host: "node-a", Label: "w-7", PaneID: "w1:p1"}
	for _, tt := range []struct {
		name     string
		panesA   []aPane
		finished bool
	}{
		{"cut host holds another job's pane", []aPane{{"w1:p9", "w-9", "/wt/hang", "other", projectOrigin}}, true},
		{"cut host holds the job's pane", []aPane{{"w1:p1", "w-7", "/wt/hang", "feature", projectOrigin}}, false},
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
			if err != nil || unknown || finished != tt.finished {
				t.Fatalf("authorFinished = finished %v, unknown %v, err %v; want finished %v, unknown false", finished, unknown, err, tt.finished)
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
