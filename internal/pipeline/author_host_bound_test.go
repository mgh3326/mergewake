package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/reap"
)

// attachHub puts host first in s's configured hosts and makes the hub reach it:
// the same panewire transport reap uses, with a service credential file.
func attachHub(t *testing.T, s *Service, hub *fakeReapHub, host reap.Host) {
	t.Helper()
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "svc-mergewake.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	s.Config.Hosts = append([]reap.Host{host}, s.Config.Hosts...)
}

// nodeAgents is the node's agent and pane rows, one pane per cwd.
func nodeAgents(cwds ...string) map[string]any {
	var agents, panes []any
	for i, cwd := range cwds {
		id := fmt.Sprintf("w2:p%d", i+1)
		agents = append(agents, map[string]any{"pane_id": id, "name": fmt.Sprintf("wk-u%d", i+1), "agent_status": "idle", "agent": "claude", "cwd": cwd})
		panes = append(panes, map[string]any{"pane_id": id, "agent_status": "idle", "cwd": cwd})
	}
	return map[string]any{"machine_id": "node-a", "state": "connected", "agents": agents, "panes": panes, "panes_ok": true}
}

// withAuthorHostBound shrinks the per-host author scan bound for one test.
func withAuthorHostBound(t *testing.T, d time.Duration) {
	t.Helper()
	old := authorHostScanBound
	authorHostScanBound = d
	t.Cleanup(func() { authorHostScanBound = old })
}

// hostFailed reports whether route names host among the hosts it could not read.
func hostFailed(rt findingsRoute, host string) bool { return slices.Contains(rt.failedHosts, host) }

// An unreachable node fails its host at the first pane: the scan does not
// wait on the node once per pane, and the route resolves on the host that
// answered, named partial.
func TestAuthorHostUnreachableFailsHostAtOnce(t *testing.T) {
	hub := newFakeReapHub()
	s, _, _ := authorPaneSetup(t, paneB)
	attachHub(t, s, hub, reap.Host{Name: "node", SSH: "ops@node.example", Machine: "node-a"})
	hub.agents = nodeAgents("/wt/u1", "/wt/u2")
	hub.inspectErr = map[string]string{"/wt/u1": "node_timeout", "/wt/u2": "node_timeout"}

	rt, err := s.findingsRoute(WithAuthorScan(context.Background()), testRepoFor(s), s.GH.(*fakeGH).pull)
	if err != nil {
		t.Fatal(err)
	}
	if rt.lane != "w-9-lane" || !strings.Contains(rt.via, "partial pane scan") || !hostFailed(rt, "node") {
		t.Fatalf("route %q via %q failed hosts %v, want w-9-lane partial with node failed", rt.lane, rt.via, rt.failedHosts)
	}
	if n := len(hub.ops("worktree.inspect")); n != 1 {
		t.Fatalf("unreachable node was asked to inspect %d worktrees, want 1 (the scan stops at the first)", n)
	}
}

// A node whose calls never answer is cut by the per-host bound: the route
// returns within it, named partial, and the host is kept failed for the TTL,
// not dropped.
func TestAuthorHostHangingScanRoutesWithinBoundAsFailed(t *testing.T) {
	hub := newFakeReapHub()
	s, _, _ := authorPaneSetup(t, paneB)
	attachHub(t, s, hub, reap.Host{Name: "node", SSH: "ops@node.example", Machine: "node-a"})
	hub.agents = nodeAgents("/wt/u1")
	hub.hangInspect = "/wt/u1"
	withAuthorHostBound(t, 50*time.Millisecond)

	start := time.Now()
	rt, err := s.findingsRoute(WithAuthorScan(context.Background()), testRepoFor(s), s.GH.(*fakeGH).pull)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("route took %v with a bound of 50ms", elapsed)
	}
	if rt.lane != "w-9-lane" || !strings.Contains(rt.via, "partial pane scan") || !hostFailed(rt, "node") {
		t.Fatalf("route %q via %q failed hosts %v, want w-9-lane partial with node failed", rt.lane, rt.via, rt.failedHosts)
	}
	hostScanMu.Lock()
	e := s.hostScans["node"]
	hostScanMu.Unlock()
	if e == nil || e.dropped || e.hs == nil || !e.hs.failed {
		t.Fatalf("host entry after a bound expiry = %+v, want kept and failed", e)
	}

	// Within the TTL a later evaluation reads the failed host, not the node.
	// The cut request's handler may still be reading hangInspect, so the
	// write takes the hub's lock.
	hub.mu.Lock()
	hub.hangInspect = ""
	hub.mu.Unlock()
	if _, err := s.findingsRoute(WithAuthorScan(context.Background()), testRepoFor(s), s.GH.(*fakeGH).pull); err != nil {
		t.Fatal(err)
	}
	if n := len(hub.ops("worktree.inspect")); n != 0 {
		t.Fatalf("a later evaluation inside the TTL inspected %d worktrees, want 0 (the failed scan is shared)", n)
	}
}

// An evaluation that waits on a host another evaluation is scanning gets that
// scan's failed result when it ends, and does not start a scan of its own.
func TestAuthorHostFailureSharedWithConcurrentEvaluation(t *testing.T) {
	hub := newFakeReapHub()
	s, _, _ := authorPaneSetup(t, paneB)
	attachHub(t, s, hub, reap.Host{Name: "node", SSH: "ops@node.example", Machine: "node-a"})
	hub.agents = nodeAgents("/wt/u1")
	hub.hangInspect = "/wt/u1"
	withAuthorHostBound(t, 200*time.Millisecond)

	first := make(chan error, 1)
	go func() {
		_, err := s.authorHosts(WithAuthorScan(context.Background()))
		first <- err
	}()
	// The first evaluation owns the host entry once it has started its scan.
	deadline := time.Now().Add(10 * time.Second)
	for {
		hostScanMu.Lock()
		owned := s.hostScans["node"] != nil
		hostScanMu.Unlock()
		if owned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first evaluation never started the node scan")
		}
		time.Sleep(time.Millisecond)
	}

	second, err := s.authorHosts(context.Background())
	if !hostScanOnly(err) || !strings.Contains(err.Error(), "node") {
		t.Fatalf("waiting evaluation error %v, want the node's scan failure", err)
	}
	var failed bool
	for _, hs := range second {
		failed = failed || hs.failed && hs.host.Name == "node"
	}
	if !failed {
		t.Fatalf("waiting evaluation did not get the node's failed scan: %+v", second)
	}
	if ferr := <-first; !hostScanOnly(ferr) {
		t.Fatalf("first evaluation error %v, want the node's scan failure", ferr)
	}
	hub.mu.Lock()
	n := hub.agentReads
	hub.mu.Unlock()
	if n != 1 {
		t.Fatalf("node was scanned %d times across two evaluations, want 1 (the waiter shares the scan)", n)
	}
}

// The default bound is above what a healthy host's scan has measured in
// production (25 to 89 s, reap_pass_hosts.go), so a healthy host is not cut.
func TestAuthorHostScanBoundCoversMeasuredScan(t *testing.T) {
	const measuredMax = 89 * time.Second
	if authorHostScanBound <= measuredMax {
		t.Fatalf("authorHostScanBound = %v, want above the measured %v scan cost", authorHostScanBound, measuredMax)
	}
}

// The two hanging host scans must be in flight at the same time. Each one
// announces its start and waits for the other to announce too; a sequential
// fan-out never opens the barrier, so the test fails by barrierWait rather
// than by a wall clock that a slow scheduler could trip.
func TestAuthorHostsBoundedConcurrentlyInConfigOrder(t *testing.T) {
	const hangingHosts = 2
	const barrierWait = 5 * time.Second
	s, _, _ := authorPaneSetup(t, paneB)
	s.Config.Reap.Transport = "ssh"
	s.Config.Hosts = []reap.Host{{Name: "first"}, {Name: "second"}, {Name: "healthy"}}
	withAuthorHostBound(t, 150*time.Millisecond)

	var mu sync.Mutex
	arrived := 0
	missed := false // a scan gave up waiting: the other host never overlapped it
	barrier := make(chan struct{})
	arrive := func() {
		mu.Lock()
		defer mu.Unlock()
		arrived++
		if arrived == hangingHosts {
			close(barrier)
		}
	}

	oldScan := authorHostScanFunc
	authorHostScanFunc = func(_ *Service, ctx context.Context, _ *reap.Hub, h reap.Host) *reapHost {
		if h.Name != "healthy" {
			arrive()
			select {
			case <-barrier:
			case <-time.After(barrierWait):
				mu.Lock()
				missed = true
				mu.Unlock()
			}
			scan, cancel := context.WithTimeout(ctx, authorHostScanBound)
			defer cancel()
			<-scan.Done()
			return &reapHost{host: h, failed: true}
		}
		return &reapHost{host: h}
	}
	t.Cleanup(func() { authorHostScanFunc = oldScan })
	ctx := WithAuthorScan(context.Background())
	hosts, err := s.authorHosts(ctx)
	mu.Lock()
	overlapped := !missed && arrived == hangingHosts
	mu.Unlock()
	if !overlapped {
		t.Fatal("the two hanging host scans did not overlap: one gave up waiting for the other to start")
	}
	if !hostScanOnly(err) {
		t.Fatalf("authorHosts error %v, want failures for both hanging hosts", err)
	}
	if got, want := []string{hosts[0].host.Name, hosts[1].host.Name, hosts[2].host.Name}, []string{"first", "second", "healthy"}; !slices.Equal(got, want) {
		t.Fatalf("host order %v, want %v", got, want)
	}
	if !hosts[0].failed || !hosts[1].failed || hosts[2].failed {
		t.Fatalf("host failures = [%v %v %v], want [true true false]", hosts[0].failed, hosts[1].failed, hosts[2].failed)
	}
	if got, want := authorScanFailures(ctx), []string{"first", "second"}; !slices.Equal(got, want) {
		t.Fatalf("failedHosts order %v, want %v", got, want)
	}
}
