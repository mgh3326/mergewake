package pipeline

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// A worktree.list the hub answers 503 node_lost is one skipped host: the
// listing's repository keeps no error, and the host is not a failure.
func TestLegacyListHubOutageIsASkippedHost(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	hub.listErr = map[string]string{listCommon: "node_lost"}
	hub.errStatus = map[string]int{"worktree.list": http.StatusServiceUnavailable}
	err := s.ReapLegacy(context.Background(), testRepo, "", plan())
	_, skips, kept := SplitReapRepoError(err)
	if len(skips) != 1 || skips[0] != (HostSkip{Host: "node", Reason: "node_lost"}) {
		t.Fatalf("skips=%v err=%v", skips, err)
	}
	if kept != nil {
		t.Fatalf("a skipped host kept a repository error: %v", kept)
	}
}

// A worktree inspect the hub's proxy answers 502 with no hub body is a skipped
// host, not a failure: the pane scan fails the host and the repository goes on.
func TestReapGatewayInspectIsASkippedHost(t *testing.T) {
	hub := newFakeReapHub()
	hub.agents = nodeAgents("/wt/u1")
	hub.inspectErr = map[string]string{"/wt/u1": "unexpected-response"}
	hub.errStatus = map[string]int{"worktree.inspect": http.StatusBadGateway}
	s, _ := hubSvc(t, hub, &scriptRunner{})
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	_, skips, kept := SplitReapRepoError(err)
	if len(skips) != 1 || skips[0] != (HostSkip{Host: "node", Reason: "unexpected-response"}) {
		t.Fatalf("skips=%v err=%v", skips, err)
	}
	if kept != nil {
		t.Fatalf("a skipped host kept a repository error: %v", kept)
	}
}

// An auth refusal on worktree.list is still a repository failure, never a host
// skip: the credential is refused, so the host's state is unknown, not down.
func TestLegacyListAuthStaysAFailure(t *testing.T) {
	s, hub, _ := legacyFixture(t)
	hub.forbid = map[string]bool{"worktree.list": true}
	err := s.ReapLegacy(context.Background(), testRepo, "", plan())
	_, skips, kept := SplitReapRepoError(err)
	if len(skips) != 0 || kept == nil {
		t.Fatalf("an auth refusal skipped the host: skips=%v kept=%v", skips, kept)
	}
}

// A reviewer checkout whose inspect the hub's proxy answers 502 is a skipped
// host for the pass: its labels wait, and the repository keeps no error.
func TestReapReviewerGatewayInspectIsASkippedHost(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	f.hub.inspectErr = map[string]string{"/repo/" + label: "unexpected-response"}
	f.hub.errStatus = map[string]int{"worktree.inspect": http.StatusBadGateway}

	err := f.reap(t, 0, Options{})
	_, skips, kept := SplitReapRepoError(err)
	if len(skips) != 1 || skips[0] != (HostSkip{Host: "node", Reason: "unexpected-response"}) || kept != nil {
		t.Fatalf("skips=%v kept=%v err=%v", skips, kept, err)
	}
}
