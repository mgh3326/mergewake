package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// midPutGH is fakeGH whose first merge PUT blocks until hold is closed, then
// writes the squash onto the base the way GitHub did in the incident, and
// answers 500. Every PR read is counted by the caller that made it.
type midPutGH struct {
	*fakeGH
	mu      sync.Mutex
	puts    int
	pulls   map[string]int
	started chan struct{}
	hold    chan struct{}
}

func (g *midPutGH) Pull(ctx context.Context, repo string, n int) (github.Pull, error) {
	g.mu.Lock()
	g.pulls[who(ctx)]++
	g.mu.Unlock()
	return g.fakeGH.Pull(ctx, repo, n)
}

func (g *midPutGH) pullsOf(w string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pulls[w]
}

func (g *midPutGH) Merge(ctx context.Context, repo string, n int, sha string) error {
	g.mu.Lock()
	g.puts++
	first := g.puts == 1
	g.mu.Unlock()
	if !first {
		return g.fakeGH.Merge(ctx, repo, n, sha)
	}
	close(g.started)
	<-g.hold
	g.fakeGH.head = shaSquashed
	g.fakeGH.parents[shaSquashed] = shaB
	g.fakeGH.messages[shaSquashed] = "Add the thing (#7)"
	return &github.UnknownOutcomeError{Err: &github.APIError{Code: 500, Method: "PUT", Path: "/pulls/7/merge"}}
}

// A caller settling while another caller's PUT is in flight waits for the
// PUT. Once the PUT ends in 5xx with its squash on the base, the attempt stays
// pending, the settle holds the PR, and no later pass sends a second merge.
func TestSettleWaitsForMidPutAttemptAndHolds(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.messages = map[string]string{}
	g.parents = map[string]string{}
	markReviewed(g)
	m := &midPutGH{fakeGH: g, pulls: map[string]int{}, started: make(chan struct{}), hold: make(chan struct{})}
	s.GH = m

	aErr := make(chan error, 1)
	go func() { aErr <- s.Land(asPR("A"), r, 7, Options{}) }()
	<-m.started

	bErr := make(chan error, 1)
	go func() { bErr <- s.Land(asPR("B"), r, 7, Options{}) }()
	settleWaitFor(t, "B's first read of the PR", func() bool { return m.pullsOf("B") >= 1 })
	time.Sleep(100 * time.Millisecond)
	if n := m.pullsOf("B"); n != 1 {
		t.Fatalf("B settled while A's PUT was in flight: %d PR reads, want 1 (its own before the settle)", n)
	}

	close(m.hold)
	if e := <-aErr; !github.IsUnknownOutcome(e) {
		t.Fatalf("A: want unknown outcome, got %v", e)
	}
	if e := <-bErr; e == nil || !strings.Contains(e.Error(), "landed on main") {
		t.Fatalf("B: want hold, got %v", e)
	}
	if e := s.Land(context.Background(), r, 7, Options{}); e == nil || !strings.Contains(e.Error(), "landed on main") {
		t.Fatalf("later pass: want hold, got %v", e)
	}
	if m.puts != 1 || g.mergeCount != 0 {
		t.Fatalf("merge PUTs %d, merges %d; want 1 and 0", m.puts, g.mergeCount)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); e != nil {
		t.Fatalf("unknown attempt not kept pending: %v", e)
	}
}

// The file lock excludes a second holder until release. A second process
// cannot be run cheaply in a test, so this exercises the helper directly: a
// second flock on the same file, from another open file description, blocks.
func TestMergeFileLockBlocksSecondAcquisition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "merges.lock")
	unlock, e := lockFile(path)
	if e != nil {
		t.Fatal(e)
	}
	got := make(chan error, 1)
	go func() {
		u, e := lockFile(path)
		if e == nil {
			u()
		}
		got <- e
	}()
	select {
	case e := <-got:
		unlock()
		t.Fatalf("second lock acquired while the first was held (err %v)", e)
	case <-time.After(150 * time.Millisecond):
	}
	unlock()
	select {
	case e := <-got:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second lock still blocked after release")
	}
}
