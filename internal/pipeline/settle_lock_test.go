package pipeline

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// settleGH is fakeGH for several PRs of one repository whose calls overlap. The
// base head moves when a merge returns, as it does on GitHub, and every call
// is logged with the PR that made it (the context carries the name).
type settleGH struct {
	*fakeGH
	mu sync.Mutex
	// base is the base branch head; a merge replaces it with next.
	base, next string
	log        []string
	merges     int
	// hold blocks the first merge until it is closed; mergeStarted is closed
	// when that merge begins.
	hold         chan struct{}
	mergeStarted chan struct{}
	// findings gives each PR one unresolved finding of its own.
	findings bool
}

type whoKey struct{}

func asPR(who string) context.Context { return context.WithValue(context.Background(), whoKey{}, who) }

func who(ctx context.Context) string {
	w, _ := ctx.Value(whoKey{}).(string)
	return w
}

func (g *settleGH) logf(format string, args ...any) {
	g.log = append(g.log, fmt.Sprintf(format, args...))
}

func (g *settleGH) events() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.log)
}

func (g *settleGH) Pull(ctx context.Context, repo string, n int) (github.Pull, error) {
	p, e := g.fakeGH.Pull(ctx, repo, n)
	p.Number = n
	return p, e
}

func (g *settleGH) Head(ctx context.Context, repo, branch string) (string, error) {
	if branch == github.RecoveryBranch {
		return g.fakeGH.Head(ctx, repo, branch)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.logf("head %s %s", who(ctx), g.base)
	return g.base, nil
}

func (g *settleGH) Merge(ctx context.Context, repo string, n int, sha string) error {
	g.mu.Lock()
	g.merges++
	first := g.merges == 1 && g.hold != nil
	g.logf("merge-start %s", who(ctx))
	g.mu.Unlock()
	if first {
		close(g.mergeStarted)
		<-g.hold
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.base = g.next
	g.logf("merge-end %s", who(ctx))
	return nil
}

func (g *settleGH) ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fakeGH.ReadFile(ctx, repo, sha, name)
}

func (g *settleGH) CheckSnapshot(ctx context.Context, repo, sha string, required []string) (map[string]github.CI, map[string]time.Time, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fakeGH.CheckSnapshot(ctx, repo, sha, required)
}

func (g *settleGH) Commit(ctx context.Context, repo, sha string) (github.Commit, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fakeGH.Commit(ctx, repo, sha)
}

func (g *settleGH) ReviewFeedback(ctx context.Context, repo string, n int) (github.ReviewFeedback, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fb, e := g.fakeGH.ReviewFeedback(ctx, repo, n)
	if g.findings {
		fb.Threads = []github.ReviewThread{ownFinding(fmt.Sprintf("T%d", n), "high", 3)}
	}
	return fb, e
}

// settleSetup is a reviewed, green PR 7..7+prs-1 of one repository on a base
// that a merge moves from shaB to shaD.
func settleSetup(t *testing.T, prs int) (*Service, *settleGH, []int, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.pull.Body = "Summary\n\nMergewake-Task: 2481\n"
	sg := &settleGH{fakeGH: g, base: shaB, next: shaD}
	var numbers []int
	for i := range prs {
		n := 7 + i
		numbers = append(numbers, n)
		g.refs[doneRef(n, shaA)] = true
	}
	s.GH = sg
	return s, sg, numbers, r
}

// (b) Two PRs of one repository that both reach the merge: the second merge
// starts only after the first returned, and it reads the base head, and gates
// against it, after the first merge moved it.
func TestSecondMergeOfARepoGatesAgainstTheBaseTheFirstLeft(t *testing.T) {
	s, g, _, r := settleSetup(t, 2)
	g.hold, g.mergeStarted = make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	land := func(who string, n int) {
		defer wg.Done()
		errs <- s.Land(asPR(who), r, n, Options{})
	}
	wg.Add(1)
	go land("A", 7)
	<-g.mergeStarted
	wg.Add(1)
	go land("B", 8)
	// B reads the base once, to decide, then waits for A's merge. Give it the
	// time to read it again before the lock, if the lock were taken too late.
	settleWaitFor(t, "B's first read of the base", func() bool { return settleCount(g.events(), "head B ") >= 1 })
	deadline := time.After(300 * time.Millisecond)
	for done := false; !done; {
		select {
		case <-deadline:
			done = true
		case <-time.After(5 * time.Millisecond):
			done = settleCount(g.events(), "head B ") >= 2
		}
	}
	if slices.Contains(g.events(), "merge-start B") {
		t.Fatalf("B's merge started while A's was in flight: %v", g.events())
	}
	close(g.hold)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	ev := g.events()
	endA, startB := slices.Index(ev, "merge-end A"), slices.Index(ev, "merge-start B")
	if endA < 0 || startB < endA {
		t.Fatalf("B's merge did not start after A's returned: %v", ev)
	}
	lastHeadB := -1
	for i, e := range ev {
		if len(e) > 7 && e[:7] == "head B " {
			lastHeadB = i
		}
	}
	if lastHeadB < endA || ev[lastHeadB] != "head B "+shaD {
		t.Fatalf("B's last read of the base is %q at %d, want %q after A's merge ended at %d: %v", ev[lastHeadB], lastHeadB, "head B "+shaD, endA, ev)
	}
	if !slices.Contains(g.fakeGH.policyReads, shaD) {
		t.Fatalf("policy was read at %v, never at the base A left (%s)", g.fakeGH.policyReads, shaD)
	}
}

func settleCount(events []string, prefix string) int {
	n := 0
	for _, e := range events {
		if len(e) >= len(prefix) && e[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func settleWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// barrierNotifier holds each event until every PR of the round has sent one,
// so all the passes are between their load and their save together.
type barrierNotifier struct {
	mu      sync.Mutex
	want    int
	arrived int
	release chan struct{}
	events  []integrations.Event
}

func (b *barrierNotifier) Notify(_ context.Context, e integrations.Event) error {
	b.mu.Lock()
	b.events = append(b.events, e)
	b.arrived++
	if b.arrived == b.want {
		close(b.release)
	}
	b.mu.Unlock()
	select {
	case <-b.release:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("only %d of %d PRs reached their delivery", b.arrived, b.want)
	}
}

// (c) Passes of different PRs of one repository that deliver at once keep
// every PR's delivery in findings-deliveries.json: none overwrites another's.
func TestConcurrentDeliveriesKeepEveryPRsEntry(t *testing.T) {
	const prs = 24
	s, g, numbers, r := settleSetup(t, prs)
	g.findings = true
	sink := &jobSink{
		lanes: map[string]string{"2481": "operator-desk"},
		jobs:  map[string]string{"2481": "w-7"},
		recs:  map[string]integrations.JobRecord{"w-7": {State: "spawned", Role: "worker", Label: "w-7", OwnerLane: "integrator"}},
	}
	n := &barrierNotifier{want: prs, release: make(chan struct{})}
	s.Sink, s.Notifier = sink, n
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	var wg sync.WaitGroup
	errs := make(chan error, prs)
	for _, num := range numbers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.DeliverFindings(asPR(fmt.Sprint(num)), r, num, Options{})
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	ds, e := s.loadDeliveries(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	got := map[int]int{}
	for _, d := range ds {
		got[d.PR]++
	}
	for _, num := range numbers {
		if got[num] != 1 {
			t.Fatalf("PR %d has %d deliveries in the file, want 1: %v", num, got[num], got)
		}
	}
}

// (c) A pass that changes PR 7's delivery after another PR's pass saved keeps
// that other PR's entry too.
func TestDeliverySaveKeepsAnotherPRsEntryAddedSinceTheLoad(t *testing.T) {
	s, _, _, r := settleSetup(t, 1)
	now := time.Now().UTC()
	mine := []findingsDelivery{{ID: "a", PR: 7, SentAt: now}}
	if e := s.saveDeliveries(r.Name, 7, mine); e != nil {
		t.Fatal(e)
	}
	// Another PR saves after PR 7's pass loaded its ledger.
	if e := s.saveDeliveries(r.Name, 8, []findingsDelivery{{ID: "b", PR: 8, SentAt: now}}); e != nil {
		t.Fatal(e)
	}
	mine[0].AckedAt = now
	mine = append(mine, findingsDelivery{ID: "c", PR: 7, SentAt: now})
	if e := s.saveDeliveries(r.Name, 7, mine); e != nil {
		t.Fatal(e)
	}
	ds, e := s.loadDeliveries(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	var ids []string
	for _, d := range ds {
		ids = append(ids, d.ID)
		if d.ID == "a" && d.AckedAt.IsZero() {
			t.Fatalf("PR 7's change to its own delivery was dropped: %+v", d)
		}
	}
	if !slices.Equal(ids, []string{"a", "b", "c"}) {
		t.Fatalf("deliveries %v, want a, b, c", ids)
	}
}

// (c) READY clocks that different PRs of one repository record at once all stay
// in pr-ready-renotify.json. startReadyClock is the one step of NotifyPR that
// rewrites the file; the passes call it together, as they do when several PRs
// of a repository are settled at the same time.
func TestConcurrentReadyClocksKeepEveryPRsClock(t *testing.T) {
	const rounds, prs = 4, 32
	s, _, _, r := settleSetup(t, 1)
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, rounds*prs)
	for round := range rounds {
		for pr := range prs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs <- s.startReadyClock(r, verdictKey("ready", round*prs+pr, shaA), "worker-lane")
			}()
		}
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	if len(clocks) != rounds*prs {
		t.Fatalf("%d READY clocks in the file, want %d", len(clocks), rounds*prs)
	}
}
