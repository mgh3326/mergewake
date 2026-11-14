package pipeline

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// lensGate holds each reviewer until every reviewer the test expects is in
// flight, and records how many were in flight together. A reviewer that no
// other joins within the wait carries on alone, so a run that starts its
// passes one after the other finishes, with a peak of one, rather than hang.
type lensGate struct {
	want int
	wait time.Duration
	mu   sync.Mutex
	in   int
	peak int
	open chan struct{}
	once sync.Once
}

func newLensGate(want int) *lensGate {
	return &lensGate{want: want, wait: 2 * time.Second, open: make(chan struct{})}
}

func (l *lensGate) hold() {
	l.mu.Lock()
	l.in++
	l.peak = max(l.peak, l.in)
	if l.in >= l.want {
		l.once.Do(func() { close(l.open) })
	}
	l.mu.Unlock()
	select {
	case <-l.open:
	case <-time.After(l.wait):
	}
	l.mu.Lock()
	l.in--
	l.mu.Unlock()
}

// lensSetup is a PR touching a blocking path (two lenses) whose reviewers
// wait at the gate until want of them are in flight.
func lensSetup(t *testing.T, want int) (*Service, *fakeGH, config.Repo, *fakeRunner, *lensGate, *bytes.Buffer) {
	t.Helper()
	s, g, _, _, r := reviewSetup(t)
	g.blocking = true
	runner := s.Runner.(*fakeRunner)
	gate := newLensGate(want)
	runner.pane.afterRun = gate.hold
	out := &bytes.Buffer{}
	s.Out = out
	return s, g, r, runner, gate, out
}

func lensOf(passes []github.PassReceipt, pass int) (github.PassReceipt, bool) {
	for _, p := range passes {
		if p.Pass == pass {
			return p, true
		}
	}
	return github.PassReceipt{}, false
}

// failPass answers the spawn of one pass with a panewire failure.
type failPass struct {
	integrations.Runner
	label string
}

func (f failPass) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" && strings.Contains(arg(args, "-l"), f.label) {
		return nil, panewireExit(1)
	}
	return f.Runner.Run(ctx, command, args, input, dir)
}

// doneOrder notes how many reviews were posted when the done marker was made.
type doneOrder struct {
	*fakeGH
	mu          sync.Mutex
	doneCreates int
	postsAtDone int
}

func (d *doneOrder) CreateRef(ctx context.Context, repo, ref, sha string) error {
	if ref == doneRef(7, shaA) {
		d.mu.Lock()
		d.doneCreates++
		d.postsAtDone = d.fakeGH.postCount
		d.mu.Unlock()
	}
	return d.fakeGH.CreateRef(ctx, repo, ref, sha)
}

// (a) and (b): both lenses start before either returns; both post; the done
// marker is made once, after the second post.
func TestLensesOfAHeadStartTogether(t *testing.T) {
	s, g, r, runner, gate, _ := lensSetup(t, 2)
	d := &doneOrder{fakeGH: g}
	s.GH = d
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if gate.peak != 2 {
		t.Fatalf("reviewers in flight together = %d, want 2", gate.peak)
	}
	if len(runner.pane.spawns) != 2 {
		t.Fatalf("spawns = %d", len(runner.pane.spawns))
	}
	passes := g.passes[shaA]
	p1, ok1 := lensOf(passes, 1)
	p2, ok2 := lensOf(passes, 2)
	if len(passes) != 2 || !ok1 || !ok2 || p1.Lens != "correctness" || p2.Lens != "state-risk" {
		t.Fatalf("passes %+v", passes)
	}
	if d.doneCreates != 1 || d.postsAtDone != 2 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("done marker: created %d times after %d posts", d.doneCreates, d.postsAtDone)
	}
}

// (c) one lens failing leaves the other posted and the head not done; the
// next run retries only the failed lens and completes the head.
func TestLensFailureDoesNotStopTheOther(t *testing.T) {
	s, g, r, runner, gate, _ := lensSetup(t, 2)
	gate.want = 1
	s.Runner = failPass{Runner: runner, label: "-p1-"}
	e := s.Review(context.Background(), r, 7, "openai", Options{})
	if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "pass 1") {
		t.Fatalf("error = %v", e)
	}
	if _, ok := lensOf(g.passes[shaA], 2); !ok || len(g.passes[shaA]) != 1 || g.postCount != 1 {
		t.Fatalf("passes %+v", g.passes[shaA])
	}
	if g.refs[doneRef(7, shaA)] {
		t.Fatal("done marker made with pass 1 missing")
	}
	// The retry runs only the lens that is missing.
	s.Runner = runner
	spawns := len(runner.pane.spawns)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	if e = s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if got := runner.pane.spawns[spawns:]; len(got) != 1 || !strings.Contains(arg(got[0], "-l"), "-p1-") {
		t.Fatalf("retry spawns %v", got)
	}
	if len(g.passes[shaA]) != 2 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("after retry: passes %+v", g.passes[shaA])
	}
}

// (d) a lens already posted is not run again.
func TestPostedLensIsNotStartedAgain(t *testing.T) {
	s, g, r, runner, gate, _ := lensSetup(t, 1)
	g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Lens: "correctness", Family: "anthropic", Reviewer: "panewire:seat-x", Executor: "panewire", Round: 1}}}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.pane.spawns) != 1 || !strings.Contains(arg(runner.pane.spawns[0], "-l"), "-p2-") || gate.peak != 1 {
		t.Fatalf("spawns %v peak %d", runner.pane.spawns, gate.peak)
	}
	if _, ok := lensOf(g.passes[shaA], 2); !ok || len(g.passes[shaA]) != 2 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("passes %+v", g.passes[shaA])
	}
}

// (e) a head with one pass posts one review, exactly as before.
func TestSingleLensPostIsUnchanged(t *testing.T) {
	s, g, r, runner, gate, out := lensSetup(t, 1)
	g.blocking = false
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if gate.peak != 1 || len(runner.pane.spawns) != 1 || g.postCount != 1 {
		t.Fatalf("peak %d spawns %d posts %d", gate.peak, len(runner.pane.spawns), g.postCount)
	}
	want := github.PassReceipt{Pass: 1, Lens: "default", Reviewer: "panewire:seat-x", Family: "anthropic", SameFamily: false, AuthorFamily: "openai", Executor: "panewire", Round: 1}
	got := g.passes[shaA][0]
	got.Patch, got.DiffSource = "", ""
	if got != want {
		t.Fatalf("receipt\n got %+v\nwant %+v", got, want)
	}
	if f := g.postedFindings; len(f) != 1 || f[0].Path != "tool" || f[0].Line != 1 || f[0].Body != "A concrete bug" {
		t.Fatalf("findings %+v", f)
	}
	if !g.refs[doneRef(7, shaA)] || strings.Contains(out.String(), "review-pending") {
		t.Fatalf("done=%v out=%s", g.refs[doneRef(7, shaA)], out.String())
	}
}

// (f) a pass another host holds does not block the other pass; the head is
// not done while that pass is held.
func TestHeldLensDoesNotBlockTheOther(t *testing.T) {
	s, g, r, runner, gate, out := lensSetup(t, 1)
	held := claimRef(7, shaA, 1)
	g.refs = map[string]bool{held: true}
	g.leaseTimes = map[string]time.Time{held: time.Now()}
	g.leaseIDs = map[string]string{held: "other-host-lease"}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.pane.spawns) != 1 || !strings.Contains(arg(runner.pane.spawns[0], "-l"), "-p2-") || gate.peak != 1 {
		t.Fatalf("spawns %v", runner.pane.spawns)
	}
	if _, ok := lensOf(g.passes[shaA], 2); !ok || len(g.passes[shaA]) != 1 {
		t.Fatalf("passes %+v", g.passes[shaA])
	}
	if g.refs[doneRef(7, shaA)] {
		t.Fatalf("done marker made with pass 1 held: out=%s", out.String())
	}
	if g.leaseIDs[held] != "other-host-lease" {
		t.Fatal("held claim was taken")
	}
}
