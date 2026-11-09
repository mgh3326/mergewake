package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// escNotifier is panewire as the conflict ESC meets it. It records an event
// once per (lane, event id) and answers a repeat as delivered, as `panewire
// emit` does for a duplicate event_id. refuse lists the lanes it rejects
// before recording anything (`emit: invalid --lane`); fail lists lanes whose
// delivery fails with an error that does not say whether the event landed.
type escNotifier struct {
	mu       sync.Mutex
	refuse   map[string]bool
	fail     map[string]error
	attempts []integrations.Event
	accepted []integrations.Event
}

func newEscNotifier() *escNotifier {
	return &escNotifier{refuse: map[string]bool{}, fail: map[string]error{}}
}

func (n *escNotifier) Notify(_ context.Context, e integrations.Event) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.attempts = append(n.attempts, e)
	if n.refuse[e.Lane] {
		return &integrations.CommandError{Command: "panewire", Err: errors.New("exit status 2"), Stderr: "emit: invalid --lane " + e.Lane}
	}
	if err := n.fail[e.Lane]; err != nil {
		return err
	}
	for _, a := range n.accepted {
		if a.Lane == e.Lane && a.ID == e.ID {
			return nil
		}
	}
	n.accepted = append(n.accepted, e)
	return nil
}

func isConflictEsc(e integrations.Event) bool { return strings.HasPrefix(e.Text, "ESC ") }

// escs are the conflict ESCs panewire accepted.
func (n *escNotifier) escs() []integrations.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []integrations.Event
	for _, e := range n.accepted {
		if isConflictEsc(e) {
			out = append(out, e)
		}
	}
	return out
}

// tries are the conflict ESC deliveries attempted, accepted or not.
func (n *escNotifier) tries() []integrations.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []integrations.Event
	for _, e := range n.attempts {
		if isConflictEsc(e) {
			out = append(out, e)
		}
	}
	return out
}

// others are the accepted events that are neither a conflict ESC nor a
// merge_overdue notice to the lead lane.
func (n *escNotifier) others() []integrations.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []integrations.Event
	for _, e := range n.accepted {
		if !isConflictEsc(e) && e.Lane != overdueLeadLane {
			out = append(out, e)
		}
	}
	return out
}

// escRig is a reviewed PR #7 that conflicts with its base, on a fake clock,
// with the desk lane "desk".
type escRig struct {
	s   *Service
	g   *fakeGH
	n   *escNotifier
	r   config.Repo
	now time.Time
}

func newEscRig(t *testing.T) *escRig {
	t.Helper()
	s, g, _, r := notifySetup(t)
	rig := &escRig{s: s, g: g, n: newEscNotifier(), r: r, now: time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC)}
	s.Notifier = rig.n
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "desk", "desk"
	s.Now = func() time.Time { return rig.now }
	rig.conflict()
	return rig
}

func (rig *escRig) conflict() {
	no := false
	rig.g.pull.Mergeable, rig.g.pull.MergeableState = &no, "dirty"
}

func (rig *escRig) mergeable(m *bool, state string) {
	rig.g.pull.Mergeable, rig.g.pull.MergeableState = m, state
}

func (rig *escRig) after(d time.Duration) { rig.now = rig.now.Add(d) }

// restart is a fresh Service on the same state dir, GitHub, sink and lanes,
// as the daemon after a restart or a second process.
func (rig *escRig) restart() *Service {
	return &Service{Config: rig.s.Config, GH: rig.g, Sink: rig.s.Sink, Notifier: rig.n, Out: io.Discard, Now: rig.s.Now}
}

// escalate observes the PR as GitHub currently reports it.
func (rig *escRig) escalate(t *testing.T) {
	t.Helper()
	if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
}

func (rig *escRig) notify(t *testing.T) {
	t.Helper()
	if e := rig.s.NotifyPR(context.Background(), rig.r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
}

// escDir is where the per-PR ESC state lives.
func (rig *escRig) escDir() string { return filepath.Join(rig.s.state(rig.r.Name), "conflict-esc") }

func (rig *escRig) rows(t *testing.T, command string) []map[string]any {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(rig.s.state(rig.r.Name), "decisions.jsonl"))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		t.Fatal(e)
	}
	return auditRecords(t, b, command)
}

func wantEscs(t *testing.T, n *escNotifier, want int) []integrations.Event {
	t.Helper()
	got := n.escs()
	if len(got) != want {
		t.Fatalf("%d conflict ESCs accepted, want %d: %+v (attempts %+v)", len(got), want, got, n.tries())
	}
	return got
}

func wantTries(t *testing.T, n *escNotifier, want int) {
	t.Helper()
	if got := n.tries(); len(got) != want {
		t.Fatalf("%d conflict ESC deliveries attempted, want %d: %+v", len(got), want, got)
	}
}

// wantEsc checks one ESC's shape: an interrupting decision whose text starts
// with ESC and names the PR, the short head, the conflict, what to do and the
// route it took.
func wantEsc(t *testing.T, e integrations.Event, lane, via, head string) {
	t.Helper()
	if e.Lane != lane || e.Type != "decision" {
		t.Fatalf("ESC lane=%q type=%q, want lane %q type decision: %+v", e.Lane, e.Type, lane, e)
	}
	for _, part := range []string{"example/project#7", "head=" + head[:7], "conflict", "rebase or merge base, then push", "lane=" + lane + " via " + via} {
		if !strings.Contains(e.Text, part) {
			t.Fatalf("ESC text %q lacks %q", e.Text, part)
		}
	}
	if !strings.HasPrefix(e.Text, "ESC ") {
		t.Fatalf("ESC text %q does not start with ESC", e.Text)
	}
	if !strings.Contains(e.ID, head) || strings.Contains(e.ID, lane) {
		t.Fatalf("event id %q must name the full head and no lane", e.ID)
	}
}

// (A1) The reasons around a conflict churn — a failed check joins it, then
// unresolved findings, then a retitle — on one head. The ESC is sent once,
// whatever else the verdict says.
func TestConflictEscReasonChurnSendsOncePerHead(t *testing.T) {
	rig := newEscRig(t)
	rig.notify(t)
	rig.g.ci = github.Red
	rig.notify(t)
	rig.g.findings = 2
	rig.notify(t)
	rig.g.pull.Title = "Retitled while conflicting"
	rig.notify(t)
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaA)
	wantTries(t, rig.n, 1)
}

// (A2) Eight processes observe the same conflicting head at once, then a
// restarted one does: one ESC is accepted. A receipt lost after panewire
// accepted the event replays the same event id to the same lane, which
// panewire answers as delivered.
func TestConflictEscConcurrentAndRestart(t *testing.T) {
	rig := newEscRig(t)
	// A body lane keeps the route off the head-commit read, which the fake
	// GitHub does not guard for concurrent callers.
	rig.g.pull.Body = "Mergewake-Notify-Lane: design-lane\n"
	pr := rig.g.pull
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		svc := rig.restart()
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- svc.EscalateConflict(context.Background(), rig.r, pr, Options{})
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "design-lane", "notify-lane trailer", shaA)
	wantTries(t, rig.n, 1)

	if e := rig.restart().EscalateConflict(context.Background(), rig.r, pr, Options{}); e != nil {
		t.Fatal(e)
	}
	wantTries(t, rig.n, 1)

	if e := os.RemoveAll(rig.escDir()); e != nil {
		t.Fatal(e)
	}
	if e := rig.restart().EscalateConflict(context.Background(), rig.r, pr, Options{}); e != nil {
		t.Fatal(e)
	}
	tries := rig.n.tries()
	if len(tries) != 2 || tries[1].ID != tries[0].ID || tries[1].Lane != tries[0].Lane {
		t.Fatalf("the replay after a lost receipt changed id or lane: %+v", tries)
	}
	wantEscs(t, rig.n, 1)
}

// (A3) Unrelated notices push far more keys through the bounded pr-notify-sent
// ledger than it keeps. The delivered head stays delivered, also after a
// restart.
func TestConflictEscSurvivesLedgerChurn(t *testing.T) {
	rig := newEscRig(t)
	rig.escalate(t)
	wantEscs(t, rig.n, 1)
	ledger, e := loadSentKeys(filepath.Join(rig.s.state(rig.r.Name), "pr-notify-sent"), 255)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 300; i++ {
		if e := ledger.add(fmt.Sprintf("blocked %d %s check_failed:CI", 100+i, shaB)); e != nil {
			t.Fatal(e)
		}
	}
	rig.escalate(t)
	rig.notify(t)
	if e := rig.restart().EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	wantEscs(t, rig.n, 1)
	wantTries(t, rig.n, 1)
}

// (A4) A force-push from A to B and back to A: A has its one ESC and B its
// own. A's conflict flapping through UNKNOWN and clean back to conflict sends
// nothing more, and UNKNOWN on a new head sends nothing.
func TestConflictEscHeadReturnAndFlap(t *testing.T) {
	rig := newEscRig(t)
	rig.escalate(t)
	rig.g.pull.Head.SHA = shaB
	rig.escalate(t)
	escs := wantEscs(t, rig.n, 2)
	wantEsc(t, escs[0], "desk", "desk", shaA)
	wantEsc(t, escs[1], "desk", "desk", shaB)

	rig.g.pull.Head.SHA = shaA
	rig.escalate(t)
	yes := true
	rig.mergeable(nil, "unknown")
	rig.escalate(t)
	rig.mergeable(&yes, "clean")
	rig.escalate(t)
	rig.conflict()
	rig.escalate(t)
	rig.g.pull.Head.SHA = shaC
	rig.mergeable(nil, "")
	rig.escalate(t)
	wantEscs(t, rig.n, 2)
	wantTries(t, rig.n, 2)
}

// (A5) The route changes after delivery — another body lane, then only a task
// lane — and the same head is observed again: nothing more is sent.
func TestConflictEscRouteEditAfterDelivery(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Body = "Mergewake-Notify-Lane: lane-x\n"
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "lane-x", "notify-lane trailer", shaA)
	rig.g.pull.Body = "Mergewake-Notify-Lane: lane-y\n"
	rig.escalate(t)
	rig.g.pull.Body = "Mergewake-Task: 42\n"
	rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}
	rig.escalate(t)
	rig.notify(t)
	wantEscs(t, rig.n, 1)
	wantTries(t, rig.n, 1)
}

// (A6) A delivery that fails without saying whether it landed is retried on
// the same lane under the same event id — never moved on to the next
// destination, also after the route changed — until that lane takes it.
func TestConflictEscAmbiguousFailureRetriesSameLane(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Body = "Mergewake-Notify-Lane: lane-x\n"
	rig.n.fail["lane-x"] = errors.New("panewire failed: signal: killed")
	rig.escalate(t)
	wantTries(t, rig.n, 1)
	wantEscs(t, rig.n, 0)
	if rows := rig.rows(t, "conflict-esc-failed"); len(rows) != 1 {
		t.Fatalf("conflict-esc-failed rows: %+v", rows)
	}

	rig.g.pull.Body = "Mergewake-Notify-Lane: lane-y\n"
	rig.after(2 * time.Minute)
	rig.escalate(t)
	wantTries(t, rig.n, 2)

	delete(rig.n.fail, "lane-x")
	rig.after(5 * time.Minute)
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "lane-x", "notify-lane trailer", shaA)
	tries := rig.n.tries()
	for _, e := range tries {
		if e.Lane != "lane-x" || e.ID != tries[0].ID {
			t.Fatalf("an ambiguous failure moved lane or id: %+v", tries)
		}
	}
}

// (B1) The first time mergewake sees the PR it already conflicts: no review,
// no CI (GitHub runs none for a conflicting PR). One ESC, and no BLOCKED.
func TestConflictEscInitialObservation(t *testing.T) {
	rig := newEscRig(t)
	rig.g.refs, rig.g.reviewed = map[string]bool{}, false
	rig.g.ci = github.Pending
	rig.notify(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaA)
	if others := rig.n.others(); len(others) != 0 {
		t.Fatalf("a second notice beside the ESC: %+v", others)
	}
}

// (B3) A draft is notified too: NotifyPR returns early for it, after the ESC.
func TestConflictEscDraft(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Draft = true
	rig.notify(t)
	rig.notify(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaA)
	if others := rig.n.others(); len(others) != 0 {
		t.Fatalf("a draft got another notice: %+v", others)
	}
}

// (B4) A lead's PR has no task and no job, only the body lane: the ESC goes
// there.
func TestConflictEscLeadPRBodyLane(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Body = "Fix it\n\nMergewake-Notify-Lane: lead-l1\n"
	rig.s.Sink = &jobSink{}
	rig.notify(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "lead-l1", "notify-lane trailer", shaA)
}

// (B5) Every destination refuses: nothing is recorded as delivered, the
// failure is audited and the head stays pending — across a restart — and a
// later attempt, once the desk takes events again, delivers exactly one ESC.
func TestConflictEscDeliveryRecovery(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Body = "Mergewake-Notify-Lane: gone-lane\n"
	rig.n.refuse["gone-lane"], rig.n.refuse["desk"] = true, true
	rig.escalate(t)
	wantEscs(t, rig.n, 0)
	wantTries(t, rig.n, 2)
	if tries := rig.n.tries(); tries[0].Lane != "gone-lane" || tries[1].Lane != "desk" || tries[0].ID != tries[1].ID {
		t.Fatalf("a refused lane did not move to the next destination under the same id: %+v", tries)
	}
	if rows := rig.rows(t, "conflict-esc-failed"); len(rows) != 1 || rows[0]["head"] != shaA {
		t.Fatalf("conflict-esc-failed rows: %+v", rows)
	}
	if rows := rig.rows(t, "conflict-esc"); len(rows) != 0 {
		t.Fatalf("an undelivered ESC was audited as sent: %+v", rows)
	}

	// Inside the retry wait nothing is attempted.
	rig.after(10 * time.Second)
	rig.escalate(t)
	wantTries(t, rig.n, 2)

	// A restart keeps the head pending, and the next attempt fails again.
	rig.s = rig.restart()
	rig.after(2 * time.Minute)
	rig.escalate(t)
	wantTries(t, rig.n, 4)
	wantEscs(t, rig.n, 0)

	rig.n.refuse["desk"] = false
	rig.after(5 * time.Minute)
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaA)
	if rows := rig.rows(t, "conflict-esc"); len(rows) != 1 || rows[0]["lane"] != "desk" || rows[0]["head"] != shaA || rows[0]["event_id"] == nil {
		t.Fatalf("conflict-esc rows: %+v", rows)
	}
	rig.after(time.Hour)
	rig.escalate(t)
	wantEscs(t, rig.n, 1)
	wantTries(t, rig.n, 6)
}

// (B6) Mergeability GitHub has not computed is neither a conflict nor a
// resolution: it sends nothing, and it does not cut short the retry wait of a
// pending ESC or re-arm a delivered one.
func TestConflictEscUnknownSequence(t *testing.T) {
	rig := newEscRig(t)
	yes := true
	for _, m := range []struct {
		mergeable *bool
		state     string
	}{{nil, ""}, {nil, "unknown"}, {&yes, "unknown"}} {
		rig.mergeable(m.mergeable, m.state)
		rig.escalate(t)
		rig.notify(t)
	}
	wantTries(t, rig.n, 0)

	rig.n.refuse["desk"] = true
	rig.conflict()
	rig.escalate(t)
	wantTries(t, rig.n, 1)
	rig.after(10 * time.Second)
	rig.mergeable(nil, "unknown")
	rig.escalate(t)
	rig.after(10 * time.Second)
	rig.conflict()
	rig.escalate(t)
	wantTries(t, rig.n, 1)

	rig.n.refuse["desk"] = false
	rig.after(2 * time.Minute)
	rig.escalate(t)
	wantEscs(t, rig.n, 1)
	rig.mergeable(nil, "unknown")
	rig.escalate(t)
	rig.conflict()
	rig.escalate(t)
	wantEscs(t, rig.n, 1)
	wantTries(t, rig.n, 2)
}

// (B7) A PR closed, merged or not against the base when observed gets
// nothing; one GitHub reports closed, merged or on another head by the time
// the ESC would go out gets nothing either. The current head, open and
// conflicting, gets its ESC.
func TestConflictEscFreshEligibility(t *testing.T) {
	rig := newEscRig(t)
	observed := rig.g.pull
	ctx := context.Background()
	for name, pr := range map[string]github.Pull{
		"closed":     func() github.Pull { p := observed; p.State = "closed"; return p }(),
		"merged":     func() github.Pull { p := observed; p.State, p.Merged = "closed", true; return p }(),
		"other base": func() github.Pull { p := observed; p.Base.Ref = "release"; return p }(),
	} {
		if e := rig.s.EscalateConflict(ctx, rig.r, pr, Options{}); e != nil {
			t.Fatalf("%s: %v", name, e)
		}
	}
	wantTries(t, rig.n, 0)

	for name, change := range map[string]func(*github.Pull){
		"closed since":   func(p *github.Pull) { p.State = "closed" },
		"merged since":   func(p *github.Pull) { p.State, p.Merged = "closed", true },
		"pushed since":   func(p *github.Pull) { p.Head.SHA = shaC },
		"resolved since": func(p *github.Pull) { yes := true; p.Mergeable, p.MergeableState = &yes, "clean" },
	} {
		rig.g.pull = observed
		change(&rig.g.pull)
		if e := rig.s.EscalateConflict(ctx, rig.r, observed, Options{}); e != nil {
			t.Fatalf("%s: %v", name, e)
		}
	}
	wantTries(t, rig.n, 0)

	rig.g.pull = observed
	rig.g.pull.Head.SHA = shaC
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaC)
}

// (B8) With notify off nothing is sent or recorded, and a dry run decides
// only: no event, no state, no audit row. Shadow reports, so it sends.
func TestConflictEscModes(t *testing.T) {
	ctx := context.Background()
	rig := newEscRig(t)
	off := false
	rig.s.Config.Daemon.PR.Notify.Enabled = &off
	rig.notify(t)
	rig.escalate(t)
	wantTries(t, rig.n, 0)
	if _, e := os.Stat(rig.escDir()); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("notify off wrote ESC state: %v", e)
	}

	rig.s.Config.Daemon.PR.Notify.Enabled = nil
	if e := rig.s.NotifyPR(ctx, rig.r, 7, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if e := rig.s.EscalateConflict(ctx, rig.r, rig.g.pull, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	wantTries(t, rig.n, 0)
	if _, e := os.Stat(rig.escDir()); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("a dry run wrote ESC state: %v", e)
	}
	if rows := rig.rows(t, "conflict-esc"); len(rows) != 0 {
		t.Fatalf("a dry run audited an ESC: %+v", rows)
	}

	if e := rig.s.EscalateConflict(ctx, rig.r, rig.g.pull, Options{Shadow: true}); e != nil {
		t.Fatal(e)
	}
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaA)
}

// (C1) A PR-body Mergewake-Notify-Lane wins over the task lane, the job lane
// and the repo lane; without it the task lane wins; without either the desk.
// Neither the job's worker lane nor the repo lane is ever used.
func TestConflictEscRoutingPrecedence(t *testing.T) {
	for _, tt := range []struct {
		name, body, lane, via string
	}{
		{"body lane", "Mergewake-Task: 42\nMergewake-Notify-Lane: design-lane\n", "design-lane", "notify-lane trailer"},
		{"task lane", "Mergewake-Task: 42\n", "task-lane", "task 42"},
		{"desk", "", "desk", "desk"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rig := newEscRig(t)
			rig.r.Lane = "repo-lane"
			rig.g.pull.Body = tt.body
			rig.s.Sink = &jobSink{
				lanes: map[string]string{"42": "task-lane"},
				jobs:  map[string]string{"42": "w-7"},
				recs:  map[string]integrations.JobRecord{"w-7": {State: "spawned", Role: "worker", Label: "worker-lane"}},
			}
			rig.escalate(t)
			wantEsc(t, wantEscs(t, rig.n, 1)[0], tt.lane, tt.via, shaA)
		})
	}
}

// (C2) Only one valid Mergewake-Notify-Lane line in the PR body counts. A
// malformed or repeated one falls through to the task lane or the desk, and a
// Notify-Lane only in the head commit message is not read for the ESC.
func TestConflictEscBodyTrailerMatrix(t *testing.T) {
	for _, tt := range []struct {
		name, body, commit, lane, via string
	}{
		{"malformed", "Mergewake-Task: 42\nMergewake-Notify-Lane: bad lane!\n", "", "task-lane", "task 42"},
		{"duplicate", "Mergewake-Task: 42\nMergewake-Notify-Lane: lane-a\nMergewake-Notify-Lane: lane-b\n", "", "task-lane", "task 42"},
		{"commit only, task", "Mergewake-Task: 42\n", "change\n\nMergewake-Notify-Lane: commit-lane\n", "task-lane", "task 42"},
		{"commit only, no task", "", "change\n\nMergewake-Notify-Lane: commit-lane\n", "desk", "desk"},
		{"body beats commit", "Mergewake-Notify-Lane: body-lane\n", "change\n\nMergewake-Notify-Lane: commit-lane\n", "body-lane", "notify-lane trailer"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rig := newEscRig(t)
			rig.g.pull.Body = tt.body
			rig.g.messages = map[string]string{shaA: tt.commit}
			rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}
			rig.escalate(t)
			wantEsc(t, wantEscs(t, rig.n, 1)[0], tt.lane, tt.via, shaA)
		})
	}
}

// hkDownSink is an hk that answers nothing; it counts the reads made of it.
type hkDownSink struct {
	fakeSink
	mu    sync.Mutex
	reads int
}

func (s *hkDownSink) read() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return errors.New("hk unavailable")
}
func (s *hkDownSink) Lane(context.Context, string) (string, error) { return "", s.read() }
func (s *hkDownSink) TaskJob(context.Context, string) (string, string, error) {
	return "", "", s.read()
}
func (s *hkDownSink) Job(context.Context, string) (integrations.JobRecord, error) {
	return integrations.JobRecord{}, s.read()
}

// (C3) A valid body lane needs nothing else: no hk read, no commit read, even
// when hk is down and the PR names a task.
func TestConflictEscBodyLaneAvoidsHK(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Body = "Mergewake-Task: 42\nMergewake-Notify-Lane: design-lane\n"
	sink := &hkDownSink{}
	rig.s.Sink = sink
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "design-lane", "notify-lane trailer", shaA)
	if sink.reads != 0 || rig.g.commitCalls != 0 {
		t.Fatalf("a body-lane ESC read hk %d times and the head commit %d times", sink.reads, rig.g.commitCalls)
	}
}

// (C5) A task-lane read that fails is not "no task lane": nothing goes to the
// desk, the error is returned, and the next observation, with hk back, sends
// to the task lane.
func TestConflictEscTaskLookupErrorRetries(t *testing.T) {
	rig := newEscRig(t)
	rig.g.pull.Body = "Mergewake-Task: 42\n"
	sink := &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}, err: errors.New("hk: timeout")}
	rig.s.Sink = sink
	if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e == nil {
		t.Fatal("a failed task-lane read was not returned")
	}
	wantTries(t, rig.n, 0)
	sink.err = nil
	rig.escalate(t)
	wantEsc(t, wantEscs(t, rig.n, 1)[0], "task-lane", "task 42", shaA)
}

// (E1) A conflict is told once, by the ESC: a BLOCKED whose only reason is the
// conflict is not sent, while a mixed verdict still sends its other reasons on
// the route they took before, without the conflict.
func TestConflictEscSingleNotificationPath(t *testing.T) {
	t.Run("conflict only", func(t *testing.T) {
		rig := newEscRig(t)
		rig.notify(t)
		rig.notify(t)
		wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk", shaA)
		if others := rig.n.others(); len(others) != 0 {
			t.Fatalf("a conflict-only BLOCKED was sent beside the ESC: %+v", others)
		}
		rows := rig.rows(t, "pr-notify")
		if len(rows) == 0 || rows[0]["state"] != "blocked" || !strings.Contains(fmt.Sprint(rows[0]["reasons"]), "conflict") {
			t.Fatalf("the verdict lost its conflict reason: %+v", rows)
		}
	})
	t.Run("mixed, author reason", func(t *testing.T) {
		rig := newEscRig(t)
		rig.g.ci = github.Red
		rig.notify(t)
		rig.notify(t)
		wantEscs(t, rig.n, 1)
		others := rig.n.others()
		if len(others) != 1 || others[0].Lane != "desk" || others[0].Type != "decision" || !strings.Contains(others[0].Text, "BLOCKED") || !strings.Contains(others[0].Text, "check_failed:CI") || strings.Contains(others[0].Text, "conflict") {
			t.Fatalf("mixed verdict: %+v", others)
		}
	})
	t.Run("mixed, routed as before", func(t *testing.T) {
		s, g, _, r := workerNotifySetup(t)
		n := newEscNotifier()
		s.Notifier = n
		no := false
		g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
		g.findings = 2
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		wantEsc(t, wantEscs(t, n, 1)[0], "design-lane", "task 42", shaA)
		others := n.others()
		if len(others) != 1 || others[0].Lane != "worker-lane" || others[0].Type != "blocked" || !strings.Contains(others[0].Text, "threads_open:2") || strings.Contains(others[0].Text, "conflict") {
			t.Fatalf("mixed verdict: %+v", others)
		}
	})
}

// (E2) Regression guard, passes before and after hk 3712: a conflicting PR
// stays out of merge_overdue — no notice, nothing to the lead lane, no count —
// and the overdue notice works as before once the conflict clears.
func TestConflictEscLeavesMergeOverdueAlone(t *testing.T) {
	rig := newOverdueRig(t, true)
	no := false
	rig.g.pull.Mergeable, rig.g.pull.MergeableState = &no, "dirty"
	rig.at(0)
	rig.at(50 * time.Minute)
	rig.at(90 * time.Minute)
	rig.wantConditions()
	if n := TakeMergeOverdue(rig.r.Name); n != 0 {
		t.Fatalf("merge_overdue counted %d while conflicting", n)
	}
	for _, e := range rig.n.delivered {
		if e.Lane == overdueLeadLane {
			t.Fatalf("the lead lane heard a conflicting PR: %+v", e)
		}
	}
	yes := true
	rig.g.pull.Mergeable, rig.g.pull.MergeableState = &yes, "clean"
	rig.at(100 * time.Minute)
	got := len(rig.n.overdueEvents())
	if got == 0 {
		t.Fatalf("no merge_overdue once the conflict cleared: %+v", rig.n.delivered)
	}
	if n := TakeMergeOverdue(rig.r.Name); n != got {
		t.Fatalf("merge_overdue counted %d, delivered %d", n, got)
	}
}

// (F2) Regression guard, passes before and after hk 3712: a failed check with
// no conflict still goes to the bound worker as BLOCKED, and to the desk as a
// decision when the author is unknown. No ESC.
func TestConflictEscKeepsNonConflictAuthorRouting(t *testing.T) {
	t.Run("bound worker", func(t *testing.T) {
		s, g, _, r := workerNotifySetup(t)
		n := newEscNotifier()
		s.Notifier = n
		g.ci = github.Red
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		wantEscs(t, n, 0)
		if others := n.others(); len(others) != 1 || others[0].Lane != "worker-lane" || others[0].Type != "blocked" || !strings.Contains(others[0].Text, "check_failed:CI") {
			t.Fatalf("%+v", others)
		}
	})
	t.Run("unknown author", func(t *testing.T) {
		s, g, _, r := notifySetup(t)
		n := newEscNotifier()
		s.Notifier = n
		g.ci = github.Red
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		wantEscs(t, n, 0)
		if others := n.others(); len(others) != 1 || others[0].Lane != "config-lane" || others[0].Type != "decision" || !strings.Contains(others[0].Text, "unknown author; desk") {
			t.Fatalf("%+v", others)
		}
	})
}

// crashNotifier is panewire taking the next ESC and the process dying before
// mergewake hears back: it records the event, then panics.
type crashNotifier struct {
	*escNotifier
	crash bool
}

func (n *crashNotifier) Notify(ctx context.Context, e integrations.Event) error {
	err := n.escNotifier.Notify(ctx, e)
	if n.crash && err == nil && isConflictEsc(e) {
		n.crash = false
		panic("crashed after panewire accepted the ESC")
	}
	return err
}

// (P1, mergewake#247 r1) A lane accepts the ESC and the process dies before
// the receipt is written; the route changes before the next attempt. The next
// attempt replays the same event id to the lane that took it, never to the new
// route, and the head is then delivered.
func TestConflictEscCrashAfterAcceptanceKeepsLane(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*escRig)
	}{
		{"body lane edited", func(rig *escRig) { rig.g.pull.Body = "Mergewake-Notify-Lane: lane-y\n" }},
		{"body lane dropped for a task lane", func(rig *escRig) {
			rig.g.pull.Body = "Mergewake-Task: 42\n"
			rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rig := newEscRig(t)
			rig.g.pull.Body = "Mergewake-Notify-Lane: lane-x\n"
			rig.s.Notifier = &crashNotifier{escNotifier: rig.n, crash: true}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("the notifier did not crash")
					}
				}()
				_ = rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{})
			}()
			wantEscs(t, rig.n, 1)

			tt.edit(rig)
			if e := rig.restart().EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
				t.Fatal(e)
			}
			tries := rig.n.tries()
			if len(tries) != 2 || tries[1].Lane != "lane-x" || tries[1].ID != tries[0].ID {
				t.Fatalf("the attempt after the crash left the lane that took the ESC: %+v", tries)
			}
			wantEsc(t, wantEscs(t, rig.n, 1)[0], "lane-x", "notify-lane trailer", shaA)
			if rows := rig.rows(t, "conflict-esc"); len(rows) != 1 || rows[0]["lane"] != "lane-x" {
				t.Fatalf("conflict-esc rows: %+v", rows)
			}
			if e := rig.restart().EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
				t.Fatal(e)
			}
			wantTries(t, rig.n, 2)
		})
	}
}

// blockedNotices are the accepted BLOCKED notices, in order.
func blockedNotices(n *escNotifier) []integrations.Event {
	var out []integrations.Event
	for _, e := range n.others() {
		if strings.Contains(e.Text, "BLOCKED") {
			out = append(out, e)
		}
	}
	return out
}

// (P2, mergewake#247 r1) The conflict is left out of the BLOCKED text, not out
// of the verdict: a head whose other reasons were already told still hears,
// once, on the author's route, when it starts to conflict; and a head whose
// conflict clears is told its remaining reasons as before hk 3712 — once per
// distinct reason set, with no resend loop.
func TestConflictEscMixedVerdictIdentity(t *testing.T) {
	conflict := func(g *fakeGH) {
		no := false
		g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	}
	clean := func(g *fakeGH) {
		yes := true
		g.pull.Mergeable, g.pull.MergeableState = &yes, "clean"
	}
	notify := func(t *testing.T, s *Service, r config.Repo) {
		t.Helper()
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	wantBlocked := func(t *testing.T, got integrations.Event, lane string) {
		t.Helper()
		if got.Lane != lane || got.Type != "blocked" || !strings.Contains(got.Text, "threads_open:2") || strings.Contains(got.Text, "conflict") {
			t.Fatalf("BLOCKED %+v, want threads_open:2 without the conflict to %s", got, lane)
		}
	}
	t.Run("conflict joins a told head", func(t *testing.T) {
		s, g, _, r := workerNotifySetup(t)
		n := newEscNotifier()
		s.Notifier = n
		g.findings = 2
		notify(t, s, r)
		blocked := blockedNotices(n)
		if len(blocked) != 1 {
			t.Fatalf("BLOCKED before the conflict: %+v", blocked)
		}
		wantBlocked(t, blocked[0], "design-lane")

		conflict(g)
		notify(t, s, r)
		notify(t, s, r)
		wantEsc(t, wantEscs(t, n, 1)[0], "design-lane", "task 42", shaA)
		blocked = blockedNotices(n)
		if len(blocked) != 2 {
			t.Fatalf("%d BLOCKED notices, want the author's once the head conflicts: %+v", len(blocked), blocked)
		}
		wantBlocked(t, blocked[1], "worker-lane")
		if blocked[1].ID == blocked[0].ID {
			t.Fatalf("the author's BLOCKED reused the earlier event id %q", blocked[0].ID)
		}
	})
	t.Run("conflict leaves the head", func(t *testing.T) {
		s, g, _, r := workerNotifySetup(t)
		n := newEscNotifier()
		s.Notifier = n
		g.findings = 2
		conflict(g)
		notify(t, s, r)
		blocked := blockedNotices(n)
		if len(blocked) != 1 {
			t.Fatalf("BLOCKED while conflicting: %+v", blocked)
		}
		wantBlocked(t, blocked[0], "worker-lane")

		clean(g)
		notify(t, s, r)
		notify(t, s, r)
		blocked = blockedNotices(n)
		if len(blocked) != 2 {
			t.Fatalf("%d BLOCKED notices, want the remaining reasons told once the conflict clears: %+v", len(blocked), blocked)
		}
		wantBlocked(t, blocked[1], "design-lane")

		// The same reason sets again: each was told once already.
		conflict(g)
		notify(t, s, r)
		clean(g)
		notify(t, s, r)
		if blocked := blockedNotices(n); len(blocked) != 2 {
			t.Fatalf("a reason set already told was sent again: %+v", blocked)
		}
		wantEscs(t, n, 1)
	})
}
