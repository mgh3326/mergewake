package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func notifySetup(t *testing.T) (*Service, *fakeGH, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, _, n, r := setup(t)
	markReviewed(g)
	g.pull.Title = "Add feature"
	yes := true
	g.pull.Mergeable = &yes
	g.pull.MergeableState = "clean"
	s.Config.Notifier.Lane = "config-lane"
	return s, g, n, r
}

func lastEvent(t *testing.T, n *fakeNotifier) string {
	t.Helper()
	if len(n.events) == 0 {
		t.Fatal("no events")
	}
	return n.events[len(n.events)-1].Text
}

// A checked-green, reviewed and mergeable head emits READY once; a repeat poll
// at the same head stays quiet, including across a service restart.
func TestNotifyReadyOncePerHeadAndRestart(t *testing.T) {
	s, g, n, r := notifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 {
		t.Fatalf("events=%d", n.count)
	}
	text := lastEvent(t, n)
	if !strings.HasPrefix(text, "[mergewake] READY example/project#7 head=aaaaaaa | Add feature") {
		t.Fatal(text)
	}
	if !strings.Contains(text, "lane=config-lane via fallback") {
		t.Fatal(text)
	}
	if n.events[0].ID != "mergewake-ready-example/project-7-"+shaA || n.events[0].Lane != "config-lane" {
		t.Fatalf("%+v", n.events[0])
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 {
		t.Fatal("repeat poll re-notified")
	}
	// A restart keeps sent keys: a fresh service on the same state dir is quiet.
	s2 := *s
	n2 := &fakeNotifier{}
	s2.Notifier = n2
	if e := s2.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n2.count != 0 {
		t.Fatal("restart re-notified")
	}
	// A new head resets: the next head notifies again.
	g.pull.Head.SHA = shaC
	markReviewed(g)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 || !strings.Contains(lastEvent(t, n), "head=ccccccc") {
		t.Fatalf("new head not notified: %d", n.count)
	}
}

// Event IDs carry the full head SHA: two heads sharing a seven-character
// prefix are distinct events, matching the dedupe key's granularity.
func TestNotifyEventIDUsesFullHead(t *testing.T) {
	s, g, n, r := notifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	// Same sha7 prefix as shaA, different tail — a new head must re-notify
	// under a distinct event ID rather than colliding with the first.
	g.pull.Head.SHA = shaA[:7] + strings.Repeat("d", 33)
	markReviewed(g)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 {
		t.Fatalf("prefix-colliding head not notified: %d", n.count)
	}
	if n.events[0].ID == n.events[1].ID || !strings.HasSuffix(n.events[1].ID, shaA[:7]+strings.Repeat("d", 33)) {
		t.Fatalf("%+v", n.events)
	}
}

// Pending required checks emit nothing; a failing required check emits BLOCKED
// naming the failed runs, once per head and state.
func TestNotifyPendingSilentAndFailingNames(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.required = []string{"vet", "test"}
	g.checks = map[string]github.CI{shaB: github.Green}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Green, "test": github.Pending}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatal("pending checks notified")
	}
	if _, e := os.Stat(filepath.Join(s.state(r.Name), "pr-notify-sent")); !os.IsNotExist(e) {
		t.Fatal("pending wrote sent keys")
	}
	g.checkNames[shaA]["vet"] = github.Red
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || !strings.Contains(lastEvent(t, n), "[mergewake] BLOCKED example/project#7 head=aaaaaaa | check_failed:vet") {
		t.Fatalf("%+v", n.events)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 {
		t.Fatal("same blocked state re-notified")
	}
}

// With checks green, BLOCKED names the closed-vocabulary reason that holds the
// head: missing review marker, open threads, a stale base or a conflict.
func TestNotifyBlockedReasons(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prepare func(*fakeGH)
		reason  string
	}{
		{"review missing", func(g *fakeGH) {
			g.refs = map[string]bool{}
		}, "review_missing"},
		{"threads open", func(g *fakeGH) {
			g.findings = 2
		}, "threads_open:2"},
		{"stale base", func(g *fakeGH) {
			g.stale = true
		}, "base_stale"},
		{"conflict", func(g *fakeGH) {
			no := false
			g.pull.Mergeable = &no
			g.pull.MergeableState = "dirty"
		}, "conflict"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			tt.prepare(g)
			g.policyExtra = "auto_update_branch: false\n"
			if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
				t.Fatal(e)
			}
			if n.count != 1 || !strings.Contains(lastEvent(t, n), "BLOCKED") || !strings.Contains(lastEvent(t, n), tt.reason) {
				t.Fatalf("%+v", n.events)
			}
		})
	}
}

// Lane routing precedence: handoffkeep task lane from the Mergewake-Task
// trailer (body, then head commit message), the Mergewake-Notify-Lane trailer
// when the task lane is unresolved, then the configured notifier lane. A sink
// read error retries rather than rerouting.
func TestNotifyLaneRouting(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prepare func(*Service, *fakeGH)
		lane    string
		via     string
		fail    bool
	}{
		{"body task lane", func(s *Service, g *fakeGH) {
			g.pull.Body = "Mergewake-Task: 42"
			s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}
		}, "task-lane", "via task 42", false},
		{"commit task lane", func(s *Service, g *fakeGH) {
			g.messages = map[string]string{shaA: "subject\n\nMergewake-Task: 43\n"}
			s.Sink = &fakeLaneSink{lanes: map[string]string{"43": "commit-lane"}}
		}, "commit-lane", "via task 43", false},
		{"unresolved falls to notify-lane trailer", func(s *Service, g *fakeGH) {
			g.pull.Body = "Mergewake-Task: 44\nMergewake-Notify-Lane: notify-lane"
			s.Sink = &fakeLaneSink{lanes: map[string]string{}}
		}, "notify-lane", "via notify-lane trailer", false},
		{"no lane sink falls to trailer", func(s *Service, g *fakeGH) {
			g.pull.Body = "Mergewake-Task: 42\nMergewake-Notify-Lane: notify-lane"
		}, "notify-lane", "via notify-lane trailer", false},
		{"config fallback", func(s *Service, g *fakeGH) {}, "config-lane", "via fallback", false},
		{"sink read error retries", func(s *Service, g *fakeGH) {
			g.pull.Body = "Mergewake-Task: 42"
			s.Sink = &fakeLaneSink{err: errors.New("offline")}
		}, "", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			tt.prepare(s, g)
			e := s.NotifyPR(context.Background(), r, 7, Options{})
			if tt.fail {
				if e == nil || n.count != 0 {
					t.Fatalf("err=%v events=%d", e, n.count)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if n.count != 1 || n.events[0].Lane != tt.lane || !strings.Contains(lastEvent(t, n), tt.via) {
				t.Fatalf("%+v", n.events)
			}
		})
	}
}

// A state change at the same head is a new event; revisiting a sent state stays
// quiet, drafts and disabled notify emit nothing, and dry-run writes no keys.
func TestNotifyStateChangesGuardsAndDryRun(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.findings = 1
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || !strings.Contains(lastEvent(t, n), "BLOCKED") {
		t.Fatalf("%+v", n.events)
	}
	g.findings = 0
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 || !strings.Contains(lastEvent(t, n), "READY") {
		t.Fatal("recovery did not notify")
	}
	g.findings = 1
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 {
		t.Fatal("repeated blocked state re-notified")
	}
	// A different BLOCKED reason set at the same head is a new verdict and a
	// distinct event ID; the earlier READY key does not suppress it.
	g.findings = 2
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 3 || n.events[2].ID == n.events[0].ID {
		t.Fatalf("changed blocked verdict not re-notified: %+v", n.events)
	}

	s, g, n, r = notifySetup(t)
	g.pull.Draft = true
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatal("draft notified")
	}

	s, g, n, r = notifySetup(t)
	off := false
	s.Config.Daemon.PR.Notify.Enabled = &off
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatal("disabled notify emitted")
	}

	s, g, n, r = notifySetup(t)
	g.pull.Mergeable = nil
	g.pull.MergeableState = ""
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatal("uncomputed mergeable notified")
	}

	s, g, n, r = notifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatal("dry-run emitted")
	}
	if _, e := os.Stat(filepath.Join(s.state(r.Name), "pr-notify-sent")); !os.IsNotExist(e) {
		t.Fatal("dry-run wrote sent keys")
	}
}

// laneFailNotifier fails deliveries per lane so fallback and parking can be
// exercised without a real panewire.
type laneFailNotifier struct {
	fail   map[string]error
	err    error
	events []integrations.Event
}

func (n *laneFailNotifier) Notify(_ context.Context, e integrations.Event) error {
	n.events = append(n.events, e)
	if err, ok := n.fail[e.Lane]; ok {
		return err
	}
	return n.err
}

// A PR whose resolved lane refuses the event is retried once on the fallback
// lane; a verdict that still cannot be delivered is parked with one logged
// decision instead of failing every loop.
func TestNotifyDeliveryFailureFallbackAndPark(t *testing.T) {
	s, g, _, r := notifySetup(t)
	n := &laneFailNotifier{fail: map[string]error{"task-lane": errors.New("exit status 2: unknown lane")}}
	s.Notifier = n
	s.Config.Notifier.FallbackLane = "config-lane"
	g.pull.Body = "Mergewake-Task: 42"
	s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(n.events) != 2 || n.events[0].Lane != "task-lane" || n.events[1].Lane != "config-lane" {
		t.Fatalf("%+v", n.events)
	}
	if !strings.Contains(n.events[1].Text, "+fallback") {
		t.Fatal(n.events[1].Text)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(n.events) != 2 {
		t.Fatal("repeat poll re-delivered")
	}

	// When delivery cannot succeed anywhere the verdict is logged once and
	// then stays quiet, never erroring every loop.
	s, g, _, r = notifySetup(t)
	n = &laneFailNotifier{err: errors.New("panewire failed: exit status 2")}
	s.Notifier = n
	for i := 0; i < 3; i++ {
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if len(n.events) != 1 {
		t.Fatalf("events=%+v", n.events)
	}
	g.pull.Head.SHA = shaB
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(n.events) != 2 {
		t.Fatal("new head re-armed notification")
	}
}

// A repo lane takes PRs no trailer routes, ahead of the global fallback; a
// task lane or a Mergewake-Notify-Lane trailer still wins over it.
func TestNotifyLaneRepoLane(t *testing.T) {
	for _, tt := range []struct {
		name, body, lane, via string
		sink                  map[string]string
	}{
		{"no trailer uses the repo lane", "", "repo-lane", "via repo lane", nil},
		{"unresolved task uses the repo lane", "Mergewake-Task: 44", "repo-lane", "via repo lane", map[string]string{}},
		{"notify-lane trailer wins", "Mergewake-Notify-Lane: notify-lane", "notify-lane", "via notify-lane trailer", nil},
		{"task lane wins", "Mergewake-Task: 42", "task-lane", "via task 42", map[string]string{"42": "task-lane"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			r.Lane = "repo-lane"
			g.pull.Body = tt.body
			if tt.sink != nil {
				s.Sink = &fakeLaneSink{lanes: tt.sink}
			}
			if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
				t.Fatal(e)
			}
			if n.count != 1 || n.events[0].Lane != tt.lane || !strings.Contains(lastEvent(t, n), tt.via) {
				t.Fatalf("%+v", n.events)
			}
		})
	}
}

// A READY PR that stays open and READY on the same head is notified again
// after daemon.pr.notify.renotify_after and stops after renotify_max repeats;
// every READY is declared --type ready so it stays unread until acked. A
// merged PR gets no reminder.
func TestNotifyReadyRenotifiesUntilMergedOrBound(t *testing.T) {
	s, g, n, r := notifySetup(t)
	now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	poll := func(d time.Duration) {
		t.Helper()
		now = now.Add(d)
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	poll(0)
	if n.count != 1 || n.events[0].Type != "ready" {
		t.Fatalf("first READY: %+v", n.events)
	}
	poll(29 * time.Minute)
	if n.count != 1 {
		t.Fatal("re-notified inside the interval")
	}
	poll(time.Minute)
	if n.count != 2 {
		t.Fatalf("no reminder after 30m: %d events", n.count)
	}
	e := n.events[1]
	if e.Type != "ready" || e.ID != "mergewake-ready-example/project-7-"+shaA+"-reminder-1" || !strings.Contains(e.Text, "[mergewake] READY example/project#7") || !strings.Contains(e.Text, "reminder 1/3, unmerged for 30m0s") {
		t.Fatalf("reminder %+v", e)
	}
	for i := 0; i < 5; i++ {
		poll(30 * time.Minute)
	}
	if n.count != 4 {
		t.Fatalf("reminders not bounded at 3: %d events", n.count)
	}

	// A merged PR gets none, however long it has been.
	s2, g2, n2, r2 := notifySetup(t)
	s2.Now = func() time.Time { return now }
	if e := s2.NotifyPR(context.Background(), r2, 7, Options{}); e != nil || n2.count != 1 {
		t.Fatal(e, n2.count)
	}
	g2.pull.State = "closed"
	now = now.Add(2 * time.Hour)
	if e := s2.NotifyPR(context.Background(), r2, 7, Options{}); e != nil || n2.count != 1 {
		t.Fatalf("merged PR re-notified: %v %d", e, n2.count)
	}
	_ = g
}

// renotify_after: off keeps one READY per head; renotify_max bounds repeats.
func TestNotifyReadyRenotifyConfig(t *testing.T) {
	for _, tt := range []struct {
		after string
		max   int
		want  int
	}{{"off", 0, 1}, {"10m", 1, 2}, {"", 0, 4}} {
		s, _, n, r := notifySetup(t)
		s.Config.Daemon.PR.Notify.RenotifyAfter, s.Config.Daemon.PR.Notify.RenotifyMax = tt.after, tt.max
		now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
		s.Now = func() time.Time { return now }
		for i := 0; i < 8; i++ {
			if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
				t.Fatal(e)
			}
			now = now.Add(time.Hour)
		}
		if n.count != tt.want {
			t.Fatalf("%+v: %d events", tt, n.count)
		}
	}
}

func TestNotifyAutoUpdateBranch(t *testing.T) {
	for _, tc := range []struct {
		name      string
		extra     string
		findings  int
		updateErr error
		wantCalls int
		reason    string
	}{
		{"stale only", "", 0, nil, 1, ""},
		{"other blocker", "", 1, nil, 0, "threads_open:1; base_stale"},
		{"update fails", "", 0, &github.APIError{Code: 422}, 1, "base_stale; update_failed:422"},
		{"disabled", "auto_update_branch: false\n", 0, nil, 0, "base_stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			g.stale, g.policyExtra, g.findings, g.updateErr = true, tc.extra, tc.findings, tc.updateErr
			for i := 0; i < 2; i++ {
				if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
					t.Fatal(err)
				}
			}
			if len(g.updateHeads) != tc.wantCalls {
				t.Fatalf("update calls: %v", g.updateHeads)
			}
			if tc.wantCalls > 0 && g.updateHeads[0] != shaA {
				t.Fatalf("expected head: %v", g.updateHeads)
			}
			if tc.reason == "" {
				if n.count != 0 {
					t.Fatalf("unexpected event: %v", n.events)
				}
			} else if n.count != 1 || !strings.Contains(lastEvent(t, n), tc.reason) {
				t.Fatalf("events: %v", n.events)
			}
		})
	}
}

func TestAutoUpdatePersistenceAndGuards(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.stale = true
	if err := s.NotifyPR(context.Background(), r, 7, Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	g.pull.Draft = true
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(g.updateHeads) != 0 {
		t.Fatal("dry-run or draft updated branch")
	}
	g.pull.Draft = false
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	restarted := *s
	if err := restarted.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(g.updateHeads) != 1 {
		t.Fatalf("restart retried: %v", g.updateHeads)
	}
	g.head = shaC
	if err := restarted.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(g.updateHeads) != 2 || n.count != 0 {
		t.Fatalf("new base: calls=%v events=%v", g.updateHeads, n.events)
	}
}

func TestNotifyShadowStaleBaseBlocksWithoutUpdate(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.stale = true
	if err := s.NotifyPR(context.Background(), r, 7, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if len(g.updateHeads) != 0 || n.count != 1 || !strings.Contains(lastEvent(t, n), "BLOCKED example/project#7 head=aaaaaaa | base_stale") {
		t.Fatalf("updates=%v events=%v", g.updateHeads, n.events)
	}
	if _, err := os.Stat(filepath.Join(s.state(r.Name), "base-updates")); !os.IsNotExist(err) {
		t.Fatalf("shadow reserved update: %v", err)
	}
}
