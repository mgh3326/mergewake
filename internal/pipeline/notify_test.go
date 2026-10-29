package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

func notifySetup(t *testing.T) (*Service, *fakeGH, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, _, n, r := setup(t)
	g.reviewed = true
	g.pull.Title = "Add feature"
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
	if !strings.Contains(text, "lane=config-lane via config") {
		t.Fatal(text)
	}
	if n.events[0].ID != "mergewake-ready-example/project-7-aaaaaaa" || n.events[0].Lane != "config-lane" {
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
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 || !strings.Contains(lastEvent(t, n), "head=ccccccc") {
		t.Fatalf("new head not notified: %d", n.count)
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
	if n.count != 1 || !strings.Contains(lastEvent(t, n), "[mergewake] BLOCKED example/project#7 head=aaaaaaa | failing checks: vet") {
		t.Fatalf("%+v", n.events)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 {
		t.Fatal("same blocked state re-notified")
	}
}

// With checks green, a blocking-class head missing its second pass reports the
// gap; open gate findings and conflicts report their own reasons.
func TestNotifyBlockedReasons(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prepare func(*fakeGH)
		reason  string
	}{
		{"needs 2nd review", func(g *fakeGH) {
			g.blocking = true
			g.reviewed = false
			g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Family: "anthropic", Lens: "correctness"}}}
		}, "needs 2nd review"},
		{"open blocking findings", func(g *fakeGH) {
			g.feedback.Threads = []github.ReviewThread{{ID: "t1", Path: "tool", Comments: []github.ReviewComment{{Author: "coderabbitai[bot]", CommitSHA: shaA, Body: "Critical problem here"}}}}
		}, "open blocking findings: 1"},
		{"unanswered findings", func(g *fakeGH) {
			g.feedback.Threads = []github.ReviewThread{{ID: "t1", Path: "tool", Comments: []github.ReviewComment{{Author: "coderabbitai[bot]", CommitSHA: shaA, Body: "Major problem here"}}}}
		}, "unanswered findings: 1"},
		{"conflicts", func(g *fakeGH) {
			no := false
			g.pull.Mergeable = &no
			g.pull.MergeableState = "dirty"
		}, "conflicts"},
		{"main CI red", func(g *fakeGH) {
			g.checks = map[string]github.CI{shaA: github.Green, shaB: github.Red}
		}, "main CI red"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			tt.prepare(g)
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
		{"config fallback", func(s *Service, g *fakeGH) {}, "config-lane", "via config", false},
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
	g.feedback.Threads = []github.ReviewThread{{ID: "t1", Path: "tool", Comments: []github.ReviewComment{{Author: "coderabbitai[bot]", CommitSHA: shaA, Body: "Critical problem here"}}}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || !strings.Contains(lastEvent(t, n), "BLOCKED") {
		t.Fatalf("%+v", n.events)
	}
	g.feedback.Threads = nil
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 || !strings.Contains(lastEvent(t, n), "READY") {
		t.Fatal("recovery did not notify")
	}
	g.feedback.Threads = []github.ReviewThread{{ID: "t2", Path: "tool", Comments: []github.ReviewComment{{Author: "coderabbitai[bot]", CommitSHA: shaA, Body: "Critical again"}}}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 2 {
		t.Fatal("repeated blocked state re-notified")
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
