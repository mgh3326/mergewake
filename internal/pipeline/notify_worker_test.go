package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// workerNotifySetup is notifySetup with the PR's task 42 bound to a job on
// worker-lane, so READY and MERGED have an author worker lane they could go to.
func workerNotifySetup(t *testing.T) (*Service, *fakeGH, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, n, r := notifySetup(t)
	g.pull.Body = "Summary\n\nMergewake-Task: 42\n"
	s.Sink = &jobSink{
		lanes: map[string]string{"42": "design-lane"},
		jobs:  map[string]string{"42": "w-7"},
		recs:  map[string]integrations.JobRecord{"w-7": {Role: "worker", Label: "worker-lane"}},
	}
	return s, g, n, r
}

func sentKeyHas(t *testing.T, s *Service, r config.Repo, key string) bool {
	t.Helper()
	keys, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		t.Fatal(e)
	}
	return keys.has(key)
}

// prNotifyRows are the pr-notify decisions logged for the repo so far.
func prNotifyRows(t *testing.T, s *Service, r config.Repo) []map[string]any {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	return auditRecords(t, b, "pr-notify")
}

// (a) READY with a job binding is not sent: the sent key, the ready clock and
// one decision marked "record only" naming the worker lane it would have used
// are recorded, and a second pass logs nothing further.
func TestRecordOnlyReadyWithJobBinding(t *testing.T) {
	s, _, n, r := workerNotifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatalf("READY delivered: %+v", n.events)
	}
	if !sentKeyHas(t, s, r, verdictKey("ready", 7, shaA)) {
		t.Fatal("sent key not recorded")
	}
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	if c, ok := clocks[verdictKey("ready", 7, shaA)]; !ok || c.First.IsZero() {
		t.Fatalf("ready clock not started: %+v", clocks)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["suppressed"] != "record only" || rows[0]["state"] != "ready" || rows[0]["sent"] != false ||
		rows[0]["lane"] != "worker-lane" || rows[0]["via"] != "task 42 job w-7" {
		t.Fatalf("decisions: %v", rows)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 || len(prNotifyRows(t, s, r)) != 1 {
		t.Fatalf("second pass: events=%+v rows=%v", n.events, prNotifyRows(t, s, r))
	}
}

// (a) With only an author pane, or no worker lane at all, READY is recorded the
// same way; the pane's lane is named in the decision, an absent lane is not.
func TestRecordOnlyReadyAuthorPaneAndNoWorker(t *testing.T) {
	s, _, _ := authorPaneSetup(t)
	r := testRepoFor(s)
	r.Policy = ".mergewake/policy.yaml"
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n := s.Notifier.(*ackNotifier); n.count != 0 {
		t.Fatalf("READY delivered: %+v", n.events)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["suppressed"] != "record only" || rows[0]["lane"] != "w-9-lane" || rows[0]["via"] != "author pane w1:p1 job w-9" {
		t.Fatalf("decisions: %v", rows)
	}

	s2, g, n, r2 := notifySetup(t)
	g.pull.Body = "Summary\n\nMergewake-Task: 42\n"
	s2.Sink = &jobSink{lanes: map[string]string{"42": "design-lane"}}
	if e := s2.NotifyPR(context.Background(), r2, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	rows = prNotifyRows(t, s2, r2)
	if n.count != 0 || !sentKeyHas(t, s2, r2, verdictKey("ready", 7, shaA)) || len(rows) != 1 || rows[0]["suppressed"] != "record only" || rows[0]["lane"] != nil {
		t.Fatalf("events=%+v decisions: %v", n.events, rows)
	}
}

// (b) MERGED with no design lane (no trailer, no repo lane) is never sent, with
// or without a worker lane: the row says "no design lane", and a repeat logs
// nothing. The worker lane is not a MERGED destination.
func TestRecordOnlyMerged(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*testing.T) (*Service, *fakeGH, *fakeNotifier, config.Repo)
	}{{"worker lane", workerNotifySetup}, {"no worker lane", notifySetup}} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := tt.setup(t)
			for i := 0; i < 2; i++ {
				if _, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil || sent {
					t.Fatalf("sent=%t err=%v", sent, e)
				}
			}
			rows := prNotifyRows(t, s, r)
			if n.count != 0 || !sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) || len(rows) != 1 || rows[0]["suppressed"] != "no design lane" || rows[0]["state"] != "merged" {
				t.Fatalf("events=%+v decisions=%v", n.events, rows)
			}
		})
	}
}

// (c) The recorded READY still starts the ready clock, so merge_overdue's
// ready_too_long falls due 45 minutes later, whether or not a worker lane
// exists.
func TestRecordOnlyReadyStillOverdue(t *testing.T) {
	for _, bound := range []bool{false, true} {
		rig := newOverdueRig(t, true)
		if bound {
			rig.g.pull.Body += "\nMergewake-Task: 42\n"
			rig.s.Sink = &jobSink{jobs: map[string]string{"42": "w-7"}, recs: map[string]integrations.JobRecord{"w-7": {Role: "worker", Label: "worker-lane"}}}
		}
		rig.at(0) // READY, recorded
		if len(rig.n.delivered) != 0 {
			t.Fatalf("bound=%t: READY delivered: %+v", bound, rig.n.delivered)
		}
		rig.at(44 * time.Minute)
		rig.wantConditions(overdueNotLanded)
		rig.at(45 * time.Minute)
		rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
		if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "ready_too_long after 45m0s") {
			t.Fatal(e.Text)
		}
	}
}

// (d) BLOCKED keeps today's routing: a review stall goes to the desk. A
// conflict is told by its ESC on the lane the task names, also when a worker
// job is bound (hk 3712).
func TestRecordOnlyBlockedKeepsRouting(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		s, g, n, r := notifySetup(t)
		g.pull.Body = "Summary\n\nMergewake-Task: 42\n"
		s.Sink = &jobSink{lanes: map[string]string{"42": "design-lane"}}
		no := false
		g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		route, err := s.findingsRoute(context.Background(), r, g.pull)
		if err != nil {
			t.Fatal(err)
		}
		if n.count != 1 || n.events[0].Lane != route.lane || n.events[0].Type != "decision" || !strings.HasPrefix(n.events[0].Text, "ESC ") || !strings.Contains(n.events[0].Text, "conflict") {
			t.Fatalf("%+v", n.events)
		}
	})
	t.Run("conflict to the task lane, not the worker lane", func(t *testing.T) {
		s, g, n, r := workerNotifySetup(t)
		no := false
		g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		route, err := s.findingsRoute(context.Background(), r, g.pull)
		if err != nil {
			t.Fatal(err)
		}
		if n.count != 1 || n.events[0].Lane != route.lane || n.events[0].Type != "decision" || !strings.HasPrefix(n.events[0].Text, "ESC ") || !strings.Contains(n.events[0].Text, "conflict") {
			t.Fatalf("%+v", n.events)
		}
	})
	t.Run("review stalled", func(t *testing.T) {
		s, g, n, _, r, now := deliverSetup(t)
		g.refs = map[string]bool{}
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		*now = now.Add(61 * time.Minute)
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		if n.count != 1 || n.events[0].Lane != "operator-desk" || !strings.Contains(n.events[0].Text, "review stalled") {
			t.Fatalf("%+v", n.events)
		}
	})
}

// (e) A recorded READY is never reminded, however long the PR stays open and
// whether or not a worker lane exists.
func TestRecordOnlyReadyNeverReminds(t *testing.T) {
	for _, setup := range []func(*testing.T) (*Service, *fakeGH, *fakeNotifier, config.Repo){workerNotifySetup, notifySetup} {
		s, _, n, r := setup(t)
		now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
		s.Now = func() time.Time { return now }
		for _, d := range []time.Duration{0, 11 * time.Minute, 30 * time.Minute, time.Hour} {
			now = now.Add(d)
			if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
				t.Fatal(e)
			}
		}
		if n.count != 0 {
			t.Fatalf("READY reminder sent: %+v", n.events)
		}
		clocks, e := s.loadReadyClocks(r.Name)
		if e != nil {
			t.Fatal(e)
		}
		if c := clocks[verdictKey("ready", 7, shaA)]; c.Repeats != 0 || c.First.IsZero() {
			t.Fatalf("clock: %+v", c)
		}
	}
}
