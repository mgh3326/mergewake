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
// worker-lane, so READY and MERGED have an author worker lane to go to.
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

// (a) READY with a job binding goes to the job's worker lane, not a design
// lane the task happens to name.
func TestWorkerLaneReadyJobBinding(t *testing.T) {
	s, _, n, r := workerNotifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "worker-lane" || !strings.Contains(n.events[0].Text, "lane=worker-lane via task 42 job w-7") {
		t.Fatalf("%+v", n.events)
	}
}

// (b) With no job binding, READY goes to the author pane's job lane, and says
// which pane chose it.
func TestWorkerLaneReadyAuthorPane(t *testing.T) {
	s, _, _ := authorPaneSetup(t)
	r := testRepoFor(s)
	r.Policy = ".mergewake/policy.yaml"
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	n := s.Notifier.(*ackNotifier)
	if n.count != 1 || n.events[0].Lane != "w-9-lane" || !strings.Contains(n.events[0].Text, "lane=w-9-lane via author pane w1:p1 job w-9") {
		t.Fatalf("%+v", n.events)
	}
}

// (c) READY with neither a job binding nor an author pane is suppressed: it is
// not sent, but the sent key, the ready clock and one decision with the
// suppressed marker are recorded.
func TestWorkerLaneReadySuppressedWithoutWorker(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.pull.Body = "Summary\n\nMergewake-Task: 42\n"
	s.Sink = &jobSink{lanes: map[string]string{"42": "design-lane"}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatalf("suppressed READY delivered: %+v", n.events)
	}
	if !sentKeyHas(t, s, r, verdictKey("ready", 7, shaA)) {
		t.Fatal("sent key not recorded")
	}
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	c, ok := clocks[verdictKey("ready", 7, shaA)]
	if !ok || c.First.IsZero() {
		t.Fatalf("ready clock not started: %+v", clocks)
	}
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	rows := auditRecords(t, b, "pr-notify")
	if len(rows) != 1 || rows[0]["suppressed"] != "no worker lane" || rows[0]["lane"] != "design-lane" || rows[0]["via"] != "task 42" {
		t.Fatalf("decisions: %v", rows)
	}
	// A second pass stays quiet and logs nothing further.
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatalf("suppressed READY re-sent: %+v", n.events)
	}
	b, e = os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	if rows = auditRecords(t, b, "pr-notify"); len(rows) != 1 {
		t.Fatalf("suppression logged again: %v", rows)
	}
}

// (d) The suppressed READY still starts the ready clock, so merge_overdue's
// ready_too_long falls due 45 minutes later.
func TestWorkerLaneSuppressedReadyStillOverdue(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // READY, suppressed: no worker lane
	if len(rig.n.delivered) != 0 {
		t.Fatalf("suppressed READY delivered: %+v", rig.n.delivered)
	}
	rig.at(44 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(45 * time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "ready_too_long after 45m0s") {
		t.Fatal(e.Text)
	}
}

// (e) MERGED goes to a bound worker lane and is suppressed without one.
func TestWorkerLaneMerged(t *testing.T) {
	s, g, n, r := workerNotifySetup(t)
	if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "worker-lane" || n.events[0].Type != "merged" {
		t.Fatalf("%+v", n.events)
	}

	s, g, n, r = notifySetup(t)
	if _, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil || sent {
		t.Fatalf("sent=%t err=%v", sent, e)
	}
	if n.count != 0 || !sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) {
		t.Fatalf("events=%+v", n.events)
	}
	if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil || n.count != 0 {
		t.Fatalf("suppressed MERGED repeated: %+v %v", n.events, e)
	}
}

// (f) BLOCKED keeps today's routing: a conflict goes to the lane the task
// names, and a review stall goes to the desk.
func TestWorkerLaneBlockedKeepsDesignRouting(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		s, g, n, r := notifySetup(t)
		g.pull.Body = "Summary\n\nMergewake-Task: 42\n"
		s.Sink = &jobSink{lanes: map[string]string{"42": "design-lane"}}
		no := false
		g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		if n.count != 1 || n.events[0].Lane != "design-lane" || !strings.Contains(n.events[0].Text, "BLOCKED") || !strings.Contains(n.events[0].Text, "conflict") {
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

// (g) A suppressed READY never reminds: no reminder may reach a design or desk
// lane, however long the PR stays open.
func TestWorkerLaneNoReadyReminderOutsideWorker(t *testing.T) {
	s, _, n, r := notifySetup(t)
	s.Config.Daemon.PR.Notify.RenotifyAfter, s.Config.Daemon.PR.Notify.RenotifyMax = "10m", 3
	now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	for _, d := range []time.Duration{0, 11 * time.Minute, 30 * time.Minute, time.Hour} {
		now = now.Add(d)
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 0 {
		t.Fatalf("reminders reached a non-worker lane: %+v", n.events)
	}
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	if c := clocks[verdictKey("ready", 7, shaA)]; c.Repeats != 0 {
		t.Fatalf("reminder counted for a suppressed READY: %+v", c)
	}
}
