package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

func TestSpawnCauseNamesTheDaemonsSpawnFailures(t *testing.T) {
	for _, tt := range []struct{ text, want string }{
		{"panewire spawn exit 70: spawn_failed: quota gate: free limit exceeded until 2026-11-19T03:34:12Z", "spawn:quota"},
		{"quota exhausted", "spawn:quota"},
		{"panewire spawn exit 78: admission refused: no eligible account", "spawn:admission-refused"},
		{"panewire reviewer deadline", "spawn:timeout"},
		{"spawn timeout after 10m", "spawn:timeout"},
		{"deadline_not_started", "spawn:timeout"},
		{"no time left to wait for a reviewer pane", "spawn:timeout"},
		{"reviewer job expired", "spawn:expired"},
		{"brief upload to hk failed", "spawn:brief-upload"},
		{"brief_doc_unavailable", "spawn:brief-upload"},
		{"auth_unavailable: no token for the pool", "spawn:auth"},
		{"workspace_unresolved", "spawn:workspace"},
		{"node_upgrade_required", "spawn:node-upgrade"},
		{"job-host-unconfigured", "spawn:host-unconfigured"},
		{"panewire reviewer spawn needs HUB in the environment (unset or empty)", "spawn:host-unconfigured"},
		{"repo root unset; no checkout for the PR head", "spawn:repo-root"},
		{"no Mergewake-Task trailer", "spawn:other"},
		{"", "spawn:other"},
	} {
		if got := spawnCause(tt.text); got != tt.want {
			t.Errorf("spawnCause(%q) = %q, want %q", tt.text, got, tt.want)
		}
	}
}

func TestLostCauseNamesTheRecordedReason(t *testing.T) {
	for reason, want := range map[string]string{
		lostOverdue:                "lost:overdue",
		lostPaneFinished:           "lost:pane-finished",
		lostNeverPlaced:            "lost:never-placed",
		"job record names no host": "lost:other",
	} {
		if got := lostCause(reason); got != want {
			t.Errorf("lostCause(%q) = %q, want %q", reason, got, want)
		}
	}
}

// causeAt counts one event of cause at the given time.
func causeAt(t *testing.T, s *Service, r config.Repo, at time.Time, cause string) {
	t.Helper()
	s.Now = func() time.Time { return at }
	if err := s.noteCause(context.Background(), r, causeEvent{Cause: cause, PR: 7, Head: shaA, Pass: 1, Detail: "test"}); err != nil {
		t.Fatal(err)
	}
}

func TestNoticeCauseThresholdSendsOnceThenQuiet(t *testing.T) {
	s, g, n, r := notifySetup(t)
	lostBurst(t, s, g, r, 11)
	if n.count != 1 {
		t.Fatalf("11 losses in the window: notices = %d, want exactly one (at the 10th)", n.count)
	}
	rows := auditRows(t, s, r, "notice-cause")
	if len(rows) != 11 || rows[8]["sent"] != false || rows[9]["sent"] != true || rows[9]["reason"] != "threshold" || rows[10]["sent"] != false {
		t.Fatalf("notice-cause rows = %v, want the 10th sent and the 11th quiet", rows)
	}
	if !strings.Contains(n.events[0].Text, "cause=lost:other, 10 in the last 1h0m0s") {
		t.Fatalf("threshold notice does not name the cause and count: %s", n.events[0].Text)
	}
}

func TestNoticeCauseThresholdRearmsAfterTheWindow(t *testing.T) {
	s, _, n, r := notifySetup(t)
	t0 := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		causeAt(t, s, r, t0.Add(time.Duration(i)*time.Minute), "lost:overdue")
	}
	if n.count != 1 {
		t.Fatalf("first ten events: notices = %d, want 1", n.count)
	}
	// Past the window the old events fall out, and a new ten notice again.
	for i := 0; i < 9; i++ {
		causeAt(t, s, r, t0.Add(70*time.Minute+time.Duration(i)*time.Minute), "lost:overdue")
	}
	if n.count != 1 {
		t.Fatalf("nine events after the window: notices = %d, want still 1", n.count)
	}
	causeAt(t, s, r, t0.Add(79*time.Minute), "lost:overdue")
	if n.count != 2 {
		t.Fatalf("ten events in a new window: notices = %d, want 2", n.count)
	}
}

func TestNoticeCauseFixNeededOncePerHour(t *testing.T) {
	s, _, n, r := notifySetup(t)
	s.Config.Notifier.ToolLane = "tool-lane"
	t0 := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	causeAt(t, s, r, t0, "spawn:auth")
	causeAt(t, s, r, t0.Add(5*time.Minute), "spawn:auth")
	causeAt(t, s, r, t0.Add(61*time.Minute), "spawn:auth")
	if n.count != 2 || n.events[0].Lane != "tool-lane" || !strings.Contains(n.events[0].Text, "the cause needs a fix") {
		t.Fatalf("fix-needed notices = %+v, want one at once and one an hour later", n.events)
	}
	rows := auditRows(t, s, r, "notice-cause")
	if len(rows) != 3 || rows[0]["sent"] != true || rows[1]["sent"] != false || rows[2]["sent"] != true {
		t.Fatalf("notice-cause rows = %v", rows)
	}
}

func TestNoticeCauseSingleSpawnFailureIsQuiet(t *testing.T) {
	s, _, n, r := notifySetup(t)
	causeAt(t, s, r, time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC), "spawn:quota")
	if n.count != 0 {
		t.Fatalf("a single quota stop notified: %+v", n.events)
	}
	if rows := auditRows(t, s, r, "notice-cause"); len(rows) != 1 || rows[0]["cause"] != "spawn:quota" || rows[0]["sent"] != false {
		t.Fatalf("notice-cause rows = %v", rows)
	}
}

// A pass's skipped reviewers are counted by their own causes: the fix-needed
// one is told, the quota stop is only recorded.
func TestNoticeCauseSpawnSkipsAreCountedByCause(t *testing.T) {
	s, g, n, r := notifySetup(t)
	skips := []reviewSkip{
		{Reviewer: "panewire", Reason: "panewire spawn exit 70: spawn_failed: quota gate: free limit exceeded"},
		{Reviewer: "panewire", Reason: "repo root unset; no checkout for the PR head"},
	}
	s.noteSpawnCauses(context.Background(), r, g.pull, 1, skips)
	if n.count != 1 || !strings.Contains(n.events[0].Text, "cause=spawn:repo-root") {
		t.Fatalf("notices = %+v, want one repo-root notice", n.events)
	}
	rows := auditRows(t, s, r, "notice-cause")
	if len(rows) != 2 || rows[0]["cause"] != "spawn:quota" || rows[0]["sent"] != false || rows[1]["cause"] != "spawn:repo-root" || rows[1]["sent"] != true {
		t.Fatalf("notice-cause rows = %v", rows)
	}
}

func TestNoticesLivenessMechanism(t *testing.T) {
	day := "2026-11-17"
	dTime := balanceDayTime(day, 12)
	mech := func(res *LivenessDayResult) LivenessMechanismResult {
		t.Helper()
		for _, m := range res.Mechanisms {
			if m.Mechanism == MechNotices {
				return m
			}
		}
		t.Fatal("no notices mechanism in the day result")
		return LivenessMechanismResult{}
	}

	// Quiet: single losses are recorded and not sent, and nothing is a finding.
	f := newLivenessFixture(t, dTime)
	f.audit(t, f.repos[0].Name, "notice-cause", map[string]any{"cause": "lost:other", "sent": false}, dTime)
	f.audit(t, f.repos[0].Name, "notice-cause", map[string]any{"cause": "lost:pane-finished", "sent": false}, dTime)
	res, err := f.s.livenessForDay(f.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	if m := mech(res); m.Stalled || m.Made != 2 || m.Done != 0 || m.Reason != "lost:other 1 (sent 0); lost:pane-finished 1 (sent 0)" {
		t.Fatalf("quiet notices: %+v", m)
	}

	// A fix-needed cause appearing at all is a finding.
	f2 := newLivenessFixture(t, dTime)
	f2.audit(t, f2.repos[0].Name, "notice-cause", map[string]any{"cause": "spawn:auth", "sent": true}, dTime)
	res2, err := f2.s.livenessForDay(f2.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	if m := mech(res2); !m.Stalled || m.Made != 1 || m.Done != 1 || !strings.Contains(m.Reason, "fix needed: spawn:auth") {
		t.Fatalf("fix-needed notices: %+v", m)
	}

	// A cause that passes the threshold in one repo that day is a finding too.
	f3 := newLivenessFixture(t, dTime)
	for i := 0; i < 10; i++ {
		f3.audit(t, f3.repos[0].Name, "notice-cause", map[string]any{"cause": "lost:overdue", "sent": i == 9}, dTime)
	}
	res3, err := f3.s.livenessForDay(f3.repos, day)
	if err != nil {
		t.Fatal(err)
	}
	if m := mech(res3); !m.Stalled || m.Made != 10 || m.Done != 1 || !strings.Contains(m.Reason, "threshold passed: lost:overdue in example/a (10 >= 10)") {
		t.Fatalf("threshold notices: %+v", m)
	}
}
