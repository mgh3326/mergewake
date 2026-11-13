package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// laneNotifier is a notifier that refuses the lanes in refuse, the way panewire
// refuses a lane that is not registered, and records what it delivered.
type laneNotifier struct {
	refuse    map[string]bool
	delivered []integrations.Event
}

func (n *laneNotifier) Notify(_ context.Context, e integrations.Event) error {
	if n.refuse[e.Lane] {
		return errors.New("lane not registered: " + e.Lane)
	}
	n.delivered = append(n.delivered, e)
	return nil
}

// overdueEvents are the delivered merge_overdue notices (not the FYI lines).
func (n *laneNotifier) overdueEvents() []integrations.Event {
	var out []integrations.Event
	for _, e := range n.delivered {
		if strings.HasPrefix(e.Text, "[mergewake] MERGE_OVERDUE ") {
			out = append(out, e)
		}
	}
	return out
}

func (n *laneNotifier) conditions() []string {
	var out []string
	for _, e := range n.overdueEvents() {
		out = append(out, strings.Fields(strings.SplitN(e.Text, "|", 3)[1])[0])
	}
	return out
}

// overdueRig is a PR whose evaluation runs on a fake clock the test moves.
type overdueRig struct {
	t   *testing.T
	s   *Service
	g   *fakeGH
	n   *laneNotifier
	r   config.Repo
	now time.Time
}

var overdueStart = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

// newOverdueRig starts a green, mergeable, not yet reviewed PR with no
// reviewer job. reviewed marks its review done.
func newOverdueRig(t *testing.T, reviewed bool) *overdueRig {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.pull.Title = "Add feature"
	g.enabled = true
	if reviewed {
		markReviewed(g)
	}
	s.Config.Notifier.Lane = "desk-lane"
	rig := &overdueRig{t: t, s: s, g: g, n: &laneNotifier{refuse: map[string]bool{}}, r: r, now: overdueStart}
	s.Notifier = rig.n
	s.Now = func() time.Time { return rig.now }
	TakeMergeOverdue(r.Name)
	return rig
}

// at moves the clock d past the start and evaluates the PR once.
func (rig *overdueRig) at(d time.Duration) {
	rig.t.Helper()
	rig.now = overdueStart.Add(d)
	rig.eval()
}

func (rig *overdueRig) eval() {
	rig.t.Helper()
	if e := rig.s.NotifyPR(context.Background(), rig.r, 7, Options{}); e != nil {
		rig.t.Fatal(e)
	}
}

func (rig *overdueRig) wantConditions(want ...string) {
	rig.t.Helper()
	if got := strings.Join(rig.n.conditions(), ","); got != strings.Join(want, ",") {
		rig.t.Fatalf("merge_overdue notices = [%s], want [%s]; events: %+v", got, strings.Join(want, ","), rig.n.delivered)
	}
}

// (a) Checks done 21 minutes ago and no reviewer started: one notice to the
// lead lane, and none on the next evaluation. The text, the audit row and the
// counter carry the notice.
func TestMergeOverdueReviewNotStarted(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.at(0)
	rig.at(19 * time.Minute)
	rig.wantConditions()
	rig.at(21 * time.Minute)
	rig.wantConditions(overdueReviewNotStarted)
	e := rig.n.overdueEvents()[0]
	if e.Lane != "lead" || e.Type != "decision" || !strings.HasSuffix(e.ID, "-"+rig.g.pull.Head.SHA+"-review_not_started") {
		t.Fatalf("%+v", e)
	}
	want := "[mergewake] MERGE_OVERDUE example/project#7 head=aaaaaaa | review_not_started after 21m0s | gate: blocked review_missing | checks: CI=green | review: passes 0/1, reviewer none"
	if e.Text != want {
		t.Fatalf("text:\n got %s\nwant %s", e.Text, want)
	}
	rig.at(22 * time.Minute)
	rig.wantConditions(overdueReviewNotStarted)
	if n := TakeMergeOverdue(rig.r.Name); n != 1 {
		t.Fatalf("counter = %d", n)
	}
	b, _ := os.ReadFile(filepath.Join(rig.s.state(rig.r.Name), "decisions.jsonl"))
	rows := auditRecords(t, b, "merge_overdue")
	if len(rows) != 1 || rows[0]["condition"] != "review_not_started" || rows[0]["route"] != "lead" || rows[0]["pr"] != float64(7) || rows[0]["head"] != shaA || rows[0]["age"] != "21m0s" || rows[0]["repo"] != "example/project" {
		t.Fatalf("audit rows: %v", rows)
	}
}

// A reviewer job recorded for the head means the review started: no
// review_not_started notice.
func TestMergeOverdueReviewerJobRecordedIsStarted(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.at(0)
	if e := fsutil.WriteJSON(rig.s.reviewerJobPath(rig.r.Name, 7, 1), reviewerJob{PR: 7, Head: shaA, Pass: 1, Label: "mwr-7-aaaaaaa-p1-1", Recorded: rig.now}); e != nil {
		t.Fatal(e)
	}
	rig.at(30 * time.Minute)
	rig.wantConditions()
}

// (b) The review finished 11 minutes ago, the gate is ready and the PR is not
// merged: not_landed. (c) The head was first seen 46 minutes ago: ready_too_long.
// Each is sent once, in order, to the lead lane.
func TestMergeOverdueNotLandedAndReadyTooLong(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0)
	rig.at(9 * time.Minute)
	rig.wantConditions()
	rig.at(11 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 11m0s | gate: ready | checks: CI=green | review: passes 1/1, reviewer none") {
		t.Fatal(e.Text)
	}
	rig.at(30 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(46 * time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "| ready_too_long after 46m0s | gate: ready") {
		t.Fatal(e.Text)
	}
	rig.at(60 * time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	for _, e := range rig.n.overdueEvents() {
		if e.Lane != "lead" {
			t.Fatalf("%+v", e)
		}
	}
}

// (d) Each exclusion keeps the notice back, and it is evaluated again on every
// pass: lifting the exclusion sends it.
func TestMergeOverdueExclusions(t *testing.T) {
	deploy := "deploy_approval:\n  - paths: ['deploy/**']\n    reason: deploy\n"
	for _, tt := range []struct {
		name    string
		reviewd bool
		prepare func(*overdueRig)
		lift    func(*overdueRig)
	}{
		{"open high finding", true, func(rig *overdueRig) {
			rig.g.policyExtra = gradedPolicy
			rig.g.feedback.Threads = []github.ReviewThread{gradedThread("review-bot", "high", false, shaB)}
			rig.g.findings = 1
		}, func(rig *overdueRig) { rig.g.feedback.Threads[0].IsResolved = true; rig.g.findings = 0 }},
		{"finding outside diff", true, func(rig *overdueRig) {
			patch := rig.s.patchIDOf(context.Background(), rig.r.Name, 7, shaA)
			rig.s.GH = &outsideGH{fakeGH: rig.g, history: []github.ReviewedPass{
				{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Lens: "correctness", Patch: patch}, OutsideBlocking: 1},
				{Head: shaA, PassReceipt: github.PassReceipt{Pass: 2, Lens: "state-risk", Patch: patch}},
			}}
		}, func(rig *overdueRig) { rig.s.GH.(*outsideGH).history[0].OutsideBlocking = 0 }},
		{"check failed", true, func(rig *overdueRig) { rig.g.ci = github.Red }, func(rig *overdueRig) { rig.g.ci = github.Green }},
		{"check absent", true, func(rig *overdueRig) {
			rig.g.ci = github.Absent
			rig.g.committedAt = map[string]time.Time{shaA: overdueStart.Add(-2 * time.Hour)}
		}, func(rig *overdueRig) { rig.g.ci = github.Green }},
		{"conflict", true, func(rig *overdueRig) {
			no := false
			rig.g.pull.Mergeable, rig.g.pull.MergeableState = &no, "dirty"
		}, func(rig *overdueRig) {
			yes := true
			rig.g.pull.Mergeable, rig.g.pull.MergeableState = &yes, "clean"
		}},
		{"T3 trailer", true, func(rig *overdueRig) { rig.g.pull.Body = "Mergewake-Tier: T3" }, func(rig *overdueRig) { rig.g.pull.Body = "Mergewake-Tier: T2" }},
		{"T3 task title", true, func(rig *overdueRig) {
			rig.g.pull.Body = "Mergewake-Task: 42"
			rig.s.Sink = &titledLaneSink{fakeLaneSink: fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}, title: "ship it (grade B, T3)"}
		}, func(rig *overdueRig) { rig.s.Sink.(*titledLaneSink).title = "ship it (grade B, T2)" }},
		{"deploy_approval path", true, func(rig *overdueRig) {
			rig.g.policyExtra = deploy
			rig.g.files = []policy.File{{Path: "deploy/app.yml", Kind: "modified"}}
		}, func(rig *overdueRig) { rig.g.files = []policy.File{{Path: "src/app.go", Kind: "modified"}} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rig := newOverdueRig(t, tt.reviewd)
			tt.prepare(rig)
			rig.at(0)
			rig.at(50 * time.Minute)
			rig.at(90 * time.Minute)
			rig.wantConditions()
			if n := TakeMergeOverdue(rig.r.Name); n != 0 {
				t.Fatalf("counter = %d", n)
			}
			tt.lift(rig)
			rig.at(100 * time.Minute)
			if len(rig.n.overdueEvents()) == 0 {
				t.Fatalf("no notice once lifted; events: %+v", rig.n.delivered)
			}
		})
	}
}

// titledLaneSink answers TaskJob with a task title for the tier marker.
type titledLaneSink struct {
	fakeLaneSink
	title string
}

func (s *titledLaneSink) TaskJob(context.Context, string) (string, string, error) {
	return "", s.title, nil
}

// (e) A new head starts its own clocks, and is announced on its own.
func TestMergeOverdueNewHeadResetsClocks(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.at(0)
	rig.at(21 * time.Minute)
	rig.wantConditions(overdueReviewNotStarted)
	rig.g.pull.Head.SHA = shaC
	rig.at(22 * time.Minute)
	rig.at(40 * time.Minute)
	rig.wantConditions(overdueReviewNotStarted)
	rig.at(43 * time.Minute)
	rig.wantConditions(overdueReviewNotStarted, overdueReviewNotStarted)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "head=ccccccc | review_not_started after 21m0s") {
		t.Fatal(e.Text)
	}
}

// (f) The notifier refuses the lead lane: the notice goes to the PR's task
// lane, and the desk hears one short FYI.
func TestMergeOverdueLeadRefusedFallsBackToTaskLane(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.g.pull.Body = "Mergewake-Task: 42"
	rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}}
	rig.n.refuse["lead"] = true
	rig.at(0)
	rig.at(21 * time.Minute)
	rig.at(25 * time.Minute)
	if len(rig.n.delivered) != 2 {
		t.Fatalf("delivered: %+v", rig.n.delivered)
	}
	notice, fyi := rig.n.delivered[0], rig.n.delivered[1]
	if notice.Lane != "task-lane" || notice.Type != "decision" || !strings.HasPrefix(notice.Text, "[mergewake] MERGE_OVERDUE example/project#7 head=aaaaaaa | review_not_started after 21m0s") || !strings.HasSuffix(notice.Text, "lead lane unavailable; sent to task-lane via task 42") {
		t.Fatalf("%+v", notice)
	}
	if fyi.Lane != "desk-lane" || fyi.Type != "info" || !strings.HasPrefix(fyi.Text, "[mergewake] FYI merge_overdue example/project#7") || strings.Contains(fyi.Text, "MERGE_OVERDUE") {
		t.Fatalf("%+v", fyi)
	}
	if n := TakeMergeOverdue(rig.r.Name); n != 1 {
		t.Fatalf("counter = %d", n)
	}
}

// When the task lane is the desk itself, it already has the notice: no FYI.
func TestMergeOverdueNoFYIWhenFallbackIsDesk(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.n.refuse["lead"] = true
	rig.at(0)
	rig.at(21 * time.Minute)
	if len(rig.n.delivered) != 1 || rig.n.delivered[0].Lane != "desk-lane" {
		t.Fatalf("delivered: %+v", rig.n.delivered)
	}
}

// A notice no lane takes is parked: it is not retried on every pass.
func TestMergeOverdueUndeliverableIsParkedOnce(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.n.refuse["lead"], rig.n.refuse["desk-lane"] = true, true
	rig.at(0)
	rig.at(21 * time.Minute)
	rig.n.refuse["lead"], rig.n.refuse["desk-lane"] = false, false
	rig.at(25 * time.Minute)
	if len(rig.n.delivered) != 0 || TakeMergeOverdue(rig.r.Name) != 0 {
		t.Fatalf("delivered: %+v", rig.n.delivered)
	}
}

// (g) The clocks and the sent keys live in the state directory: a restarted
// service keeps the clock and does not repeat a notice.
func TestMergeOverdueSurvivesRestart(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.at(0)
	restarted := *rig.s
	n2 := &laneNotifier{refuse: map[string]bool{}}
	restarted.Notifier = n2
	rig.now = overdueStart.Add(21 * time.Minute)
	if e := restarted.NotifyPR(context.Background(), rig.r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if got := n2.conditions(); len(got) != 1 || got[0] != overdueReviewNotStarted {
		t.Fatalf("restart lost the clock: %+v", n2.delivered)
	}
	again := *rig.s
	n3 := &laneNotifier{refuse: map[string]bool{}}
	again.Notifier = n3
	if e := again.NotifyPR(context.Background(), rig.r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(n3.delivered) != 0 {
		t.Fatalf("restart repeated the notice: %+v", n3.delivered)
	}
}

// (h) The review clock starts when the required checks are done, not when the
// head was seen: CI still running after 25 minutes owes no review yet.
func TestMergeOverdueCIRunningStartsNoReviewClock(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.g.ci = github.Pending
	rig.at(0)
	rig.at(25 * time.Minute)
	rig.wantConditions()
	rig.g.ci = github.Green
	rig.at(26 * time.Minute)
	rig.at(44 * time.Minute)
	rig.wantConditions()
	rig.at(47 * time.Minute)
	if got := rig.n.conditions(); len(got) == 0 || got[0] != overdueReviewNotStarted {
		t.Fatalf("conditions %v", got)
	}
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "review_not_started after 21m0s") {
		t.Fatal(e.Text)
	}
}

// (b) An author push is a head of its own: no update made it, so its clocks
// start at the push and its READY is the one sent for it.
func TestMergeOverdueAuthorPushRestartsClocks(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // READY for aaaaaaa
	rig.g.pull.Head.SHA = shaC
	rig.g.parents2 = map[string][]string{shaC: {shaA}}
	markReviewed(rig.g)
	rig.at(31 * time.Minute)
	rig.at(45 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(76 * time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "head=ccccccc | ready_too_long after 45m0s | gate: ready") || strings.Contains(e.Text, "carried from") {
		t.Fatal(e.Text)
	}
}

// (e) ready_too_long counts from the READY verdict, not from the head's first
// pass: a head seen 59 minutes ago with its READY sent 9 minutes ago owes
// nothing yet.
func TestMergeOverdueReadyTooLongCountsFromReady(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.g.ci = github.Pending
	rig.at(0) // head seen, CI still running: no READY
	rig.at(50 * time.Minute)
	rig.g.ci = github.Green
	rig.at(50 * time.Minute)
	rig.at(59 * time.Minute)
	rig.wantConditions()
	rig.at(60 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(95 * time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "ready_too_long after 45m0s | gate: ready") {
		t.Fatal(e.Text)
	}
}

// (f) No READY was sent for the head: no ready_too_long, however long the head
// waits.
func TestMergeOverdueNoReadyNoReadyTooLong(t *testing.T) {
	rig := newOverdueRig(t, false)
	rig.at(0)
	if e := fsutil.WriteJSON(rig.s.reviewerJobPath(rig.r.Name, 7, 1), reviewerJob{PR: 7, Head: shaA, Pass: 1, Label: "mwr-7-aaaaaaa-p1-1", Recorded: rig.now}); e != nil {
		t.Fatal(e)
	}
	rig.at(120 * time.Minute)
	rig.wantConditions()
}

// (g) A pending verdict that waits only on GitHub's mergeability computation
// holds the notice for that evaluation.
func TestMergeOverdueHeldWhileMergeableUnknown(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // READY at 0
	rig.at(10 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.g.pull.Mergeable, rig.g.pull.MergeableState = nil, "unknown"
	rig.at(50 * time.Minute)
	rig.wantConditions(overdueNotLanded)
	yes := true
	rig.g.pull.Mergeable, rig.g.pull.MergeableState = &yes, "clean"
	rig.at(55 * time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "ready_too_long after 55m0s") {
		t.Fatal(e.Text)
	}
}

// (h) A pending verdict that waits on CI does not hold the notice: a head
// READY 50 minutes ago whose checks are rerunning is ready_too_long. The
// pending path reads no review record, so review_not_started falls due too.
func TestMergeOverdueCIWaitStillReadyTooLong(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // READY at 0
	rig.g.ci = github.Pending
	rig.at(50 * time.Minute)
	rig.wantConditions(overdueReviewNotStarted, overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[2]; !strings.Contains(e.Text, "ready_too_long after 50m0s | gate: pending | checks: CI=pending") {
		t.Fatal(e.Text)
	}
}

// (i) A clocks file written before the update records and the READY clock
// existed is a bare map of clocks: it still loads, keeps its times, and is
// rewritten in the current shape.
func TestMergeOverdueLegacyClocksFileLoads(t *testing.T) {
	rig := newOverdueRig(t, false)
	file := rig.s.overdueClocksFile(rig.r.Name)
	legacy := map[string]overdueClock{"7 " + shaA: {
		HeadSeen:   overdueStart.Add(-time.Hour),
		ChecksDone: overdueStart.Add(-21 * time.Minute),
		Touched:    overdueStart.Add(-2 * time.Hour),
	}}
	if e := fsutil.WriteJSON(file, legacy); e != nil {
		t.Fatal(e)
	}
	rig.at(0)
	rig.wantConditions(overdueReviewNotStarted)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "review_not_started after 21m0s") {
		t.Fatal(e.Text)
	}
	st, e := readOverdueState(file)
	if e != nil {
		t.Fatal(e)
	}
	if c, ok := st.Clocks["7 "+shaA]; !ok || !c.HeadSeen.Equal(overdueStart.Add(-time.Hour)) {
		t.Fatalf("legacy clocks lost: %+v", st)
	}
	b, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	var raw map[string]json.RawMessage
	if e := json.Unmarshal(b, &raw); e != nil {
		t.Fatal(e)
	}
	if _, ok := raw["clocks"]; !ok {
		t.Fatalf("file not rewritten in the current shape: %s", b)
	}
}
