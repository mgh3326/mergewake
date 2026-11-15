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

// (j) A draft is a deliberate hold, not the author's wait: while the PR is a
// draft no condition fires and its clocks do not count. The pass that sees it
// leave draft restarts the head's clocks there, so a READY 8 hours before the
// hold owes its threshold from the draft's end.
func TestMergeOverdueDraftHoldDoesNotCount(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // reviewed and READY at overdueStart
	rig.g.pull.Draft = true
	rig.at(0) // the next pass sees the draft at the same instant (fake clock)
	rig.at(8 * time.Hour)
	rig.wantConditions() // a draft is held: no condition fires
	rig.g.pull.Draft = false
	rig.at(8 * time.Hour) // leaving draft starts the clocks again here
	rig.wantConditions()
	rig.at(8*time.Hour + 9*time.Minute)
	rig.wantConditions()
	rig.at(8*time.Hour + 10*time.Minute)
	rig.wantConditions(overdueNotLanded)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 10m0s |") {
		t.Fatal(e.Text)
	}
	rig.at(8*time.Hour + 45*time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "| ready_too_long after 45m0s |") {
		t.Fatal(e.Text)
	}
}

// A draft hold is anchored at the last pass that saw the PR open, not at the
// first pass that saw it a draft: the draft time between those two passes is
// not counted as open time. READY at t0, a draft from t0+1m first seen at
// t0+6m, and the PR ready again at t0+8h: the head owes the full threshold
// from t0+8h.
func TestMergeOverdueDraftHoldAnchorsAtLastOpenPass(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // reviewed and READY at overdueStart; the last pass that sees it open
	rig.g.pull.Draft = true
	rig.at(6 * time.Minute) // the draft from t0+1m is first seen here
	rig.at(8 * time.Hour)
	rig.wantConditions() // a draft is held: no condition fires
	rig.g.pull.Draft = false
	rig.at(8 * time.Hour) // ready again: the whole hold is off the clocks
	rig.wantConditions()
	rig.at(8*time.Hour + 44*time.Minute) // not_landed counts from the hold's end; ready_too_long does not owe yet
	rig.wantConditions(overdueNotLanded)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 44m0s |") {
		t.Fatal(e.Text)
	}
	rig.at(8*time.Hour + 45*time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "| ready_too_long after 45m0s |") {
		t.Fatal(e.Text)
	}
}

// A hold a clocks file from an earlier release recorded has no last_open: it
// keeps that anchor, counting the hold from the pass that first saw the draft.
func TestMergeOverdueLegacyDraftHoldKeepsItsAnchor(t *testing.T) {
	rig := newOverdueRig(t, true)
	file := rig.s.overdueClocksFile(rig.r.Name)
	legacy := map[string]overdueClock{"7 " + shaA: {
		HeadSeen:   overdueStart,
		ChecksDone: overdueStart,
		ReviewDone: overdueStart,
		Ready:      overdueStart,
		DraftSince: overdueStart.Add(6 * time.Minute),
		Touched:    overdueStart.Add(6 * time.Minute),
	}}
	if e := fsutil.WriteJSON(file, legacy); e != nil {
		t.Fatal(e)
	}
	rig.g.pull.Draft = true
	rig.at(8 * time.Hour) // still a draft: the entry is kept as it is
	rig.g.pull.Draft = false
	rig.at(8 * time.Hour) // the hold counts from t0+6m, as before
	rig.wantConditions()
	rig.at(8*time.Hour + 38*time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(8*time.Hour + 39*time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "| ready_too_long after 45m0s |") {
		t.Fatal(e.Text)
	}
}

// A pass records an open PR before the reads that can fail: a pass that errors
// before the gate still anchors a draft hold that starts after it.
func TestMergeOverdueOpenPassRecordedBeforeFailedPass(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // READY and the last open pass at overdueStart
	rig.s.GH = &headFailGH{fakeGH: rig.g}
	rig.now = overdueStart.Add(30 * time.Minute)
	if e := rig.s.NotifyPR(context.Background(), rig.r, 7, Options{}); e == nil {
		t.Fatal("the pass did not fail")
	}
	rig.s.GH = rig.g
	rig.g.pull.Draft = true
	rig.at(time.Hour) // the hold is anchored at the failed pass, not at t0
	rig.at(8 * time.Hour)
	rig.wantConditions() // a draft is held: no condition fires
	rig.g.pull.Draft = false
	rig.at(8 * time.Hour)
	rig.wantConditions(overdueNotLanded) // the 30m it was seen open still counts
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 30m0s |") {
		t.Fatal(e.Text)
	}
	rig.at(8*time.Hour + 14*time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(8*time.Hour + 15*time.Minute) // ready_too_long from the 30m anchor
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "| ready_too_long after 45m0s |") {
		t.Fatal(e.Text)
	}
}

// A hold ends at the pass that sees the PR open, even when that pass fails
// later: the open time those passes saw is not counted as hold.
func TestMergeOverdueHoldEndsOnFailedOpenPass(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0) // READY and the last open pass at overdueStart
	rig.g.pull.Draft = true
	rig.at(6 * time.Minute) // the hold is anchored at t0
	rig.g.pull.Draft = false
	rig.s.GH = &headFailGH{fakeGH: rig.g}
	for _, at := range []time.Duration{30 * time.Minute, 40 * time.Minute} {
		rig.now = overdueStart.Add(at)
		if e := rig.s.NotifyPR(context.Background(), rig.r, 7, Options{}); e == nil {
			t.Fatal("the pass did not fail")
		}
	}
	rig.s.GH = rig.g
	rig.g.pull.Draft = true
	rig.at(50 * time.Minute) // the second hold is anchored at the 40m pass
	rig.at(2 * time.Hour)
	rig.wantConditions()
	rig.g.pull.Draft = false
	rig.at(2 * time.Hour) // the 10m the failed passes saw open
	rig.wantConditions(overdueNotLanded)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 10m0s |") {
		t.Fatal(e.Text)
	}
	rig.at(2*time.Hour + 34*time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(2*time.Hour + 35*time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	if e := rig.n.overdueEvents()[1]; !strings.Contains(e.Text, "| ready_too_long after 45m0s |") {
		t.Fatal(e.Text)
	}
}

// A pass that sees the PR open ends an open draft hold before the reads that
// can fail: draft at t1, open at t2 with GH.Head failing, draft at t3, open at
// t4. The hold is closed into Holds at t2, so the second hold starts from that
// pass's sighting, not from the first hold's anchor, and the clocks themselves
// are never moved: every open minute before t4 is covered by a span.
func TestMergeOverdueFailedOpenPassEndsDraftHold(t *testing.T) {
	rig := newOverdueRig(t, true)
	read := func() overdueClock {
		t.Helper()
		st, e := readOverdueState(rig.s.overdueClocksFile(rig.r.Name))
		if e != nil {
			t.Fatal(e)
		}
		return st.Clocks[overdueKey(rig.g.pull)]
	}
	rig.at(0) // READY and the last open pass at overdueStart
	rig.g.pull.Draft = true
	rig.at(10 * time.Minute) // the hold is anchored at t0
	if c := read(); c.DraftSince.IsZero() {
		t.Fatalf("the draft pass recorded no hold: %+v", c)
	}
	rig.s.GH = &headFailGH{fakeGH: rig.g}
	rig.g.pull.Draft = false
	rig.now = overdueStart.Add(30 * time.Minute)
	if e := rig.s.NotifyPR(context.Background(), rig.r, 7, Options{}); e == nil {
		t.Fatal("the pass did not fail")
	}
	c := read()
	if !c.LastOpen.Equal(overdueStart.Add(30 * time.Minute)) {
		t.Fatalf("the failed pass did not record its open sighting: last_open=%v", c.LastOpen.Sub(overdueStart))
	}
	if !c.DraftSince.IsZero() {
		t.Fatalf("the hold is still open after the open sighting: draft_since=%v", c.DraftSince.Sub(overdueStart))
	}
	if len(c.Holds) != 1 || !c.Holds[0].From.Equal(overdueStart) || !c.Holds[0].To.Equal(overdueStart.Add(30*time.Minute)) {
		t.Fatalf("the hold was not closed into the sighting's span: %+v", c.Holds)
	}
	if !c.HeadSeen.Equal(overdueStart) {
		t.Fatalf("the hold moved a raw clock: head_seen=%v", c.HeadSeen.Sub(overdueStart))
	}
	rig.s.GH = rig.g
	rig.g.pull.Draft = true
	rig.at(50 * time.Minute)
	if c := read(); !c.DraftSince.Equal(overdueStart.Add(30 * time.Minute)) {
		t.Fatalf("the second hold is not anchored at the open sighting: draft_since=%v", c.DraftSince.Sub(overdueStart))
	}
	rig.g.pull.Draft = false
	rig.at(2 * time.Hour)
	rig.wantConditions()
	c = read()
	if !c.DraftSince.IsZero() || len(c.Holds) != 2 || !c.LastOpen.Equal(overdueStart.Add(2*time.Hour)) {
		t.Fatalf("leaving the second hold did not close it into its span: %+v", c)
	}
	// The spans cover every open minute before t4, so the ages are zero there
	// and the threshold counts from t4: the reports the old shift produced,
	// without the shift.
	now := overdueStart.Add(2 * time.Hour)
	if conditions, _ := overdueDue(c, true, now); len(conditions) != 0 {
		t.Fatalf("conditions at t4 = %v, want none", conditions)
	}
	rig.at(2*time.Hour + 10*time.Minute)
	rig.wantConditions(overdueNotLanded)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 10m0s |") {
		t.Fatal(e.Text)
	}
}

// The open sighting does no fallible read of its own: a corrupt READY file (the
// read that used to run before the write) cannot keep it from recording the
// sighting or closing the hold into a span, and it never fills the READY clock
// itself. A later tick fills that clock with its raw record time.
func TestMergeOverdueOpenSightingDoesNotReadReady(t *testing.T) {
	rig := newOverdueRig(t, true)
	file := rig.s.overdueClocksFile(rig.r.Name)
	readyFile := rig.s.readyClocksFile(rig.r.Name)
	key := overdueKey(rig.g.pull)
	read := func() overdueClock {
		t.Helper()
		st, e := readOverdueState(file)
		if e != nil {
			t.Fatal(e)
		}
		return st.Clocks[key]
	}
	rig.at(0) // READY and the last open pass at overdueStart
	rig.g.pull.Draft = true
	rig.at(6 * time.Minute) // the hold is anchored at t0
	good, e := os.ReadFile(readyFile)
	if e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(readyFile, []byte("{"), 0o644); e != nil {
		t.Fatal(e)
	}
	rig.g.pull.Draft = false
	rig.now = overdueStart.Add(30 * time.Minute)
	if e := rig.s.noteOpenPass(rig.r, rig.g.pull); e != nil {
		t.Fatalf("the open sighting read the READY file: %v", e)
	}
	c := read()
	if !c.LastOpen.Equal(rig.now) {
		t.Fatalf("the pass did not record its open sighting: last_open=%v", c.LastOpen.Sub(overdueStart))
	}
	if !c.DraftSince.IsZero() {
		t.Fatalf("the hold is still open after the pass: draft_since=%v", c.DraftSince.Sub(overdueStart))
	}
	if len(c.Holds) != 1 || !c.Holds[0].From.Equal(overdueStart) || !c.Holds[0].To.Equal(rig.now) {
		t.Fatalf("the hold was not closed into its span: %+v", c.Holds)
	}
	if !c.HeadSeen.Equal(overdueStart) {
		t.Fatalf("the hold moved a raw clock: head_seen=%v", c.HeadSeen.Sub(overdueStart))
	}
	if !c.Ready.IsZero() {
		t.Fatalf("the open sighting filled the READY clock: %v", c.Ready.Sub(overdueStart))
	}
	// The next pass fills the READY with the time it was recorded, not with the
	// hold's end: the age is computed against the span when it is read.
	if e := os.WriteFile(readyFile, good, 0o644); e != nil {
		t.Fatal(e)
	}
	rig.at(31 * time.Minute)
	if c = read(); !c.Ready.Equal(overdueStart) {
		t.Fatalf("the later READY fill was shifted by the hold: ready=%v", c.Ready.Sub(overdueStart))
	}
}

// r4579172925: the READY is recorded at t0, a draft holds from t1 to t2 and a
// later pass fills the READY clock. The age is (now-t0) less the hold, whether
// the READY read failed on the pass that closed the hold (case a) or succeeded
// there (case b): the two orders are the same facts, and they report the same
// ages. Nothing was shifted, so a late fill cannot miss a hold that closed.
func TestMergeOverdueReadyFillOrderDoesNotMatter(t *testing.T) {
	// run drives the same scenario, failing the READY read on the closing pass
	// when failAtClose is set, and returns the stored clock at t3 plus the
	// conditions 45 minutes of open time after t0.
	run := func(failAtClose bool) (overdueClock, []string) {
		t.Helper()
		rig := newOverdueRig(t, true)
		file := rig.s.overdueClocksFile(rig.r.Name)
		readyFile := rig.s.readyClocksFile(rig.r.Name)
		key := overdueKey(rig.g.pull)
		read := func() overdueClock {
			t.Helper()
			st, e := readOverdueState(file)
			if e != nil {
				t.Fatal(e)
			}
			return st.Clocks[key]
		}
		// The READY verdict is recorded after the clocks tick, so the first pass
		// leaves the READY clock at zero.
		rig.at(0)
		if c := read(); !c.Ready.IsZero() {
			t.Fatalf("the tick saw a READY the same pass had not recorded yet: %v", c.Ready.Sub(overdueStart))
		}
		// t1: an open pass whose READY read fails, so the sighting moves and the
		// clock stays zero (the read that would fill it did not answer).
		good, e := os.ReadFile(readyFile)
		if e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(readyFile, []byte("{"), 0o644); e != nil {
			t.Fatal(e)
		}
		rig.now = overdueStart.Add(10 * time.Minute)
		rig.eval()
		if c := read(); !c.LastOpen.Equal(rig.now) || !c.Ready.IsZero() {
			t.Fatalf("the failed pass did not leave last_open at t1 and ready at zero: %+v", c)
		}
		// t1..t2: a draft hold, anchored at that sighting.
		rig.g.pull.Draft = true
		rig.at(20 * time.Minute)
		if c := read(); !c.DraftSince.Equal(overdueStart.Add(10 * time.Minute)) {
			t.Fatalf("the hold is not anchored at the sighting: draft_since=%v", c.DraftSince.Sub(overdueStart))
		}
		// t2: the PR is open again and the hold closes. The READY read succeeds
		// here in case b.
		if !failAtClose {
			if e := os.WriteFile(readyFile, good, 0o644); e != nil {
				t.Fatal(e)
			}
		}
		rig.g.pull.Draft = false
		rig.at(80 * time.Minute)
		if e := os.WriteFile(readyFile, good, 0o644); e != nil {
			t.Fatal(e)
		}
		if failAtClose {
			if c := read(); !c.Ready.IsZero() {
				t.Fatalf("the failed read filled the READY clock: %v", c.Ready.Sub(overdueStart))
			}
		}
		// t3: a later pass fills the READY, raw.
		rig.at(81 * time.Minute)
		c := read()
		if !c.Ready.Equal(overdueStart) {
			t.Fatalf("the READY fill was shifted by the hold: ready=%v", c.Ready.Sub(overdueStart))
		}
		if len(c.Holds) != 1 || !c.Holds[0].From.Equal(overdueStart.Add(10*time.Minute)) || !c.Holds[0].To.Equal(overdueStart.Add(80*time.Minute)) {
			t.Fatalf("the hold was not kept as one span: %+v", c.Holds)
		}
		// 45 minutes of open time from t0 is due at t0+45m plus the 70m hold,
		// however long ago t0 is by then.
		rig.at(115 * time.Minute)
		return c, rig.n.conditions()
	}
	clockA, conditionsA := run(true)
	clockB, conditionsB := run(false)
	want := strings.Join([]string{overdueNotLanded, overdueReadyTooLong}, ",")
	for i, conditions := range [][]string{conditionsA, conditionsB} {
		if got := strings.Join(conditions, ","); got != want {
			t.Fatalf("order %d: conditions = [%s], want [%s]", i, got, want)
		}
	}
	for i, c := range []overdueClock{clockA, clockB} {
		if age := openAge(c, c.Ready, overdueStart.Add(115*time.Minute)); age != 45*time.Minute {
			t.Fatalf("order %d: ready age = %v, want 45m", i, age)
		}
	}
}

// headFailGH fails Head, the read NotifyPR makes after it sees the PR open.
type headFailGH struct {
	*fakeGH
}

func (g *headFailGH) Head(context.Context, string, string) (string, error) {
	return "", errors.New("head unavailable")
}

// The same 8 hours without a draft are unchanged: the clocks count the wall
// time from the READY and the review completion.
func TestMergeOverdueNoDraftCountsAsBefore(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.at(0)
	rig.at(8 * time.Hour)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
	for _, e := range rig.n.overdueEvents() {
		if !strings.Contains(e.Text, "after 8h0m0s |") {
			t.Fatal(e.Text)
		}
	}
}

// A head first seen while the PR is a draft starts its clocks at the pass that
// sees the PR ready: the report's "a head first seen after it became ready".
func TestMergeOverdueHeadFirstSeenInDraft(t *testing.T) {
	rig := newOverdueRig(t, true)
	rig.g.pull.Draft = true
	rig.at(0) // seen only as a draft
	rig.at(8 * time.Hour)
	rig.wantConditions()
	rig.g.pull.Draft = false
	rig.at(8 * time.Hour) // the ready pass starts the clocks
	rig.wantConditions()
	rig.at(8*time.Hour + 9*time.Minute)
	rig.wantConditions()
	rig.at(8*time.Hour + 10*time.Minute)
	rig.wantConditions(overdueNotLanded)
	rig.at(8*time.Hour + 45*time.Minute)
	rig.wantConditions(overdueNotLanded, overdueReadyTooLong)
}

// The age of a clock is the time since it started, less every draft hold it
// overlaps, computed at read time: the stored times are raw and no pass moves
// them.
func TestOverdueOpenAge(t *testing.T) {
	t0 := overdueStart
	t1 := t0.Add(10 * time.Minute)
	t2 := t0.Add(time.Hour)
	t3 := t0.Add(3 * time.Hour)
	for _, tc := range []struct {
		name  string
		clock overdueClock
		start time.Time
		now   time.Time
		want  time.Duration
	}{
		{name: "no hold", clock: overdueClock{}, start: t0, now: t3, want: 3 * time.Hour},
		{name: "a closed hold is taken off", clock: overdueClock{Holds: []holdSpan{{From: t1, To: t2}}}, start: t0, now: t3, want: 3*time.Hour - 50*time.Minute},
		{name: "a hold after now is not", clock: overdueClock{Holds: []holdSpan{{From: t3, To: t3.Add(time.Hour)}}}, start: t0, now: t3, want: 3 * time.Hour},
		{name: "a clock inside a closed hold counts from its end", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t2}}}, start: t1, now: t3, want: 2 * time.Hour},
		{name: "a hold before the clock takes nothing", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t2}}}, start: t2.Add(time.Minute), now: t3, want: time.Hour + 59*time.Minute},
		{name: "the hold in progress counts to now", clock: overdueClock{DraftSince: t1}, start: t0, now: t3, want: 10 * time.Minute},
		{name: "a clock inside the hold in progress counts nothing", clock: overdueClock{DraftSince: t0}, start: t1, now: t3, want: 0},
		{name: "every hold is taken off", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t1}, {From: t2, To: t2.Add(10 * time.Minute)}}}, start: t0, now: t3, want: 3*time.Hour - 20*time.Minute},
		{name: "overlapping holds are taken off once", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t1}, {From: t0, To: t2}}}, start: t0, now: t3, want: 2 * time.Hour},
		{name: "a hold inside another is taken off once", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t3}, {From: t1, To: t2}}}, start: t0, now: t3, want: 0},
		{name: "touching holds are taken off once", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t1}, {From: t1, To: t2}}}, start: t0, now: t3, want: 2 * time.Hour},
		{name: "holds in any order are taken off", clock: overdueClock{Holds: []holdSpan{{From: t2, To: t2.Add(10 * time.Minute)}, {From: t0, To: t1}}}, start: t0, now: t3, want: 3*time.Hour - 20*time.Minute},
		{name: "the hold in progress overlaps a closed one", clock: overdueClock{Holds: []holdSpan{{From: t1, To: t2}}, DraftSince: t1}, start: t0, now: t3, want: 10 * time.Minute},
		{name: "a clock inside overlapping holds counts from their union's end", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t2}, {From: t1, To: t3}}}, start: t1, now: t3, want: 0},
		{name: "closed and in progress", clock: overdueClock{Holds: []holdSpan{{From: t0, To: t1}}, DraftSince: t2}, start: t0, now: t3, want: 50 * time.Minute},
		{name: "no start", clock: overdueClock{DraftSince: t1}, start: time.Time{}, now: t3, want: 0},
		{name: "a start after now", clock: overdueClock{}, start: t3.Add(time.Hour), now: t3, want: 0},
	} {
		if got := openAge(tc.clock, tc.start, tc.now); got != tc.want {
			t.Errorf("%s: age = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The ages are a function of the raw event times, the hold spans and now, not
// of the order the facts were recorded in: every permutation of (fill Ready,
// fill ReviewDone, open hold, close hold, a pass whose READY read failed and
// recorded nothing) gives the same conditions at the same age. This is the
// class the four patches before this one kept moving: a hold consumed at one
// instant left every later-filled clock uncovered.
func TestOverdueDueOrderIndependent(t *testing.T) {
	t0 := overdueStart
	t1 := t0.Add(10 * time.Minute)
	t2 := t0.Add(time.Hour)
	now := t0.Add(3 * time.Hour)
	facts := []func(c *overdueClock){
		func(c *overdueClock) { c.Ready = t0 },
		func(c *overdueClock) { c.ReviewDone = t0 },
		func(c *overdueClock) { c.Holds = append(c.Holds, holdSpan{From: t1, To: t2}) },
		func(*overdueClock) {}, // the failed pass: nothing was observed, nothing recorded
	}
	wantAge := 3*time.Hour - 50*time.Minute
	order := make([]int, len(facts))
	var walk func(int, []bool)
	permutations := 0
	walk = func(i int, used []bool) {
		if i == len(order) {
			permutations++
			var c overdueClock
			for _, fact := range order {
				facts[fact](&c)
			}
			conditions, ages := overdueDue(c, true, now)
			if strings.Join(conditions, ",") != strings.Join([]string{overdueNotLanded, overdueReadyTooLong}, ",") {
				t.Fatalf("%v: conditions = %v", order, conditions)
			}
			for _, name := range []string{overdueNotLanded, overdueReadyTooLong} {
				if ages[name] != wantAge {
					t.Fatalf("%v: %s age = %v, want %v", order, name, ages[name], wantAge)
				}
			}
			return
		}
		for j := range facts {
			if used[j] {
				continue
			}
			used[j], order[i] = true, j
			walk(i+1, used)
			used[j] = false
		}
	}
	walk(0, make([]bool, len(facts)))
	if permutations != 24 {
		t.Fatalf("permutations walked = %d, want 24", permutations)
	}
}

// Holds are bounded: past 32 spans the two oldest are merged, not dropped, so
// the oldest hold still starts where it did.
func TestOverdueCloseHoldCapsSpans(t *testing.T) {
	var c overdueClock
	count := overdueHoldSpans + 8
	for i := range count {
		from := overdueStart.Add(time.Duration(i) * 2 * time.Hour)
		c.DraftSince = from
		if !closeHold(&c, from.Add(time.Hour)) {
			t.Fatalf("hold %d was not closed", i)
		}
	}
	if len(c.Holds) != overdueHoldSpans {
		t.Fatalf("holds = %d, want %d", len(c.Holds), overdueHoldSpans)
	}
	if !c.Holds[0].From.Equal(overdueStart) {
		t.Fatalf("the oldest hold was dropped rather than merged: %+v", c.Holds[0])
	}
	last := overdueStart.Add(time.Duration(count-1) * 2 * time.Hour)
	if want := last.Add(time.Hour); !c.Holds[len(c.Holds)-1].To.Equal(want) {
		t.Fatalf("the newest hold = %+v, want To %v", c.Holds[len(c.Holds)-1], want)
	}
}

// A pass can close a draft hold without recording its open sighting: a dry-run
// pass reports nothing, and an open sighting whose write fails leaves LastOpen
// where it was. The close itself must move LastOpen all the same: a draft that
// starts after it anchors there, not before the hold that just ended, so the
// spans never overlap and the age is the true open time.
func TestMergeOverdueCloseWithoutSightingThenDraft(t *testing.T) {
	rig := newOverdueRig(t, true)
	file := rig.s.overdueClocksFile(rig.r.Name)
	key := overdueKey(rig.g.pull)
	read := func() overdueClock {
		t.Helper()
		st, e := readOverdueState(file)
		if e != nil {
			t.Fatal(e)
		}
		return st.Clocks[key]
	}
	rig.at(0) // READY and the last open pass at overdueStart
	rig.g.pull.Draft = true
	rig.at(6 * time.Minute) // the hold is anchored at t0
	// t1: the PR is open again and the hold is closed by tickOverdueClocks
	// alone, the way a pass whose open sighting did not land closes it.
	rig.g.pull.Draft = false
	rig.now = overdueStart.Add(30 * time.Minute)
	if _, e := rig.s.tickOverdueClocks(context.Background(), rig.r, rig.g.pull, gateVerdict{}); e != nil {
		t.Fatal(e)
	}
	if c := read(); !c.DraftSince.IsZero() || len(c.Holds) != 1 || !c.Holds[0].To.Equal(overdueStart.Add(30*time.Minute)) {
		t.Fatalf("the pass did not close the hold into its span: %+v", c)
	}
	if c := read(); !c.LastOpen.Equal(overdueStart.Add(30 * time.Minute)) {
		t.Fatalf("the close left the last open before the hold it ended: last_open=%v", c.LastOpen.Sub(overdueStart))
	}
	// t2: the PR is a draft again. The hold starts where the closed one ended,
	// so every minute of it is held once and nothing before it is.
	rig.g.pull.Draft = true
	rig.at(40 * time.Minute)
	if c := read(); !c.DraftSince.Equal(overdueStart.Add(30 * time.Minute)) {
		t.Fatalf("the second hold is anchored before the closed one ended: draft_since=%v", c.DraftSince.Sub(overdueStart))
	}
	// t3: open again. Every minute before t3 is held, so the open age is zero
	// there and the thresholds count from t3.
	rig.g.pull.Draft = false
	rig.at(2 * time.Hour)
	rig.wantConditions()
	rig.at(2*time.Hour + 10*time.Minute)
	rig.wantConditions(overdueNotLanded)
	if e := rig.n.overdueEvents()[0]; !strings.Contains(e.Text, "| not_landed after 10m0s |") {
		t.Fatal(e.Text)
	}
}
