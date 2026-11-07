package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// (a) A BLOCKED whose only reason is review_missing is mergewake's own queue
// state: it sends nothing, however many passes see it.
func TestNotifyActionableReviewMissingOnlyIsSilent(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.refs = map[string]bool{}
	for i := 0; i < 6; i++ {
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		*now = now.Add(9 * time.Minute)
	}
	if n.count != 0 {
		t.Fatalf("review_missing alone sent: %+v", n.events)
	}
}

// (b) The same head waiting more than 60 minutes gets exactly one stall alert
// on notifyLane's lane, none after it, also after a restart; a new head that
// is reviewed and blocks again follows the normal rules.
func TestNotifyActionableStallAlertsOnceAcrossRestart(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.refs = map[string]bool{}
	poll := func(svc *Service) {
		t.Helper()
		if e := svc.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	poll(s)
	*now = now.Add(60 * time.Minute)
	poll(s)
	if n.count != 0 {
		t.Fatalf("alert at exactly 60 minutes: %+v", n.events)
	}
	*now = now.Add(time.Minute)
	poll(s)
	if n.count != 1 {
		t.Fatalf("want one stall alert: %+v", n.events)
	}
	ev := n.events[0]
	if ev.Lane != "operator-desk" || ev.Type != "blocked" {
		t.Fatalf("alert lane/type: %+v", ev)
	}
	for _, want := range []string{"example/project#7", "head=aaaaaaa", "review stalled", "1h1m0s", "passes posted ", "no reviewer job known", "via task 2481"} {
		if !strings.Contains(ev.Text, want) {
			t.Fatalf("alert text lacks %q: %s", want, ev.Text)
		}
	}
	restarted := *s
	for i := 0; i < 3; i++ {
		*now = now.Add(2 * time.Hour)
		poll(&restarted)
		poll(s)
	}
	if n.count != 1 {
		t.Fatalf("stall alert repeated: %+v", n.events)
	}
	g.pull.Head.SHA = shaB
	poll(s)
	if n.count != 1 {
		t.Fatalf("new head alerted at once: %+v", n.events)
	}
	markReviewed(g)
	g.required = []string{"vet"}
	g.checkNames = map[string]map[string]github.CI{shaB: {"vet": github.Red}}
	poll(s)
	if n.count != 2 || n.events[1].Lane != "w-7" || !strings.Contains(n.events[1].Text, "check_failed:vet") {
		t.Fatalf("%+v", n.events)
	}
}

func TestNotifyActionableStallNamesReviewerJob(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.refs = map[string]bool{}
	rj := reviewerJob{PR: 7, Head: shaA, Pass: 1, Label: "review-7-p1"}
	if e := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, 1), rj); e != nil {
		t.Fatal(e)
	}
	for _, d := range []time.Duration{0, 61 * time.Minute} {
		*now = now.Add(d)
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "reviewer job review-7-p1 (pass 1)") {
		t.Fatalf("%+v", n.events)
	}
}

// (c) An author reason with a live job goes to the job lane only.
func TestNotifyActionableBlockingFindingsGoToJobLaneOnly(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	lane, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "blocked", "blocking_findings:2")
	if e != nil || !sent || lane != "w-7" {
		t.Fatalf("lane %q sent %t: %v", lane, sent, e)
	}
	if n.count != 1 || n.events[0].Lane != "w-7" || n.events[0].Type != "blocked" {
		t.Fatalf("%+v", n.events)
	}
}

// (d) With no live job an author reason keeps today's fallback: the task lane
// unless it is the desk, then the desk as a decision (h).
func TestNotifyActionableNoLiveJobKeepsFallbackLane(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	delete(sink.jobs, "2481")
	sink.lanes["2481"] = "author-lane"
	g.required = []string{"vet"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Red}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "author-lane" || n.events[0].Type != "blocked" || !strings.Contains(n.events[0].Text, "via task 2481") {
		t.Fatalf("%+v", n.events)
	}
}

// (e) review_missing next to an author reason: one event naming the author
// reason only, on the job lane.
func TestNotifyActionableMixedSetNamesActionableReasonsOnly(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	g.refs = map[string]bool{}
	g.required = []string{"vet"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Red}}
	for i := 0; i < 2; i++ {
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 1 || n.events[0].Lane != "w-7" || !strings.Contains(n.events[0].Text, "| check_failed:vet |") || strings.Contains(n.events[0].Text, "review_missing") {
		t.Fatalf("%+v", n.events)
	}
}

// A human-decision reason keeps its routing and type next to an internal wait.
func TestNotifyActionableMixedHumanReasonKeepsDeskRouting(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	g.refs = map[string]bool{}
	g.findings = 2
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "operator-desk" || n.events[0].Type != "blocked" || !strings.Contains(n.events[0].Text, "| threads_open:2 |") || strings.Contains(n.events[0].Text, "review_missing") {
		t.Fatalf("%+v", n.events)
	}
}

// (f) A review_missing with a block detail waits on the author: it takes the
// author route (findingsRoute), which here has no job, so the desk decides.
func TestNotifyActionableReviewMissingWithBlockIsAuthorRoute(t *testing.T) {
	f := familyBlockSetup(t)
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	on := true
	f.s.Config.Daemon.PR.Notify.Enabled = &on
	before := f.n.count
	if e := f.s.NotifyPR(context.Background(), f.r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	route, e := f.s.findingsRoute(context.Background(), f.r, f.g.pull)
	if e != nil {
		t.Fatal(e)
	}
	if f.n.count != before+1 {
		t.Fatalf("want one event: %+v", f.n.events)
	}
	ev := f.n.events[before]
	if ev.Lane != route.lane || ev.Type != "decision" || !strings.Contains(ev.Text, "review_missing (blocked: author-family-unknown)") || !strings.Contains(ev.Text, "via "+route.via) {
		t.Fatalf("event %+v route %+v", ev, route)
	}
}

// (g) READY and MERGED are recorded, not sent, even with an author worker lane.
func TestNotifyActionableReadyAndMergedAreRecordedOnly(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	markReviewed(g)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 || !sentKeyHas(t, s, r, verdictKey("ready", 7, g.pull.Head.SHA)) || !sentKeyHas(t, s, r, verdictKey("merged", 7, g.pull.Head.SHA)) {
		t.Fatalf("%+v", n.events)
	}
}

// (h) An author reason with no job and a desk task lane is an unknown author:
// the desk decides.
func TestNotifyActionableUnknownAuthorStaysDeskDecision(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	delete(sink.jobs, "2481")
	g.required = []string{"vet"}
	g.checkNames = map[string]map[string]github.CI{shaA: {"vet": github.Red}}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "operator-desk" || n.events[0].Type != "decision" || !strings.Contains(n.events[0].Text, "unknown author; desk") {
		t.Fatalf("%+v", n.events)
	}
}

// One head that waits 90 minutes for its review yields exactly one stall
// event in total, polled every 10 minutes and across a restart.
func TestNotifyActionableNinetyMinuteWaitIsOneStallEvent(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.refs = map[string]bool{}
	for i := 0; i <= 9; i++ {
		svc := s
		if i >= 5 {
			restarted := *s
			svc = &restarted
		}
		if e := svc.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		*now = now.Add(10 * time.Minute)
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "review stalled") || strings.Contains(n.events[0].Text, "STALLED") {
		t.Fatalf("want exactly one review-stalled event: %+v", n.events)
	}
}
