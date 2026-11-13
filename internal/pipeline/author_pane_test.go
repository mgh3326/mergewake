package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

const projectOrigin = "git@github.com:example/project.git"

// aPane is one agent pane in a linked worktree on branch of origin.
type aPane struct{ id, name, cwd, branch, origin string }

// authorHost scripts the host's agent list, pane list and per-cwd worktree
// inspection for panes.
func authorHost(panes ...aPane) *scriptRunner {
	var agents, all []string
	outs := map[string]string{}
	for _, p := range panes {
		agents = append(agents, fmt.Sprintf(`{"pane_id":%q,"name":%q,"agent_status":"working","agent":"claude","cwd":%q}`, p.id, p.name, p.cwd))
		all = append(all, fmt.Sprintf(`{"pane_id":%q,"cwd":%q}`, p.id, p.cwd))
		outs[key("git", "-C", p.cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")] = "/repo/.git/worktrees/" + p.id + "\n/repo/.git\n" + p.cwd + "\n"
		outs[key("git", "-C", p.cwd, "branch", "--show-current")] = p.branch + "\n"
		outs[key("git", "-C", p.cwd, "rev-parse", "HEAD")] = "aaaa\n"
		outs[key("git", "-C", p.cwd, "remote", "get-url", "origin")] = p.origin + "\n"
	}
	outs[key("herdr", "agent", "list")] = `{"result":{"agents":[` + strings.Join(agents, ",") + `]}}`
	outs[key("herdr", "pane", "list")] = `{"result":{"panes":[` + strings.Join(all, ",") + `]}}`
	return &scriptRunner{outs: outs}
}

// authorPaneSetup is an open PR on branch "feature" whose task 2481 has no
// bound job and whose body names a Notify-Lane trailer, on one configured
// host holding one worker pane on that branch. The worker's job record names
// lane w-9-lane.
func authorPaneSetup(t *testing.T, panes ...aPane) (*Service, *jobSink, *scriptRunner) {
	t.Helper()
	s, g, _, _, _, _ := deliverSetup(t)
	sink := s.Sink.(*jobSink)
	delete(sink.jobs, "2481")
	sink.lanes["2481"] = "task-lane"
	sink.recs["w-9"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-9", HubLane: "w-9-lane"}
	if len(panes) == 0 {
		panes = []aPane{{"w1:p1", "w-9", "/wt/a", "feature", projectOrigin}}
	}
	run := authorHost(panes...)
	s.Runner = run
	s.Config.Hosts = []reap.Host{{Name: "local"}}
	g.pull.Body = "Summary\n\nMergewake-Task: 2481\nMergewake-Notify-Lane: trailer-lane\n"
	return s, sink, run
}

// A task with a bound job and no other live author on the PR's branch keeps
// routing to the job's lane. The branch is still scanned for a takeover.
func TestAuthorPaneJobBindingWithoutAuthorKeepsJob(t *testing.T) {
	s, _, run := takeoverSetup(t, aPane{"w1:p9", "w-5", "/wt/z", "other-branch", projectOrigin})
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7" || rt.via != "task 2481 job w-7" || len(run.calls) == 0 {
		t.Fatalf("route %q via %q, runner calls %v", rt.lane, rt.via, run.calls)
	}
}

// Takeover (hk 3492): a bound job's lane is the author's lane only while no
// other live pane on the PR's head branch has taken the PR over. The bound
// job w-7 has no lane of its own: its label names lane w-7. Panes named w-9
// and w-8 each carry their own job lane.
var (
	paneA = aPane{"w1:p2", "w-7", "/wt/a2", "feature", projectOrigin}
	paneB = aPane{"w1:p1", "w-9", "/wt/a", "feature", projectOrigin}
	paneC = aPane{"w1:p3", "w-8", "/wt/c", "feature", projectOrigin}
)

func takeoverSetup(t *testing.T, panes ...aPane) (*Service, *jobSink, *scriptRunner) {
	t.Helper()
	s, sink, run := authorPaneSetup(t, panes...)
	sink.jobs["2481"] = "w-7"
	sink.recs["w-7"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-7", OwnerLane: "integrator"}
	sink.recs["w-8"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-8", HubLane: "w-8-lane"}
	return s, sink, run
}

// (takeover a) The bound job's pane is gone and one other live pane is on the
// PR's branch: that pane is the current author.
func TestTakeoverRoutesToTheLiveAuthor(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneB)
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-9-lane" || rt.via != "takeover pane w-9" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (takeover b) The bound job's pane is still live beside the other pane: the
// bound job is the old author, so the other pane wins.
func TestTakeoverWithTheBoundPaneLiveRoutesToTheOther(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneA, paneB)
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-9-lane" || rt.via != "takeover pane w-9" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (takeover c) Only the bound job's own pane, no pane, or no hosts: the bound
// lane, as before. A terminal bound job keeps its lane the same way.
func TestTakeoverKeepsTheBoundLaneWithoutAnotherAuthor(t *testing.T) {
	other := aPane{"w1:p9", "w-5", "/wt/z", "other-branch", projectOrigin}
	for _, tt := range []struct {
		name  string
		panes []aPane
		hosts bool
	}{
		{"only the bound pane", []aPane{paneA}, true},
		{"no pane on the branch", []aPane{other}, true},
		{"no host configured", []aPane{paneB}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _, _ := takeoverSetup(t, tt.panes...)
			if !tt.hosts {
				s.Config.Hosts = nil
			}
			rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
			if e != nil {
				t.Fatal(e)
			}
			if rt.lane != "w-7" || rt.via != "task 2481 job w-7" {
				t.Fatalf("route %q via %q", rt.lane, rt.via)
			}
		})
	}
}

// (takeover e) A mergewake reviewer pane (labelled mwr-…) on the branch is not
// an author: the bound lane stands.
func TestTakeoverIgnoresReviewerPanes(t *testing.T) {
	reviewer := aPane{"w1:p4", "mwr-7-abc1234-p1-100", "/wt/r", "feature", projectOrigin}
	s, sink, _ := takeoverSetup(t, paneA, reviewer)
	// The reviewer's job has a usable lane: without the exclusion it would take over.
	sink.recs[reviewer.name] = integrations.JobRecord{State: "spawned", Role: "reviewer", Label: reviewer.name, HubLane: "reviewer-lane"}
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-7" || rt.via != "task 2481 job w-7" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (takeover d) Two other live panes on the branch are ambiguous: the bound
// lane stands and one findings-route-ambiguous row names the candidates.
func TestTakeoverAmbiguousKeepsTheBoundLaneAndAudits(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneB, paneC)
	var out bytes.Buffer
	s.Out = &out
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	if e := s.DeliverFindings(context.Background(), testRepoFor(s), 7, Options{}); e != nil {
		t.Fatal(e)
	}
	n := s.Notifier.(*ackNotifier)
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("%+v", n.events)
	}
	rows := auditRecords(t, out.Bytes(), "findings-route-ambiguous")
	if len(rows) != 1 || rows[0]["pr"] != float64(7) {
		t.Fatalf("want one findings-route-ambiguous row for PR 7, got %+v", rows)
	}
	cands := fmt.Sprint(rows[0]["candidates"])
	if !strings.Contains(cands, "w-9") || !strings.Contains(cands, "w-8") {
		t.Fatalf("candidates %s", cands)
	}
}

// (takeover f) A delivery stored for the bound job, then another live pane
// takes over, then a re-send: the re-send goes to the new lane, the stored
// lane is updated, and one findings-route-changed row records the move.
func TestTakeoverResendFollowsTheNewAuthor(t *testing.T) {
	s, _, _ := takeoverSetup(t, aPane{"w1:p9", "w-5", "/wt/z", "other-branch", projectOrigin})
	var out bytes.Buffer
	s.Out = &out
	n := s.Notifier.(*ackNotifier)
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, testRepoFor(s))
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("first delivery: %+v", n.events)
	}
	s.Runner = authorHost(paneB)
	base := s.now()
	s.Now = func() time.Time { return base.Add(s.Config.Daemon.PR.Findings.AckWindow() + time.Minute) }
	deliver(t, s, testRepoFor(s))
	if n.count != 2 || n.events[1].Lane != "w-9-lane" || !strings.HasSuffix(n.events[1].ID, "-resend") {
		t.Fatalf("re-send did not follow the takeover: %+v", n.events)
	}
	ledger, e := s.loadDeliveries(testRepoFor(s).Name)
	if e != nil {
		t.Fatal(e)
	}
	if len(ledger) != 1 || ledger[0].Lane != "w-9-lane" || !strings.Contains(ledger[0].Text, "for lane w-9-lane") {
		t.Fatalf("stored delivery %+v", ledger)
	}
	rows := auditRecords(t, out.Bytes(), "findings-route-changed")
	if len(rows) != 1 || rows[0]["from"] != "w-7" || rows[0]["to"] != "w-9-lane" || rows[0]["via"] != "takeover pane w-9" {
		t.Fatalf("route-changed rows %+v", rows)
	}
}

// A bound author lane is durable when one configured host cannot be scanned.
// FINDINGS must use the healthy host result and record the partial routing choice.
func TestPartialAuthorScanDeliversBoundLane(t *testing.T) {
	s, _, run := takeoverSetup(t, paneA)
	setPartialAuthorScan(s, run)
	var out bytes.Buffer
	s.Out = &out
	n := s.Notifier.(*ackNotifier)
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	if e := s.DeliverFindings(context.Background(), testRepoFor(s), 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("want one event on bound lane w-7, got %+v", n.events)
	}
	rows := auditRecords(t, out.Bytes(), "findings-route-partial")
	if len(rows) != 1 || rows[0]["lane"] != "w-7" || !strings.Contains(fmt.Sprint(rows[0]["failed_hosts"]), "unavailable") {
		t.Fatalf("partial-route audit rows %+v", rows)
	}
}

func setPartialAuthorScan(s *Service, run *scriptRunner) {
	s.Config.Hosts = []reap.Host{{Name: "healthy"}, {Name: "unavailable", SSH: "unavailable"}}
	run.errs = map[string]error{key("ssh", "-o", "BatchMode=yes", "-T", "-n", "unavailable", reap.Join([]string{"herdr", "agent", "list"})): errors.New("pane list failed")}
}

func TestPartialAuthorScanKeepsBoundLaneAndAuditsCandidate(t *testing.T) {
	s, _, run := takeoverSetup(t, paneB)
	setPartialAuthorScan(s, run)
	var out bytes.Buffer
	s.Out = &out
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	if e := s.DeliverFindings(context.Background(), testRepoFor(s), 7, Options{}); e != nil {
		t.Fatal(e)
	}
	n := s.Notifier.(*ackNotifier)
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("partial scan did not keep bound lane: %+v calls=%v", n.events, run.calls)
	}
	rows := auditRecords(t, out.Bytes(), "findings-route-partial")
	if len(rows) != 1 || rows[0]["lane"] != "w-7" || !strings.Contains(fmt.Sprint(rows[0]["candidates"]), "w-9") {
		t.Fatalf("partial takeover audit rows %+v", rows)
	}
}

func TestPartialAuthorScanReadyRecordKeepsBoundLane(t *testing.T) {
	s, _, _, r := workerNotifySetup(t)
	run := authorHost(paneB)
	s.Runner = run
	setPartialAuthorScan(s, run)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["lane"] != "worker-lane" || !strings.Contains(fmt.Sprint(rows[0]["via"]), "partial pane scan") {
		t.Fatalf("partial READY route: %+v", rows)
	}
}

func TestPartialAuthorScanStoredDeliveryDoesNotResendToCandidate(t *testing.T) {
	s, _, _ := takeoverSetup(t, aPane{"w1:p9", "w-5", "/wt/z", "other-branch", projectOrigin})
	var out bytes.Buffer
	s.Out = &out
	n := s.Notifier.(*ackNotifier)
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, testRepoFor(s))
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("initial delivery: %+v", n.events)
	}
	s.Runner = authorHost(paneB)
	setPartialAuthorScan(s, s.Runner.(*scriptRunner))
	base := s.now()
	s.Now = func() time.Time { return base.Add(s.Config.Daemon.PR.Findings.AckWindow() + time.Minute) }
	deliver(t, s, testRepoFor(s))
	if n.count != 2 || n.events[1].Lane != "w-7" || !strings.HasSuffix(n.events[1].ID, "-resend") {
		t.Fatalf("partial re-send moved to candidate: %+v", n.events)
	}
}

func TestFullAuthorScanStillTakesOverUniquePane(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneB)
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-9-lane" || rt.via != "takeover pane w-9" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

func TestPartialAuthorScanWithoutBoundJobRetries(t *testing.T) {
	s, sink, run := takeoverSetup(t, paneB)
	delete(sink.jobs, "2481")
	setPartialAuthorScan(s, run)
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	if e := s.DeliverFindings(context.Background(), testRepoFor(s), 7, Options{}); e == nil {
		t.Fatal("partial scan without a bound job did not return an error")
	}
	if n := s.Notifier.(*ackNotifier); n.count != 0 {
		t.Fatalf("event sent without a bound author lane: %+v", n.events)
	}
}

func TestPartialAuthorScanWorkerVerdictRouteAndReadyAudit(t *testing.T) {
	s, _, _, r := workerNotifySetup(t)
	run := authorHost()
	s.Runner = run
	setPartialAuthorScan(s, run)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["lane"] != "worker-lane" || !strings.Contains(fmt.Sprint(rows[0]["via"]), "partial pane scan") || !strings.Contains(fmt.Sprint(rows[0]["failed_hosts"]), "unavailable") {
		t.Fatalf("partial READY route missing from audit: %+v", rows)
	}

	// A failed check is the author verdict here: a conflict is told by its
	// ESC, which never scans for author panes (hk 3712).
	s3, g3, n3, r3 := workerNotifySetup(t)
	run3 := authorHost()
	s3.Runner = run3
	setPartialAuthorScan(s3, run3)
	g3.ci = github.Red
	if e := s3.NotifyPR(context.Background(), r3, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n3.count != 1 || n3.events[0].Lane != "worker-lane" {
		t.Fatalf("partial author verdict did not reach bound lane: %+v", n3.events)
	}
	rows = prNotifyRows(t, s3, r3)
	if len(rows) != 1 || !strings.Contains(fmt.Sprint(rows[0]["via"]), "partial pane scan") || !strings.Contains(fmt.Sprint(rows[0]["failed_hosts"]), "unavailable") {
		t.Fatalf("partial verdict route missing from audit: %+v", rows)
	}

	s2, _, _, r2 := workerNotifySetup(t)
	run2 := authorHost()
	s2.Runner = run2
	setPartialAuthorScan(s2, run2)
	delete(s2.Sink.(*jobSink).jobs, "42")
	s2.GH.(*fakeGH).ci = github.Red
	if e := s2.NotifyPR(context.Background(), r2, 7, Options{}); e == nil {
		t.Fatal("author verdict without a bound job did not retry on partial scan")
	}
	if n := s2.Notifier.(*fakeNotifier); n.count != 0 {
		t.Fatalf("verdict delivered without a bound author lane: %+v", n.events)
	}
}

func TestAuthorPaneHubErrorStillRetriesBoundLane(t *testing.T) {
	s, _, run := takeoverSetup(t)
	s.Runner = run
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: "://invalid"}
	if rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull); e == nil {
		t.Fatalf("hub error selected %q via %q", rt.lane, rt.via)
	}
	if len(run.calls) != 0 {
		t.Fatalf("host scan began despite hub setup error: %v", run.calls)
	}
}

// (takeover g) A host read error during the takeover scan is returned: the
// pass retries and sends nothing, rather than picking a lane.
func TestTakeoverScanErrorRetriesWithoutSending(t *testing.T) {
	s, _, run := takeoverSetup(t, paneB)
	run.errs = map[string]error{key("herdr", "agent", "list"): errors.New("herdr unreachable")}
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	if e := s.DeliverFindings(context.Background(), testRepoFor(s), 7, Options{}); e == nil {
		t.Fatal("scan error was not returned")
	}
	if n := s.Notifier.(*ackNotifier); n.count != 0 {
		t.Fatalf("event sent during a failed scan: %+v", n.events)
	}
	if rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull); e == nil {
		t.Fatalf("route picked %q via %q", rt.lane, rt.via)
	}
}

// (takeover h) The READY/MERGED record names the live author, the same lane
// FINDINGS goes to.
func TestTakeoverReadyRecordNamesTheLiveAuthor(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneB)
	lane, via, e := s.workerLane(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if lane != "w-9-lane" || via != "takeover pane w-9" {
		t.Fatalf("record lane %q via %q", lane, via)
	}
}

// (b) No job binding, one pane on the PR's branch in this repository: its
// job's lane, and the event says which pane chose it.
func TestAuthorPaneFindingsRoute(t *testing.T) {
	s, _, _ := authorPaneSetup(t)
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "w-9-lane" || rt.via != "author pane w1:p1 job w-9" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (c) READY reaches the same worker, and shares FINDINGS' host scan when the
// evaluation shares a context.
func TestAuthorPaneReadyRouteSharesScan(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	ctx := WithAuthorScan(context.Background())
	pr := s.GH.(*fakeGH).pull
	rt, e := s.findingsRoute(ctx, testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	lane, via, e := s.notifyLane(ctx, testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	if lane != "w-9-lane" || via != "author pane w1:p1 job w-9" || rt.lane != lane {
		t.Fatalf("ready %q via %q, findings %q", lane, via, rt.lane)
	}
	lists := 0
	for _, c := range run.calls {
		if strings.Contains(c, "herdr\x00agent\x00list") {
			lists++
		}
	}
	if lists != 1 {
		t.Fatalf("agent list read %d times for one evaluation: %q", lists, run.calls)
	}
}

// (d) Two panes on the PR's branch are ambiguous: the Notify-Lane trailer.
func TestAuthorPaneAmbiguousFallsThrough(t *testing.T) {
	s, _, _ := authorPaneSetup(t,
		aPane{"w1:p1", "w-9", "/wt/a", "feature", projectOrigin},
		aPane{"w1:p2", "w-8", "/wt/b", "feature", projectOrigin})
	pr := s.GH.(*fakeGH).pull
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	lane, via, e := s.notifyLane(context.Background(), testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "trailer-lane" || rt.via != "notify-lane trailer" || lane != "task-lane" || via != "task 2481" {
		t.Fatalf("findings %q via %q; ready %q via %q", rt.lane, rt.via, lane, via)
	}
}

// (e) No pane on the branch: today's order — trailer, then task lane, then desk.
func TestAuthorPaneNoMatchKeepsOrder(t *testing.T) {
	s, sink, _ := authorPaneSetup(t, aPane{"w1:p1", "w-9", "/wt/a", "other-branch", projectOrigin})
	pr := s.GH.(*fakeGH).pull
	r := testRepoFor(s)
	rt, e := s.findingsRoute(context.Background(), r, pr)
	if e != nil || rt.lane != "trailer-lane" || rt.via != "notify-lane trailer" {
		t.Fatalf("trailer: %+v, %v", rt, e)
	}
	s.GH.(*fakeGH).pull.Body = "Mergewake-Task: 2481\n"
	pr = s.GH.(*fakeGH).pull
	rt, e = s.findingsRoute(context.Background(), r, pr)
	if e != nil || rt.lane != "task-lane" || rt.via != "task 2481" {
		t.Fatalf("task lane: %+v, %v", rt, e)
	}
	sink.lanes["2481"] = "operator-desk"
	rt, e = s.findingsRoute(context.Background(), r, pr)
	if e != nil || rt.lane != "operator-desk" || rt.via != "unknown author; desk" {
		t.Fatalf("desk: %+v, %v", rt, e)
	}
}

// (f) A pane on the same branch name in another repository is not this PR's author.
func TestAuthorPaneOtherRepositoryNotMatched(t *testing.T) {
	s, _, _ := authorPaneSetup(t, aPane{"w1:p1", "w-9", "/wt/a", "feature", "git@github.com:example/other.git"})
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if rt.lane != "trailer-lane" || rt.via != "notify-lane trailer" {
		t.Fatalf("route %q via %q", rt.lane, rt.via)
	}
}

// (g) A host that cannot list its panes is an error to retry, in both routes,
// not a reroute to the trailer.
func TestAuthorPaneHostErrorRetries(t *testing.T) {
	s, _, run := authorPaneSetup(t)
	run.errs = map[string]error{key("herdr", "agent", "list"): errors.New("herdr unreachable")}
	pr := s.GH.(*fakeGH).pull
	if rt, e := s.findingsRoute(context.Background(), testRepoFor(s), pr); e == nil {
		t.Fatalf("findings rerouted to %q via %q", rt.lane, rt.via)
	}
	if lane, via, e := s.notifyLane(context.Background(), testRepoFor(s), pr); e == nil {
		t.Fatalf("ready rerouted to %q via %q", lane, via)
	}
}

// (h) The author-scope FINDINGS text tells the author how to acknowledge, by
// the lane the pane resolved.
func TestAuthorPaneFindingsTextCarriesLaneInAck(t *testing.T) {
	s, _, _ := authorPaneSetup(t)
	pr := s.GH.(*fakeGH).pull
	rt, e := s.findingsRoute(context.Background(), testRepoFor(s), pr)
	if e != nil {
		t.Fatal(e)
	}
	text := findingsText(testRepoFor(s), pr, "author", rt.lane, []pendingFinding{{thread: "T1", severity: "high", path: "tool", line: 3}})
	for _, want := range []string{
		"ack: panewire inbox list --lane w-9-lane --unread",
		"read each item, act on it or record its disposition",
		"panewire inbox ack <id>", "acknowledging does not resolve a finding",
		"for lane w-9-lane",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %q", want, text)
		}
	}
}

func testRepoFor(*Service) config.Repo { return config.Repo{Name: "example/project", Branch: "main"} }
