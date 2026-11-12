package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// otherBranchPane is a live pane that is not on the PR's branch: it proves the
// host was read without making its pane the author.
var otherBranchPane = aPane{"w1:p5", "w-5", "/wt/z", "other-branch", projectOrigin}

// finishJob sets the bound job w-7 to state, as hk reports it.
func finishJob(sink *jobSink, state string) {
	rec := sink.recs["w-7"]
	rec.State = state
	sink.recs["w-7"] = rec
}

// (a) A completed bound job whose host was read with no pane for it is not the
// author: the findings go to the Notify-Lane trailer as type findings.
func TestFindingsFinishedAuthorTrailerLane(t *testing.T) {
	s, sink, _ := takeoverSetup(t, otherBranchPane)
	finishJob(sink, "completed")
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, testRepoFor(s))
	n := s.Notifier.(*ackNotifier)
	if n.count != 1 || n.events[0].Lane != "trailer-lane" || n.events[0].Type != "findings" {
		t.Fatalf("finished author's findings: %+v, want one findings event on trailer-lane", n.events)
	}
}

// (b) A lost or escalated bound job with no trailer falls to the task lane; a
// task lane that is the desk takes the findings as a decision.
func TestFindingsFinishedAuthorWithoutTrailer(t *testing.T) {
	for _, state := range []string{"lost", "escalated"} {
		t.Run(state+" task lane", func(t *testing.T) {
			s, sink, _ := takeoverSetup(t, otherBranchPane)
			finishJob(sink, state)
			s.GH.(*fakeGH).pull.Body = "Summary\n\nMergewake-Task: 2481\n"
			sink.lanes["2481"] = "task-lane"
			s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
			deliver(t, s, testRepoFor(s))
			n := s.Notifier.(*ackNotifier)
			if n.count != 1 || n.events[0].Lane != "task-lane" || n.events[0].Type != "findings" {
				t.Fatalf("%s author's findings: %+v, want one findings event on task-lane", state, n.events)
			}
		})
		t.Run(state+" desk", func(t *testing.T) {
			s, sink, _ := takeoverSetup(t, otherBranchPane)
			finishJob(sink, state)
			s.GH.(*fakeGH).pull.Body = "Summary\n\nMergewake-Task: 2481\n"
			sink.lanes["2481"] = "operator-desk"
			s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
			deliver(t, s, testRepoFor(s))
			n := s.Notifier.(*ackNotifier)
			if n.count != 1 || n.events[0].Lane != "operator-desk" || n.events[0].Type != "decision" {
				t.Fatalf("%s author's findings to the desk: %+v, want one decision on operator-desk", state, n.events)
			}
		})
	}
}

// (c) guard: a terminal bound job with a live takeover pane routes to the
// takeover lane. (d) guard: a terminal job whose pane is still live keeps its
// lane, and so does a spawned job with no pane at all.
func TestFindingsLiveAuthorKeepsOrTakesOver(t *testing.T) {
	cases := []struct {
		name  string
		state string
		panes []aPane
		want  string
	}{
		{"terminal with a live takeover", "completed", []aPane{paneB}, "w-9-lane"},
		{"terminal with its pane still live", "completed", []aPane{paneA}, "w-7"},
		{"spawned with no pane", "spawned", []aPane{otherBranchPane}, "w-7"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, sink, _ := takeoverSetup(t, c.panes...)
			finishJob(sink, c.state)
			rt, e := s.findingsRoute(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
			if e != nil {
				t.Fatal(e)
			}
			if rt.lane != c.want {
				t.Fatalf("route %q via %q, want %q", rt.lane, rt.via, c.want)
			}
		})
	}
}

// (e) guard: the job's host scan failed, so liveness is unknown: the bound
// lane stands and the route is audited.
func TestFindingsFinishedAuthorHostScanFailedKeepsLane(t *testing.T) {
	s, sink, run := takeoverSetup(t, otherBranchPane)
	setPartialAuthorScan(s, run)
	finishJob(sink, "completed")
	var out bytes.Buffer
	s.Out = &out
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, testRepoFor(s))
	n := s.Notifier.(*ackNotifier)
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("liveness unknown: %+v, want the bound lane w-7", n.events)
	}
	rows := auditRecords(t, out.Bytes(), "findings-route-partial")
	if len(rows) != 1 || rows[0]["lane"] != "w-7" || !strings.Contains(fmt.Sprint(rows[0]["failed_hosts"]), "unavailable") {
		t.Fatalf("partial route audit rows %+v", rows)
	}
}

// (f) A delivery already re-sent and unacked, whose author then finishes with
// no pane, is sent once more to the new route with the -reroute event id, and
// never again. An acked delivery is not rerouted.
func TestFindingsRerouteOnceWhenAuthorFinishes(t *testing.T) {
	t.Run("unacked reroutes once", func(t *testing.T) {
		s, sink, n, now, out := rerouteRig(t)
		first := n.events[0]
		*now = now.Add(16 * time.Minute)
		deliver(t, s, testRepoFor(s))
		finishJob(sink, "completed")
		*now = now.Add(5 * time.Minute)
		deliver(t, s, testRepoFor(s))
		*now = now.Add(5 * time.Minute)
		deliver(t, s, testRepoFor(s))
		reroutes := reroutesOf(n)
		if len(reroutes) != 1 || reroutes[0].ID != first.ID+"-reroute" || reroutes[0].Lane != "trailer-lane" || reroutes[0].Type != "findings" {
			t.Fatalf("reroute events %+v, want one %s-reroute findings event on trailer-lane", reroutes, first.ID)
		}
		if rows := auditRecords(t, out.Bytes(), "findings-route-changed"); len(rows) != 1 {
			t.Fatalf("findings-route-changed rows %+v, want one", rows)
		}
	})
	t.Run("acked does not reroute", func(t *testing.T) {
		s, sink, n, now, _ := rerouteRig(t)
		*now = now.Add(16 * time.Minute)
		deliver(t, s, testRepoFor(s))
		n.acked[n.events[0].ID+"-resend"] = true
		finishJob(sink, "completed")
		*now = now.Add(5 * time.Minute)
		deliver(t, s, testRepoFor(s))
		if reroutes := reroutesOf(n); len(reroutes) != 0 {
			t.Fatalf("acked delivery rerouted: %+v", reroutes)
		}
	})
}

// rerouteRig delivers one finding to the live bound job w-7 at t0, the first
// send, on a fake clock the caller advances.
func rerouteRig(t *testing.T) (*Service, *jobSink, *ackNotifier, *time.Time, *bytes.Buffer) {
	t.Helper()
	s, sink, _ := takeoverSetup(t, otherBranchPane)
	n := s.Notifier.(*ackNotifier)
	now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	var out bytes.Buffer
	s.Out = &out
	s.GH.(*fakeGH).feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, testRepoFor(s))
	if len(n.events) != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("first send %+v, want one event on w-7", n.events)
	}
	return s, sink, n, &now, &out
}

func reroutesOf(n *ackNotifier) []integrations.Event {
	var out []integrations.Event
	for _, e := range n.events {
		if strings.HasSuffix(e.ID, "-reroute") {
			out = append(out, e)
		}
	}
	return out
}

// (g) workerLane names the trailer lane for a finished author.
func TestWorkerLaneFinishedAuthorUsesTrailer(t *testing.T) {
	s, sink, _ := takeoverSetup(t, otherBranchPane)
	finishJob(sink, "completed")
	lane, _, e := s.workerLane(context.Background(), testRepoFor(s), s.GH.(*fakeGH).pull)
	if e != nil {
		t.Fatal(e)
	}
	if lane != "trailer-lane" {
		t.Fatalf("worker lane %q for a finished author, want trailer-lane", lane)
	}
}

// Liveness unknown with no hosts configured keeps the bound lane and says so
// in the audit.
func TestFindingsFinishedAuthorUnknownLivenessIsAudited(t *testing.T) {
	s, _, n := setupForUnknownLiveness(t)
	var out bytes.Buffer
	s.Out = &out
	deliver(t, s, testRepoFor(s))
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("unknown liveness: %+v, want the bound lane w-7", n.events)
	}
	rows := auditRecords(t, out.Bytes(), "findings-route-partial")
	if len(rows) != 1 || rows[0]["liveness"] != "unknown" {
		t.Fatalf("liveness-unknown audit rows %+v", rows)
	}
}

// setupForUnknownLiveness is the deliver fixture with the bound job finished
// and no hosts configured, so no pane list was read.
func setupForUnknownLiveness(t *testing.T) (*Service, *jobSink, *ackNotifier) {
	t.Helper()
	s, g, n, sink, _, _ := deliverSetup(t)
	finishJob(sink, "completed")
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	return s, sink, n
}
