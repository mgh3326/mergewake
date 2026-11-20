package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A consult a push superseded before its answer keeps being followed on its
// row: no second POST, no rewrite of its question, and the answer goes out
// once with the next author delivery, naming the head it was asked for.
func TestConsultCarry_AnsweredAfterAPushIsDeliveredOnceOnTheLaterHead(t *testing.T) {
	s, g, n, sink, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}

	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 || len(sink.writes) != 1 {
		t.Fatalf("first pass: requests=%d events=%d writes=%v", run.requestCount(), n.count, sink.writes)
	}
	key := sink.writes[0]
	question := sink.docs[key]

	// The author pushes before the answer; the same patch keeps the round.
	pushHead(g, shaC, "patch-3", 3, true)
	g.feedback.Threads = append(g.feedback.Threads, ownFinding("T2", "high", 20))
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("a consult was requested after the supersession: %v", run.requests)
	}
	if len(sink.writes) != 1 || sink.docs[key] != question {
		t.Fatalf("the question was rewritten after the supersession: writes=%v", sink.writes)
	}
	if hub.getCount() == 0 {
		t.Fatal("the superseded row is no longer followed")
	}
	// The new head's findings are not held behind the old row.
	if n.count != 1 || strings.Contains(n.events[0].Text, "hub consult") {
		t.Fatalf("new head's findings held or given the row's text: %+v", n.events)
	}

	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "answered", AnswerDoc: "reports/c-1/sha256/aa.md", Verdict: "fix"}
	deliver(t, s, r)
	deliver(t, s, r)
	if n.count != 2 {
		t.Fatalf("the outcome did not go out exactly once: %+v", n.events)
	}
	text := n.events[1].Text
	if !strings.Contains(text, "hub consult answered: reports/c-1/sha256/aa.md") || !strings.Contains(text, "asked for head "+short7(shaA)) {
		t.Fatalf("outcome missing the answer or the head it was asked for: %s", text)
	}
	if run.requestCount() != 1 || len(sink.writes) != 1 {
		t.Fatalf("requests=%d writes=%v", run.requestCount(), sink.writes)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultOwed || !st.directiveSent() {
		t.Fatalf("outcome not marked delivered: %+v", st)
	}
	if len(sink.comments) != 1 || !strings.Contains(sink.comments[0], "c-1") {
		t.Fatalf("the settled row is not recorded on the task: %v", sink.comments)
	}
}

// A superseded consult whose row is refused delivers the could-not-be-had line
// once, naming the head it was asked for.
func TestConsultCarry_RefusedAfterAPushDeliversTheFallbackOnce(t *testing.T) {
	s, g, n, _, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}
	deliver(t, s, r)

	pushHead(g, shaC, "patch-3", 3, true)
	deliver(t, s, r)
	before := n.count
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "refused", Reason: "no seat free"}
	deliver(t, s, r)
	deliver(t, s, r)
	if n.count != before+1 {
		t.Fatalf("the could-not-be-had line did not go out exactly once: %+v", n.events)
	}
	text := n.events[n.count-1].Text
	for _, want := range []string{"hub consult could not be had", "no seat free", "asked for head " + short7(shaA)} {
		if !strings.Contains(text, want) {
			t.Fatalf("delivery missing %q: %s", want, text)
		}
	}
	if run.requestCount() != 1 {
		t.Fatalf("a refused row was requested again: %v", run.requests)
	}
}

// A POST that ended without an exit status may have reached the hub: after a
// push it is never replayed (that could restart a finished paid consult) and
// its question is not rewritten; once the wait is over the fallback goes out
// once.
func TestConsultCarry_AmbiguousPostIsNeverReplayedAfterAPush(t *testing.T) {
	s, g, n, sink, _, _, r, now := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	tr := &timeoutRunner{inner: s.Runner}
	s.Runner = tr

	deliver(t, s, r)
	if tr.calls != 1 || len(sink.writes) != 1 {
		t.Fatalf("first pass: calls=%d writes=%v", tr.calls, sink.writes)
	}

	pushHead(g, shaC, "patch-3", 3, true)
	deliver(t, s, r)
	if tr.calls != 1 || len(sink.writes) != 1 {
		t.Fatalf("the ambiguous POST was replayed: calls=%d writes=%v", tr.calls, sink.writes)
	}

	*now = now.Add(s.Config.Review.RoundRuleHubConsultWait() + time.Minute)
	deliver(t, s, r)
	deliver(t, s, r)
	if tr.calls != 1 || len(sink.writes) != 1 {
		t.Fatalf("the ambiguous POST was replayed after the wait: calls=%d writes=%v", tr.calls, sink.writes)
	}
	last := n.events[n.count-1].Text
	if !strings.Contains(last, "hub consult could not be had") || !strings.Contains(last, "asked for head "+short7(shaA)) {
		t.Fatalf("no fallback naming the head: %s", last)
	}
	if got := strings.Count(strings.Join(eventTexts(n), "\n"), "could not be had"); got != 1 {
		t.Fatalf("fallback delivered %d times", got)
	}
}

func eventTexts(n *ackNotifier) []string {
	var out []string
	for _, e := range n.events {
		out = append(out, e.Text)
	}
	return out
}

// A force-push back to the requested head does not make a carried row
// replayable: the POST marker, not the current head, decides.
func TestConsultCarry_ForcePushBackToTheRequestedHeadDoesNotReplayThePost(t *testing.T) {
	s, g, _, sink, _, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	tr := &timeoutRunner{inner: s.Runner}
	s.Runner = tr

	deliver(t, s, r)
	pushHead(g, shaC, "patch-3", 3, true)
	deliver(t, s, r)
	pushHead(g, shaA, "patch-3", 3, true)
	deliver(t, s, r)
	deliver(t, s, r)
	if tr.calls != 1 || len(sink.writes) != 1 {
		t.Fatalf("the POST was replayed after a force-push back: calls=%d writes=%v", tr.calls, sink.writes)
	}
}

// A reason of 200 bytes or more is clipped before the head label is added, so
// the label always survives.
func TestConsultCarry_LongFailureReasonKeepsTheHeadLabel(t *testing.T) {
	st := &roundRuleState{HubConsultHead: shaA, HubConsultState: "failed", HubConsultReason: strings.Repeat("x", 400)}
	if d := st.hubConsultDirective(shaC); !strings.Contains(d, "asked for head "+short7(shaA)) {
		t.Fatalf("head label clipped away: %s", d)
	}
}
