package pipeline

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// headMovesTo records the author's push: the new head closes the same
// consult round, so the reset of the old head's row runs on the next pass.
func headMovesTo(g *roundRuleGH, sha string) {
	g.pull.Head.SHA = sha
	g.passes[sha] = []github.PassReceipt{{Pass: 1, Patch: "patch-3", Round: 3}}
	g.refs[doneRef(7, sha)] = true
}

// A sink without Comment (sink.type ""/"noop", a supported config) can never
// record the superseded consult's task comment, so it must not hold the next
// head: the reset proceeds and the round's findings go out with the new
// consult's outcome.
func TestHubConsult_NoCommentSinkDoesNotHoldDelivery(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	s.Sink = integrations.Noop{}
	run.queue = []consultResp{
		{rc: 0, out: `{"id":"c-1","state":"queued"}`},
		{rc: 0, out: `{"id":"c-2","state":"answered","answer_doc":"reports/c-2/sha256/bb.md"}`},
	}

	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 {
		t.Fatalf("first pass: requests=%d events=%d", run.requestCount(), n.count)
	}

	headMovesTo(g, shaC)
	deliver(t, s, r)

	if n.count != 1 {
		t.Fatalf("findings held by a comment no sink can record: %+v", n.events)
	}
	if !strings.Contains(n.events[0].Text, "reports/c-2/") {
		t.Fatalf("delivery did not carry the new head's consult: %s", n.events[0].Text)
	}
	if run.requestCount() != 2 {
		t.Fatalf("new head's consult not requested: %v", run.requests)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultID != "c-2" || st.HubConsultHead != shaC {
		t.Fatalf("state not re-keyed to the new head: %+v", st)
	}
}

// A PR with no task (no Mergewake-Task trailer) has nowhere to record the
// comment, so it must not hold either: the reset proceeds and the round's
// findings go out with the fallback directive.
func TestHubConsult_NoTaskDoesNotHoldDelivery(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.queue = []consultResp{{rc: 0, out: `{"id":"c-1","state":"queued"}`}}

	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 {
		t.Fatalf("first pass: requests=%d events=%d", run.requestCount(), n.count)
	}

	// The trailer is gone from the PR body: the comment has no task.
	g.pull.Body = "Summary\n"
	headMovesTo(g, shaC)
	deliver(t, s, r)

	if n.count != 1 {
		t.Fatalf("findings held by a comment no PR task can carry: %+v", n.events)
	}
	if !strings.Contains(n.events[0].Text, "no Mergewake-Task trailer") {
		t.Fatalf("delivery did not carry the no-task reason: %s", n.events[0].Text)
	}
	if run.requestCount() != 1 {
		t.Fatalf("a consult was requested without a task: %v", run.requests)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultOwed {
		t.Fatalf("no-task consult left owed: %+v", st)
	}
}

// A failing Comment holds the next head only inside the bound: past it the
// reset proceeds, the findings go out, and the dropped comment is audited
// once.
func TestHubConsult_CommentFailureBoundedThenDropped(t *testing.T) {
	s, g, n, sink, run, _, r, now := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.queue = []consultResp{
		{rc: 0, out: `{"id":"c-1","state":"queued"}`},
		{rc: 0, out: `{"id":"c-2","state":"answered","answer_doc":"reports/c-2/sha256/bb.md"}`},
	}

	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 {
		t.Fatalf("first pass: requests=%d events=%d", run.requestCount(), n.count)
	}

	sink.commentErr = errors.New("hk unavailable")
	headMovesTo(g, shaC)
	deliver(t, s, r)
	if n.count != 0 {
		t.Fatalf("findings delivered before the comment bound: %+v", n.events)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	if st.HubConsultCommentAt.IsZero() {
		t.Fatalf("comment bound not started: %+v", st)
	}
	if st.HubConsultID != "c-1" {
		t.Fatalf("old row reset before the bound: %+v", st)
	}

	// Inside the bound the hold stands.
	*now = now.Add(consultCommentBound - time.Minute)
	deliver(t, s, r)
	if n.count != 0 || run.requestCount() != 1 {
		t.Fatalf("bound released early: events=%d requests=%d", n.count, run.requestCount())
	}

	// At the bound the reset proceeds, so the findings are never held
	// longer, and the old comment is recorded as dropped.
	*now = now.Add(time.Minute)
	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("findings held past the comment bound: %+v", n.events)
	}
	if !strings.Contains(n.events[0].Text, "reports/c-2/") {
		t.Fatalf("delivery did not carry the new head's consult: %s", n.events[0].Text)
	}
	if run.requestCount() != 2 {
		t.Fatalf("new head's consult not requested after the bound: %v", run.requests)
	}
	rows := auditRows(t, s, r, "consult-comment-dropped")
	if len(rows) != 1 {
		t.Fatalf("want 1 drop row, got %v", rows)
	}
	if rows[0]["id"] != "c-1" || rows[0]["head"] != shaA {
		t.Fatalf("drop row names the wrong row: %v", rows[0])
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultID != "c-2" || st.HubConsultHead != shaC {
		t.Fatalf("state not re-keyed to the new head: %+v", st)
	}

	// The drop is audited once, never again on later passes.
	deliver(t, s, r)
	if rows := auditRows(t, s, r, "consult-comment-dropped"); len(rows) != 1 {
		t.Fatalf("drop audited more than once: %v", rows)
	}
	if n.count != 1 {
		t.Fatalf("findings re-delivered: %+v", n.events)
	}
}
