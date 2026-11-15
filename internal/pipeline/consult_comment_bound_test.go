package pipeline

import (
	"context"
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

// A failed task read is a transient error, not proof that the comment can
// never be recorded: the PR body carries no trailer, so the task comes from
// the head commit, and that read fails. It must hold the next head inside the
// bound exactly as a failed write does — the old row is kept and the findings
// wait — and only past the bound does the reset proceed with the drop audited
// once. The read clears inside the bound and the write keeps failing, which is
// the way the bound is reached with the comment still unrecorded.
func TestHubConsult_TaskReadErrorHoldsThenDrops(t *testing.T) {
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

	// The push drops the body trailer; the head commit read that would supply
	// the task fails.
	g.pull.Body = "Summary\n"
	g.messages = map[string]string{shaC: "Fix the thing\n\nMergewake-Task: 2481\n"}
	g.commitErr = map[string]error{shaC: errors.New("GitHub GET /commits: HTTP 502")}
	headMovesTo(g, shaC)
	// Routing needs the same task, so this pass also reports the read error;
	// the gate must still hold the old row instead of resetting it.
	_ = s.DeliverFindings(context.Background(), r, 7, Options{})
	if n.count != 0 {
		t.Fatalf("findings delivered while the task read still fails: %+v", n.events)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultCommentAt.IsZero() {
		t.Fatalf("comment bound not started on a task read error: %+v", st)
	}
	if rows := auditRows(t, s, r, "consult-comment-dropped"); len(rows) != 0 {
		t.Fatalf("a held comment audited as dropped: %v", rows)
	}

	// Inside the bound the hold stands: the read now works and the write
	// fails, so the comment still cannot be recorded.
	g.commitErr = nil
	sink.commentErr = errors.New("hk unavailable")
	*now = now.Add(consultCommentBound - time.Minute)
	deliver(t, s, r)
	if n.count != 0 || run.requestCount() != 1 {
		t.Fatalf("bound released early: events=%d requests=%d", n.count, run.requestCount())
	}

	// At the bound the reset proceeds, the findings go out with the new
	// head's consult, and the old comment is audited as dropped once.
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
}

// A read error that clears is never a lost comment: the next pass records it
// on the old row's task and releases the next head at once, with no drop.
func TestHubConsult_TaskReadErrorClearsAndRecords(t *testing.T) {
	s, g, n, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.queue = []consultResp{
		{rc: 0, out: `{"id":"c-1","state":"queued"}`},
		{rc: 0, out: `{"id":"c-2","state":"answered","answer_doc":"reports/c-2/sha256/bb.md"}`},
	}

	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 {
		t.Fatalf("first pass: requests=%d events=%d", run.requestCount(), n.count)
	}

	g.pull.Body = "Summary\n"
	g.messages = map[string]string{shaC: "Fix the thing\n\nMergewake-Task: 2481\n"}
	g.commitErr = map[string]error{shaC: errors.New("GitHub GET /commits: HTTP 502")}
	headMovesTo(g, shaC)
	// Routing needs the same task, so this pass also reports the read error;
	// the gate must still hold the old row instead of resetting it.
	_ = s.DeliverFindings(context.Background(), r, 7, Options{})
	if n.count != 0 {
		t.Fatalf("findings delivered while the task read still fails: %+v", n.events)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultID != "c-1" || st.HubConsultHead != shaA {
		t.Fatalf("old row reset on a task read error: %+v", st)
	}

	g.commitErr = nil
	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("findings held after the read cleared: %+v", n.events)
	}
	// The old row's comment is recorded, then the new head's own consult
	// settles and records its own: the read error cost neither.
	if len(sink.comments) != 2 || !strings.HasPrefix(sink.comments[0], "2481: hub consult c-1:") ||
		!strings.HasPrefix(sink.comments[1], "2481: hub consult c-2:") {
		t.Fatalf("comments not recorded once the read cleared: %v", sink.comments)
	}
	if rows := auditRows(t, s, r, "consult-comment-dropped"); len(rows) != 0 {
		t.Fatalf("recorded comment audited as dropped: %v", rows)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultID != "c-2" || st.HubConsultHead != shaC {
		t.Fatalf("state not re-keyed to the new head: %+v", st)
	}
}
