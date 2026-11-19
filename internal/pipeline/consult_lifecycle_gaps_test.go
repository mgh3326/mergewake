package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// Gap 1: fallback paths must keep HubConsultOwed set (Done) until the outcome
// is delivered. If sendFindings fails after a fallback settled, the next pass
// must redeliver the stored directive, never POST a second request.
func TestHubConsult_FallbackKeepsOwedUntilDelivered(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.def = consultResp{rc: 2, out: "credential not admitted"}

	// The fallback is ready but its delivery fails.
	n.err = errors.New("notify down")
	if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err == nil {
		t.Fatal("want the failed findings delivery to fail the pass")
	}
	if run.requestCount() != 1 {
		t.Fatalf("want 1 consult request, got %d: %v", run.requestCount(), run.requests)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	if !st.HubConsultDone || !st.HubConsultOwed {
		t.Fatalf("fallback must stay owed until delivered: %+v", st)
	}

	// A retry delivers the stored outcome without a second POST.
	n.err = nil
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("fallback delivery re-requested: %v", run.requests)
	}
	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d", n.count)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultOwed {
		t.Fatalf("owed not cleared after delivery: %+v", st)
	}
}

// timeoutRunner fails the first `panewire consult request` with a
// context-deadline error that carries no exit status, then answers queued.
type timeoutRunner struct {
	inner    integrations.Runner
	mu       sync.Mutex
	calls    int
	requests [][]string
}

func (r *timeoutRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "consult" {
		r.mu.Lock()
		r.calls++
		r.requests = append(r.requests, append([]string(nil), args...))
		n := r.calls
		r.mu.Unlock()
		if n == 1 {
			return nil, &integrations.CommandError{Command: "panewire", Err: context.DeadlineExceeded}
		}
		return []byte(`{"id":"c-9","state":"queued"}`), nil
	}
	return r.inner.Run(ctx, command, args, input, dir)
}

// Gap 2: a request that dies without an exit status (timeout, cancel, killed
// child) after the POST may have created a paid row. It must hold and
// reconcile on the next pass, never settle as failed.
func TestHubConsult_NoExitStatusHoldsForReconcile(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	tr := &timeoutRunner{inner: run.inner}
	s.Runner = tr

	if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 {
		t.Fatalf("timed-out request settled as fallback: %+v", n.events)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	if st.HubConsultDone || !st.HubConsultOwed || !st.HubConsultPosted {
		t.Fatalf("timed-out request must hold owed for reconcile: %+v", st)
	}

	if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	calls := tr.calls
	tr.mu.Unlock()
	if calls != 2 {
		t.Fatalf("want a reconcile re-request, got %d calls", calls)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultDone {
		t.Fatalf("reconciling request settled early: %+v", st)
	}
	if n.count != 0 {
		t.Fatalf("consult queued again, findings must stay held: %+v", n.events)
	}
}

// Gap 3: answer_doc and the verdict reach a lane event through the directive,
// so they must pass through emitSafe like the fallback reason does.
func TestHubConsult_DirectiveSanitisesAnswerFields(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.def = consultResp{rc: 0, out: `{"id":"c-1","state":"answered","answer_doc":"reports/c-1/\u0001\u0002.md","verdict":"needs\u0000fix"}`}

	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d", n.count)
	}
	text := n.events[0].Text
	for _, r := range text {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			t.Fatalf("control byte U+%04X in the delivery: %q", r, text)
		}
	}
	if !strings.Contains(text, "reports/c-1/") || !strings.Contains(text, "needs fix") {
		t.Fatalf("sanitised answer missing: %s", text)
	}
	if st := s.loadRoundRuleState(r.Name, 7); strings.ContainsAny(st.HubConsultAnswerDoc, "\x01\x02") || strings.Contains(st.HubConsultVerdict, "\x00") {
		t.Fatalf("answer fields stored raw: %+v", st)
	}
}

// Gap 4: when the head moves, the old consult's task comment must be finished
// or retried before the new head resets the state, or its ID is lost.
func TestHubConsult_SupersededCommentRetriedBeforeReset(t *testing.T) {
	s, g, n, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.queue = []consultResp{
		{rc: 0, out: `{"id":"c-1","state":"queued"}`},
		{rc: 0, out: `{"id":"c-2","state":"queued"}`},
	}

	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 {
		t.Fatalf("first pass: requests=%d events=%d", run.requestCount(), n.count)
	}

	// The author pushes; the same round owes a new-head consult while the
	// supersession comment cannot be recorded.
	g.pull.Head.SHA = shaC
	g.passes[shaC] = []github.PassReceipt{{Pass: 1, Patch: "patch-3", Round: 3}}
	g.refs[doneRef(7, shaC)] = true
	sink.commentErr = errors.New("hk unavailable")
	if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if run.requestCount() != 1 {
		t.Fatalf("new head re-requested before the old comment was retried: %v", run.requests)
	}
	if len(sink.comments) != 0 {
		t.Fatalf("comment recorded despite the failure: %v", sink.comments)
	}

	// Once hk recovers the old comment goes out, then the new head requests.
	sink.commentErr = nil
	deliver(t, s, r)
	if len(sink.comments) != 1 || !strings.Contains(sink.comments[0], "c-1") {
		t.Fatalf("old consult comment lost on reset: %v", sink.comments)
	}
	if run.requestCount() != 2 {
		t.Fatalf("want the new head consult after the retry, got %v", run.requests)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultID != "c-2" || st.HubConsultHead != shaC {
		t.Fatalf("state not re-keyed to the new head: %+v", st)
	}
	if n.count != 0 {
		t.Fatalf("findings delivered while the new consult is queued: %+v", n.events)
	}
}
