package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// consultRunner scripts `panewire consult request` answers; every other
// command goes to the wrapped fakeRunner.
type consultRunner struct {
	inner    integrations.Runner
	mu       sync.Mutex
	requests [][]string
	queue    []consultResp
	def      consultResp
}

type consultResp struct {
	rc  int
	out string
}

func (r *consultRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "consult" {
		r.mu.Lock()
		r.requests = append(r.requests, append([]string(nil), args...))
		resp := r.def
		if len(r.queue) > 0 {
			resp, r.queue = r.queue[0], r.queue[1:]
		}
		r.mu.Unlock()
		if resp.rc != 0 {
			return []byte(resp.out), &integrations.CommandError{
				Command: "panewire",
				Err:     fmt.Errorf("exit status %d", resp.rc),
				Stderr:  resp.out,
			}
		}
		return []byte(resp.out), nil
	}
	return r.inner.Run(ctx, command, args, input, dir)
}

func (r *consultRunner) requestCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func (r *consultRunner) lastRequest() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requests) == 0 {
		return nil
	}
	return append([]string(nil), r.requests[len(r.requests)-1]...)
}

// consultHub fakes GET /v1/consults/{id}; rows hold the scripted states.
type consultHub struct {
	mu   sync.Mutex
	rows map[string]reap.ConsultRow
	gets int
}

func (h *consultHub) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gets++
	id := strings.TrimPrefix(req.URL.Path, "/v1/consults/")
	row, ok := h.rows[id]
	if !ok {
		http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(row)
}

func (h *consultHub) getCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.gets
}

// consultSink is a jobSink that captures Record'ed documents and task
// comments; recordErr and commentErr script an hk failure.
type consultSink struct {
	*jobSink
	docs       map[string]string
	writes     []string // every question key written, in order
	comments   []string
	recordErr  error
	commentErr error
}

func (s *consultSink) Record(_ context.Context, key string, data []byte) error {
	if s.recordErr != nil {
		return s.recordErr
	}
	s.docs[key] = string(data)
	s.writes = append(s.writes, key)
	return nil
}

func (s *consultSink) Comment(_ context.Context, task, body string) error {
	if s.commentErr != nil {
		return s.commentErr
	}
	s.comments = append(s.comments, task+": "+body)
	return nil
}

// hubConsultSetup is roundRuleSetup with the hub consult switch on, a
// scripted consult runner, a document-capturing sink and a fake hub
// answering GET /v1/consults/{id}.
func hubConsultSetup(t *testing.T, rounds int) (*Service, *roundRuleGH, *ackNotifier, *consultSink, *consultRunner, *consultHub, config.Repo, *time.Time) {
	t.Helper()
	s, g, n, sink, r, now := deliverSetup(t)
	run := &consultRunner{
		inner: s.Runner,
		def:   consultResp{rc: 0, out: `{"id":"c-1","state":"queued"}`},
	}
	rgh := &roundRuleGH{fakeGH: g}
	s.GH = rgh
	s.Runner = run
	csink := &consultSink{jobSink: sink, docs: map[string]string{}}
	s.Sink = csink

	sink.lanes["2481"] = "design-lane"
	s.Config.Review.RoundRule.HubConsult = true

	hub := &consultHub{rows: map[string]reap.ConsultRow{}}
	srv := httptest.NewServer(hub)
	t.Cleanup(srv.Close)
	tok := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(tok, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: srv.URL, TokenFile: tok}

	passes := map[string][]github.PassReceipt{}
	for i := 1; i <= rounds; i++ {
		sha := shaA
		if i < rounds {
			sha = strings.Repeat(string(rune('0'+i)), 40)
		}
		passes[sha] = []github.PassReceipt{{Pass: 1, Patch: "patch-" + string(rune('0'+i)), Round: i}}
		if i < rounds {
			rgh.fakeGH.refs["mergewake/reviews/7/"+sha+"-done"] = true
		}
	}
	rgh.fakeGH.passes = passes
	markReviewed(rgh.fakeGH)

	return s, rgh, n, csink, run, hub, r, now
}

func argvHas(args []string, flag, want string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == want {
			return true
		}
	}
	return false
}

// OFF: a round-3 PR gets exactly today's directive and delivery — no
// consult request, no question document. The assertions use only what a
// 9ccabd4 build has: the switch off is the base behaviour.
func TestHubConsult_OffKeepsDirective(t *testing.T) {
	s, g, n, js, _, r := roundRuleSetup(t, 3)
	run := &consultRunner{inner: s.Runner}
	s.Runner = run
	sink := &consultSink{jobSink: js, docs: map[string]string{}}
	s.Sink = sink
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}

	deliver(t, s, r)

	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d", n.count)
	}
	if !strings.Contains(n.events[0].Text, Round3Directive) {
		t.Fatalf("missing round-3 directive: %s", n.events[0].Text)
	}
	if len(sink.docs) != 0 {
		t.Fatalf("OFF wrote question docs: %v", sink.docs)
	}
	if run.requestCount() != 0 {
		t.Fatalf("OFF requested a consult: %v", run.requests)
	}
}

// ON: a round-3 PR with a high finding writes the question doc, requests
// once with the contract argv and stores the id; a second pass polls
// (GET) instead of requesting again.
func TestHubConsult_RequestsOnceThenPolls(t *testing.T) {
	s, g, n, sink, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
		ownFinding("T2", "low", 20),
	}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}

	deliver(t, s, r)

	if run.requestCount() != 1 {
		t.Fatalf("want 1 consult request, got %d: %v", run.requestCount(), run.requests)
	}
	args := run.lastRequest()
	for flag, want := range map[string]string{
		"--task":       "2481",
		"--pr":         r.Name + "#7",
		"--head":       shaA,
		"--requester":  "mergewake/" + r.Name + "#7",
		"--author-job": "w-7",
	} {
		if !argvHas(args, flag, want) {
			t.Fatalf("consult request missing %s %q: %v", flag, want, args)
		}
	}
	var qkey string
	for i, a := range args {
		if a == "--question" && i+1 < len(args) {
			qkey = args[i+1]
		}
	}
	if !strings.HasPrefix(qkey, "hk:doc/mergewake/consult/") {
		t.Fatalf("question flag %q", qkey)
	}
	doc, ok := sink.docs[strings.TrimPrefix(qkey, "hk:doc/")]
	if !ok {
		t.Fatalf("question doc %q not recorded; docs: %v", qkey, sink.docs)
	}
	for _, want := range []string{r.Name + "#7", shaA, "T1", "high", "tool:10", "T2", "low", "2481"} {
		if !strings.Contains(doc, want) {
			t.Fatalf("question doc missing %q:\n%s", want, doc)
		}
	}

	// Findings wait while the consult is queued.
	if n.count != 0 {
		t.Fatalf("findings delivered while consult queued: %+v", n.events)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	if st.HubConsultID != "c-1" || st.HubConsultState != "queued" || st.HubConsultDone {
		t.Fatalf("consult state not stored: %+v", st)
	}

	// A second pass (e.g. after a restart) polls the row and does not
	// request again.
	restarted := *s
	deliver(t, &restarted, r)
	if run.requestCount() != 1 {
		t.Fatalf("second pass requested again: %v", run.requests)
	}
	if hub.getCount() == 0 {
		t.Fatal("second pass did not poll the consult row")
	}
	if n.count != 0 {
		t.Fatalf("findings delivered while consult queued: %+v", n.events)
	}
}

// ON, answered at request: the findings go out with the answer_doc
// pointer and verdict, no ask-a-consult directive, one hk task comment.
func TestHubConsult_AnsweredAtRequest(t *testing.T) {
	s, g, n, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.def = consultResp{rc: 0, out: `{"id":"c-9","state":"answered","answer_doc":"reports/consult-9/sha256/deadbeef.md","verdict":"split","seat":{"profile":"astra","family":"openai","grade":"SS"},"job":"consult-2481-a"}`}

	deliver(t, s, r)

	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d: %+v", n.count, n.events)
	}
	text := n.events[0].Text
	if !strings.Contains(text, "reports/consult-9/sha256/deadbeef.md") {
		t.Fatalf("delivery missing answer_doc pointer: %s", text)
	}
	if !strings.Contains(text, "split") {
		t.Fatalf("delivery missing verdict: %s", text)
	}
	if strings.Contains(text, Round3Directive) {
		t.Fatalf("answered delivery still tells the author to consult: %s", text)
	}
	if !strings.Contains(text, "T1") && !strings.Contains(text, "tool:10") {
		t.Fatalf("delivery lost the finding: %s", text)
	}
	if len(sink.comments) != 1 || !strings.Contains(sink.comments[0], "c-9") || !strings.Contains(sink.comments[0], "answered") {
		t.Fatalf("want one task comment with id and state, got %v", sink.comments)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	if !st.HubConsultDone || st.HubConsultState != "answered" {
		t.Fatalf("consult not settled: %+v", st)
	}

	// Later passes do not request, poll or comment again.
	deliver(t, s, r)
	if run.requestCount() != 1 || len(sink.comments) != 1 {
		t.Fatalf("settled consult acted again: requests=%d comments=%v", run.requestCount(), sink.comments)
	}
}

// ON, queued then answered: the poll delivers the answer pointer.
func TestHubConsult_PollUntilAnswered(t *testing.T) {
	s, g, n, _, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}

	deliver(t, s, r)
	if n.count != 0 || run.requestCount() != 1 {
		t.Fatalf("first pass: events=%d requests=%d", n.count, run.requestCount())
	}

	hub.mu.Lock()
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "answered", AnswerDoc: "reports/c-1/sha256/aa.md"}
	hub.mu.Unlock()

	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("want findings after answer, got %d", n.count)
	}
	if !strings.Contains(n.events[0].Text, "reports/c-1/sha256/aa.md") {
		t.Fatalf("delivery missing answer_doc: %s", n.events[0].Text)
	}
	if run.requestCount() != 1 {
		t.Fatalf("poll re-requested: %v", run.requests)
	}
}

// ON, the consult cannot be had — refused row, rc 2, rc 1 and an expired
// wait all deliver the findings as today plus the could-not-be-had line.
func TestHubConsult_Fallbacks(t *testing.T) {
	t.Run("refused_row", func(t *testing.T) {
		s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
		g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
		run.def = consultResp{rc: 0, out: `{"id":"c-2","state":"refused","reason":"no seat"}`}
		deliver(t, s, r)
		assertFallbackDelivered(t, n, "no seat")
	})

	t.Run("rc_2", func(t *testing.T) {
		s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
		g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
		run.def = consultResp{rc: 2, out: "credential not admitted"}
		deliver(t, s, r)
		assertFallbackDelivered(t, n, "")
	})

	t.Run("rc_1", func(t *testing.T) {
		s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
		g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
		run.def = consultResp{rc: 1, out: "hub unavailable"}
		deliver(t, s, r)
		assertFallbackDelivered(t, n, "")
	})

	t.Run("wait_expired", func(t *testing.T) {
		s, g, n, _, run, hub, r, now := hubConsultSetup(t, 3)
		g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
		hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "running"}
		deliver(t, s, r)
		if n.count != 0 {
			t.Fatal("delivered while consult running inside the wait")
		}
		*now = now.Add(3 * time.Hour)
		deliver(t, s, r)
		assertFallbackDelivered(t, n, "no terminal state")
		if run.requestCount() != 1 {
			t.Fatalf("wait expiry re-requested: %v", run.requests)
		}
	})
}

func assertFallbackDelivered(t *testing.T, n *ackNotifier, reason string) {
	t.Helper()
	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d: %+v", n.count, n.events)
	}
	text := n.events[0].Text
	if !strings.Contains(text, "consult could not be had") {
		t.Fatalf("missing could-not-be-had line: %s", text)
	}
	if reason != "" && !strings.Contains(text, reason) {
		t.Fatalf("missing reason %q: %s", reason, text)
	}
	if !strings.Contains(text, Round3Directive) {
		t.Fatalf("fallback dropped today's directive: %s", text)
	}
	if !strings.Contains(text, "ESC --type decision") || !strings.Contains(text, "do not call any model API") {
		t.Fatalf("fallback directive missing the ESC route or the model-API ban: %s", text)
	}
	if !strings.Contains(text, "tool:10") {
		t.Fatalf("fallback lost the finding: %s", text)
	}
}

// A failed row is terminal: it is never re-requested.
func TestHubConsult_NeverRerequestFailed(t *testing.T) {
	s, g, n, _, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.def = consultResp{rc: 0, out: `{"id":"c-3","state":"failed","reason":"answer lacks a section"}`}

	deliver(t, s, r)
	deliver(t, s, r)
	deliver(t, s, r)

	if run.requestCount() != 1 {
		t.Fatalf("failed consult re-requested: %v", run.requests)
	}
	if hub.getCount() != 0 {
		t.Fatalf("failed consult polled %d times", hub.getCount())
	}
	if n.count != 1 {
		t.Fatalf("want exactly one delivery, got %d", n.count)
	}
}

// A row that fails between polls is terminal all the same: the poll
// settles it and never re-POSTs it (a re-POST would retry it).
func TestHubConsult_PollFailedNeverRerequests(t *testing.T) {
	s, g, n, _, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}

	deliver(t, s, r)
	if n.count != 0 {
		t.Fatal("delivered while consult queued")
	}

	hub.mu.Lock()
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "failed", Reason: "answer lacks a section"}
	hub.mu.Unlock()

	deliver(t, s, r)
	assertFallbackDelivered(t, n, "")
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("failed row re-requested on poll: %v", run.requests)
	}
}

// An all-low round has no finding above low, so no consult is requested.
func TestHubConsult_AllLowRequestsNothing(t *testing.T) {
	s, g, _, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "low", 10)}
	deliver(t, s, r)
	if run.requestCount() != 0 {
		t.Fatalf("all-low round requested a consult: %v", run.requests)
	}
	if len(sink.docs) != 0 {
		t.Fatalf("all-low round wrote a question doc: %v", sink.docs)
	}
}

// A round that ends with an open medium finding and nothing above it is not
// all-low: it owes the consult (operator decision 250), and the answer goes
// out with the finding.
func TestHubConsult_MediumOnlyRequestsAndDelivers(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "medium", 10),
		ownFinding("T2", "low", 20),
	}
	run.def = consultResp{rc: 0, out: `{"id":"c-4","state":"answered","answer_doc":"reports/c-4/sha256/bb.md"}`}

	deliver(t, s, r)

	if run.requestCount() != 1 {
		t.Fatalf("want 1 consult request for a medium-only round, got %d: %v", run.requestCount(), run.requests)
	}
	if n.count != 1 {
		t.Fatalf("want 1 findings delivery with the answer, got %d: %+v", n.count, n.events)
	}
	text := n.events[0].Text
	if !strings.Contains(text, "reports/c-4/sha256/bb.md") {
		t.Fatalf("delivery missing the answer_doc: %s", text)
	}
	if !strings.Contains(text, "T1") && !strings.Contains(text, "tool:10") {
		t.Fatalf("delivery missing the open medium finding: %s", text)
	}
	if strings.Contains(text, Round3Directive) {
		t.Fatalf("answered delivery still tells the author to consult: %s", text)
	}
}

// A medium finding delivered in an earlier round still owes the consult at
// the consult round, and the answer goes out with it: the round is not
// waiting on an undelivered finding.
func TestHubConsult_MediumAlreadyDeliveredStillDeliversAnswer(t *testing.T) {
	s, g, n, _, run, _, r, now := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "medium", 10)}
	ledger := []findingsDelivery{{ID: "d-1", PR: 7, Head: shaA, Scope: "author", Threads: []string{"T1"}, Lane: "w-7", SentAt: *now}}
	if e := s.saveDeliveries(r.Name, 7, ledger); e != nil {
		t.Fatal(e)
	}
	run.def = consultResp{rc: 0, out: `{"id":"c-5","state":"answered","answer_doc":"reports/c-5/sha256/cc.md"}`}

	deliver(t, s, r)

	if run.requestCount() != 1 {
		t.Fatalf("want 1 consult request, got %d: %v", run.requestCount(), run.requests)
	}
	if n.count != 1 {
		t.Fatalf("answer delivered without the finding it answers, got %d events", n.count)
	}
	text := n.events[0].Text
	if !strings.Contains(text, "reports/c-5/sha256/cc.md") {
		t.Fatalf("delivery missing the answer_doc: %s", text)
	}
	if !strings.Contains(text, "T1") && !strings.Contains(text, "tool:10") {
		t.Fatalf("delivery lost the open medium finding: %s", text)
	}
}

// OFF: a round-3 PR whose only open finding is medium keeps today's
// directive path — the finding goes out, no directive, no consult.
func TestHubConsult_OffMediumKeepsNoDirective(t *testing.T) {
	s, g, n, js, _, r := roundRuleSetup(t, 3)
	run := &consultRunner{inner: s.Runner}
	s.Runner = run
	sink := &consultSink{jobSink: js, docs: map[string]string{}}
	s.Sink = sink
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "medium", 10)}

	deliver(t, s, r)

	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d", n.count)
	}
	if strings.Contains(n.events[0].Text, Round3Directive) {
		t.Fatalf("OFF medium-only round got the round-3 directive: %s", n.events[0].Text)
	}
	if run.requestCount() != 0 || len(sink.docs) != 0 {
		t.Fatalf("OFF requested a consult: requests=%d docs=%v", run.requestCount(), sink.docs)
	}
}

// A PR without a Mergewake-Task trailer skips the consult and the
// findings go out as today.
func TestHubConsult_NoTaskFallsBack(t *testing.T) {
	s, g, n, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.pull.Body = "no trailers here"
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	deliver(t, s, r)
	if run.requestCount() != 0 {
		t.Fatalf("taskless PR requested a consult: %v", run.requests)
	}
	if len(sink.docs) != 0 {
		t.Fatalf("taskless PR wrote a question doc: %v", sink.docs)
	}
	if n.count != 1 {
		t.Fatalf("want findings delivered, got %d", n.count)
	}
	if !strings.Contains(n.events[0].Text, "consult could not be had") {
		t.Fatalf("missing could-not-be-had line: %s", n.events[0].Text)
	}
}

// A task comment hk refused once is retried on later passes, even after the
// findings were delivered.
func TestHubConsult_CommentRetriedAfterDelivery(t *testing.T) {
	s, g, n, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.def = consultResp{rc: 0, out: `{"id":"c-7","state":"answered","answer_doc":"reports/c-7/sha256/aa.md"}`}
	sink.commentErr = errors.New("hk unavailable")

	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("findings held by a failed task comment: %+v", n.events)
	}
	if len(sink.comments) != 0 {
		t.Fatalf("comment recorded despite the failure: %v", sink.comments)
	}

	sink.commentErr = nil
	deliver(t, s, r)
	if len(sink.comments) != 1 || !strings.Contains(sink.comments[0], "c-7") {
		t.Fatalf("comment not retried after delivery: %v", sink.comments)
	}
	if st := s.loadRoundRuleState(r.Name, 7); !st.HubConsultCommented {
		t.Fatalf("comment not marked done: %+v", st)
	}

	// A further pass does not comment twice.
	deliver(t, s, r)
	if len(sink.comments) != 1 {
		t.Fatalf("comment repeated: %v", sink.comments)
	}
}

// A failed task read or question write retries only inside the wait: past it
// the findings go out with the fallback, so an hk outage cannot hold them.
func TestHubConsult_WaitBoundsPrerequestFailure(t *testing.T) {
	s, g, n, sink, run, _, r, now := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	sink.recordErr = errors.New("hk unavailable")

	deliver(t, s, r)
	if n.count != 0 || run.requestCount() != 0 {
		t.Fatalf("failed pre-work delivered or requested: events=%d requests=%d", n.count, run.requestCount())
	}

	*now = now.Add(time.Hour)
	deliver(t, s, r)
	if n.count != 0 {
		t.Fatalf("findings delivered before the wait expired: %+v", n.events)
	}

	*now = now.Add(2 * time.Hour)
	deliver(t, s, r)
	assertFallbackDelivered(t, n, "no consult row within")
	if run.requestCount() != 0 {
		t.Fatalf("a consult was requested after the pre-work failed: %v", run.requests)
	}
	if st := s.loadRoundRuleState(r.Name, 7); !st.HubConsultDone {
		t.Fatalf("pre-request failure not settled: %+v", st)
	}
}

// The request row is journaled with its head before the state file: an id
// lost to a failed state write is reconciled from the journal, never
// re-requested.
func TestHubConsult_ReconcilesLostID(t *testing.T) {
	s, g, n, _, run, hub, r, now := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-5"] = reap.ConsultRow{ID: "c-5", State: "queued"}

	// The request landed and was audited, then the state write was lost.
	st := s.loadRoundRuleState(r.Name, 7)
	st.HubConsultOwed = true
	st.HubConsultHead = shaA
	st.HubConsultPosted = true
	st.HubConsultAt = *now
	if e := s.saveRoundRuleState(r.Name, 7, st); e != nil {
		t.Fatal(e)
	}
	if e := s.audit(r.Name, "consult-request", map[string]any{"pr": 7, "head": shaA, "id": "c-5", "state": "queued"}, false); e != nil {
		t.Fatal(e)
	}

	deliver(t, s, r)

	if run.requestCount() != 0 {
		t.Fatalf("lost id re-requested: %v", run.requests)
	}
	if hub.getCount() == 0 {
		t.Fatal("reconciled row not polled")
	}
	if got := s.loadRoundRuleState(r.Name, 7); got.HubConsultID != "c-5" {
		t.Fatalf("id not recovered: %+v", got)
	}
	if rows := auditRows(t, s, r, "consult-recovered"); len(rows) != 1 {
		t.Fatalf("recovery not audited: %v", rows)
	}
	if n.count != 0 {
		t.Fatalf("delivered while the recovered consult is queued: %+v", n.events)
	}
}

// The poll reads reap.hub over HTTP whatever reap.transport is: the request
// goes through the panewire CLI with the same credential, so an ssh reap
// transport must not blind the poll.
func TestHubConsult_PollsOnSSHTransport(t *testing.T) {
	s, g, n, _, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}
	s.Config.Reap.Transport = config.ReapTransportSSH

	deliver(t, s, r)
	if n.count != 0 || run.requestCount() != 1 {
		t.Fatalf("request pass: events=%d requests=%d", n.count, run.requestCount())
	}
	deliver(t, s, r)
	if n.count != 0 || hub.getCount() == 0 {
		t.Fatalf("ssh transport did not poll the row: events=%d gets=%d", n.count, hub.getCount())
	}

	hub.mu.Lock()
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "answered", AnswerDoc: "reports/c-1/sha256/aa.md"}
	hub.mu.Unlock()

	deliver(t, s, r)
	if n.count != 1 || !strings.Contains(n.events[0].Text, "reports/c-1/sha256/aa.md") {
		t.Fatalf("answer not delivered over ssh reap transport: %+v", n.events)
	}
	if run.requestCount() != 1 {
		t.Fatalf("poll re-requested: %v", run.requests)
	}
}

// A multiline CLI error reaches the lane event as one printable line: emit
// refuses control bytes, so the fallback would otherwise never deliver.
func TestHubConsult_ErrorDetailIsOneLine(t *testing.T) {
	s, g, n, _, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	run.def = consultResp{rc: 1, out: "hub unavailable\nerror: dial tcp 10.0.0.1: connect: refused\n"}

	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("want 1 findings event, got %d", n.count)
	}
	text := n.events[0].Text
	if strings.ContainsAny(text, "\n\r\t") {
		t.Fatalf("control byte in the delivery: %q", text)
	}
	if !strings.Contains(text, "hub unavailable error: dial tcp") {
		t.Fatalf("error detail not flattened: %s", text)
	}
}
