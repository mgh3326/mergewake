package pipeline

import (
	"context"
	"encoding/json"
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
// comments.
type consultSink struct {
	*jobSink
	docs     map[string]string
	comments []string
}

func (s *consultSink) Record(_ context.Context, key string, data []byte) error {
	s.docs[key] = string(data)
	return nil
}

func (s *consultSink) Comment(_ context.Context, task, body string) error {
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
	if strings.Contains(text, "ask a consult") {
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
