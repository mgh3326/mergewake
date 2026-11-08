package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func hkEventServer(t *testing.T, h http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
}

// The event goes to POST /v1/task-events as hk's TaskSourceEvent, with empty
// revert fields left out, and hk's result comes back.
func TestPostTaskEvent(t *testing.T) {
	var body map[string]any
	var path, auth string
	hkEventServer(t, func(w http.ResponseWriter, r *http.Request) {
		path, auth = r.Method+" "+r.URL.Path, r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		_, _ = w.Write([]byte(`{"applied":false,"redelivered":true,"state":"merged","task":{"id":5}}`))
	})
	at := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	url := "https://github.com/example/project/pull/7"
	res, e := Handoffkeep{}.PostTaskEvent(context.Background(), TaskEvent{Source: SourceMerge, SourceRef: url, TaskID: 5, PR: url, At: &at})
	if e != nil || res.Applied || !res.Redelivered || res.State != "merged" {
		t.Fatalf("%+v %v", res, e)
	}
	if path != "POST /v1/task-events" || auth != "Bearer tok" {
		t.Fatalf("%q %q", path, auth)
	}
	want := map[string]any{"source": SourceMerge, "source_ref": url, "task_id": float64(5), "pr": url, "at": "2026-11-05T00:00:00Z"}
	if len(body) != len(want) {
		t.Fatalf("body %v", body)
	}
	for k, v := range want {
		if body[k] != v {
			t.Fatalf("body %v", body)
		}
	}
}

// A 5xx is retried up to EventAttempts; a 4xx is final and carries hk's
// error code; a code outside hk's shape is withheld; a cancelled context
// stops the retries.
func TestPostTaskEventRetries(t *testing.T) {
	var hits atomic.Int32
	status, reply := 503, `{"error":"unavailable"}`
	hkEventServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	})
	h := Handoffkeep{EventAttempts: 4, EventBackoff: time.Millisecond}
	ev := TaskEvent{Source: SourcePR, SourceRef: "https://github.com/example/project/pull/7", TaskID: 5}
	_, e := h.PostTaskEvent(context.Background(), ev)
	var hk *HKError
	if !errors.As(e, &hk) || hk.Status != 503 || hits.Load() != 4 || !strings.Contains(e.Error(), "after 4 attempts") {
		t.Fatalf("%v hits=%d", e, hits.Load())
	}
	hits.Store(0)
	status, reply = 409, `{"error":"triage_gate","reason":"x"}`
	if _, e = h.PostTaskEvent(context.Background(), ev); !errors.As(e, &hk) || hk.Code != "triage_gate" || hits.Load() != 1 {
		t.Fatalf("%v hits=%d", e, hits.Load())
	}
	hits.Store(0)
	status, reply = 400, `{"error":"Bearer secret value"}`
	if _, e = h.PostTaskEvent(context.Background(), ev); !errors.As(e, &hk) || hk.Code != "" || strings.Contains(e.Error(), "secret") {
		t.Fatalf("%v", e)
	}
	hits.Store(0)
	status = 500
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = (Handoffkeep{EventAttempts: 5, EventBackoff: time.Hour}).PostTaskEvent(ctx, ev); e == nil || hits.Load() > 1 {
		t.Fatalf("%v hits=%d", e, hits.Load())
	}
}

func TestSHAAndToken(t *testing.T) {
	good := strings.Repeat("a", 40)
	for in, want := range map[string]string{good: good, "": "", strings.Repeat("A", 40): "", good + "0": "", "abc": ""} {
		if SHA(in) != want {
			t.Fatalf("SHA(%q)", in)
		}
	}
	for in, want := range map[string]string{"state-risk": "state-risk", " State Risk ": "state-risk", "a/b\n": "a-b", "é": "", "": "", "v1.2_x": "v1.2_x", strings.Repeat("a", 80): strings.Repeat("a", 64)} {
		if Token(in) != want {
			t.Fatalf("Token(%q) = %q", in, Token(in))
		}
	}
}

// A 400 to an extended payload is resent without the optional fields (here hk
// refuses head and reviews alike, so the resend that works is the plain one)
// and the result says hk refused them; a 400 to a plain event and any other
// status are not retried further, and a 400 to every form ends with hk's error.
func TestPostTaskEventFieldsRefused(t *testing.T) {
	var bodies []map[string]any
	status := func(map[string]any) (int, string) { return 200, `{"applied":true}` }
	hkEventServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		bodies = append(bodies, m)
		code, reply := status(m)
		w.WriteHeader(code)
		_, _ = w.Write([]byte(reply))
	})
	h := Handoffkeep{EventAttempts: 3, EventBackoff: time.Millisecond}
	url := "https://github.com/example/project/pull/7"
	ext := TaskEvent{Source: SourceReview, SourceRef: url, TaskID: 5, PR: url, HeadSHA: strings.Repeat("a", 40), Reviews: []ReviewRecord{{ReviewedHead: strings.Repeat("a", 40), Pass: 1}}}
	plain := TaskEvent{Source: SourceReview, SourceRef: url, TaskID: 5, PR: url}

	rejectNew := func(m map[string]any) (int, string) {
		if m["head_sha"] != nil || m["reviews"] != nil {
			return 400, `{"error":"invalid_event"}`
		}
		return 200, `{"applied":true}`
	}
	status = rejectNew
	res, e := h.PostTaskEvent(context.Background(), ext)
	if e != nil || !res.Applied || res.FieldsRefused != "invalid_event" || res.FieldsResend != ResendPlain || len(bodies) != 3 {
		t.Fatalf("%+v %v bodies=%d", res, e, len(bodies))
	}
	if bodies[0]["reviews"] == nil || bodies[1]["head_sha"] == nil || bodies[1]["reviews"] != nil || bodies[2]["head_sha"] != nil || bodies[2]["reviews"] != nil || bodies[2]["source_ref"] != url || bodies[2]["task_id"] != float64(5) {
		t.Fatalf("bodies %v", bodies)
	}

	bodies = nil
	status = func(map[string]any) (int, string) { return 400, `{"error":"invalid_event"}` }
	var hk *HKError
	if _, e = h.PostTaskEvent(context.Background(), ext); !errors.As(e, &hk) || hk.Code != "invalid_event" || len(bodies) != 3 {
		t.Fatalf("%v bodies=%d", e, len(bodies))
	}
	bodies = nil
	if _, e = h.PostTaskEvent(context.Background(), plain); !errors.As(e, &hk) || len(bodies) != 1 {
		t.Fatalf("plain 400: %v bodies=%d", e, len(bodies))
	}
	bodies = nil
	status = func(map[string]any) (int, string) { return 404, `{"error":"not_found"}` }
	if _, e = h.PostTaskEvent(context.Background(), ext); !errors.As(e, &hk) || hk.Status != 404 || len(bodies) != 1 {
		t.Fatalf("404: %v bodies=%d", e, len(bodies))
	}
}

// The compatibility resend spends what is left of EventAttempts: two 5xx
// tries, then the 400, leave one try for the plain event; the budget is never
// exceeded, and the resend always gets at least one try.
func TestPostTaskEventFieldsRefusedSharesBudget(t *testing.T) {
	var hits, flaky atomic.Int32
	flaky.Store(2)
	hkEventServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		b, _ := io.ReadAll(r.Body)
		switch {
		case n <= flaky.Load():
			w.WriteHeader(503)
		case strings.Contains(string(b), "head_sha"):
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":"invalid_event"}`))
		default:
			w.WriteHeader(503)
		}
	})
	url := "https://github.com/example/project/pull/7"
	ev := TaskEvent{Source: SourcePR, SourceRef: url, TaskID: 5, PR: url, HeadSHA: strings.Repeat("a", 40)}
	_, e := Handoffkeep{EventAttempts: 4, EventBackoff: time.Millisecond}.PostTaskEvent(context.Background(), ev)
	// 503, 503, 400 (3 of 4 tries), then the resend gets the last one.
	if e == nil || hits.Load() != 4 {
		t.Fatalf("%v hits=%d", e, hits.Load())
	}
	hits.Store(0)
	flaky.Store(0)
	_, e = Handoffkeep{EventAttempts: 1, EventBackoff: time.Millisecond}.PostTaskEvent(context.Background(), ev)
	if e == nil || hits.Load() != 2 {
		t.Fatalf("one-try budget: %v hits=%d", e, hits.Load())
	}
}

// A 400 that blames only reviews[] is resent with head and base and without the
// reviews, so a bad review record never strips them. An event whose only
// optional field is reviews goes straight to the plain form. hk's message on the
// first 400 comes back redacted and bounded.
func TestPostTaskEventReviewsRefusedKeepsHeadBase(t *testing.T) {
	var bodies []map[string]any
	message := "reviews[0]: lens must be a short token api_key=sk-live-0123456789abcdefghij é" + strings.Repeat("x", 300)
	hkEventServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		bodies = append(bodies, m)
		if m["reviews"] != nil {
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_context", "message": message})
			return
		}
		_, _ = w.Write([]byte(`{"applied":true}`))
	})
	h := Handoffkeep{EventAttempts: 3, EventBackoff: time.Millisecond}
	url := "https://github.com/example/project/pull/7"
	sha := strings.Repeat("a", 40)
	ev := TaskEvent{Source: SourceReview, SourceRef: url, TaskID: 5, PR: url, HeadSHA: sha, BaseSHA: sha, Reviews: []ReviewRecord{{ReviewedHead: sha, Pass: 1}}}
	res, e := h.PostTaskEvent(context.Background(), ev)
	if e != nil || res.FieldsRefused != "invalid_context" || res.FieldsResend != ResendWithoutReviews || len(bodies) != 2 {
		t.Fatalf("%+v %v bodies=%d", res, e, len(bodies))
	}
	if bodies[1]["head_sha"] != sha || bodies[1]["base_sha"] != sha || bodies[1]["reviews"] != nil {
		t.Fatalf("resend %v", bodies[1])
	}
	if !strings.HasPrefix(res.FieldsRefusedMessage, "reviews[0]: lens must be a short token") || strings.Contains(res.FieldsRefusedMessage, "sk-live") || len(res.FieldsRefusedMessage) > 200 || !utf8.ValidString(res.FieldsRefusedMessage) {
		t.Fatalf("message %q", res.FieldsRefusedMessage)
	}

	bodies = nil
	ev.HeadSHA, ev.BaseSHA = "", ""
	res, e = h.PostTaskEvent(context.Background(), ev)
	if e != nil || res.FieldsResend != ResendPlain || len(bodies) != 2 || bodies[1]["reviews"] != nil {
		t.Fatalf("%+v %v bodies=%d", res, e, len(bodies))
	}
}

// ForHK clears each field hk would refuse and nothing else, and drops a record
// only when its head, pass, round or lens is unusable.
func TestReviewRecordForHK(t *testing.T) {
	sha := strings.Repeat("a", 40)
	at := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	good := ReviewRecord{ReviewedHead: sha, ReviewedBase: sha, PatchID: "abc1234", Round: 2, Pass: 1, Lens: "correctness", Family: "openai", Profile: "sonnet@high", Critical: 1, StartedAt: &at, FinishedAt: &at}
	if out, cleared, drop := good.ForHK(); drop != "" || len(cleared) != 0 || out != good {
		t.Fatalf("a valid record changed: %+v %v %q", out, cleared, drop)
	}
	for name, mutate := range map[string]func(*ReviewRecord){
		"bad head":   func(r *ReviewRecord) { r.ReviewedHead = "abc" },
		"pass 0":     func(r *ReviewRecord) { r.Pass = 0 },
		"pass 1001":  func(r *ReviewRecord) { r.Pass = 1001 },
		"round 0":    func(r *ReviewRecord) { r.Round = 0 },
		"no lens":    func(r *ReviewRecord) { r.Lens = "" },
		"lens space": func(r *ReviewRecord) { r.Lens = "state risk" },
	} {
		r := good
		mutate(&r)
		if _, _, drop := r.ForHK(); drop == "" {
			t.Fatalf("%s: record kept", name)
		}
	}
	bad := good
	bad.ReviewedBase, bad.PatchID, bad.Family, bad.Verdict, bad.Profile = "x", "abc-def", "Open AI", "OK!", "a b"
	bad.Model, bad.Executor, bad.Harness = strings.Repeat("m", 201), "a\nb", "x"
	bad.Critical, bad.Low = -1, 2_000_000
	earlier := at.Add(-time.Hour)
	bad.FinishedAt = &earlier
	out, cleared, drop := bad.ForHK()
	want := []string{"critical", "executor", "family", "finished_at", "low", "model", "patch_id", "profile", "reviewed_base", "verdict"}
	if drop != "" || !slices.Equal(cleared, want) {
		t.Fatalf("drop=%q cleared=%v want %v", drop, cleared, want)
	}
	if out.Harness != "x" || out.Lens != "correctness" || out.Round != 2 || out.Critical != 0 || out.Low != 1_000_000 || out.FinishedAt != nil || out.StartedAt == nil {
		t.Fatalf("%+v", out)
	}
}
