package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func hkEventServer(t *testing.T, h http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "tok")
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
