package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type changeFake struct {
	mu                       sync.Mutex
	requests                 int
	seen                     map[string]time.Time
	batches                  []map[string]json.RawMessage
	missing, reject, noBatch bool
}

func (f *changeFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	if f.missing || (f.noBatch && r.URL.Path == "/v1/changes/batch") {
		http.NotFound(w, r)
		return
	}
	var body map[string]json.RawMessage
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		http.Error(w, "bad json", 400)
		return
	}
	f.batches = append(f.batches, body)
	if f.reject && r.URL.Path == "/v1/changes/batch" && body["checks"] != nil {
		w.WriteHeader(400)
		fmt.Fprint(w, `{"error":"invalid_context","reason":"bad check"}`)
		return
	}
	if f.reject && r.URL.Path == "/v1/changes/checks" {
		w.WriteHeader(400)
		fmt.Fprint(w, `{"error":"invalid_context","reason":"bad check"}`)
		return
	}
	sections := map[string]any{}
	names := []string{"state", "checks", "threads", "findings", "gate"}
	for _, name := range names {
		raw := body[name]
		if r.URL.Path == "/v1/changes/"+name {
			raw, _ = json.Marshal(body)
		}
		if raw == nil {
			continue
		}
		var section struct {
			ObservedAt time.Time `json:"observed_at"`
			DecidedAt  time.Time `json:"decided_at"`
		}
		_ = json.Unmarshal(raw, &section)
		at := section.ObservedAt
		if at.IsZero() {
			at = section.DecidedAt
		}
		if f.seen == nil {
			f.seen = map[string]time.Time{}
		}
		applied := !at.Before(f.seen[name])
		result := map[string]any{"applied": applied}
		if !applied {
			result["reason"] = "stale"
		} else {
			f.seen[name] = at
		}
		sections[name] = result
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"sections": sections})
}
func changeTestWriter(t *testing.T, f *changeFake) (*ChangeWriter, *bytes.Buffer) {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	var logs bytes.Buffer
	w := NewChangeWriter(slog.New(slog.NewTextHandler(&logs, nil)))
	w.endpoint = func() (*hkJobs, error) { return &hkJobs{base: server.URL, token: "test"}, nil }
	return w, &logs
}
func drainChange(t *testing.T, w *ChangeWriter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.Wait(ctx)
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
}
func TestHKChangeBurstAndFreshness(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	now := time.Now().UTC()
	for i := 0; i < 50; i++ {
		at := now.Add(time.Duration(i) * time.Millisecond)
		w.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"head_sha": "x", "observed_at": at, "source": "webhook", "delivery_id": fmt.Sprint(i)}})
	}
	drainChange(t, w)
	f.mu.Lock()
	count := f.requests
	last := f.seen["state"]
	f.mu.Unlock()
	if count > 3 || !last.Equal(now.Add(49*time.Millisecond)) {
		t.Fatalf("requests=%d latest=%v", count, last)
	}
	old := now.Add(-time.Second)
	w.Queue("o/r", 7, "state", ChangeSection{At: old, Value: map[string]any{"observed_at": old, "source": "webhook"}})
	drainChange(t, w)
	if got := w.Counts().Stale; got != 1 {
		t.Fatalf("stale=%d", got)
	}
}
func TestHKChangeUnavailableBackoff(t *testing.T) {
	f := &changeFake{missing: true}
	w, logs := changeTestWriter(t, f)
	at := time.Now().UTC()
	for i := 0; i < 2; i++ {
		w.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
		drainChange(t, w)
	}
	f.mu.Lock()
	count := f.requests
	f.mu.Unlock()
	if count != 2 || w.Counts().Unavailable != 1 || strings.Count(logs.String(), "hk change state unavailable") != 1 {
		t.Fatalf("requests=%d counts=%+v logs=%s", count, w.Counts(), logs.String())
	}
}
func TestHKChangeRejectIsolatesSection(t *testing.T) {
	f := &changeFake{reject: true}
	w, logs := changeTestWriter(t, f)
	at := time.Now().UTC()
	for _, name := range []string{"state", "checks", "gate"} {
		w.Queue("o/r", 7, name, ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
	}
	drainChange(t, w)
	if w.Counts().Rejected != 1 || w.Counts().Writes != 2 || !strings.Contains(logs.String(), "bad check") {
		t.Fatalf("counts=%+v logs=%s", w.Counts(), logs.String())
	}
}

func TestHKChangeFallbackToSections(t *testing.T) {
	f := &changeFake{noBatch: true}
	w, logs := changeTestWriter(t, f)
	at := time.Now().UTC()
	w.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
	drainChange(t, w)
	if w.Counts().Writes != 1 || w.Counts().Unavailable != 0 || strings.Contains(logs.String(), "unavailable") {
		t.Fatalf("counts=%+v logs=%s", w.Counts(), logs.String())
	}
}
