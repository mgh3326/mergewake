package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	now := time.Now().UTC()
	// The flush timer starts with the first Queue and fires after a fixed
	// window, so a slow queue loop (persist writes under -race or load) can
	// split the burst. Holding sendMu makes the flush wait until all 50 are
	// queued, then send them as one batch.
	w.sendMu.Lock()
	for i := 0; i < 50; i++ {
		at := now.Add(time.Duration(i) * time.Millisecond)
		w.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"head_sha": "x", "observed_at": at, "source": "webhook", "delivery_id": fmt.Sprint(i)}})
	}
	w.sendMu.Unlock()
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

func TestHKChangeOutboxReplaysAfterFailure(t *testing.T) {
	var mu sync.Mutex
	fail := true
	var got []map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			http.Error(w, "temporary", 503)
			return
		}
		var body map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body)
		fmt.Fprint(w, `{"sections":{"findings":{"applied":true}}}`)
	}))
	defer srv.Close()
	dir := t.TempDir()
	var logs bytes.Buffer
	w := NewChangeWriter(slog.New(slog.NewTextHandler(&logs, nil)))
	w.outbox = filepath.Join(dir, "hk-change-outbox")
	w.endpoint = func() (*hkJobs, error) { return &hkJobs{base: srv.URL, token: "test"}, nil }
	at := time.Now().UTC()
	w.Queue("o/r", 7, "findings", ChangeSection{At: at, Value: map[string]any{"reviewed_head": "head", "observed_at": at, "findings": []any{map[string]any{"id": "finding"}}}})
	drainChange(t, w)
	files, err := os.ReadDir(w.outbox)
	if err != nil || len(files) != 1 || !strings.Contains(logs.String(), "hk change request failed") {
		t.Fatalf("files=%v err=%v logs=%s", files, err, logs.String())
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	restarted := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), dir)
	drainChange(t, restarted)
	files, err = os.ReadDir(w.outbox)
	mu.Lock()
	defer mu.Unlock()
	if err != nil || len(files) != 0 || len(got) != 1 || got[0]["findings"] == nil {
		t.Fatalf("files=%v err=%v got=%v", files, err, got)
	}
}

func TestHKChangeOutboxSurvivesBoundedShutdown(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		http.Error(w, "offline", 503)
	}))
	defer slow.Close()
	var mu sync.Mutex
	received := 0
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received++
		mu.Unlock()
		fmt.Fprint(w, `{"sections":{"state":{"applied":true}}}`)
	}))
	defer fast.Close()
	dir := filepath.Join(t.TempDir(), "outbox")
	w := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.outbox = dir
	w.endpoint = func() (*hkJobs, error) { return &hkJobs{base: slow.URL, token: "test"}, nil }
	at := time.Now().UTC()
	for i := 1; i <= 5; i++ {
		w.Queue("o/r", i, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	w.Wait(ctx)
	cancel()
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 5 {
		close(release)
		t.Fatalf("files=%v err=%v", files, err)
	}
	restarted := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	restarted.outbox = dir
	restarted.endpoint = func() (*hkJobs, error) { return &hkJobs{base: fast.URL, token: "test"}, nil }
	restarted.reload()
	drainChange(t, restarted)
	mu.Lock()
	count := received
	mu.Unlock()
	if count != 5 {
		close(release)
		t.Fatalf("replayed=%d", count)
	}
	close(release)
	drainChange(t, w)
}

func TestHKChangeOutboxRetainsEndpointFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "outbox")
	var logs bytes.Buffer
	w := NewChangeWriter(slog.New(slog.NewTextHandler(&logs, nil)))
	w.outbox = dir
	w.endpoint = func() (*hkJobs, error) { return nil, errors.New("endpoint unavailable") }
	at := time.Now().UTC()
	w.Queue("o/r", 7, "gate", ChangeSection{At: at, Value: map[string]any{"decided_at": at}})
	drainChange(t, w)
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 || !strings.Contains(logs.String(), "endpoint unavailable") {
		t.Fatalf("files=%v err=%v logs=%s", files, err, logs.String())
	}
	f := &changeFake{}
	srv := httptest.NewServer(f)
	defer srv.Close()
	restarted := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	restarted.outbox = dir
	restarted.endpoint = func() (*hkJobs, error) { return &hkJobs{base: srv.URL, token: "test"}, nil }
	restarted.reload()
	drainChange(t, restarted)
	files, err = os.ReadDir(dir)
	if err != nil || len(files) != 0 || restarted.Counts().Writes != 1 {
		t.Fatalf("files=%v err=%v counts=%+v", files, err, restarted.Counts())
	}
}

func TestHKChangeOutboxReplaysFindingsInOrder(t *testing.T) {
	var mu sync.Mutex
	fail := true
	var times []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			http.Error(w, "temporary", 503)
			return
		}
		var b struct {
			Findings struct {
				ObservedAt time.Time `json:"observed_at"`
			} `json:"findings"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		times = append(times, b.Findings.ObservedAt)
		fmt.Fprint(w, `{"sections":{"findings":{"applied":true}}}`)
	}))
	defer srv.Close()
	dir := filepath.Join(t.TempDir(), "outbox")
	w := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.outbox = dir
	w.endpoint = func() (*hkJobs, error) { return &hkJobs{base: srv.URL, token: "test"}, nil }
	at := time.Now().UTC()
	w.Queue("o/r", 7, "findings", ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
	drainChange(t, w)
	later := at.Add(time.Second)
	w.Queue("o/r", 7, "findings", ChangeSection{At: later, Value: map[string]any{"observed_at": later}})
	drainChange(t, w)
	mu.Lock()
	fail = false
	mu.Unlock()
	restarted := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	restarted.outbox = dir
	restarted.endpoint = w.endpoint
	restarted.reload()
	drainChange(t, restarted)
	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 || !times[0].Equal(at) || !times[1].Equal(later) {
		t.Fatalf("times=%v", times)
	}
}

// A rejected change batch or section logs hk's reason with no run of 32 or
// more hex digits.
func TestHKChangeRejectRedactsHexRuns(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid_context","reason":"bad credential `+hex64+`"}`)
	}))
	t.Cleanup(server.Close)
	var logs bytes.Buffer
	w := NewChangeWriter(slog.New(slog.NewTextHandler(&logs, nil)))
	w.endpoint = func() (*hkJobs, error) { return &hkJobs{base: server.URL, token: "test"}, nil }
	at := time.Now().UTC()
	w.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
	drainChange(t, w)
	if !strings.Contains(logs.String(), "reason=") || !strings.Contains(logs.String(), "[redacted]") || hexRunLong.MatchString(logs.String()) {
		t.Fatalf("logs=%s", logs.String())
	}
}
