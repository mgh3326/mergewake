package integrations

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// dropClaims releases every outbox claim w holds and stops its replay timer,
// simulating the process exit that makes the kernel drop the flocks.
func dropClaims(w *ChangeWriter) {
	w.mu.Lock()
	if w.retryTimer != nil {
		w.retryTimer.Stop()
		w.retryTimer = nil
	}
	claims := w.claims
	w.claims = nil
	w.mu.Unlock()
	for _, unlock := range claims {
		unlock()
	}
}

// An entry whose writer exits without sending it is picked up by the next
// process's replay, sent once and removed.
func TestHKChangeOutboxClaimEndsWithWriter(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.Write([]byte(`{"sections":{"state":{"applied":true}}}`))
	}))
	defer srv.Close()
	changeEnv(t, srv.URL)
	dir := t.TempDir()
	a := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.outbox = filepath.Join(dir, "hk-change-outbox")
	a.endpoint = func() (*hkJobs, error) { return nil, errors.New("endpoint unavailable") }
	at := time.Now().UTC()
	a.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at, "source": "webhook"}})
	drainChange(t, a)
	if files, err := os.ReadDir(a.outbox); err != nil || len(files) != 1 {
		t.Fatalf("outbox files=%v err=%v", files, err)
	}
	dropClaims(a)
	b := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), dir)
	drainChange(t, b)
	mu.Lock()
	n := requests
	mu.Unlock()
	if n != 1 {
		t.Fatalf("hk requests=%d, want 1", n)
	}
	if files, err := os.ReadDir(a.outbox); err != nil || len(files) != 0 {
		t.Fatalf("outbox files=%v err=%v", files, err)
	}
}
