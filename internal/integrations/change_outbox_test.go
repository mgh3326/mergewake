package integrations

import (
	"fmt"
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

// changeEnv points hkJobEndpoint at base for writers built without an
// explicit endpoint, the way TestHKChangeOutboxReplaysAfterFailure does.
func changeEnv(t *testing.T, base string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", base)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
}

// Two ChangeWriters over one state dir stand in for two processes: the
// outbox entry's flock conflicts between their open files.
func TestHKChangeOutboxSingleSender(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		held := requests == 1
		mu.Unlock()
		if held {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		fmt.Fprint(w, `{"sections":{"state":{"applied":true}}}`)
	}))
	defer srv.Close()
	changeEnv(t, srv.URL)
	dir := t.TempDir()
	a := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), dir)
	at := time.Now().UTC()
	a.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at, "source": "webhook"}})
	<-entered
	// B starts while A's send is held at hk: it must not send A's entry.
	b := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), dir)
	drainChange(t, b)
	close(release)
	drainChange(t, a)
	mu.Lock()
	n := requests
	mu.Unlock()
	if n != 1 {
		t.Fatalf("hk requests=%d, want the observation sent once", n)
	}
	if files, err := os.ReadDir(filepath.Join(dir, "hk-change-outbox")); err != nil || len(files) != 0 {
		t.Fatalf("outbox files=%v err=%v", files, err)
	}
}

func TestHKChangeOutboxClaimOutlivesFailedSend(t *testing.T) {
	var mu sync.Mutex
	fail := true
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		f := fail
		mu.Unlock()
		if f {
			http.Error(w, "temporary", 503)
			return
		}
		fmt.Fprint(w, `{"sections":{"state":{"applied":true}}}`)
	}))
	defer srv.Close()
	changeEnv(t, srv.URL)
	dir := t.TempDir()
	a := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), dir)
	at := time.Now().UTC()
	a.Queue("o/r", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at, "source": "webhook"}})
	drainChange(t, a)
	// A's send failed and A stays alive: it still holds the claim, so a
	// second writer over the same dir must not send the entry.
	b := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), dir)
	drainChange(t, b)
	mu.Lock()
	n := requests
	mu.Unlock()
	if n != 1 {
		t.Fatalf("hk requests=%d while A alive, want 1", n)
	}
	// A's next retry (the backoff elapsed, then the retry path's reload)
	// sends the entry once.
	mu.Lock()
	fail = false
	mu.Unlock()
	a.mu.Lock()
	a.unavailableUntil = time.Time{}
	a.mu.Unlock()
	a.reload()
	drainChange(t, a)
	mu.Lock()
	n = requests
	mu.Unlock()
	if n != 2 {
		t.Fatalf("hk requests=%d after A's retry, want 2", n)
	}
	if files, err := os.ReadDir(filepath.Join(dir, "hk-change-outbox")); err != nil || len(files) != 0 {
		t.Fatalf("outbox files=%v err=%v", files, err)
	}
}
