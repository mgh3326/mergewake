package integrations

import (
	"encoding/json"
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

// claimHK is an httptest hk for /v1/changes/batch that counts requests by the
// marker in each observation. holdFirst blocks the first request after it is
// counted, until open is called.
type claimHK struct {
	srv        *httptest.Server
	holdFirst  bool
	arrived    chan struct{}
	hold       chan struct{}
	arriveOnce sync.Once
	openOnce   sync.Once

	mu       sync.Mutex
	failing  bool
	requests map[string]int
	applied  map[string]int
}

func newClaimHK(t *testing.T, holdFirst, failing bool) *claimHK {
	t.Helper()
	h := &claimHK{holdFirst: holdFirst, failing: failing, arrived: make(chan struct{}), hold: make(chan struct{}), requests: map[string]int{}, applied: map[string]int{}}
	h.srv = httptest.NewServer(h)
	t.Cleanup(h.srv.Close)
	t.Cleanup(h.open)
	return h
}

func (h *claimHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		State struct {
			Marker string `json:"marker"`
		} `json:"state"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	marker := body.State.Marker
	h.mu.Lock()
	h.requests[marker]++
	failing := h.failing
	h.mu.Unlock()
	if h.holdFirst {
		first := false
		h.arriveOnce.Do(func() { first = true; close(h.arrived) })
		if first {
			<-h.hold
		}
	}
	if failing {
		http.Error(w, "temporary", http.StatusServiceUnavailable)
		return
	}
	h.mu.Lock()
	h.applied[marker]++
	h.mu.Unlock()
	fmt.Fprint(w, `{"sections":{"state":{"applied":true}}}`)
}

func (h *claimHK) open() { h.openOnce.Do(func() { close(h.hold) }) }

func (h *claimHK) setFailing(v bool) {
	h.mu.Lock()
	h.failing = v
	h.mu.Unlock()
}

func (h *claimHK) waitHeld(t *testing.T) {
	t.Helper()
	select {
	case <-h.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("hk never received the held request")
	}
}

// count returns the requests received and the requests applied for marker.
func (h *claimHK) count(marker string) (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.requests[marker], h.applied[marker]
}

// claimWriter is a writer over dir's outbox. Like the existing replay tests it
// sets outbox and endpoint before reload, so no flush timer reads a stale endpoint.
func claimWriter(dir, url string) *ChangeWriter {
	w := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.outbox = filepath.Join(dir, "hk-change-outbox")
	w.endpoint = func() (*hkJobs, error) { return &hkJobs{base: url, token: "test"}, nil }
	return w
}

func claimObservation(at time.Time, marker string) ChangeSection {
	return ChangeSection{At: at, Value: map[string]any{"observed_at": at, "marker": marker}}
}

func outboxEmpty(t *testing.T, dir string) bool {
	t.Helper()
	files, err := os.ReadDir(filepath.Join(dir, "hk-change-outbox"))
	if err != nil {
		t.Fatal(err)
	}
	return len(files) == 0
}

// A second writer over the same outbox must not send an entry that the first
// writer is still sending.
func TestHKChangeOutboxHeldEntryIsNotResent(t *testing.T) {
	hk := newClaimHK(t, true, false)
	dir := t.TempDir()
	a := claimWriter(dir, hk.srv.URL)
	at := time.Now().UTC()
	a.Queue("o/r", 7, "state", claimObservation(at, "a"))
	hk.waitHeld(t)
	b := claimWriter(dir, hk.srv.URL)
	b.reload()
	drainChange(t, b)
	if req, _ := hk.count("a"); req != 1 {
		t.Fatalf("writer B sent an entry A holds: requests=%d", req)
	}
	hk.open()
	drainChange(t, a)
	req, applied := hk.count("a")
	t.Logf("requests for a=%d applied=%d", req, applied)
	if req != 1 || applied != 1 || !outboxEmpty(t, dir) {
		t.Fatalf("requests=%d applied=%d outbox empty=%v", req, applied, outboxEmpty(t, dir))
	}
}

// A failed send keeps the entry claimed by the writer that failed. A second
// writer must skip it, and the first writer's retry sends it once.
func TestHKChangeOutboxFailedSendKeepsClaim(t *testing.T) {
	hk := newClaimHK(t, false, true)
	dir := t.TempDir()
	a := claimWriter(dir, hk.srv.URL)
	at := time.Now().UTC()
	a.Queue("o/r", 7, "state", claimObservation(at, "b"))
	drainChange(t, a)
	b := claimWriter(dir, hk.srv.URL)
	b.reload()
	drainChange(t, b)
	if req, _ := hk.count("b"); req != 1 {
		t.Fatalf("writer B resent a failed entry A holds: requests=%d", req)
	}
	hk.setFailing(false)
	// Same state the retry timer leaves: the unavailable window has passed.
	a.mu.Lock()
	a.unavailableUntil = time.Time{}
	a.mu.Unlock()
	a.reload()
	drainChange(t, a)
	req, applied := hk.count("b")
	t.Logf("requests for b=%d applied=%d", req, applied)
	if req != 2 || applied != 1 || !outboxEmpty(t, dir) {
		t.Fatalf("requests=%d applied=%d outbox empty=%v", req, applied, outboxEmpty(t, dir))
	}
}

// A writer that exits without closing its claim leaves the entry for the next
// replay, which sends it once and removes it.
func TestHKChangeOutboxClaimEndsWithProcess(t *testing.T) {
	hk := newClaimHK(t, false, true)
	dir := t.TempDir()
	a := claimWriter(dir, hk.srv.URL)
	at := time.Now().UTC()
	a.Queue("o/r", 7, "state", claimObservation(at, "c"))
	drainChange(t, a)
	abandonClaims(a)
	hk.setFailing(false)
	b := claimWriter(dir, hk.srv.URL)
	b.reload()
	drainChange(t, b)
	req, applied := hk.count("c")
	if req != 2 || applied != 1 || !outboxEmpty(t, dir) {
		t.Fatalf("requests=%d applied=%d outbox empty=%v", req, applied, outboxEmpty(t, dir))
	}
}
