package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
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

// logSink is a log writer the server goroutines and the test share.
type logSink struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// receiptDaemon serves one webhook receiver whose receipts go to dir.
func receiptDaemon(t *testing.T, dir string, logs io.Writer) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(logs, nil))
	d := Daemon{
		Log: log,
		Webhook: &Webhook{
			Path:     "/github",
			Secret:   testSecret,
			Repos:    map[string]bool{"o/r": true},
			Receipts: &Receipts{Dir: dir, Log: log},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	t.Cleanup(srv.Close)
	return srv
}

func readReceipts(t *testing.T, dir string, now time.Time) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, now.UTC().Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("receipt line is not JSON: %v", err)
		}
		out = append(out, m)
	}
	return out
}

// Covers brief (b): a verified delivery writes one line with the Desired 1
// fields, and no body, signature or other header.
func TestVerifiedDeliveryWritesOneReceiptLine(t *testing.T) {
	dir := t.TempDir()
	srv := receiptDaemon(t, dir, io.Discard)
	body := []byte(`{"repository":{"full_name":"o/r"},"marker":"BODY-MARK"}`)
	resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", "guid-1", body, testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	rows := readReceipts(t, dir, time.Now())
	if len(rows) != 1 {
		t.Fatalf("receipt lines = %d, want 1", len(rows))
	}
	row := rows[0]
	for _, key := range []string{"at", "guid", "event", "repo", "hook_id"} {
		if _, ok := row[key]; !ok {
			t.Fatalf("receipt lacks %q: %v", key, row)
		}
	}
	if row["guid"] != "guid-1" || row["event"] != "pull_request" || row["repo"] != "o/r" {
		t.Fatalf("receipt = %v", row)
	}
	if _, ok := row["repeat"]; ok {
		t.Fatalf("first delivery carries repeat: %v", row)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, time.Now().UTC().Format("2006-01-02")+".jsonl"))
	if strings.Contains(string(raw), "BODY-MARK") || strings.Contains(string(raw), "sha256=") {
		t.Fatalf("receipt holds the body or the signature: %s", raw)
	}
}

// Covers brief (b): a redelivered GUID writes a receipt with repeat true.
func TestRepeatDeliveryWritesRepeatReceipt(t *testing.T) {
	dir := t.TempDir()
	srv := receiptDaemon(t, dir, io.Discard)
	body := []byte(`{"repository":{"full_name":"o/r"}}`)
	for range 2 {
		resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", "guid-2", body, testSecret)
		resp.Body.Close()
	}
	rows := readReceipts(t, dir, time.Now())
	if len(rows) != 2 || rows[1]["repeat"] != true {
		t.Fatalf("receipts = %v; want the second line with repeat true", rows)
	}
}

// Covers brief (b) and Desired 1: a failed receipt write never changes the
// 202, and it is logged once per day.
func TestFailingReceiptWriterStillAccepts(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs logSink
	srv := receiptDaemon(t, blocked, &logs)
	body := []byte(`{"repository":{"full_name":"o/r"}}`)
	for i, id := range []string{"guid-a", "guid-b"} {
		resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", id, body, testSecret)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("delivery %d: status = %d, want 202", i, resp.StatusCode)
		}
	}
	if n := strings.Count(logs.String(), "webhook receipt not written"); n != 1 {
		t.Fatalf("receipt failure logged %d times, want once per day: %s", n, logs.String())
	}
}
