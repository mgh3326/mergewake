package daemon

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// Covers brief (c): groups by UTC day of delivered_at and event; deliveries
// outside [from, until) or before the earliest receipt are not judged at all.
func TestDeliveryComparisonGroupsAndWindow(t *testing.T) {
	ds := []github.AppDelivery{
		{GUID: "g1", Event: "pull_request", DeliveredAt: at("2026-11-12T02:00:00Z"), StatusCode: 200},
		{GUID: "g2", Event: "pull_request", DeliveredAt: at("2026-11-12T03:00:00Z"), StatusCode: 500},
		{GUID: "g3", Event: "check_run", DeliveredAt: at("2026-11-12T23:00:00Z"), StatusCode: 200, Redelivery: true},
		{GUID: "g4", Event: "check_run", DeliveredAt: at("2026-11-13T01:00:00Z"), StatusCode: 200},
		{GUID: "g5", Event: "push", DeliveredAt: at("2026-11-13T02:00:00Z"), StatusCode: 404},
		{GUID: "g6", Event: "push", DeliveredAt: at("2026-11-13T05:00:00Z"), StatusCode: 200},
		{GUID: "g7", Event: "push", DeliveredAt: at("2026-11-12T00:10:00Z"), StatusCode: 200},
		{GUID: "g8", Event: "push", DeliveredAt: at("2026-11-12T01:00:00Z"), StatusCode: 200},
	}
	received := map[string]bool{"g1": true, "g2": true, "g3": true}
	from := at("2026-11-12T00:30:00Z")
	until := at("2026-11-13T04:00:00Z")
	earliest := at("2026-11-12T01:30:00Z")

	got := compareDeliveries(ds, received, earliest, from, until)

	want := map[string]map[string]DeliveryCounts{
		"2026-11-12": {
			"pull_request": {Sent: 2, Failed: 1},
			"check_run":    {Sent: 1, Redelivery: 1},
		},
		"2026-11-13": {
			"check_run": {Sent: 1, Missing: 1},
			"push":      {Sent: 1, Failed: 1, Missing: 1},
		},
	}
	if !reflect.DeepEqual(got.Groups, want) {
		t.Fatalf("groups = %+v\nwant %+v", got.Groups, want)
	}
	if got.MissingTotal != 2 || len(got.Missing) != 2 {
		t.Fatalf("missing = %d listed %d; want 2", got.MissingTotal, len(got.Missing))
	}
}

// Covers brief (c): at most 20 missing deliveries are listed; the total is
// still exact.
func TestDeliveryComparisonCapsMissingList(t *testing.T) {
	var ds []github.AppDelivery
	for i := range 25 {
		ds = append(ds, github.AppDelivery{
			GUID: fmt.Sprintf("m%d", i), Event: "push",
			DeliveredAt: at("2026-11-12T02:00:00Z"), StatusCode: 200,
		})
	}
	got := compareDeliveries(ds, map[string]bool{}, at("2026-11-12T01:00:00Z"),
		at("2026-11-12T00:00:00Z"), at("2026-11-12T10:00:00Z"))
	if got.MissingTotal != 25 || len(got.Missing) != 20 {
		t.Fatalf("missing total = %d listed = %d; want 25 and 20", got.MissingTotal, len(got.Missing))
	}
}

type fakeDeliverySource struct {
	page   github.DeliveryPage
	err    error
	sinces []time.Time
}

func (f *fakeDeliverySource) AppHookDeliveries(_ context.Context, since time.Time) (github.DeliveryPage, error) {
	f.sinces = append(f.sinces, since)
	return f.page, f.err
}

func writeReceiptFile(t *testing.T, dir string, day string, rows ...string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, day+".jsonl"), []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Covers brief (d): a failed request logs nothing into state and leaves the
// saved until where it was; the retry uses the same window start.
func TestDeliveryCheckFailureLeavesUntilUnchanged(t *testing.T) {
	now := at("2026-11-13T10:00:00Z")
	dir := t.TempDir()
	receipts := filepath.Join(dir, "receipts")
	writeReceiptFile(t, receipts, "2026-11-13",
		`{"at":"2026-11-13T08:00:00Z","guid":"r1","event":"push","repo":"o/r","hook_id":1}`)
	statePath := filepath.Join(dir, "webhook-delivery-check.json")
	src := &fakeDeliverySource{err: errors.New("GitHub App request failed")}
	check := &DeliveryCheck{
		Source: src, Receipts: &Receipts{Dir: receipts}, StatePath: statePath,
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil)), now: func() time.Time { return now },
	}

	if err := check.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce returned nil on a failed request")
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("state written after a failed run: %v", err)
	}

	src.err = nil
	if err := check.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(src.sinces) != 2 || !src.sinces[0].Equal(src.sinces[1]) {
		t.Fatalf("window starts = %v; want the failed run's start reused", src.sinces)
	}
	if !src.sinces[0].Equal(at("2026-11-13T08:00:00Z")) {
		t.Fatalf("window start = %v; want the earliest receipt", src.sinces[0])
	}
	b, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		Until time.Time `json:"until"`
	}
	if err := json.Unmarshal(b, &st); err != nil || !st.Until.Equal(now.Add(-10*time.Minute)) {
		t.Fatalf("state = %s; want until = now-10m", b)
	}
}

// Covers brief (e): the captured log and state file carry neither the App
// JWT nor the user token. The real client signs with a generated key.
func TestDeliveryCheckLogAndStateHoldNoSecrets(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const userToken = "user-token-secret-marker"
	var seenAuth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = append(seenAuth, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"id":2,"guid":"lost","delivered_at":"2026-11-13T08:45:00Z","event":"push","action":"","redelivery":false,"status_code":500},`+
			`{"id":1,"guid":"r1","delivered_at":"2026-11-13T08:30:00Z","event":"push","action":"","redelivery":false,"status_code":200}]`)
	}))
	defer srv.Close()

	client := github.New(userToken)
	client.BaseURL, client.UploadURL, client.HTTP = srv.URL, srv.URL, srv.Client()
	client.EnableApp(4242, key)

	dir := t.TempDir()
	receipts := filepath.Join(dir, "receipts")
	writeReceiptFile(t, receipts, "2026-11-13",
		`{"at":"2026-11-13T08:31:00Z","guid":"r1","event":"push","repo":"o/r","hook_id":1}`)
	statePath := filepath.Join(dir, "webhook-delivery-check.json")
	var logs logSink
	check := &DeliveryCheck{
		Source: client, Receipts: &Receipts{Dir: receipts}, StatePath: statePath,
		Log: slog.New(slog.NewJSONHandler(&logs, nil)),
		now: func() time.Time { return at("2026-11-13T10:00:00Z") },
	}
	if err := check.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(seenAuth) == 0 {
		t.Fatal("no request reached the fake GitHub")
	}
	jwt := strings.TrimPrefix(seenAuth[0], "Bearer ")
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"log": logs.String(), "state": string(state)} {
		if strings.Contains(text, jwt) || strings.Contains(text, userToken) {
			t.Fatalf("%s carries a credential", name)
		}
	}
	if !strings.Contains(logs.String(), "webhook delivery check") || !strings.Contains(logs.String(), `"guid":"lost"`) {
		t.Fatalf("check line missing or lacks the missing GUID: %s", logs.String())
	}
}
