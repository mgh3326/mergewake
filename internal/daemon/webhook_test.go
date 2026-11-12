package daemon

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

var testSecret = []byte("test-webhook-secret")

func sign(body []byte, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// post sends one delivery; a nil secret leaves the signature header off and an
// empty secret signs with a wrong key.
func post(t *testing.T, client *http.Client, url, event, id string, body, secret []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if secret != nil {
		req.Header.Set("X-Hub-Signature-256", sign(body, secret))
	}
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", id)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func webhookDaemon(triggered *[]string) Daemon {
	var mu sync.Mutex
	return Daemon{
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Webhook: &Webhook{
			Path:   "/github",
			Secret: testSecret,
			Repos:  map[string]bool{"o/r": true},
			Trigger: func(role string) bool {
				mu.Lock()
				defer mu.Unlock()
				*triggered = append(*triggered, role)
				return true
			},
		},
	}
}

func TestWebhookDaemonServesAndStopsWithContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a",
		Webhook: &Webhook{Listen: addr, Path: "/github", Secret: testSecret, Repos: map[string]bool{"o/r": true}}}
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "watch", Interval: time.Hour, Run: func(context.Context) error {
			started <- struct{}{}
			return nil
		}}})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("loop did not start")
	}
	resp := post(t, http.DefaultClient, "http://"+addr+"/github", "ping", "d-1", []byte(`{}`), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ping status=%d", resp.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Run waited on the receiver goroutines: the port is free again and a
	// fresh daemon can bind it, proving nothing is left serving.
	probe, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("receiver still bound after cancel: %v", err)
	}
	probe.Close()
}

func TestWebhookRequestHandling(t *testing.T) {
	var triggered []string
	d := webhookDaemon(&triggered)
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	body := []byte(`{"repository":{"full_name":"o/r"}}`)

	resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", "d-2", body, testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || !slices.Equal(triggered, []string{"pr", "watch"}) {
		t.Fatalf("valid delivery: status=%d triggered=%v", resp.StatusCode, triggered)
	}

	triggered = nil
	resp = post(t, srv.Client(), srv.URL+"/github", "pull_request", "d-3", body, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || len(triggered) != 0 {
		t.Fatalf("missing signature: status=%d triggered=%v", resp.StatusCode, triggered)
	}
	resp = post(t, srv.Client(), srv.URL+"/github", "pull_request", "d-4", body, []byte("wrong"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || len(triggered) != 0 {
		t.Fatalf("wrong signature: status=%d triggered=%v", resp.StatusCode, triggered)
	}

	resp, err := srv.Client().Get(srv.URL + "/github")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status=%d", resp.StatusCode)
	}

	resp = post(t, srv.Client(), srv.URL+"/other", "pull_request", "d-5", body, testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong path status=%d", resp.StatusCode)
	}

	big := append(bytes.Repeat([]byte("x"), MaxWebhookBody+1), ' ')
	resp = post(t, srv.Client(), srv.URL+"/github", "pull_request", "d-6", big, testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge || len(triggered) != 0 {
		t.Fatalf("oversized body: status=%d triggered=%v", resp.StatusCode, triggered)
	}
}

func TestWebhookEventRoles(t *testing.T) {
	for _, tt := range []struct {
		event string
		roles []string
	}{
		{"pull_request", []string{"pr", "watch"}},
		{"pull_request_review", []string{"pr", "watch"}},
		{"pull_request_review_thread", []string{"pr", "watch"}},
		// A check or status delivery wakes watch, never the full pr sweep; the
		// PR it names is evaluated from the queue. These payloads name no
		// branch, so watch cannot be ruled out.
		{"check_suite", []string{"watch"}},
		{"check_run", []string{"watch"}},
		{"status", []string{"watch"}},
		{"release", []string{"release", "rollout"}},
		{"workflow_run", []string{"release", "rollout"}},
		{"push", []string{"release", "rollout"}},
		{"ping", nil},
		{"repository", nil},
	} {
		var triggered []string
		d := webhookDaemon(&triggered)
		srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
		body := []byte(`{"repository":{"full_name":"o/r"}}`)
		resp := post(t, srv.Client(), srv.URL+"/github", tt.event, "d-"+tt.event, body, testSecret)
		resp.Body.Close()
		srv.Close()
		if resp.StatusCode != http.StatusAccepted || !slices.Equal(triggered, tt.roles) {
			t.Fatalf("event %s: status=%d triggered=%v want %v", tt.event, resp.StatusCode, triggered, tt.roles)
		}
	}
}

func TestWebhookDisabledRolesAndUnlistedRepo(t *testing.T) {
	var triggered []string
	var mu sync.Mutex
	d := Daemon{
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Webhook: &Webhook{
			Path:   "/github",
			Secret: testSecret,
			Repos:  map[string]bool{"o/r": true},
			Trigger: func(role string) bool {
				if role == "watch" {
					return false // disabled role: no wake channel
				}
				mu.Lock()
				defer mu.Unlock()
				triggered = append(triggered, role)
				return true
			},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", "d-7", []byte(`{"repository":{"full_name":"o/r"}}`), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || !slices.Equal(triggered, []string{"pr"}) {
		t.Fatalf("disabled role: status=%d triggered=%v", resp.StatusCode, triggered)
	}
	triggered = nil
	resp = post(t, srv.Client(), srv.URL+"/github", "push", "d-8", []byte(`{"repository":{"full_name":"other/repo"}}`), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || len(triggered) != 0 {
		t.Fatalf("unlisted repo: status=%d triggered=%v", resp.StatusCode, triggered)
	}
}

func TestWebhookLogRedaction(t *testing.T) {
	var buf bytes.Buffer
	d := Daemon{
		Log: slog.New(slog.NewJSONHandler(&buf, nil)),
		Webhook: &Webhook{
			Path:    "/github",
			Secret:  []byte("MARKER-SECRET"),
			Repos:   map[string]bool{"o/r": true},
			Trigger: func(string) bool { return true },
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	body := []byte(`{"repository":{"full_name":"o/r"},"marker":"MARKER-BODY"}`)
	resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", "delivery-id-1", body, []byte("MARKER-SECRET"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	out := buf.String()
	if !strings.Contains(out, "pull_request") || !strings.Contains(out, "delivery-id-1") {
		t.Fatalf("log missing event or delivery id: %s", out)
	}
	for _, marker := range []string{"MARKER-BODY", "MARKER-SECRET", "sha256="} {
		if strings.Contains(out, marker) {
			t.Fatalf("log leaks %s: %s", marker, out)
		}
	}
}

// The form content type carries the JSON in the payload field; the signature
// still covers the raw body.
func TestWebhookFormEncodedPayload(t *testing.T) {
	var triggered []string
	d := webhookDaemon(&triggered)
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	body := []byte("payload=" + url.QueryEscape(`{"repository":{"full_name":"o/r"}}`))
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/github", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Hub-Signature-256", sign(body, testSecret))
	req.Header.Set("X-GitHub-Event", "push")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || !slices.Equal(triggered, []string{"release", "rollout"}) {
		t.Fatalf("form delivery: status=%d triggered=%v", resp.StatusCode, triggered)
	}
}

func TestWebhookInvalidatesEvidenceBeforeWake(t *testing.T) {
	var triggered []string
	d := webhookDaemon(&triggered)
	invalidated := ""
	d.Webhook.Invalidate = func(repo string) { invalidated = repo }
	d.Webhook.Trigger = func(role string) bool {
		if invalidated != "o/r" {
			t.Error("wake preceded invalidation")
		}
		return true
	}
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	resp := post(t, srv.Client(), srv.URL+"/github", "check_run", "evidence", []byte(`{"repository":{"full_name":"o/r"}}`), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || invalidated != "o/r" {
		t.Fatalf("status=%d invalidated=%q", resp.StatusCode, invalidated)
	}
}

func TestWebhookKeepsDeliveryStateAndSurvivesMalformedPayloads(t *testing.T) {
	var triggered []string
	d := webhookDaemon(&triggered)
	logs := &syncBuffer{}
	d.Log = slog.New(slog.NewJSONHandler(logs, nil))
	state := webhookstate.Open(t.TempDir())
	d.Webhook.State = state
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	send := func(event, id, body string) int {
		resp := post(t, srv.Client(), srv.URL+"/github", event, id, []byte(body), testSecret)
		resp.Body.Close()
		return resp.StatusCode
	}
	pr := func(head, updated string) string {
		return `{"action":"synchronize","repository":{"full_name":"o/r"},"pull_request":{"number":7,"state":"open","head":{"sha":"` + head + `","ref":"t"},"base":{"sha":"b","ref":"main"},"updated_at":"` + updated + `"}}`
	}

	if code := send("pull_request", "d1", pr("h1", "2026-11-06T00:01:00Z")); code != http.StatusAccepted {
		t.Fatalf("status=%d", code)
	}
	if got, ok := state.PR("o/r", 7); !ok || got.HeadSHA != "h1" {
		t.Fatalf("delivery not kept: %+v", got)
	}
	// An older delivery arriving later does not overwrite, a repeated id is a no-op.
	send("pull_request", "d0", pr("h0", "2026-11-06T00:00:00Z"))
	send("pull_request", "d1", pr("h2", "2026-11-06T00:02:00Z"))
	if got, _ := state.PR("o/r", 7); got.HeadSHA != "h1" {
		t.Fatalf("head = %q, want h1", got.HeadSHA)
	}

	// (h) malformed payloads answer as before and still wake the roles they name.
	triggered = nil
	for i, tt := range []struct{ event, body string }{
		{"pull_request", `{"repository":{"full_name":"o/r"},"pull_request":{"number":7,"updated_at":"never"}}`},
		{"check_run", `{"repository":{"full_name":"o/r"},"check_run":{"id":"not a number"}}`},
		{"check_run", `{"repository":{"full_name":"o/r"},"check_run":{"id":"again"}}`},
		{"pull_request_review", `{"repository":{"full_name":"o/r"}}`},
	} {
		if code := send(tt.event, "bad"+string(rune('0'+i)), tt.body); code != http.StatusAccepted {
			t.Fatalf("%s: status=%d", tt.event, code)
		}
	}
	if got, want := triggered, []string{"pr", "watch", "watch", "watch", "pr", "watch"}; !slices.Equal(got, want) {
		t.Fatalf("malformed deliveries woke %v, want %v", got, want)
	}
	if got, _ := state.PR("o/r", 7); got.HeadSHA != "h1" {
		t.Fatalf("a malformed delivery changed the state: %+v", got)
	}
	warned := strings.Count(logs.String(), "webhook state ignored a delivery")
	if warned != 3 {
		t.Fatalf("parse errors logged %d times, want once per event type (3)\n%s", warned, logs.String())
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// (g, daemon side) a delivery kept just before shutdown is on disk after Run returns.
func TestWebhookStateIsWrittenOnShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a", Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Webhook: &Webhook{Listen: addr, Path: "/github", Secret: testSecret, Repos: map[string]bool{"o/r": true}, State: webhookstate.Open(dir)}}
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "watch", Interval: time.Hour, Run: func(context.Context) error {
			started <- struct{}{}
			return nil
		}}})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("loop did not start")
	}
	body := `{"repository":{"full_name":"o/r"},"pull_request":{"number":7,"state":"open","head":{"sha":"h1","ref":"t"},"base":{"sha":"b","ref":"main"},"updated_at":"2026-11-06T00:01:00Z"}}`
	resp := post(t, http.DefaultClient, "http://"+addr+"/github", "pull_request", "d-1", []byte(body), testSecret)
	resp.Body.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if pr, ok := webhookstate.Open(dir).PR("o/r", 7); !ok || pr.HeadSHA != "h1" {
		t.Fatalf("state after shutdown = %+v (found %v)", pr, ok)
	}
}
