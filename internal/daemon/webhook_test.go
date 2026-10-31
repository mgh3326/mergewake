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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
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
		{"check_suite", []string{"pr", "watch"}},
		{"check_run", []string{"pr", "watch"}},
		{"status", []string{"pr", "watch"}},
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
