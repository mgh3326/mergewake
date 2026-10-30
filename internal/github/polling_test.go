package github

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestConditionalGetAndMutationInvalidation(t *testing.T) {
	gets := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(201)
			return
		}
		gets++
		if gets == 2 {
			if r.Header.Get("If-None-Match") != `"a"` {
				t.Error("missing conditional header")
			}
			w.WriteHeader(304)
			return
		}
		if gets == 3 && r.Header.Get("If-None-Match") != "" {
			t.Error("mutation left stale cache")
		}
		w.Header().Set("ETag", `"a"`)
		w.Write([]byte(`{"value":1}`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	for i := 0; i < 2; i++ {
		b, e := c.request(context.Background(), "GET", "/value", "application/json", nil)
		if e != nil || string(b) != `{"value":1}` {
			t.Fatalf("%s %v", b, e)
		}
	}
	if _, e := c.request(context.Background(), "POST", "/mutation", "application/json", []byte(`{}`)); e != nil {
		t.Fatal(e)
	}
	if _, e := c.request(context.Background(), "GET", "/value", "application/json", nil); e != nil {
		t.Fatal(e)
	}
}
func TestRateLimitAndCancellation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", "4102444800")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	if _, e := c.request(context.Background(), "GET", "/limited", "application/json", nil); e == nil {
		t.Fatal("rate error hidden")
	}
	if c.RateWait() < time.Second {
		t.Fatal("rate limit not retained")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, e := c.request(ctx, "GET", "/other", "application/json", nil); e == nil {
		t.Fatal("throttle ignored cancellation")
	}
	if calls != 1 {
		t.Fatal("requests continued while rate limited")
	}
}
func TestThrottleWaitIsLogged(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/limited":
			w.WriteHeader(403)
		case "/paced":
			w.Header().Set("X-RateLimit-Remaining", "3")
			w.Header().Set("X-RateLimit-Reset", "4102444800")
			w.Write([]byte(`{}`))
		case "/interval":
			w.Header().Set("X-Poll-Interval", "30")
			w.Write([]byte(`{}`))
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()

	buf := &bytes.Buffer{}
	c := New("")
	c.BaseURL = server.URL
	c.Stderr = buf

	// A 403 with no headers backs off a minute; the next request logs the wait.
	if _, e := c.request(context.Background(), "GET", "/limited", "application/json", nil); e == nil {
		t.Fatal("403 hidden")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, _ = c.request(ctx, "GET", "/other", "application/json", nil)
	cancel()
	if line := buf.String(); !strings.Contains(line, "403-429 backoff") || !strings.Contains(line, "HTTP 403") || !strings.Contains(line, "until 2") {
		t.Fatalf("log %q", line)
	}

	// Rate-limit pacing on a 2xx response logs its own reason and status.
	buf.Reset()
	c = New("")
	c.BaseURL = server.URL
	c.Stderr = buf
	if _, e := c.request(context.Background(), "GET", "/paced", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, _ = c.request(ctx, "GET", "/other", "application/json", nil)
	cancel()
	if line := buf.String(); !strings.Contains(line, "rate-limit pacing") || !strings.Contains(line, "HTTP 200") {
		t.Fatalf("log %q", line)
	}

	// Poll intervals throttle per request key and log the interval's status.
	buf.Reset()
	c = New("")
	c.BaseURL = server.URL
	c.Stderr = buf
	c.Polling = true
	if _, e := c.request(context.Background(), "GET", "/interval", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, _ = c.request(ctx, "GET", "/interval", "application/json", nil)
	cancel()
	if line := buf.String(); !strings.Contains(line, "poll interval") || !strings.Contains(line, "HTTP 200") {
		t.Fatalf("log %q", line)
	}
	// A different key is not held back by another key's poll interval.
	buf.Reset()
	if _, e := c.request(context.Background(), "GET", "/free", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if buf.Len() != 0 {
		t.Fatalf("log %q", buf.String())
	}
}
func TestPollIntervalOnlyInPollingLoops(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/interval":
			w.Header().Set("X-Poll-Interval", "30")
		case "/retry":
			w.Header().Set("Retry-After", "30")
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	// A one-shot client records a poll interval but never waits on it.
	c := New("")
	c.BaseURL = server.URL
	buf := &bytes.Buffer{}
	c.Stderr = buf
	for i := 0; i < 2; i++ {
		if _, e := c.request(context.Background(), "GET", "/interval", "application/json", nil); e != nil {
			t.Fatal(e)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("one-shot logged a throttle wait: %q", buf.String())
	}

	// A polling client waits on the same interval.
	c = New("")
	c.BaseURL = server.URL
	c.Polling = true
	if _, e := c.request(context.Background(), "GET", "/interval", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, e := c.request(ctx, "GET", "/interval", "application/json", nil); e == nil {
		t.Fatal("polling client ignored poll interval")
	}

	// Retry-After still throttles one-shot clients.
	c = New("")
	c.BaseURL = server.URL
	if _, e := c.request(context.Background(), "GET", "/retry", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, e := c.request(ctx, "GET", "/other", "application/json", nil); e == nil {
		t.Fatal("one-shot client ignored Retry-After")
	}

	// And it throttles polling clients too.
	c = New("")
	c.BaseURL = server.URL
	c.Polling = true
	if _, e := c.request(context.Background(), "GET", "/retry", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, e := c.request(ctx, "GET", "/other", "application/json", nil); e == nil {
		t.Fatal("polling client ignored Retry-After")
	}
}

// A key inside its X-Poll-Interval window replays the cached response when one
// is held and is skipped otherwise; either way requests for other keys are
// not serialised behind the deadline.
func TestPollIntervalServesCacheOrSkips(t *testing.T) {
	gets := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gets[r.URL.Path]++
		switch r.URL.Path {
		case "/cached":
			w.Header().Set("ETag", `"e"`)
			w.Header().Set("X-Poll-Interval", "300")
			w.Write([]byte(`{"v":1}`))
		case "/uncached":
			w.Header().Set("X-Poll-Interval", "300")
			w.Write([]byte(`{"v":2}`))
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	c.Stderr = io.Discard

	for _, path := range []string{"/cached", "/uncached"} {
		if _, e := c.request(context.Background(), "GET", path, "application/json", nil); e != nil {
			t.Fatal(path, e)
		}
	}
	b, e := c.request(context.Background(), "GET", "/cached", "application/json", nil)
	if e != nil || string(b) != `{"v":1}` {
		t.Fatalf("%s %v", b, e)
	}
	if _, e := c.request(context.Background(), "GET", "/uncached", "application/json", nil); !errors.Is(e, ErrPollSkipped) {
		t.Fatalf("skip = %v", e)
	}
	if _, e := c.request(context.Background(), "GET", "/other", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if gets["/cached"] != 1 || gets["/uncached"] != 1 || gets["/other"] != 1 {
		t.Fatalf("gets %v", gets)
	}

	// Without Polling the interval is still ignored for one-shot commands.
	c.Polling = false
	if _, e := c.request(context.Background(), "GET", "/uncached", "application/json", nil); e != nil {
		t.Fatal(e)
	}
}
