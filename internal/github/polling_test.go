package github

import (
	"context"
	"net/http"
	"net/http/httptest"
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
