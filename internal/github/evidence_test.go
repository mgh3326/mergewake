package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEvidenceRefreshInvalidationAndFreshDecision(t *testing.T) {
	now := time.Unix(1, 0)
	c := New("")
	c.Polling = true
	c.Now = func() time.Time { return now }
	ctx := context.Background()
	key := evidenceKey{"o/r", "checks", "sha"}
	calls := 0
	read := func() (int, error) { calls++; return calls, nil }
	get := func(ctx context.Context) int {
		t.Helper()
		v, e := cachedEvidence(ctx, c, key, read)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	if get(ctx) != 1 || get(ctx) != 1 || calls != 1 {
		t.Fatalf("steady state fetched %d times", calls)
	}
	if get(FreshEvidence(ctx)) != 2 || calls != 2 {
		t.Fatalf("fresh decision fetched %d times", calls)
	}
	c.InvalidateEvidence("o/r")
	if get(ctx) != 3 {
		t.Fatal("invalidation did not refresh")
	}
	now = now.Add(10 * time.Minute)
	if get(ctx) != 4 {
		t.Fatal("expired evidence did not refresh")
	}
}
func TestEvidenceDoesNotCacheFailuresOrInFlightInvalidation(t *testing.T) {
	c := New("")
	c.Polling = true
	ctx := context.Background()
	key := evidenceKey{"o/r", "threads", "1"}
	calls := 0
	read := func() (int, error) {
		calls++
		if calls == 1 {
			return 0, errors.New("failed")
		}
		if calls == 2 {
			c.InvalidateEvidence("o/r")
		}
		return calls, nil
	}
	for i := 0; i < 4; i++ {
		_, _ = cachedEvidence(ctx, c, key, read)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestCheckEvidenceCountsRequests(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if strings.Contains(r.URL.Path, "check-runs") {
			_, _ = w.Write([]byte(`{"check_runs":[{"id":1,"name":"ci","status":"completed","conclusion":"success"}]}`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	ctx := context.Background()
	read := func(ctx context.Context) {
		t.Helper()
		states, err := c.CheckStates(ctx, "o/r", "abc", []string{"ci"})
		if err != nil || states["ci"] != Green {
			t.Fatalf("states=%v err=%v", states, err)
		}
	}
	read(ctx)
	read(ctx)
	if requests != 2 {
		t.Fatalf("two observations: %d requests, want 2", requests)
	}
	read(FreshEvidence(ctx))
	if requests != 4 {
		t.Fatalf("fresh decision: %d requests, want 4", requests)
	}
	c.InvalidateEvidence("o/r")
	read(ctx)
	if requests != 6 {
		t.Fatalf("invalidated observation: %d requests, want 6", requests)
	}
}

func TestFreshEvidenceBypassesPollWindow(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("X-Poll-Interval", "300")
		w.Header().Set("ETag", `"e"`)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	c.Stderr = io.Discard
	ctx := context.Background()
	for _, use := range []context.Context{ctx, ctx, freshRequest(FreshEvidence(ctx))} {
		if _, err := c.request(use, "GET", "/repos/o/r/pulls/1", "application/json", nil); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("HTTP calls = %d, want 2", calls)
	}
}

func TestEvidenceExpiresDespitePollWindow(t *testing.T) {
	now := time.Unix(1, 0)
	requests := 0
	conclusion := "success"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("X-Poll-Interval", "3600")
		if strings.HasSuffix(r.URL.Path, "/check-runs") {
			fmt.Fprintf(w, `{"check_runs":[{"id":1,"name":"ci","status":"completed","conclusion":%q}]}`, conclusion)
		} else {
			fmt.Fprint(w, `[]`)
		}
	}))
	defer srv.Close()
	c := New("")
	c.BaseURL = srv.URL
	c.Polling = true
	c.Stderr = io.Discard
	c.Now = func() time.Time { return now }
	read := func() CI {
		t.Helper()
		states, err := c.CheckStates(context.Background(), "o/r", "sha", []string{"ci"})
		if err != nil {
			t.Fatal(err)
		}
		return states["ci"]
	}
	if read() != Green {
		t.Fatal("initial state")
	}
	conclusion = "failure"
	if read() != Green || requests != 2 {
		t.Fatalf("cached requests %d", requests)
	}
	now = now.Add(10 * time.Minute)
	if read() != Red || requests != 4 {
		t.Fatalf("expired read did not refresh, requests=%d", requests)
	}
}
