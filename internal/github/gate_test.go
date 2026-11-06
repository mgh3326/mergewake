package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gateTestClient builds a client against h, answering the App's installation
// lookup and token mint when withApp is set. The user token is the fake's
// user token and the installation token is "inst-gate-token".
func gateTestClient(t *testing.T, withApp bool, h http.HandlerFunc) *Client {
	t.Helper()
	if withApp {
		inner := h
		h = func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/installation") && r.Method == "GET":
				reply(w, map[string]any{"id": appTestInstall})
				return
			case strings.HasPrefix(r.URL.Path, "/app/installations/") && r.Method == "POST":
				w.WriteHeader(http.StatusCreated)
				reply(w, map[string]any{"token": "inst-gate-token", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
				return
			}
			inner(w, r)
		}
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := New(appTestUserToken)
	c.BaseURL, c.UploadURL, c.HTTP = srv.URL, srv.URL, srv.Client()
	c.Stderr = io.Discard
	if withApp {
		c.EnableApp(appTestAppID, testAppKey(t))
	}
	return c
}

// inFlightSample records the peak number of handler calls in flight and lets
// the run through once want of them overlap, so the peak is not left to
// scheduling luck.
type inFlightSample struct {
	mu    sync.Mutex
	in    int
	peak  int
	want  int
	ready chan struct{}
	once  sync.Once
}

func newInFlightSample(want int) *inFlightSample {
	return &inFlightSample{want: want, ready: make(chan struct{})}
}

func (s *inFlightSample) enter() {
	s.mu.Lock()
	s.in++
	if s.in > s.peak {
		s.peak = s.in
	}
	if s.in >= s.want {
		s.once.Do(func() { close(s.ready) })
	}
	s.mu.Unlock()
	select {
	case <-s.ready:
	case <-time.After(2 * time.Second):
	}
}

func (s *inFlightSample) leave() {
	s.mu.Lock()
	s.in--
	s.mu.Unlock()
}

func (s *inFlightSample) max() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// A credential may have githubConcurrency requests in flight: ten GETs on one
// token do not queue behind a single slot, and all of them complete.
func TestGateAllowsFourInFlightPerCredential(t *testing.T) {
	sample := newInFlightSample(githubConcurrency)
	c := gateTestClient(t, false, func(w http.ResponseWriter, r *http.Request) {
		sample.enter()
		defer sample.leave()
		reply(w, map[string]any{})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := c.request(ctx, "GET", fmt.Sprintf("/value/%d", i), "application/json", nil)
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if got := sample.max(); got != githubConcurrency {
		t.Fatalf("peak requests in flight = %d, want %d", got, githubConcurrency)
	}
}

// The user token and an App installation have separate gates: a user write
// proceeds while the App gate is full.
func TestGatesArePerCredential(t *testing.T) {
	entered := make(chan struct{}, githubConcurrency)
	hold := make(chan struct{})
	var holdOnce sync.Once
	release := func() { holdOnce.Do(func() { close(hold) }) }
	var inFlight atomic.Int32
	var peak atomic.Int32
	c := gateTestClient(t, true, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer inst-") {
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			entered <- struct{}{}
			<-hold
			inFlight.Add(-1)
		}
		reply(w, map[string]any{})
	})
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range githubConcurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = c.request(ctx, "GET", fmt.Sprintf("/repos/o/r/pulls/%d", i+1), "application/json", nil)
		}(i)
	}
	for range githubConcurrency {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("the App gate never let the reads through")
		}
	}
	if got := int(peak.Load()); got != githubConcurrency {
		t.Fatalf("peak App reads in flight = %d, want %d", got, githubConcurrency)
	}
	// A write on the user token must not queue behind the full App gate.
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer writeCancel()
	start := time.Now()
	if _, e := c.request(writeCtx, "POST", "/repos/o/r/pulls/1/reviews", "application/json", []byte(`{}`)); e != nil {
		t.Fatalf("a user write was held behind the App gate: %v", e)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("a user write waited %s behind App reads", d)
	}
	release()
	wg.Wait()
}

// A throttle that one response lands holds later requests of that credential
// only.
func TestThrottleHoldsOnlyItsCredential(t *testing.T) {
	var appHits atomic.Int32
	c := gateTestClient(t, true, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer inst-") && appHits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		reply(w, map[string]any{})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// One App read lands the App credential's throttle.
	if _, e := c.request(ctx, "GET", "/repos/o/r/pulls/1", "application/json", nil); e == nil {
		t.Fatal("a 429 was hidden")
	}
	// The next App read waits it out and never reaches the server.
	short, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	if _, e := c.request(short, "GET", "/repos/o/r/pulls/2", "application/json", nil); e == nil {
		t.Fatal("an App read ignored the App throttle")
	}
	if got := appHits.Load(); got != 1 {
		t.Fatalf("a throttled read reached the server: %d App requests", got)
	}
	// The user token is unaffected.
	start := time.Now()
	if _, e := c.request(ctx, "POST", "/repos/o/r/pulls/1/reviews", "application/json", []byte(`{}`)); e != nil {
		t.Fatalf("a user write was held by the App throttle: %v", e)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("a user write waited %s on the App throttle", d)
	}
}

// A throttle that lands while a request waits for its gate is still honoured:
// the re-check after taking the gate releases it and waits out the deadline.
func TestGateRecheckWaitsOutThrottleLandedWhileQueued(t *testing.T) {
	entered := make(chan struct{}, githubConcurrency+1)
	hold := make(chan struct{})
	limited := make(chan struct{})
	var limitedOnce, holdOnce sync.Once
	releaseLimited := func() { limitedOnce.Do(func() { close(limited) }) }
	releaseHold := func() { holdOnce.Do(func() { close(hold) }) }
	c := gateTestClient(t, false, func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		if r.URL.Path == "/limited" {
			<-limited
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		<-hold
		reply(w, map[string]any{})
	})
	t.Cleanup(func() {
		releaseLimited()
		releaseHold()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Fill the credential's gate: all of these hold a slot inside the handler.
	for i := range githubConcurrency - 1 {
		go func(i int) {
			_, _ = c.request(ctx, "GET", fmt.Sprintf("/hold/%d", i), "application/json", nil)
		}(i)
	}
	go func() { _, _ = c.request(ctx, "GET", "/limited", "application/json", nil) }()
	for range githubConcurrency {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the gate never reached its capacity")
		}
	}
	queued := make(chan error, 1)
	go func() {
		_, e := c.request(ctx, "GET", "/after", "application/json", nil)
		queued <- e
	}()
	time.Sleep(100 * time.Millisecond) // let the request reach the full gate
	// The 429 lands the throttle, and its slot frees. Wait for the throttle to
	// be recorded before freeing any other slot, so the queued request cannot
	// slip through on a slot freed by an earlier request.
	releaseLimited()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.poll.mu.Lock()
		until := c.poll.rate["user"].until
		c.poll.mu.Unlock()
		if until.After(time.Now()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the 429 never landed a throttle")
		}
		time.Sleep(time.Millisecond)
	}
	releaseHold()
	select {
	case e := <-queued:
		t.Fatalf("the queued request passed a throttle that landed while it waited: %v", e)
	case <-time.After(300 * time.Millisecond):
	}
	select {
	case e := <-queued:
		if e != nil {
			t.Fatalf("the queued request failed after the throttle: %v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the queued request never completed after the throttle expired")
	}
}
