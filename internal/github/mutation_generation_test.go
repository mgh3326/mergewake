package github

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// overlapServer answers GETs of /repos/{o}/{r}/events with a poll window and an
// ETag, holding each one until release is closed, and counts GETs per path. A
// PUT answers at once.
type overlapServer struct {
	*httptest.Server
	started chan string
	release chan struct{}
	mu      sync.Mutex
	gets    map[string]int
}

func newOverlapServer(t *testing.T) *overlapServer {
	t.Helper()
	s := &overlapServer{started: make(chan string, 8), release: make(chan struct{}), gets: map[string]int{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `{"merged":true}`)
			return
		}
		s.mu.Lock()
		s.gets[r.URL.Path]++
		first := s.gets[r.URL.Path] == 1
		s.mu.Unlock()
		if first {
			s.started <- r.URL.Path
			<-s.release
		}
		w.Header().Set("ETag", `"e"`)
		w.Header().Set("X-Poll-Interval", "300")
		w.Header().Set("X-RateLimit-Remaining", "42")
		io.WriteString(w, `{"before":"write"}`)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *overlapServer) getCount(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[path]
}

func (s *overlapServer) client() *Client {
	c := New("tok")
	c.BaseURL, c.HTTP = s.URL, s.Client()
	c.Polling = true
	c.Stderr = io.Discard
	return c
}

// overlap runs a GET of path while a merge PUT on o/r completes, and returns
// once the GET has returned its (pre-write) body.
func (s *overlapServer) overlap(t *testing.T, c *Client, path string) {
	t.Helper()
	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		b, e := c.request(ctx, "GET", path, "application/json", nil)
		if e == nil && string(b) != `{"before":"write"}` {
			e = io.ErrUnexpectedEOF
		}
		done <- e
	}()
	if got := <-s.started; got != path {
		t.Fatalf("held GET of %s, want %s", got, path)
	}
	if _, e := c.request(ctx, "PUT", "/repos/o/r/pulls/1/merge", "application/json", []byte(`{}`)); e != nil {
		t.Fatal(e)
	}
	close(s.release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}

// A GET in flight while a merge on the same repo completes returns its body to
// its own caller but stores neither the body nor its poll window: both predate
// the write that cleared the repo.
func TestOverlappingGetDoesNotRestoreClearedCache(t *testing.T) {
	s := newOverlapServer(t)
	c := s.client()
	s.overlap(t, c, "/repos/o/r/events")

	key := "GET " + s.URL + "/repos/o/r/events application/json " + credUser
	c.poll.mu.Lock()
	_, cached := c.poll.cache.get(key)
	_, window := c.poll.polls[key]
	c.poll.mu.Unlock()
	if cached || window {
		t.Fatalf("overlapped GET stored cache=%v poll window=%v", cached, window)
	}
	if _, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if n := s.getCount("/repos/o/r/events"); n != 2 {
		t.Fatalf("server saw %d GETs, want 2: the next read must not replay the pre-write body", n)
	}
}

// A GET on another repository is unaffected by the merge and still caches.
func TestOverlappingGetOnOtherRepoStillCaches(t *testing.T) {
	s := newOverlapServer(t)
	c := s.client()
	s.overlap(t, c, "/repos/o/other/events")

	if _, e := c.request(context.Background(), "GET", "/repos/o/other/events", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if n := s.getCount("/repos/o/other/events"); n != 1 {
		t.Fatalf("server saw %d GETs, want 1: the poll window must replay the cached body", n)
	}
}

// The overlapped response's rate-limit headers still count.
func TestOverlappingGetStillObservesRateLimit(t *testing.T) {
	s := newOverlapServer(t)
	c := s.client()
	s.overlap(t, c, "/repos/o/r/events")

	if got := c.RateRemaining()[credUser]; got != 42 {
		t.Fatalf("remaining = %d, want 42 from the overlapped GET", got)
	}
}
