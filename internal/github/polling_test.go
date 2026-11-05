package github

import (
	"bytes"
	"context"
	"fmt"
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
		case "/repos/o/r/events":
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
	if _, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, _ = c.request(ctx, "GET", "/repos/o/r/events", "application/json", nil)
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
	hits := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits[r.URL.Path]++
		switch r.URL.Path {
		case "/repos/o/r/events":
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
		if _, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil); e != nil {
			t.Fatal(e)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("one-shot logged a throttle wait: %q", buf.String())
	}

	// A polling client honours the same interval: the second read inside it
	// is served from the last body and never reaches the server.
	c = New("")
	c.BaseURL = server.URL
	c.Polling = true
	hits["/repos/o/r/events"] = 0
	if _, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if b, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil); e != nil || string(b) != `{}` {
		t.Fatalf("polling replay = %s %v", b, e)
	}
	if hits["/repos/o/r/events"] != 1 {
		t.Fatalf("polling client ignored poll interval: %d server hits", hits["/repos/o/r/events"])
	}

	// Retry-After still throttles one-shot clients.
	c = New("")
	c.BaseURL = server.URL
	if _, e := c.request(context.Background(), "GET", "/retry", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
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
		case "/repos/o/r/events":
			w.Header().Set("ETag", `"e"`)
			w.Header().Set("X-Poll-Interval", "300")
			w.Write([]byte(`{"v":1}`))
		case "/orgs/o/events":
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

	for _, path := range []string{"/repos/o/r/events", "/orgs/o/events"} {
		if _, e := c.request(context.Background(), "GET", path, "application/json", nil); e != nil {
			t.Fatal(path, e)
		}
	}
	b, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil)
	if e != nil || string(b) != `{"v":1}` {
		t.Fatalf("%s %v", b, e)
	}
	// An uncached key inside its poll window replays its last body instead of
	// being skipped: a skip here left PR reviews unstarted round after round.
	if b, e := c.request(context.Background(), "GET", "/orgs/o/events", "application/json", nil); e != nil || string(b) != `{"v":2}` {
		t.Fatalf("replay = %s %v", b, e)
	}
	if _, e := c.request(context.Background(), "GET", "/other", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if gets["/repos/o/r/events"] != 1 || gets["/orgs/o/events"] != 1 || gets["/other"] != 1 {
		t.Fatalf("gets %v", gets)
	}

	// Without Polling the interval is still ignored for one-shot commands.
	c.Polling = false
	if _, e := c.request(context.Background(), "GET", "/orgs/o/events", "application/json", nil); e != nil {
		t.Fatal(e)
	}
}

// A mutation wipes the cache; the cached key's poll window must go with it, or
// the daemon's next read of that key in the same round is skipped forever.
func TestMutationDropsPollWindows(t *testing.T) {
	gets := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			gets++
			w.Header().Set("ETag", `"e"`)
			w.Header().Set("X-Poll-Interval", "300")
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	c.Stderr = io.Discard
	ctx := context.Background()
	if _, e := c.request(ctx, "GET", "/repos/o/r/events", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	if _, e := c.request(ctx, "POST", "/comment", "application/json", []byte(`{}`)); e != nil {
		t.Fatal(e)
	}
	if _, e := c.request(ctx, "GET", "/repos/o/r/events", "application/json", nil); e != nil {
		t.Fatalf("read after mutation = %v", e)
	}
	if gets != 2 {
		t.Fatalf("gets = %d, want 2 (refetch after mutation)", gets)
	}
}

// A replayed no-ETag body is served only to a polling client inside the
// key's live poll window: one-shot clients and expired windows fetch fresh.
func TestPollReplayOnlyInsideLiveWindowForPollingClients(t *testing.T) {
	n := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		w.Header().Set("X-Poll-Interval", "60")
		fmt.Fprintf(w, `{"n":%d}`, n)
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	c.Stderr = io.Discard
	get := func() string {
		b, e := c.request(context.Background(), "GET", "/events", "application/json", nil)
		if e != nil {
			t.Fatal(e)
		}
		return string(b)
	}
	if got := get(); got != `{"n":1}` {
		t.Fatal(got)
	}
	if got := get(); got != `{"n":1}` || n != 1 {
		t.Fatalf("inside the window: %s after %d hits", got, n)
	}
	c.Polling = false
	if got := get(); got != `{"n":2}` {
		t.Fatalf("one-shot client got a replay: %s", got)
	}
	c.Polling = true
	c.poll.mu.Lock()
	for k, w := range c.poll.polls {
		w.until = time.Now().Add(-time.Second)
		c.poll.polls[k] = w
	}
	c.poll.mu.Unlock()
	if got := get(); got != `{"n":3}` {
		t.Fatalf("expired window served a replay: %s", got)
	}
}

// A polling endpoint can replay a 404 inside its window; a mutation drops it.
func TestPollReplaysNotFoundInsideWindow(t *testing.T) {
	n := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			n++
			w.Header().Set("X-Poll-Interval", "60")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	c.Stderr = io.Discard
	for i := 0; i < 2; i++ {
		_, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil)
		if !NotFound(e) {
			t.Fatalf("read %d: want a 404, got %v", i+1, e)
		}
	}
	if n != 1 {
		t.Fatalf("the window was not honoured: %d server hits", n)
	}
	if _, e := c.request(context.Background(), "POST", "/repos/o/r/git/refs", "application/json", []byte(`{}`)); e != nil {
		t.Fatal(e)
	}
	if _, e := c.request(context.Background(), "GET", "/repos/o/r/events", "application/json", nil); !NotFound(e) || n != 2 {
		t.Fatalf("after a mutation the ref must be read again: %v, %d hits", e, n)
	}
}

func TestMutationPreservesOtherRepositoryETag(t *testing.T) {
	var conditional bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/repos/o/other/pulls" && r.Header.Get("If-None-Match") == `"e"` {
			conditional = true
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"e"`)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	ctx := context.Background()
	for _, req := range []struct{ method, route string }{{"GET", "/repos/o/other/pulls"}, {"POST", "/repos/o/changed/issues/1/comments"}, {"GET", "/repos/o/other/pulls"}} {
		if _, err := c.request(ctx, req.method, req.route, "application/json", nil); err != nil {
			t.Fatal(err)
		}
	}
	if !conditional {
		t.Fatal("unrelated repository was fetched without its ETag after mutation")
	}
}

func TestRequestCountsAttributeStatusAndRole(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"e"`)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	ctx, counts := CountRequests(context.Background(), "watch")
	path := "/repos/o/r/pulls/42/commits/a123456789012345678901234567890123456789?per_page=100"
	for i := 0; i < 2; i++ {
		if _, err := c.request(ctx, "GET", path, "application/json", nil); err != nil {
			t.Fatal(err)
		}
	}
	got := counts.Snapshot()
	if len(got) != 2 {
		t.Fatalf("counts = %+v", got)
	}
	for _, count := range got {
		if count.Role != "watch" || count.Method != "GET" || count.Route != "/repos/o/r/pulls/{number}/commits/{sha}" || count.Count != 1 || (count.Status != 200 && count.Status != 304) {
			t.Fatalf("unexpected count: %+v", count)
		}
	}
}

func TestCommentPreservesOtherPullETag(t *testing.T) {
	conditional := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/repos/o/r/pulls/2" && r.Header.Get("If-None-Match") != "" {
			conditional = true
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", `"e"`)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := New("")
	c.BaseURL = srv.URL
	ctx := context.Background()
	for _, v := range []struct{ method, path string }{{"GET", "/repos/o/r/pulls/2"}, {"POST", "/repos/o/r/issues/1/comments"}, {"GET", "/repos/o/r/pulls/2"}} {
		if _, err := c.request(ctx, v.method, v.path, "application/json", nil); err != nil {
			t.Fatal(err)
		}
	}
	if !conditional {
		t.Fatal("comment on PR 1 discarded PR 2 ETag")
	}
}

func TestCommentInvalidatesItsPullAndList(t *testing.T) {
	seen := map[string][]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			seen[r.URL.Path] = append(seen[r.URL.Path], r.Header.Get("If-None-Match"))
		}
		w.Header().Set("ETag", `"e"`)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := New("")
	c.BaseURL = srv.URL
	ctx := context.Background()
	paths := []string{"/repos/o/r/pulls/1", "/repos/o/r/pulls", "/repos/o/r/issues/1/comments"}
	for _, path := range paths {
		if _, e := c.request(ctx, "GET", path, "application/json", nil); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := c.request(ctx, "POST", "/repos/o/r/issues/1/comments", "application/json", nil); e != nil {
		t.Fatal(e)
	}
	for _, path := range paths {
		if _, e := c.request(ctx, "GET", path, "application/json", nil); e != nil {
			t.Fatal(e)
		}
		if len(seen[path]) != 2 || seen[path][1] != "" {
			t.Fatalf("%s was not invalidated: %v", path, seen[path])
		}
	}
}

func TestGraphQLPostIsNotReplayedAfterMutation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/graphql" {
			calls++
			w.Header().Set("ETag", `"same"`)
			w.Header().Set("X-Poll-Interval", "3600")
			fmt.Fprintf(w, `{"version":%d}`, calls)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	c.Stderr = io.Discard
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		if i == 2 {
			if _, err := c.request(ctx, "POST", "/repos/o/r/issues/1/comments", "application/json", []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
		}
		queryCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		b, err := c.request(queryCtx, "POST", "/graphql", "application/json", []byte(`{}`))
		cancel()
		if err != nil || string(b) != fmt.Sprintf(`{"version":%d}`, i) {
			t.Fatalf("query %d returned %s, %v", i, b, err)
		}
	}
	if calls != 2 {
		t.Fatalf("GraphQL server requests = %d, want 2", calls)
	}
}

func TestRouteTemplateKeepsEmbeddedHexAndLabelsNumbers(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tt := range []struct{ route, want string }{
		{"/repos/o/r/git/ref/heads/foo" + sha + "bar", "/repos/o/r/git/ref/heads/foo" + sha + "bar"},
		{"/repos/o/r/issues/" + strings.Repeat("7", 40), "/repos/o/r/issues/{number}"},
		{"/repos/o/r/compare/" + sha + "..." + strings.Repeat("b", 40), "/repos/o/r/compare/{sha}...{sha}"},
		{"/repos/o/r/compare/main..." + sha, "/repos/o/r/compare/{ref}...{sha}"},
		{"/repos/o/r/compare/release%2F1.2..." + sha, "/repos/o/r/compare/{ref}...{sha}"},
		{"/repos/o/r/commits/mergewake-" + sha, "/repos/o/r/commits/mergewake-{sha}"},
		{"/repos/o/r/git/ref/heads/mergewake-" + sha, "/repos/o/r/git/ref/heads/mergewake-" + sha},
		{"/repos/o/r/git/ref/heads/mergewake/reviews/7/" + sha + "-done", "/repos/o/r/git/ref/heads/mergewake/reviews/{number}/{sha}-done"},
	} {
		if got := routeTemplate(tt.route); got != tt.want {
			t.Errorf("routeTemplate(%q) = %q; want %q", tt.route, got, tt.want)
		}
	}
}

func TestPollScopeGitRef404(t *testing.T) {
	hits := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("X-Poll-Interval", "300")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer s.Close()
	c := New("")
	c.BaseURL = s.URL
	c.Polling = true
	var log bytes.Buffer
	c.Stderr = &log
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := c.request(ctx, "GET", "/repos/o/r/git/ref/heads/marker", "application/json", nil)
		cancel()
		if !NotFound(err) {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if hits != 2 || log.Len() != 0 {
		t.Fatalf("hits=%d log=%q", hits, log.String())
	}
}

func TestPollScopeRoutes(t *testing.T) {
	for _, tt := range []struct {
		path    string
		polling bool
	}{
		{"/events", true}, {"/repos/o/r/events?per_page=100", true}, {"/orgs/o/events", true},
		{"/users/u/events", true}, {"/users/u/events/public", true}, {"/users/u/events/orgs/o", true},
		{"/users/u/received_events", true}, {"/users/u/received_events/public", true},
		{"/notifications", true}, {"/repos/o/r/notifications", true},
		{"/repos/o/r/pulls", false}, {"/repos/o/r/git/ref/heads/events", false},
		{"/repos/o/r/events/1", false}, {"/repos/o/r/issues/1/notifications", false},
	} {
		t.Run(tt.path, func(t *testing.T) {
			hits := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits++
				w.Header().Set("X-Poll-Interval", "60")
				_, _ = w.Write([]byte(`{}`))
			}))
			defer s.Close()
			c := New("")
			c.BaseURL = s.URL
			c.Polling = true
			c.Stderr = io.Discard
			for i := 0; i < 2; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, err := c.request(ctx, "GET", tt.path, "application/json", nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
			want := 2
			if tt.polling {
				want = 1
			}
			if hits != want {
				t.Fatalf("hits=%d, want %d", hits, want)
			}
		})
	}
}
