package github

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// memoServer answers pulls, the main ref and reviews, counting each route.
type memoServer struct {
	mu    sync.Mutex
	count map[string]int
}

func (m *memoServer) n(route string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count[route]
}

func (m *memoServer) client(t *testing.T) *Client {
	m.count = map[string]int{}
	return api(t, func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.count[r.Method+" "+r.URL.Path]++
		seen := m.count[r.Method+" "+r.URL.Path]
		m.mu.Unlock()
		switch {
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/pulls/7"):
			reply(w, Pull{Number: 7, Title: strings.Repeat("v", seen)})
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/git/ref/heads/main"):
			reply(w, map[string]any{"object": Ref{SHA: strings.Repeat("0", 39) + string(rune('0'+seen))}})
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/pulls/7/reviews"):
			reply(w, []Review{{ID: 1, CommitID: testSHA, State: "COMMENTED", Body: Marker(testSHA) + "\n", User: struct {
				Login string `json:"login"`
			}{"bot"}}})
		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/pulls/7/update-branch"):
			w.WriteHeader(http.StatusAccepted)
			reply(w, map[string]any{})
		default:
			http.NotFound(w, r)
		}
	})
}

const (
	pullRoute = "GET /repos/example/project/pulls/7"
	headRoute = "GET /repos/example/project/git/ref/heads/main"
	revRoute  = "GET /repos/example/project/pulls/7/reviews"
)

func TestPassMemoPull(t *testing.T) {
	var srv memoServer
	c := srv.client(t)
	ctx, counts := CountRequests(context.Background(), "pr")
	first, e := c.Pull(ctx, "example/project", 7)
	if e != nil {
		t.Fatal(e)
	}
	again, _ := c.Pull(ctx, "example/project", 7)
	if srv.n(pullRoute) != 1 || again.Title != first.Title {
		t.Fatalf("second Pull in a pass made a request: %d", srv.n(pullRoute))
	}
	fresh, _ := c.Pull(FreshEvidence(ctx), "example/project", 7)
	if srv.n(pullRoute) != 2 || fresh.Title == first.Title {
		t.Fatalf("fresh-evidence Pull did not reach GitHub: %d requests", srv.n(pullRoute))
	}
	// The fresh answer is the newest the pass knows, so later plain reads use it.
	if later, _ := c.Pull(ctx, "example/project", 7); later.Title != fresh.Title || srv.n(pullRoute) != 2 {
		t.Fatalf("plain read after a fresh one: %q, %d requests", later.Title, srv.n(pullRoute))
	}
	if _, e = c.Pull(freshRequest(ctx), "example/project", 7); e != nil || srv.n(pullRoute) != 3 {
		t.Fatalf("fresh-request Pull did not reach GitHub: %d requests", srv.n(pullRoute))
	}
	if counts.CacheStats().MemoHits != 2 {
		t.Fatalf("%+v", counts.CacheStats())
	}
	newPass, _ := CountRequests(context.Background(), "pr")
	if _, e = c.Pull(newPass, "example/project", 7); e != nil || srv.n(pullRoute) != 4 {
		t.Fatalf("a new pass reused the old memo: %d requests", srv.n(pullRoute))
	}
	// Outside a pass nothing is memoized.
	c.Pull(context.Background(), "example/project", 7)
	c.Pull(context.Background(), "example/project", 7)
	if srv.n(pullRoute) != 6 {
		t.Fatalf("%d requests", srv.n(pullRoute))
	}
}

func TestPassMemoHeadAndReviews(t *testing.T) {
	var srv memoServer
	c := srv.client(t)
	ctx, _ := CountRequests(context.Background(), "pr")
	a, _ := c.Head(ctx, "example/project", "main")
	b, _ := c.Head(ctx, "example/project", "main")
	if srv.n(headRoute) != 1 || a != b {
		t.Fatalf("Head made %d requests in a pass", srv.n(headRoute))
	}
	if f, _ := c.Head(FreshEvidence(ctx), "example/project", "main"); srv.n(headRoute) != 2 || f == a {
		t.Fatalf("fresh Head: %d requests, %s vs %s", srv.n(headRoute), f, a)
	}
	h1, e := c.ReviewHistory(ctx, "example/project", 7, "bot")
	if e != nil || len(h1) != 1 {
		t.Fatalf("%v %v", h1, e)
	}
	h1[0].Head = "mutated"
	h2, _ := c.ReviewHistory(ctx, "example/project", 7, "bot")
	passes, _ := c.ReviewPasses(ctx, "example/project", 7, testSHA, "bot")
	if srv.n(revRoute) != 1 || h2[0].Head != testSHA || len(passes) != 1 {
		t.Fatalf("%d review requests; memo shared a caller's slice: %q", srv.n(revRoute), h2[0].Head)
	}
	if _, e := c.ReviewHistory(FreshEvidence(ctx), "example/project", 7, "bot"); e != nil || srv.n(revRoute) != 2 {
		t.Fatalf("fresh ReviewHistory: %d requests", srv.n(revRoute))
	}
	if _, e := c.ReviewHistory(ctx, "example/project", 7, ""); e == nil {
		t.Fatal("empty login accepted")
	}
}

func TestPassMemoDroppedByWrite(t *testing.T) {
	var srv memoServer
	c := srv.client(t)
	ctx, _ := CountRequests(context.Background(), "pr")
	c.Pull(ctx, "example/project", 7)
	c.Head(ctx, "example/project", "main")
	if e := c.UpdateBranch(ctx, "example/project", 7, testSHA); e != nil {
		t.Fatal(e)
	}
	c.Pull(ctx, "example/project", 7)
	c.Head(ctx, "example/project", "main")
	if srv.n(pullRoute) != 2 || srv.n(headRoute) != 2 {
		t.Fatalf("memo survived a write: pull %d, head %d", srv.n(pullRoute), srv.n(headRoute))
	}
}

func TestPassMemoErrorsAreNotKept(t *testing.T) {
	var srv memoServer
	c := srv.client(t)
	ctx, _ := CountRequests(context.Background(), "pr")
	if _, e := c.Pull(ctx, "example/project", 8); e == nil {
		t.Fatal("expected 404")
	}
	if _, e := c.Pull(ctx, "example/project", 8); e == nil || srv.n("GET /repos/example/project/pulls/8") != 2 {
		t.Fatalf("an error was memoized: %d requests", srv.n("GET /repos/example/project/pulls/8"))
	}
}
