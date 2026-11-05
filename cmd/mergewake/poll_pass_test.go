package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// A looping command shares one context across iterations. Each iteration must
// be its own pass: a Pull read in iteration one is read again in iteration two.
func TestPollRunsEachIterationAsItsOwnPass(t *testing.T) {
	var pulls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pulls.Add(1)
		_ = json.NewEncoder(w).Encode(github.Pull{Number: 7})
	}))
	defer server.Close()
	gh := github.New("placeholder")
	gh.BaseURL, gh.HTTP = server.URL, server.Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, counts := github.CountRequests(ctx, "watch")
	iterations := 0
	err := poll(ctx, "1ms", false, func(ctx context.Context) error {
		iterations++
		for i := 0; i < 3; i++ {
			if _, e := gh.Pull(ctx, "example/project", 7); e != nil {
				return e
			}
		}
		if iterations == 3 {
			cancel()
		}
		return nil
	}, io.Discard)
	if err != nil || iterations != 3 {
		t.Fatalf("%v after %d iterations", err, iterations)
	}
	if got := pulls.Load(); got != 3 {
		t.Fatalf("%d Pull requests over 3 iterations, want one per iteration", got)
	}
	if s := counts.CacheStats(); s.MemoHits != 6 {
		t.Fatalf("counters did not run on across iterations: %+v", s)
	}
}
