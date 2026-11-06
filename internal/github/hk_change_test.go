package github_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	. "github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestHKChangeThreadsAndPostedFindings(t *testing.T) {
	var mu sync.Mutex
	var batches []map[string]any
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		fmt.Fprint(w, `{"sections":{"threads":{"applied":true},"findings":{"applied":true}}}`)
	}))
	defer hk.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	fs := []Finding{{Severity: "high", Path: "a.go", Line: 3, Body: "inline bug"}, {Severity: "low", Path: "a.go", Line: 90, Body: "file bug", Anchor: AnchorFile}, {Severity: "medium", Path: "other.go", Line: 2, Body: "outside bug", Anchor: AnchorOutside}}
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/graphql" {
			fmt.Fprint(w, `{}`)
			return
		}
		node := func(id, body string, line int) map[string]any {
			return map[string]any{"id": id, "path": "a.go", "line": line, "comments": map[string]any{"nodes": []any{map[string]any{"author": map[string]any{"login": "review-bot", "__typename": "Bot"}, "body": body}}, "pageInfo": map[string]any{"hasNextPage": false}}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"author": map[string]any{"login": "author"}, "headRefOid": testSHA, "reviews": map[string]any{"nodes": []any{}}, "reviewThreads": map[string]any{"nodes": []any{node("T1", FindingBody(fs[0]), 3), node("T2", FindingBody(fs[1])+"\n\n(Line 90 is not changed by this PR.)", 0)}, "pageInfo": map[string]any{"hasNextPage": false}}}}}})
	})
	c.ChangeLogin = "review-bot"
	c.Changes = integrations.NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := c.ReviewFeedback(context.Background(), "example/project", 7); err != nil {
		t.Fatal(err)
	}
	if err := c.PostReview(context.Background(), "example/project", 7, testSHA, fs, PassReceipt{Round: 2, Pass: 1, Lens: "default"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.Changes.Wait(ctx)
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	var foundThreads, foundFindings bool
	for _, b := range batches {
		if th, ok := b["threads"].(map[string]any); ok {
			foundThreads = true
			if th["complete"] != true || len(th["threads"].([]any)) != 2 {
				t.Fatalf("threads=%v", th)
			}
		}
		if f, ok := b["findings"].(map[string]any); ok {
			foundFindings = true
			items := f["findings"].([]any)
			if f["reviewed_head"] != testSHA || f["round"] != float64(2) || f["pass"] != float64(1) || f["lens"] != "default" || len(items) != 3 {
				t.Fatalf("findings=%v", f)
			}
			for i, id := range []string{"T1", "T2"} {
				if items[i].(map[string]any)["thread_id"] != id {
					t.Fatalf("item %d=%v", i, items[i])
				}
			}
			if _, ok := items[2].(map[string]any)["thread_id"]; ok {
				t.Fatalf("outside=%v", items[2])
			}
		}
	}
	if !foundThreads || !foundFindings {
		t.Fatalf("batches=%v", batches)
	}
}

const testSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func changeAPI(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c := New("")
	c.BaseURL = server.URL
	c.HTTP = server.Client()
	return c
}
