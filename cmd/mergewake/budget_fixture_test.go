package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// TestBudgetPassFixture exercises two real loop passes over three repositories.
func TestBudgetPassFixture(t *testing.T) {
	const head = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const base = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	policy := []byte("version: 1\nenabled: false\nrequired_checks: [CI]\n")
	pull := fmt.Sprintf(`{"number":1,"state":"open","title":"change","head":{"sha":%q},"base":{"ref":"main"},"user":{"login":"author","type":"User"},"mergeable":true,"mergeable_state":"clean"}`, head)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("ETag", `"fixture"`)
			if r.Header.Get("If-None-Match") != "" {
				w.WriteHeader(304)
				return
			}
		}
		path := r.URL.Path
		switch {
		case path == "/graphql":
			fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"author":{"login":"author","__typename":"User"},"headRefOid":%q,"reviewDecision":"APPROVED","reviews":{"nodes":[]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`, head)
		case strings.HasSuffix(path, "/pulls"):
			if r.URL.Query().Get("state") == "closed" {
				fmt.Fprint(w, `[]`)
			} else {
				fmt.Fprintf(w, "[%s]", pull)
			}
		case strings.HasSuffix(path, "/pulls/1"):
			fmt.Fprint(w, pull)
		case strings.HasSuffix(path, "/git/ref/heads/main"):
			fmt.Fprintf(w, `{"object":{"sha":%q}}`, base)
		case strings.Contains(path, "/git/ref/heads/mergewake/"):
			fmt.Fprint(w, `{}`)
		case strings.HasSuffix(path, "/git/trees/tree"):
			fmt.Fprint(w, `{"tree":[{"path":"policy.yaml","mode":"100644","type":"blob","sha":"blob"}],"truncated":false}`)
		case strings.HasSuffix(path, "/git/blobs/blob"):
			fmt.Fprintf(w, `{"content":%q,"encoding":"base64","size":%d}`, base64.StdEncoding.EncodeToString(policy), len(policy))
		case strings.HasSuffix(path, "/actions/runs"):
			fmt.Fprint(w, `{"workflow_runs":[]}`)
		case strings.HasSuffix(path, "/check-runs"):
			fmt.Fprint(w, `{"check_runs":[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]}`)
		case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/reviews"), strings.HasSuffix(path, "/files"):
			fmt.Fprint(w, `[]`)
		case strings.Contains(path, "/compare/"):
			fmt.Fprintf(w, `{"status":"ahead","merge_base_commit":{"sha":%q}}`, base)
		case strings.HasSuffix(path, "/commits/"+base), strings.HasSuffix(path, "/commits/"+head):
			fmt.Fprint(w, `{"commit":{"tree":{"sha":"tree"}}}`)
		default:
			t.Errorf("unexpected GitHub route: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected route", 500)
		}
	}))
	defer srv.Close()
	repos := []config.Repo{{Name: "o/one", Branch: "main", Policy: "policy.yaml"}, {Name: "o/two", Branch: "main", Policy: "policy.yaml"}, {Name: "o/three", Branch: "main", Policy: "policy.yaml"}}
	cfg := config.Config{StateDir: t.TempDir(), Repos: repos, WatchLimit: 10, Review: config.Review{Login: "reviewer"}}
	cfg.Discord.Enabled = true
	cfg.Discord.Kinds = map[string]string{"pr": "fixture", "ci": "fixture"}
	gh := github.New("")
	gh.BaseURL = srv.URL
	gh.Polling = true
	gh.Stderr = io.Discard
	s := &pipeline.Service{Config: cfg, GH: gh, Runner: integrations.Exec{}, Sink: integrations.Noop{}, Notifier: integrations.Noop{}, Out: io.Discard}
	s.Feed = pipeline.NewFeed(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for pass := 0; pass < 2; pass++ {
		for _, role := range []string{"pr", "watch"} {
			ctx, counts := github.CountRequests(context.Background(), role)
			for _, repo := range repos {
				var err error
				if role == "pr" {
					err = pollPRs(ctx, s, gh, repo, pipeline.Options{Shadow: true})
				} else {
					err = s.Watch(ctx, repo, pipeline.Options{Shadow: true})
				}
				if err != nil {
					t.Fatalf("%s pass %d %s: %v", role, pass, repo.Name, err)
				}
			}
			snapshot := counts.Snapshot()
			b, _ := json.Marshal(snapshot)
			t.Logf("%s pass=%d %s", role, pass, b)
			if pass == 1 {
				non304 := 0
				for _, count := range snapshot {
					if count.Status != http.StatusNotModified {
						non304 += count.Count
					}
				}
				if role == "pr" && non304 != 2*len(repos) {
					t.Errorf("steady PR pass charged %d requests, want %d fresh GraphQL reads", non304, 2*len(repos))
				}
				if role == "watch" && non304 != 0 {
					t.Errorf("steady watch pass charged %d requests", non304)
				}
			}
		}
	}
	// Exercise the same paths in the CLI's dry-run mode as well.
	for _, repo := range repos {
		opts := pipeline.Options{DryRun: true, Shadow: true}
		if err := pollPRs(context.Background(), s, gh, repo, opts); err != nil {
			t.Fatalf("dry-run pr %s: %v", repo.Name, err)
		}
		if err := s.Watch(context.Background(), repo, opts); err != nil {
			t.Fatalf("dry-run watch %s: %v", repo.Name, err)
		}
	}
	// The fixture returns an existing review ref but no review receipts. A real
	// acting poll must still attempt review and persist the failure for retry.
	if err := pollPRs(context.Background(), s, gh, repos[0], pipeline.Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "o/one", "review-retry-1.json")); err != nil {
		t.Fatalf("poll did not schedule review: %v", err)
	}
}
