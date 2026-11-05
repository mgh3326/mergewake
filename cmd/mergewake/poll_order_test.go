package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// A PR whose head already carries the review done marker is settled before
// another PR's review runs: a slow review no longer holds every land decision
// behind it for the whole pass.
func TestPollPRsSettlesReviewedHeadsBeforeReviews(t *testing.T) {
	const headA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const headB = "cccccccccccccccccccccccccccccccccccccccc"
	const base = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	policy := []byte("version: 1\nenabled: true\nrequired_checks: [CI]\n")
	pull := func(n int, head string) string {
		return fmt.Sprintf(`{"number":%d,"state":"open","title":"change","head":{"sha":%q,"ref":"b%d"},"base":{"ref":"main"},"user":{"login":"author","type":"User"},"mergeable":true,"mergeable_state":"clean"}`, n, head, n)
	}
	var mu sync.Mutex
	var log []string
	note := func(s string) { mu.Lock(); log = append(log, s); mu.Unlock() }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/graphql":
			body, _ := io.ReadAll(r.Body)
			head := headA
			if bytes.Contains(body, []byte(`"number":2`)) || bytes.Contains(body, []byte("number:2")) {
				head = headB
			}
			fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"author":{"login":"author","__typename":"User"},"headRefOid":%q,"reviewDecision":"","reviews":{"nodes":[]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`, head)
		case strings.HasSuffix(path, "/pulls"):
			if r.URL.Query().Get("state") == "closed" {
				fmt.Fprint(w, `[]`)
			} else {
				fmt.Fprintf(w, "[%s,%s]", pull(1, headA), pull(2, headB))
			}
		case strings.HasSuffix(path, "/pulls/1"):
			fmt.Fprint(w, pull(1, headA))
		case strings.HasSuffix(path, "/pulls/2"):
			fmt.Fprint(w, pull(2, headB))
		case strings.HasSuffix(path, "/git/ref/heads/mergewake/reviews/2/"+headB+"-done"):
			note("done-2")
			fmt.Fprint(w, `{}`)
		case strings.Contains(path, "/git/ref/heads/mergewake/reviews/1/"):
			note("ref-1:" + path[strings.LastIndex(path, "/")+1:])
			http.Error(w, `{"message":"Not Found"}`, 404)
		case strings.Contains(path, "/git/ref/heads/mergewake/"):
			http.Error(w, `{"message":"Not Found"}`, 404)
		case strings.HasSuffix(path, "/git/ref/heads/main"):
			fmt.Fprintf(w, `{"object":{"sha":%q}}`, base)
		case strings.HasSuffix(path, "/git/trees/tree"):
			fmt.Fprint(w, `{"tree":[{"path":"policy.yaml","mode":"100644","type":"blob","sha":"blob"}],"truncated":false}`)
		case strings.HasSuffix(path, "/git/blobs/blob"):
			fmt.Fprintf(w, `{"content":%q,"encoding":"base64","size":%d}`, base64.StdEncoding.EncodeToString(policy), len(policy))
		case strings.HasSuffix(path, "/actions/runs"):
			fmt.Fprint(w, `{"workflow_runs":[]}`)
		case strings.HasSuffix(path, "/check-runs"):
			if strings.Contains(path, headB) {
				note("checks-2")
			} else {
				note("checks-1")
			}
			fmt.Fprint(w, `{"check_runs":[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]}`)
		case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/reviews"), strings.HasSuffix(path, "/files"), strings.HasSuffix(path, "/comments"):
			fmt.Fprint(w, `[]`)
		case strings.Contains(path, "/compare/"):
			fmt.Fprintf(w, `{"status":"ahead","merge_base_commit":{"sha":%q}}`, base)
		case strings.Contains(path, "/commits/"):
			fmt.Fprint(w, `{"commit":{"tree":{"sha":"tree"},"message":"change"}}`)
		default:
			note("other:" + r.Method + " " + path)
			http.Error(w, `{"message":"Not Found"}`, 404)
		}
	}))
	defer srv.Close()
	repo := config.Repo{Name: "o/one", Branch: "main", Policy: "policy.yaml"}
	cfg := config.Config{StateDir: t.TempDir(), Repos: []config.Repo{repo}, WatchLimit: 10, Review: config.Review{Login: "reviewer"}}
	gh := github.New("")
	gh.BaseURL = srv.URL
	gh.Stderr = io.Discard
	s := &pipeline.Service{Config: cfg, GH: gh, Runner: integrations.Exec{}, Sink: integrations.Noop{}, Notifier: integrations.Noop{}, Out: io.Discard}
	_ = pollPRs(context.Background(), s, gh, repo, pipeline.Options{Shadow: true})
	mu.Lock()
	defer mu.Unlock()
	t.Logf("order: %v", log)
	first := func(name string) int {
		for i, v := range log {
			if v == name {
				return i
			}
		}
		return -1
	}
	settled, other := first("checks-2"), first("checks-1")
	if settled < 0 || other < 0 {
		t.Fatalf("both PRs must be settled: %v", log)
	}
	// PR 1 comes first in the listing and has no done marker; under the old
	// order it was reviewed and settled before PR 2 was looked at.
	if other < settled {
		t.Fatalf("PR 1 was settled before the reviewed PR 2: %v", log)
	}
}
