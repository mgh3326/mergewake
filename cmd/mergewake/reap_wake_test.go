package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

const (
	mergeHubHead = "1111111111111111111111111111111111111111"
	mergeHubBase = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	mergeHubPass = "version: 1\nenabled: true\nrequired_checks: [CI]\n"
)

// mergeHub serves one open PR by a person that is green, mergeable and
// reviewed by "reviewer" at its head, so the review gate is ready. It records
// every merge.
type mergeHub struct {
	mu     sync.Mutex
	merges []string
}

func (h *mergeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	path := r.URL.Path
	pull := fmt.Sprintf(`{"number":1,"state":"open","title":"add feature","body":"","changed_files":1,"head":{"sha":%q,"ref":"feature"},"base":{"ref":"main","sha":%q},"user":{"login":"someone","type":"User"},"mergeable":true,"mergeable_state":"clean"}`, mergeHubHead, mergeHubBase)
	switch {
	case path == "/graphql":
		fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"author":{"login":"someone","__typename":"User"},"headRefOid":%q,"reviewDecision":"","reviews":{"nodes":[]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`, mergeHubHead)
	case strings.HasSuffix(path, "/pulls"):
		if r.URL.Query().Get("state") == "closed" {
			fmt.Fprint(w, `[]`)
			return
		}
		fmt.Fprintf(w, "[%s]", pull)
	case strings.HasSuffix(path, "/pulls/1"):
		fmt.Fprint(w, pull)
	case strings.HasSuffix(path, "/pulls/1/merge"):
		var in struct{ SHA string }
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &in)
		h.merges = append(h.merges, in.SHA)
		fmt.Fprint(w, `{"merged":true}`)
	case strings.HasSuffix(path, "/pulls/1/reviews"):
		fmt.Fprintf(w, `[{"id":1,"body":%q,"commit_id":%q,"state":"COMMENTED","user":{"login":"reviewer"}}]`, github.Marker(mergeHubHead)+"\nLooks good.", mergeHubHead)
	case strings.Contains(path, "/git/ref/heads/mergewake/reviews/1/"):
		fmt.Fprint(w, `{"object":{"sha":"d"}}`)
	case strings.HasSuffix(path, "/git/ref/heads/main"):
		fmt.Fprintf(w, `{"object":{"sha":%q}}`, mergeHubBase)
	case strings.HasSuffix(path, "/git/trees/tree"):
		fmt.Fprint(w, `{"tree":[{"path":"policy.yaml","mode":"100644","type":"blob","sha":"blob"}],"truncated":false}`)
	case strings.HasSuffix(path, "/git/blobs/blob"):
		fmt.Fprintf(w, `{"content":%q,"encoding":"base64","size":%d}`, base64.StdEncoding.EncodeToString([]byte(mergeHubPass)), len(mergeHubPass))
	case strings.HasSuffix(path, "/actions/runs"):
		fmt.Fprint(w, `{"workflow_runs":[]}`)
	case strings.HasSuffix(path, "/check-runs"):
		fmt.Fprint(w, `{"check_runs":[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]}`)
	case strings.HasSuffix(path, "/files"):
		fmt.Fprint(w, `[{"filename":"main.go","status":"modified","additions":1,"deletions":1,"changes":2}]`)
	case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/comments"):
		fmt.Fprint(w, `[]`)
	case strings.Contains(path, "/compare/"):
		fmt.Fprintf(w, `{"status":"ahead","merge_base_commit":{"sha":%q}}`, mergeHubBase)
	case strings.Contains(path, "/commits/"):
		fmt.Fprint(w, `{"commit":{"tree":{"sha":"tree"},"message":"update"}}`)
	default:
		http.Error(w, `{"message":"Not Found"}`, 404)
	}
}

// reapPasses counts the loop-completed log lines of the reap role by what
// started the pass.
func reapPasses(log string) (interval, webhook int) {
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, `"loop completed"`) || !strings.Contains(line, `"role":"reap"`) {
			continue
		}
		switch {
		case strings.Contains(line, `"wake":"webhook"`):
			webhook++
		case strings.Contains(line, `"wake":"interval"`):
			interval++
		}
	}
	return interval, webhook
}

// A pr pass that merges a PR wakes the reap role once, ahead of its hour-long
// poll; a pr pass that merges nothing does not.
func TestDaemonMergeWakesReapRole(t *testing.T) {
	for _, tt := range []struct {
		name   string
		opts   pipeline.Options
		merges int
		wakes  int
	}{
		{"merge", pipeline.Options{}, 1, 1},
		{"shadow decides only", pipeline.Options{Shadow: true}, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := &mergeHub{}
			srv := httptest.NewServer(h)
			defer srv.Close()
			repo := config.Repo{Name: "o/one", Branch: "main", Policy: "policy.yaml"}
			cfg := config.Config{StateDir: t.TempDir(), Repos: []config.Repo{repo}, WatchLimit: 10, PollInterval: "1h", Review: config.Review{Login: "reviewer"}}
			cfg.Daemon.PR.SkipBots = true
			cfg.Daemon.PR.Loop = config.Loop{Enabled: true, Interval: "1h"}
			cfg.Reap = config.Reap{Enabled: true}
			gh := github.New("")
			gh.BaseURL = srv.URL
			gh.Stderr = io.Discard
			s := &pipeline.Service{Config: cfg, GH: gh, Runner: integrations.Exec{}, Sink: integrations.Noop{}, Notifier: integrations.Noop{}, Out: io.Discard}

			var stderr lockedBuffer
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exit := make(chan error, 1)
			go func() { exit <- runDaemonLoops(ctx, s, gh, "", tt.opts, &stderr) }()
			waitFor(t, "first reap pass", func() bool { i, _ := reapPasses(stderr.String()); return i == 1 })
			if tt.wakes > 0 {
				waitFor(t, "reap pass woken by the merge", func() bool { _, w := reapPasses(stderr.String()); return w >= tt.wakes })
			} else {
				// The pr pass ran at start too; give a wake time to show itself.
				waitFor(t, "pr pass", func() bool {
					return strings.Contains(stderr.String(), `"role":"pr"`) && strings.Contains(stderr.String(), `"loop completed"`)
				})
				time.Sleep(300 * time.Millisecond)
			}
			cancel()
			select {
			case <-exit:
			case <-time.After(20 * time.Second):
				t.Fatal("daemon did not stop")
			}
			_, wakes := reapPasses(stderr.String())
			h.mu.Lock()
			merges := len(h.merges)
			h.mu.Unlock()
			if merges != tt.merges || wakes != tt.wakes {
				t.Fatalf("merges=%d wakes=%d, want %d and %d\n%s", merges, wakes, tt.merges, tt.wakes, stderr.String())
			}
		})
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
