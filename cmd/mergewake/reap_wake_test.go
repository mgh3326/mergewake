package main

import (
	"bytes"
	"context"
	"io"
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
		policy string
		opts   pipeline.Options
		merges int
		wakes  int
	}{
		{"merge", "version: 1\nenabled: true\nrequired_checks: [CI]\nbot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [\"**/package-lock.json\"]\n  land: patch\n", pipeline.Options{}, 1, 1},
		{"shadow decides only", "version: 1\nenabled: true\nrequired_checks: [CI]\nbot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [\"**/package-lock.json\"]\n  land: patch\n", pipeline.Options{Shadow: true}, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := &botHub{policy: tt.policy}
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
