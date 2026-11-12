package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// (f) An evaluation that only a check event queued reads the PR without fresh
// evidence when the check ran on the head the queue knows: the check result is
// the new fact, not the PR. A check naming another head, and every other
// source, reads fresh as before.
func TestCheckEventEvaluationReadsThePRWithoutFreshEvidence(t *testing.T) {
	f := newSkipFixture(t, 1)
	e := newPREngine(f.s, f.gh, pipeline.Options{}, []config.Repo{f.repo}, nil,
		slog.New(slog.NewJSONHandler(io.Discard, nil)), nil)
	head := f.pr(1).head
	other := strings.Repeat("d", 40)
	// What the last delivery or evaluation left the queue knowing.
	e.q.SetOpen(f.repo.Name, map[int]string{1: head})
	key := daemon.PRKey{Repo: f.repo.Name, PR: 1}
	for _, tt := range []struct {
		name    string
		sources []daemon.Source
		sha     string
		fresh   bool
	}{
		{"a check on the PR head", []daemon.Source{"check"}, head, false},
		{"a check on another head", []daemon.Source{"check"}, other, true},
		{"a check that names no head", []daemon.Source{"check"}, "", true},
		{"a check beside a sweep", []daemon.Source{"check", daemon.SourceSweep}, head, true},
		{"a pull_request delivery", []daemon.Source{daemon.SourceWebhook}, head, true},
		{"a reviewer wake", []daemon.Source{daemon.SourceReviewer}, head, true},
		{"the sweep alone", []daemon.Source{daemon.SourceSweep}, "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := daemon.Work{Key: key, Sources: tt.sources, CheckSHA: tt.sha}
			if got := e.freshRead(w); got != tt.fresh {
				t.Fatalf("freshRead(%v, sha %q) = %t, want %t", tt.sources, tt.sha, got, tt.fresh)
			}
		})
	}
}

// (g) The "github requests" line breaks the evaluations' requests down by
// route, as it does the sweep's.
func TestGitHubRequestsLineBreaksEvaluationsDownByRoute(t *testing.T) {
	home := t.TempDir() // the daemon's role locks live under the cache dir
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", home)
	f := newSkipFixture(t, 1)
	f.s.Config.Daemon.PR.Loop.Enabled = true
	f.s.Config.Daemon.PR.Loop.Interval = "100ms"
	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	t.Cleanup(func() {
		if err := <-done; err != nil {
			t.Errorf("daemon: %v", err)
		}
	})
	t.Cleanup(cancel)
	go func() { done <- runDaemonLoops(ctx, f.s, f.gh, "", pipeline.Options{Shadow: true}, logs) }()

	deadline := time.Now().Add(15 * time.Second)
	for {
		for _, line := range strings.Split(logs.take(), "\n") {
			var rec struct {
				Msg          string                `json:"msg"`
				Role         string                `json:"role"`
				EvalRequests int                   `json:"eval_requests"`
				EvalRoutes   []github.RequestCount `json:"eval_routes"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Msg != "github requests" || rec.Role != "pr" || rec.EvalRequests == 0 {
				continue
			}
			if len(rec.EvalRoutes) == 0 {
				t.Fatalf("the line has %d evaluation requests and no per-route breakdown: %s", rec.EvalRequests, line)
			}
			total := 0
			for _, route := range rec.EvalRoutes {
				if route.Role != "pr" || route.Method == "" || route.Route == "" || route.Status == 0 || route.Count == 0 {
					t.Fatalf("evaluation route %+v", route)
				}
				total += route.Count
			}
			// The line lists the busiest ten routes, as the sweep's does.
			if total > rec.EvalRequests || (len(rec.EvalRoutes) < 10 && total != rec.EvalRequests) {
				t.Fatalf("the routes total %d requests, the line counts %d: %s", total, rec.EvalRequests, line)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no github requests line with evaluation requests: %s", logs.take())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
