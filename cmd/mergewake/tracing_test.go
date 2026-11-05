package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/pipeline"
	"github.com/mgh3326/mergewake/internal/tracing"
)

type traceTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
	cancel context.CancelFunc
}

func (t *traceTransport) Configure(sentry.ClientOptions) {}
func (t *traceTransport) SendEvent(e *sentry.Event) {
	t.mu.Lock()
	t.events = append(t.events, e)
	t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
}
func (t *traceTransport) Flush(time.Duration) bool { return true }
func (t *traceTransport) Close()                   {}
func (t *traceTransport) snapshot() []*sentry.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*sentry.Event(nil), t.events...)
}

type traceRunner struct{ *blockingRunner }

func (r traceRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	b, err := r.blockingRunner.Run(ctx, command, args, input, dir)
	return bytes.ReplaceAll(b, []byte("model=seat-x"), []byte("model=claude")), err
}

type handlerTransport struct{ handler http.Handler }

func (h handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	h.handler.ServeHTTP(w, r)
	return w.Result(), nil
}

func traceFixture(t *testing.T) *parallelFixture {
	t.Helper()
	trace := &eventLog{}
	hub := &reviewHub{n: 2, refs: map[string]string{}, commits: map[string]string{}, reviews: map[int][]map[string]any{}, trace: trace}
	hub.refs["mergewake/reviews/1/"+hubHead(1)+"-done"] = hubHead(1)
	hub.reviews[1] = []map[string]any{{"id": 1, "user": map[string]string{"login": "reviewer"}, "commit_id": hubHead(1), "body": github.PassMarker(github.PassReceipt{Pass: 1, Reviewer: "claude", Family: "anthropic"}), "state": "COMMENTED"}}
	repo := config.Repo{Name: "o/one", Branch: "main", Policy: "policy.yaml", Root: t.TempDir()}
	cfg := config.Config{StateDir: t.TempDir(), Repos: []config.Repo{repo}, WatchLimit: 10,
		Review: config.Review{Login: "reviewer", Timeout: "60s", Executor: "panewire", Panewire: config.PanewireReview{Command: "panewire", Workspace: "reviews", Host: "auto", Jobs: filepath.Join(t.TempDir(), "jobs"), Checkouts: filepath.Join(t.TempDir(), "checkouts")}, Pairings: map[string][]string{"openai": {"claude"}}, Profiles: map[string]config.Reviewer{"claude": {Family: "anthropic", Command: "pi", Args: []string{"--model", "example-model"}}}}, Notifier: config.Notifier{Lane: "private-lane"}}
	cfg.Daemon.PR.ReviewParallel = 2
	cfg.Daemon.PR.Loop = config.Loop{Enabled: true, Interval: "1h"}
	gh := github.New("")
	gh.BaseURL = "https://fake.invalid"
	gh.HTTP = &http.Client{Transport: handlerTransport{hub}}
	gh.Stderr = io.Discard
	runner := &blockingRunner{trace: trace, release: make(chan struct{}), changed: make(chan struct{}, 1)}
	close(runner.release)
	out := &bytes.Buffer{}
	service := &pipeline.Service{Config: cfg, GH: gh, Runner: traceRunner{runner}, Sink: parallelSink{}, Notifier: recordingNotifier{trace}, Out: out}
	return &parallelFixture{s: service, gh: gh, repo: repo, runner: runner, trace: trace, out: out}
}

func TestTracingPassTreeAndPrivacy(t *testing.T) {
	t.Setenv(tracing.DSNEnv, "https://key@example.invalid/1")
	t.Setenv(tracing.RateEnv, "1")
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	f := traceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := &traceTransport{cancel: cancel}
	ctx, stop := tracing.Start(ctx, io.Discard, rec)
	defer stop()
	if err := runDaemonLoops(ctx, f.s, f.gh, "", pipeline.Options{}, io.Discard); err != nil {
		t.Fatal(err)
	}
	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("got %d events", len(events))
	}
	e := events[0]
	if e.Type != "transaction" || e.Transaction != "pr pass" || e.Tags["role"] != "pr" {
		t.Fatalf("transaction: %+v", e)
	}
	var repo *sentry.Span
	prs := map[int]*sentry.Span{}
	byParent := map[sentry.SpanID][]*sentry.Span{}
	for _, s := range e.Spans {
		byParent[s.ParentSpanID] = append(byParent[s.ParentSpanID], s)
		if s.Op == "mergewake.repo" {
			repo = s
		}
		if s.Op == "mergewake.pr" {
			prs[s.Data["pr"].(int)] = s
		}
	}
	if repo == nil || repo.Tags["repo"] != "o/one" || len(prs) != 2 {
		t.Fatalf("repo/pr spans missing: %+v", e.Spans)
	}
	has := func(parent *sentry.Span, op string) *sentry.Span {
		t.Helper()
		for _, s := range byParent[parent.SpanID] {
			if s.Op == op {
				return s
			}
		}
		t.Fatalf("%s missing below %s", op, parent.Op)
		return nil
	}
	for n := 1; n <= 2; n++ {
		pr := prs[n]
		if pr.ParentSpanID != repo.SpanID {
			t.Fatalf("PR %d parent", n)
		}
		has(pr, "mergewake.observe")
		settle := has(pr, "mergewake.settle")
		has(settle, "mergewake.deliver")
		has(settle, "mergewake.notify")
		land := has(settle, "mergewake.land")
		if land.Data["verdict"] == nil {
			t.Fatalf("PR %d no verdict", n)
		}
		review := has(pr, "mergewake.review")
		if n == 2 {
			pass := has(review, "mergewake.review.pass")
			if pass.Data["reviewer"] != "claude" {
				t.Fatalf("missing reviewer: %v", pass.Data)
			}
			if pass.Data["pass"] != 1 {
				t.Fatal(pass.Data)
			}
			has(pass, "mergewake.spawn_wait")
			has(pass, "mergewake.collect")
		}
	}
	if e.Extra["github_requests"] == nil {
		t.Fatalf("missing request count: %+v", e.Extra)
	}
	raw, _ := json.Marshal(struct {
		Tags  map[string]string
		Spans []*sentry.Span
		Extra map[string]interface{}
	}{e.Tags, e.Spans, e.Extra})
	forbidden := regexp.MustCompile(`(?i)([a-z0-9-]+\.(?:com|net|invalid)|w[0-9a-z]+:p|private-lane|/(?:Users|home)/)`)
	if bad := forbidden.Find(raw); bad != nil {
		t.Fatalf("private span data: %s", bad)
	}
	t.Log("tree: pr pass → repo o/one → PR 1, PR 2 → observe, settle (deliver, notify, land), review → pass 1 → spawn_wait, collect")
}

func TestTracingPassSampling(t *testing.T) {
	for _, tc := range []struct {
		name, dsn, rate, role string
		wantEvent             bool
	}{
		{"pr at zero", "https://key@example.invalid/1", "0", "pr", true},
		{"reap at zero", "https://key@example.invalid/1", "0", "reap", true},
		{"release at zero", "https://key@example.invalid/1", "0", "release", true},
		{"rollout at zero", "https://key@example.invalid/1", "0", "rollout", true},
		{"watch at zero", "https://key@example.invalid/1", "0", "watch", false},
		{"watch at one", "https://key@example.invalid/1", "1", "watch", true},
		{"no DSN", "", "1", "pr", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tracing.DSNEnv, tc.dsn)
			t.Setenv(tracing.RateEnv, tc.rate)
			rec := &traceTransport{}
			ctx, stop := tracing.Start(context.Background(), io.Discard, rec)
			if tc.dsn == "" && sentry.GetHubFromContext(ctx) != nil {
				t.Fatal("client without DSN")
			}
			ctx, pass := tracing.Pass(ctx, tc.role)
			_, child := tracing.Span(ctx, "test.child")
			tracing.Finish(child)
			tracing.Finish(pass)
			stop()
			events := rec.snapshot()
			if tc.wantEvent {
				if len(events) != 1 || events[0].Transaction != tc.role+" pass" || events[0].Tags["role"] != tc.role {
					t.Fatalf("expected %s transaction, got %+v", tc.role, events)
				}
				if len(events[0].Spans) != 1 || events[0].Spans[0].Op != "test.child" {
					t.Fatalf("missing child span: %+v", events[0].Spans)
				}
			} else if len(events) != 0 {
				t.Fatalf("unexpected events: %+v", events)
			}
		})
	}
}

func TestTracingInvalidWatchRate(t *testing.T) {
	t.Setenv(tracing.DSNEnv, "https://key@example.invalid/1")
	t.Setenv(tracing.RateEnv, "invalid")
	var logs bytes.Buffer
	ctx, stop := tracing.Start(context.Background(), &logs, &traceTransport{})
	defer stop()
	if got := sentry.GetHubFromContext(ctx).Client().Options().TracesSampleRate; got != 0.2 {
		t.Fatalf("watch sample rate = %v, want 0.2", got)
	}
	if got := strings.Count(logs.String(), "invalid SENTRY_TRACES_SAMPLE_RATE_MERGEWAKE"); got != 1 {
		t.Fatalf("logs: %q", logs.String())
	}
}

func TestTracingPreservesPollResults(t *testing.T) {
	run := func(enabled bool) (string, []string) {
		f := traceFixture(t)
		ctx := context.Background()
		if enabled {
			t.Setenv(tracing.DSNEnv, "https://key@example.invalid/1")
			t.Setenv(tracing.RateEnv, "1")
			var stop func()
			ctx, stop = tracing.Start(ctx, io.Discard, &traceTransport{})
			defer stop()
			var span *sentry.Span
			ctx, span = tracing.Pass(ctx, "pr")
			defer tracing.Finish(span)
		}
		err := pollPRs(ctx, f.s, f.gh, f.repo, pipeline.Options{})
		return fmt.Sprint(err), steps(f.trace.snapshot())
	}
	a, eventsA := run(false)
	b, eventsB := run(true)
	slices.Sort(eventsA)
	slices.Sort(eventsB)
	if a != b || fmt.Sprint(eventsA) != fmt.Sprint(eventsB) {
		t.Fatalf("off %q %v; on %q %v", a, eventsA, b, eventsB)
	}
}
