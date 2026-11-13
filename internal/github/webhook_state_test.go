package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

// stateFake answers the three reads the stored state stands in for and counts
// every request it gets. Its PR can be moved on between calls.
type stateFake struct {
	requests atomic.Int64
	mu       sync.Mutex
	head     string
	updated  string
	checks   string
}

func (f *stateFake) set(head, updated string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.head, f.updated = head, updated
}

func (f *stateFake) handler(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case strings.HasSuffix(r.URL.Path, "/pulls/7"):
		reply(w, map[string]any{"number": 7, "state": "open", "title": "t", "body": "body", "updated_at": f.updated,
			"head": map[string]any{"sha": f.head, "ref": "topic"}, "base": map[string]any{"sha": "b1", "ref": "main"},
			"labels": []map[string]string{{"name": "fast"}}, "mergeable": true, "mergeable_state": "clean"})
	case strings.HasSuffix(r.URL.Path, "/check-runs"):
		fmt.Fprint(w, `{"check_runs":[{"id":5,"name":"test","status":"completed","conclusion":"success","completed_at":"2026-11-06T00:01:00Z","check_suite":{"id":9}}]}`)
	case strings.HasSuffix(r.URL.Path, "/statuses"):
		fmt.Fprint(w, `[]`)
	case strings.HasSuffix(r.URL.Path, "/pulls/7/reviews"):
		fmt.Fprint(w, `[]`)
	default:
		http.NotFound(w, r)
	}
}

type logCapture struct {
	mu    sync.Mutex
	lines []map[string]any
}

func (l *logCapture) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		var m map[string]any
		if json.Unmarshal(line, &m) == nil {
			l.lines = append(l.lines, m)
		}
	}
	return len(b), nil
}

func (l *logCapture) mismatches() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, m := range l.lines {
		if m["msg"] == "webhook_state" && m["result"] == "mismatch" {
			out = append(out, m)
		}
	}
	return out
}

func shadowClient(t *testing.T, f *stateFake) (*Client, *webhookstate.Store, *logCapture) {
	t.Helper()
	c := api(t, f.handler)
	logs := &logCapture{}
	store := webhookstate.Open(t.TempDir())
	store.Log = slog.New(slog.NewJSONHandler(logs, nil))
	c.Webhook = store
	return c, store, logs
}

func deliverPR(t *testing.T, s *webhookstate.Store, id, head, updated string) {
	t.Helper()
	body := fmt.Sprintf(`{"action":"synchronize","pull_request":{"number":7,"state":"open","title":"t","body":"body","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"},"labels":[{"name":"fast"}],"updated_at":%q}}`, head, updated)
	if e := s.Apply("o/r", "pull_request", id, []byte(body)); e != nil {
		t.Fatal(e)
	}
}

// (d) phase 1: a matching stored PR logs nothing, a differing one logs one line
// per differing field, and the PR the caller gets is the API's either way.
func TestShadowPullComparesWithoutChangingTheResult(t *testing.T) {
	f := &stateFake{}
	f.set("h1", "2026-11-06T00:01:00Z")
	c, store, logs := shadowClient(t, f)
	ctx := context.Background()

	deliverPR(t, store, "d1", "h1", "2026-11-06T00:01:00Z")
	p, e := c.Pull(ctx, "o/r", 7)
	if e != nil || p.Head.SHA != "h1" || p.MergeableState != "clean" {
		t.Fatalf("pull = %+v, %v", p, e)
	}
	if got := logs.mismatches(); len(got) != 0 {
		t.Fatalf("matching state logged %d mismatches: %v", len(got), got)
	}
	if f.requests.Load() != 1 {
		t.Fatalf("phase 1 made %d requests, want 1: it still reads the API", f.requests.Load())
	}

	// The API moved on to h2 (updated later); the stored state still says h1.
	f.set("h2", "2026-11-06T00:02:00Z")
	p, _ = c.Pull(ctx, "o/r", 7)
	if p.Head.SHA != "h2" {
		t.Fatalf("phase 1 returned the stored head %q instead of the API's", p.Head.SHA)
	}
	got := logs.mismatches()
	fields := map[string]bool{}
	for _, m := range got {
		fields[m["field"].(string)] = true
		if m["pr"].(float64) != 7 || m["head"] != "h2" || m["stored"] == nil || m["api"] == nil {
			t.Errorf("mismatch line lacks pr, head, stored or api: %v", m)
		}
	}
	if len(got) != 2 || !fields["head_sha"] || !fields["updated_at"] {
		t.Fatalf("mismatches = %v, want one line each for head_sha and updated_at", got)
	}
	if c := store.Counters(); c.Hits < 2 || c.Mismatches != 2 {
		t.Fatalf("counters = %+v", c)
	}
	// The API read re-seeded the store, so the next read matches.
	before := len(logs.mismatches())
	_, _ = c.Pull(NewPass(ctx), "o/r", 7)
	if len(logs.mismatches()) != before {
		t.Fatal("a repeated read mismatched after the store was seeded")
	}
}

func TestWriteDistrustsWebhookStateUsingOriginalRepoSpelling(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	store := webhookstate.Open(t.TempDir())
	store.Now = func() time.Time { return now }
	store.SeedPR("Owner/Repo", webhookstate.PR{Number: 7, State: "open", HeadSHA: "h1"})
	if _, ok := store.UsablePR("Owner/Repo", 7); !ok {
		t.Fatal("fixture PR is not usable before the write")
	}
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c.Webhook = store
	if _, err := c.request(context.Background(), "PUT", "/repos/Owner/Repo/pulls/7/merge", "application/json", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.UsablePR("Owner/Repo", 7); ok {
		t.Fatal("write left the exact-spelling webhook entry trusted")
	}
}

func TestShadowPullIgnoresAStoredPRNewerThanTheAPI(t *testing.T) {
	f := &stateFake{}
	f.set("h1", "2026-11-06T00:01:00Z")
	c, store, logs := shadowClient(t, f)
	deliverPR(t, store, "d1", "h2", "2026-11-06T00:02:00Z") // arrived while the API read was in flight
	if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
		t.Fatal(e)
	}
	if got := logs.mismatches(); len(got) != 0 {
		t.Fatalf("a delivery newer than the API read was reported as a mismatch: %v", got)
	}
}

func TestShadowChecksAndReviewsCompareAndSeed(t *testing.T) {
	f := &stateFake{}
	c, store, logs := shadowClient(t, f)
	ctx := context.Background()
	// A stored run that still says in_progress where the API says success.
	body := `{"check_run":{"id":5,"name":"test","head_sha":"h1","status":"in_progress","started_at":"2026-11-06T00:00:10Z","check_suite":{"id":9}}}`
	if e := store.Apply("o/r", "check_run", "c1", []byte(body)); e != nil {
		t.Fatal(e)
	}
	if _, e := c.CheckStates(ctx, "o/r", "h1", []string{"test"}); e != nil {
		t.Fatal(e)
	}
	got := logs.mismatches()
	if len(got) != 1 || got[0]["field"] != "check test" || got[0]["stored"] != "pending" || got[0]["api"] != "green" || got[0]["head"] != "h1" {
		t.Fatalf("mismatches = %v", got)
	}
	if _, e := c.ReviewHistory(ctx, "o/r", 7, "bot"); e != nil {
		t.Fatal(e)
	}
	if store.Counters().Misses == 0 {
		t.Fatal("a review read with no stored reviews did not count as a miss")
	}
}

// (e) phase 2, enabled here only: a fresh stored PR and head are served with
// zero requests, and a missing entry falls back to the API.
func TestServeFromStoredStateAvoidsTheAPI(t *testing.T) {
	f := &stateFake{}
	f.set("h1", "2026-11-06T00:01:00Z")
	c, store, _ := shadowClient(t, f)
	c.serveWebhookState = true
	ctx := context.Background()

	if _, e := c.Pull(ctx, "o/r", 8); e == nil {
		t.Fatal("PR 8 does not exist on the fake; the read should have failed through to the API")
	}
	if f.requests.Load() != 1 {
		t.Fatalf("a missing entry made %d requests, want 1 (fall back to the API)", f.requests.Load())
	}

	// PR from a delivery; head checks and reviews from one API read each.
	deliverPR(t, store, "d1", "h1", "2026-11-06T00:01:00Z")
	if _, e := c.CheckStates(ctx, "o/r", "h1", []string{"test"}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.ReviewHistory(ctx, "o/r", 7, "bot"); e != nil {
		t.Fatal(e)
	}
	f.requests.Store(0)

	p, e := c.Pull(NewPass(ctx), "o/r", 7)
	if e != nil || p.Head.SHA != "h1" || len(p.Labels) != 1 || p.Labels[0].Name != "fast" || p.Body != "body" {
		t.Fatalf("stored pull = %+v, %v", p, e)
	}
	if p.Mergeable != nil || p.MergeableState != "" {
		t.Fatalf("mergeability came from stored state: %v %q", p.Mergeable, p.MergeableState)
	}
	states, e := c.CheckStates(ctx, "o/r", "h1", []string{"test", "other"})
	if e != nil || states["test"] != Green || states["other"] != Absent {
		t.Fatalf("stored checks = %v, %v", states, e)
	}
	if _, e := c.ReviewHistory(NewPass(ctx), "o/r", 7, "bot"); e != nil {
		t.Fatal(e)
	}
	if n := f.requests.Load(); n != 0 {
		t.Fatalf("served reads made %d GitHub requests, want 0", n)
	}

	// A head the store never saw falls back to the API.
	if _, e := c.CheckStates(ctx, "o/r", "h9", []string{"test"}); e != nil {
		t.Fatal(e)
	}
	if f.requests.Load() == 0 {
		t.Fatal("a head with no stored checks was not read from the API")
	}
}

// (e, continued) a write by this process withdraws what the store can answer
// until an API read confirms it again.
func TestWriteSendsReadsBackToTheAPI(t *testing.T) {
	f := &stateFake{}
	f.set("h1", "2026-11-06T00:01:00Z")
	c, store, _ := shadowClient(t, f)
	c.serveWebhookState = true
	deliverPR(t, store, "d1", "h1", "2026-11-06T00:01:00Z")
	ctx := context.Background()
	if _, e := c.Pull(ctx, "o/r", 7); e != nil || f.requests.Load() != 0 {
		t.Fatalf("setup: %v, %d requests", e, f.requests.Load())
	}
	time.Sleep(2 * time.Millisecond)
	_ = c.json(ctx, "POST", "/repos/o/r/issues/7/comments", map[string]string{"body": "x"}, nil)
	f.requests.Store(0)
	if _, e := c.Pull(NewPass(ctx), "o/r", 7); e != nil {
		t.Fatal(e)
	}
	if f.requests.Load() != 1 {
		t.Fatalf("after a write, Pull made %d requests, want 1", f.requests.Load())
	}
}

// (f) the land-time reads run under FreshEvidence and always call the API,
// even with phase 2 on and a stored value that would answer.
func TestLandTimeReadsAlwaysCallTheAPI(t *testing.T) {
	f := &stateFake{}
	f.set("h1", "2026-11-06T00:01:00Z")
	c, store, _ := shadowClient(t, f)
	c.serveWebhookState = true
	deliverPR(t, store, "d1", "h1", "2026-11-06T00:01:00Z")
	ctx := context.Background()
	if _, e := c.CheckStates(ctx, "o/r", "h1", []string{"test"}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.ReviewHistory(ctx, "o/r", 7, "bot"); e != nil {
		t.Fatal(e)
	}
	f.requests.Store(0)

	fresh := FreshEvidence(NewPass(ctx))
	p, e := c.Pull(fresh, "o/r", 7)
	if e != nil || f.requests.Load() != 1 || p.MergeableState != "clean" {
		t.Fatalf("fresh Pull: %v, %d requests, mergeable_state %q", e, f.requests.Load(), p.MergeableState)
	}
	f.requests.Store(0)
	if _, e := c.CheckStates(fresh, "o/r", "h1", []string{"test"}); e != nil || f.requests.Load() == 0 {
		t.Fatalf("fresh CheckStates: %v, %d requests", e, f.requests.Load())
	}
	f.requests.Store(0)
	if _, e := c.ReviewHistory(fresh, "o/r", 7, "bot"); e != nil || f.requests.Load() == 0 {
		t.Fatalf("fresh ReviewHistory: %v, %d requests", e, f.requests.Load())
	}
	// The aggregate check the merge path calls never reads stored state.
	f.requests.Store(0)
	if _, e := c.Check(ctx, "o/r", "h1", []string{"test"}); e != nil || f.requests.Load() == 0 {
		t.Fatalf("Check: %v, %d requests", e, f.requests.Load())
	}
}

func TestPhaseOneIsTheShippedDefault(t *testing.T) {
	if ServeWebhookState || New("x").serveWebhookState {
		t.Fatal("phase 2 must ship off")
	}
}
