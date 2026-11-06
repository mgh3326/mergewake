package daemon

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/prstatus"
)

type statusFixture struct {
	srv      *httptest.Server
	stateDir string
	requests *atomic.Int32
	graphql  *atomic.Int32
	// head is what the fake GitHub reports as the PR head.
	head *atomic.Value
	// wakes and drops count the pr-role wakes and evidence invalidations.
	wakes, drops *atomic.Int32
}

// newStatusFixture serves the daemon's listener handler with a fake GitHub
// behind the fresh reader, counting every request that reaches GitHub.
func newStatusFixture(t *testing.T, triggered *[]string) statusFixture {
	t.Helper()
	f := statusFixture{stateDir: t.TempDir(), requests: new(atomic.Int32), graphql: new(atomic.Int32), head: new(atomic.Value), wakes: new(atomic.Int32), drops: new(atomic.Int32)}
	f.head.Store("aaaaaaa")
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		if r.Method == http.MethodPost && r.URL.Path == "/graphql" {
			f.graphql.Add(1)
		}
		fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"headRefOid":%q,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","reviewThreads":{"nodes":[{"isResolved":false},{"isResolved":true}],"pageInfo":{"hasNextPage":false}},"commits":{"nodes":[{"commit":{"statusCheckRollup":{"state":"SUCCESS"}}}]}}}}}`, f.head.Load())
	}))
	t.Cleanup(gh.Close)
	client := github.New("test-token")
	client.BaseURL = gh.URL
	d := webhookDaemon(triggered)
	d.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	d.Webhook.Status = prstatus.Handler{StateDir: f.stateDir, Fresh: client.PRFresh,
		Wake:       func() { f.wakes.Add(1); (*triggered) = append(*triggered, "pr") },
		Invalidate: func(string) { f.drops.Add(1) }}
	f.srv = httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	t.Cleanup(f.srv.Close)
	return f
}

func (f statusFixture) write(t *testing.T, repo string, n int, state string) prstatus.Snapshot {
	t.Helper()
	one := 1
	snap := prstatus.Snapshot{Repo: repo, PR: n, Head: "aaaaaaa", State: state, Reasons: []string{"review_missing"}, Threads: prstatus.Threads{Unresolved: &one},
		Checks: []prstatus.Check{{Name: "test", State: "green"}}, ObservedAt: time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC)}
	if err := prstatus.Write(f.stateDir, snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

func (f statusFixture) get(t *testing.T, query string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(f.srv.URL + "/v1/pr-status?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if len(b) > prstatus.MaxResponse {
		t.Fatalf("response is %d bytes", len(b))
	}
	return resp.StatusCode, b
}

// (b) The endpoint returns the snapshot and makes no GitHub request.
func TestPRStatusEndpointServesSnapshotWithoutGitHub(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	want := f.write(t, "o/r", 7, "blocked")
	code, body := f.get(t, "repo=o/r&pr=7")
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["state"] != "blocked" || got["head"] != want.Head || got["source"] != "last_pass" || got["observed_at"] != "2026-11-06T01:02:03Z" || got["fresh"] != nil {
		t.Fatalf("body: %s", body)
	}
	if n := f.requests.Load(); n != 0 {
		t.Fatalf("%d GitHub requests for a plain read", n)
	}
}

// (c) A PR with no snapshot is 404 and a malformed query is 400.
func TestPRStatusEndpointRejectsUnknownAndBadInput(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	f.write(t, "o/r", 7, "blocked")
	for query, want := range map[string]int{
		"repo=o/r&pr=8":           http.StatusNotFound,
		"repo=o/other&pr=7":       http.StatusNotFound,
		"repo=notarepo&pr=7":      http.StatusBadRequest,
		"repo=../etc&pr=7":        http.StatusBadRequest,
		"repo=o/..&pr=7":          http.StatusBadRequest,
		"pr=7":                    http.StatusBadRequest,
		"repo=o/r&pr=abc":         http.StatusBadRequest,
		"repo=o/r&pr=0":           http.StatusBadRequest,
		"repo=o/r&pr=7&fresh=yes": http.StatusBadRequest,
		"repo=o/r&fresh=1":        http.StatusBadRequest,
		"repo=o/r&pr=8&fresh=1":   http.StatusNotFound,
	} {
		if code, body := f.get(t, query); code != want {
			t.Errorf("%s: status %d, want %d: %s", query, code, want, body)
		}
	}
	if n := f.requests.Load(); n != 0 {
		t.Fatalf("%d GitHub requests for rejected reads", n)
	}
	resp, err := http.Post(f.srv.URL+"/v1/pr-status?repo=o/r&pr=7", "text/plain", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status %d", resp.StatusCode)
	}
}

// (d) fresh=1 makes exactly one GraphQL call and writes it through: head,
// fresh block and fresh_at move, the gate fields stay as the last pass wrote
// them. A changed head marks the PR dirty, drops stale evidence and wakes the
// pr role once.
func TestPRStatusFreshWritesThroughAndMarksChangedHead(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	f.write(t, "o/r", 7, "blocked")
	_, plain := f.get(t, "repo=o/r&pr=7")
	f.head.Store("fffffff")
	code, body := f.get(t, "repo=o/r&pr=7&fresh=1")
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	if f.requests.Load() != 1 || f.graphql.Load() != 1 {
		t.Fatalf("GitHub requests=%d graphql=%d, want 1/1", f.requests.Load(), f.graphql.Load())
	}
	var withFresh, without map[string]any
	if err := json.Unmarshal(body, &withFresh); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(plain, &without); err != nil {
		t.Fatal(err)
	}
	if without["fresh_at"] != nil || without["fresh"] != nil {
		t.Fatalf("plain read carries a fresh read: %s", plain)
	}
	fresh, _ := withFresh["fresh"].(map[string]any)
	if fresh["head"] != "fffffff" || fresh["checks_rollup"] != "SUCCESS" || fresh["unresolved_threads"] != float64(1) || fresh["mergeable"] != "MERGEABLE" || fresh["merge_state"] != "CLEAN" {
		t.Fatalf("fresh: %s", body)
	}
	if withFresh["fresh_at"] == nil || withFresh["head"] != "fffffff" || withFresh["gate_head"] != "aaaaaaa" {
		t.Fatalf("write-through: %s", body)
	}
	for _, k := range []string{"state", "reasons", "checks", "review", "threads", "findings", "observed_at", "source"} {
		a, _ := json.Marshal(withFresh[k])
		b, _ := json.Marshal(without[k])
		if string(a) != string(b) {
			t.Errorf("gate field %s changed: %s -> %s", k, b, a)
		}
	}
	if withFresh["source"] != "last_pass" || withFresh["observed_at"] != "2026-11-06T01:02:03Z" {
		t.Fatalf("gate time or source moved: %s", body)
	}
	// The write-through is durable: a plain read now carries it, free.
	code, again := f.get(t, "repo=o/r&pr=7")
	var stored map[string]any
	if err := json.Unmarshal(again, &stored); err != nil || code != 200 || stored["head"] != "fffffff" || stored["fresh_at"] != withFresh["fresh_at"] || stored["state"] != "blocked" {
		t.Fatalf("stored snapshot: %d %s %v", code, again, err)
	}
	if !prstatus.DirtyPRs(f.stateDir, "o/r")[7] {
		t.Fatal("changed head left no dirty marker")
	}
	if f.wakes.Load() != 1 || f.drops.Load() != 1 || len(triggered) != 1 || triggered[0] != "pr" {
		t.Fatalf("wakes=%d drops=%d triggered=%v", f.wakes.Load(), f.drops.Load(), triggered)
	}
	// The same news twice is not news: no second wake.
	f.get(t, "repo=o/r&pr=7&fresh=1")
	if f.wakes.Load() != 1 {
		t.Fatalf("repeat fresh woke again: %d", f.wakes.Load())
	}
}

// A fresh read that agrees with the snapshot stamps fresh_at and nothing else:
// no marker, no wake.
func TestPRStatusFreshUnchangedLeavesNoMarker(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	f.write(t, "o/r", 7, "blocked")
	code, body := f.get(t, "repo=o/r&pr=7&fresh=1")
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil || code != 200 || got["fresh_at"] == nil || got["head"] != "aaaaaaa" {
		t.Fatalf("%d %s %v", code, body, err)
	}
	if len(prstatus.DirtyPRs(f.stateDir, "o/r")) != 0 || f.wakes.Load() != 0 || f.drops.Load() != 0 || len(triggered) != 0 {
		t.Fatalf("dirty=%v wakes=%d drops=%d", prstatus.DirtyPRs(f.stateDir, "o/r"), f.wakes.Load(), f.drops.Load())
	}
}

// A changed unresolved thread count alone also marks the PR.
func TestPRStatusFreshThreadChangeMarksPR(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	snap := f.write(t, "o/r", 7, "blocked")
	zero := 0
	snap.Threads.Unresolved = &zero
	if err := prstatus.Write(f.stateDir, snap); err != nil {
		t.Fatal(err)
	}
	f.get(t, "repo=o/r&pr=7&fresh=1")
	if !prstatus.DirtyPRs(f.stateDir, "o/r")[7] || f.wakes.Load() != 1 {
		t.Fatalf("dirty=%v wakes=%d", prstatus.DirtyPRs(f.stateDir, "o/r"), f.wakes.Load())
	}
}

// A gate decision after the marker clears it; one before the pass does not.
func TestPRStatusDirtyMarkerClearsOnlyForALaterPass(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 11, 6, 2, 0, 0, 0, time.UTC)
	if err := prstatus.MarkDirty(dir, "o/r", 7, at); err != nil {
		t.Fatal(err)
	}
	prstatus.ClearDirty(dir, "o/r", 7, at.Add(-time.Second))
	if !prstatus.DirtyPRs(dir, "o/r")[7] {
		t.Fatal("a pass that began before the marker cleared it")
	}
	prstatus.ClearDirty(dir, "o/r", 7, at.Add(time.Second))
	if len(prstatus.DirtyPRs(dir, "o/r")) != 0 {
		t.Fatal("a later pass left the marker")
	}
}

// A marker written while a pass is clearing survives: ClearDirty reads and
// removes under the lock MarkDirty takes, so it cannot delete a newer marker it
// never read.
func TestPRStatusClearDirtyDoesNotDeleteConcurrentMarker(t *testing.T) {
	dir := t.TempDir()
	started := time.Date(2026, 11, 6, 2, 0, 0, 0, time.UTC)
	if err := prstatus.MarkDirty(dir, "o/r", 7, started.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	// Hold MarkDirty's lock the way a marker write in flight does.
	unlock, err := fsutil.Lock(filepath.Join(dir, "o/r", "pr-status", "7.dirty.lock"), true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		prstatus.ClearDirty(dir, "o/r", 7, started)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("ClearDirty ran without taking the marker lock")
	case <-time.After(200 * time.Millisecond):
	}
	if err := fsutil.Atomic(filepath.Join(dir, "o/r", "pr-status", "7.dirty"), []byte(started.Add(time.Minute).Format(time.RFC3339Nano)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	unlock()
	<-done
	if !prstatus.DirtyPRs(dir, "o/r")[7] {
		t.Fatal("a marker written during the pass was cleared")
	}
}

// (f) The list form returns the repo's open PRs only.
func TestPRStatusListReturnsOpenPRsOnly(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	f.write(t, "o/r", 5, "ready")
	f.write(t, "o/r", 6, "merged")
	f.write(t, "o/r", 7, "blocked")
	f.write(t, "o/r", 8, "closed")
	f.write(t, "o/other", 9, "ready")
	code, body := f.get(t, "repo=o/r")
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	var list struct {
		Repo string
		PRs  []prstatus.Snapshot
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	if list.Repo != "o/r" || len(list.PRs) != 2 || list.PRs[0].PR != 7 || list.PRs[1].PR != 5 {
		t.Fatalf("list: %s", body)
	}
	if code, body := f.get(t, "repo=o/none"); code != http.StatusOK || !strings.Contains(string(body), `"prs":[]`) {
		t.Fatalf("empty repo: %d %s", code, body)
	}
	if n := f.requests.Load(); n != 0 {
		t.Fatalf("%d GitHub requests for a list", n)
	}
}

// (g) Adding the status route leaves the webhook endpoint as it was: the
// delivery path still needs a signature and a POST, and other paths stay 404.
func TestPRStatusLeavesWebhookEndpointUnchanged(t *testing.T) {
	var triggered []string
	f := newStatusFixture(t, &triggered)
	resp := post(t, f.srv.Client(), f.srv.URL+"/github", "check_run", "d1", []byte(`{"repository":{"full_name":"o/r"}}`), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || len(triggered) == 0 {
		t.Fatalf("signed delivery: status %d triggered %v", resp.StatusCode, triggered)
	}
	resp = post(t, f.srv.Client(), f.srv.URL+"/github", "check_run", "d2", []byte(`{}`), []byte("wrong"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad signature: status %d", resp.StatusCode)
	}
	if r, err := http.Get(f.srv.URL + "/github"); err != nil || r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /github: %v %v", r, err)
	}
	if r, err := http.Get(f.srv.URL + "/elsewhere"); err != nil || r.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /elsewhere: %v %v", r, err)
	}
	// Without a Status handler the status path is a plain 404.
	d := webhookDaemon(&triggered)
	d.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	bare := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer bare.Close()
	if r, err := http.Get(bare.URL + "/v1/pr-status?repo=o/r&pr=7"); err != nil || r.StatusCode != http.StatusNotFound {
		t.Fatalf("status path without handler: %v %v", r, err)
	}
}
