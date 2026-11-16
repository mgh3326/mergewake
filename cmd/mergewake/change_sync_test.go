package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// syncHK is a fake hk for the unreconciled read and the change-state batch
// write, reached through the real svc-mergewake credential. With paged it
// honors after_id and limit and answers next_after_id; without it the read
// ignores after_id, returns every row, and sends no next_after_id, like an
// hk without paging.
type syncHK struct {
	mu       sync.Mutex
	rows     []integrations.UnreconciledChange
	limits   []string
	afters   []string
	paged    bool
	batches  [][]byte
	readFail int // status of the next unreconciled read; 0 serves the rows
}

func (h *syncHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/changes/unreconciled":
		h.mu.Lock()
		h.limits = append(h.limits, r.URL.Query().Get("limit"))
		h.afters = append(h.afters, r.URL.Query().Get("after_id"))
		paged := h.paged
		fail := h.readFail
		h.readFail = 0
		rows := append([]integrations.UnreconciledChange(nil), h.rows...)
		h.mu.Unlock()
		if fail != 0 {
			w.WriteHeader(fail)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !paged {
			_ = json.NewEncoder(w).Encode(map[string]any{"changes": rows})
			return
		}
		afterID, _ := strconv.ParseInt(r.URL.Query().Get("after_id"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		var page []integrations.UnreconciledChange
		for _, row := range rows {
			if row.ID > afterID {
				page = append(page, row)
			}
		}
		var next any
		if limit > 0 && len(page) > limit {
			page = page[:limit]
			next = page[len(page)-1].ID
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"changes": page, "next_after_id": next})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/changes/batch":
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.batches = append(h.batches, body)
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sections":{"state":{"applied":true}}}`))
	default:
		http.NotFound(w, r)
	}
}

// syncGH is a fake GitHub serving one repository's pulls and the merge-commit
// GraphQL query, which is where a merged pull's merge commit comes from.
type syncGH struct {
	mu          sync.Mutex
	pulls       map[int]string
	reads       []int
	graphqlFail bool
}

func (g *syncGH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/graphql":
		body, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		fail := g.graphqlFail
		g.mu.Unlock()
		if fail {
			http.Error(w, `{"message":"boom"}`, http.StatusBadGateway)
			return
		}
		n := 0
		if m := regexp.MustCompile(`"number":(\d+)`).FindSubmatch(body); m != nil {
			n, _ = strconv.Atoi(string(m[1]))
		}
		fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"mergedAt":"2026-11-02T00:00:00Z","mergeCommit":{"oid":%q}}}}}`, syncMergeSHA(n))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/o/one/pulls/"):
		n, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/repos/o/one/pulls/"))
		g.mu.Lock()
		g.reads = append(g.reads, n)
		body, ok := g.pulls[n]
		g.mu.Unlock()
		if !ok {
			http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(body))
	default:
		http.NotFound(w, r)
	}
}

func syncMergeSHA(n int) string { return fmt.Sprintf("%040x", 9000+n) }

// syncPull is the REST pull object of a PR in one of the states the sync
// classifies. REST carries no merge_commit_sha under the client's API version,
// so a merged pull's commit comes from GraphQL.
func syncPull(n int, state string) string {
	p := map[string]any{"number": n, "head": map[string]any{"sha": fmt.Sprintf("%040x", n), "ref": fmt.Sprintf("b%d", n)}, "base": map[string]any{"ref": "main", "sha": syncMergeSHA(0)}, "user": map[string]any{"login": "author"}, "title": fmt.Sprintf("change %d", n)}
	switch state {
	case "merged":
		p["state"], p["merged"], p["merged_at"] = "closed", true, "2026-11-02T00:00:00Z"
	case "closed":
		p["state"], p["merged"] = "closed", false
	default:
		p["state"], p["merged"], p["draft"] = "open", false, false
	}
	raw, _ := json.Marshal(p)
	return string(raw)
}

type syncFixture struct {
	sync   *changeSync
	gh     *github.Client
	hk     *syncHK
	hub    *syncGH
	logs   *bytes.Buffer
	writer *integrations.ChangeWriter
}

func newSyncFixture(t *testing.T) *syncFixture {
	t.Helper()
	hk := &syncHK{}
	hkSrv := httptest.NewServer(hk)
	t.Cleanup(hkSrv.Close)
	hub := &syncGH{pulls: map[int]string{}}
	hubSrv := httptest.NewServer(hub)
	t.Cleanup(hubSrv.Close)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hkSrv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	gh := github.New("token")
	gh.BaseURL = hubSrv.URL
	gh.Stderr = io.Discard
	writer := integrations.NewChangeWriter(log)
	gh.Changes = writer
	f := &syncFixture{gh: gh, hk: hk, hub: hub, logs: logs, writer: writer}
	f.sync = &changeSync{
		rows: integrations.Handoffkeep{}.UnreconciledChanges,
		pull: gh.ReadPull,
		record: func(ctx context.Context, repo string, p github.Pull) {
			gh.RecordPull(ctx, repo, p, time.Now().UTC())
		},
		repos: map[string]bool{"o/one": true},
		log:   log,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		writer.Wait(ctx)
	})
	return f
}

// run is one sync run, then a drain of the queued change-state writes so the
// calls the fake hk received are readable.
func (f *syncFixture) run(t *testing.T) {
	t.Helper()
	f.sync.run(github.NewPass(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f.writer.Wait(ctx)
}

// stateWrite is a change-state batch as the sync sends it.
type stateWrite struct {
	Repo     string `json:"repo"`
	PR       int    `json:"pr"`
	State    string `json:"state"`
	MergeSHA string `json:"merge_sha"`
	MergedAt string `json:"merged_at"`
}

func (w stateWrite) hasMergeSHA() bool { return w.MergeSHA != "" }

// writes decodes the state batches the fake hk received.
func (f *syncFixture) writes(t *testing.T) []stateWrite {
	t.Helper()
	f.hk.mu.Lock()
	raw := append([][]byte(nil), f.hk.batches...)
	f.hk.mu.Unlock()
	out := make([]stateWrite, 0, len(raw))
	for _, b := range raw {
		var batch struct {
			Repo  string          `json:"repo"`
			PR    int             `json:"pr"`
			State json.RawMessage `json:"state"`
		}
		if e := json.Unmarshal(b, &batch); e != nil {
			t.Fatalf("batch %s: %v", b, e)
		}
		var w stateWrite
		if len(batch.State) > 0 {
			if e := json.Unmarshal(batch.State, &w); e != nil {
				t.Fatalf("state %s: %v", batch.State, e)
			}
		}
		w.Repo, w.PR = batch.Repo, batch.PR
		out = append(out, w)
	}
	return out
}

// logLines returns the decoded sync-run log lines.
func (f *syncFixture) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(f.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if e := json.Unmarshal([]byte(line), &rec); e != nil {
			t.Fatalf("log line %q: %v", line, e)
		}
		if rec["msg"] == "hk change sync run" {
			out = append(out, rec)
		}
	}
	return out
}

func (f *syncFixture) pulls() []int {
	f.hub.mu.Lock()
	defer f.hub.mu.Unlock()
	return append([]int(nil), f.hub.reads...)
}

// A row whose PR GitHub holds merged is written back as merged, with the merge
// commit and time the state write needs.
func TestChangeSyncWritesMerged(t *testing.T) {
	f := newSyncFixture(t)
	f.hub.pulls[2] = syncPull(2, "merged")
	f.hk.rows = []integrations.UnreconciledChange{{ID: 1, Repo: "o/one", PRNumber: 2, State: "open"}}
	f.run(t)
	writes := f.writes(t)
	if len(writes) != 1 {
		t.Fatalf("%d writes, want 1", len(writes))
	}
	w := writes[0]
	if w.Repo != "o/one" || w.PR != 2 || w.State != "merged" || w.MergeSHA != syncMergeSHA(2) || w.MergedAt == "" {
		t.Fatalf("write %+v", w)
	}
	if lines := f.logLines(t); len(lines) != 1 || lines[0]["merged"] != float64(1) || lines[0]["open"] != float64(0) {
		t.Fatalf("log %v", lines)
	}
}

// A row whose PR GitHub holds closed without a merge is written back closed.
func TestChangeSyncWritesClosed(t *testing.T) {
	f := newSyncFixture(t)
	f.hub.pulls[3] = syncPull(3, "closed")
	f.hk.rows = []integrations.UnreconciledChange{{ID: 1, Repo: "o/one", PRNumber: 3, State: "open"}}
	f.run(t)
	writes := f.writes(t)
	if len(writes) != 1 || writes[0].PR != 3 || writes[0].State != "closed" || writes[0].hasMergeSHA() {
		t.Fatalf("writes %+v", writes)
	}
	if lines := f.logLines(t); len(lines) != 1 || lines[0]["closed"] != float64(1) {
		t.Fatalf("log %v", lines)
	}
}

// A row whose PR is still open is read and counted, and nothing is written.
func TestChangeSyncSkipsOpenRow(t *testing.T) {
	f := newSyncFixture(t)
	f.hub.pulls[4] = syncPull(4, "open")
	f.hk.rows = []integrations.UnreconciledChange{{ID: 1, Repo: "o/one", PRNumber: 4, State: "open"}}
	f.run(t)
	if writes := f.writes(t); len(writes) != 0 {
		t.Fatalf("open row written: %+v", writes)
	}
	if got := f.pulls(); len(got) != 1 || got[0] != 4 {
		t.Fatalf("pulls %v", got)
	}
	if lines := f.logLines(t); len(lines) != 1 || lines[0]["open"] != float64(1) {
		t.Fatalf("log %v", lines)
	}
}

// A row for a repository this host does not configure is counted and skipped,
// never read.
func TestChangeSyncSkipsOtherRepos(t *testing.T) {
	f := newSyncFixture(t)
	f.hub.pulls[2] = syncPull(2, "merged")
	f.hk.rows = []integrations.UnreconciledChange{
		{ID: 1, Repo: "o/other", PRNumber: 9, State: "open"},
		{ID: 2, Repo: "o/one", PRNumber: 2, State: "open"},
	}
	f.run(t)
	if got := f.pulls(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("pulls %v", got)
	}
	if writes := f.writes(t); len(writes) != 1 || writes[0].PR != 2 {
		t.Fatalf("writes %+v", writes)
	}
	lines := f.logLines(t)
	if len(lines) != 1 || lines[0]["other_repo"] != float64(1) || lines[0]["merged"] != float64(1) {
		t.Fatalf("log %v", lines)
	}
}

// An hk without after_id paging answers one page, so a run checks its bound
// from where the previous run stopped inside it and wraps at the end: rows
// that stay open at the head do not starve the rest.
func TestChangeSyncBoundRotates(t *testing.T) {
	oldRows, oldScan := changeSyncRows, changeSyncScan
	changeSyncRows, changeSyncScan = 2, 6
	t.Cleanup(func() { changeSyncRows, changeSyncScan = oldRows, oldScan })

	f := newSyncFixture(t)
	for i := 1; i <= 6; i++ {
		f.hub.pulls[10+i] = syncPull(10+i, "open")
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: 10 + i, State: "open"})
	}
	for range 4 {
		f.run(t)
	}
	want := []int{11, 12, 13, 14, 15, 16, 11, 12}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
	f.hk.mu.Lock()
	limits := append([]string(nil), f.hk.limits...)
	f.hk.mu.Unlock()
	if len(limits) != 4 || limits[0] != "6" {
		t.Fatalf("read limits %v", limits)
	}
}

// An hk that lacks the endpoint or fails is logged, and the next run retries
// and writes.
func TestChangeSyncRetriesAfterReadFailure(t *testing.T) {
	for name, status := range map[string]int{"missing endpoint": http.StatusNotFound, "server error": http.StatusBadGateway} {
		t.Run(name, func(t *testing.T) {
			f := newSyncFixture(t)
			f.hub.pulls[2] = syncPull(2, "merged")
			f.hk.rows = []integrations.UnreconciledChange{{ID: 1, Repo: "o/one", PRNumber: 2, State: "open"}}
			f.hk.readFail = status
			f.run(t)
			if writes := f.writes(t); len(writes) != 0 {
				t.Fatalf("failed read still wrote: %+v", writes)
			}
			lines := f.logLines(t)
			if len(lines) != 1 || lines[0]["errors"] != float64(1) || lines[0]["error"] == nil {
				t.Fatalf("log %v", lines)
			}
			f.run(t)
			if writes := f.writes(t); len(writes) != 1 || writes[0].State != "merged" {
				t.Fatalf("retry wrote %+v", writes)
			}
		})
	}
}

// A merged pull whose merge commit cannot be read is left for the next run
// instead of being written without one.
func TestChangeSyncMergedWithoutMergeCommitWritesNothing(t *testing.T) {
	f := newSyncFixture(t)
	f.hub.pulls[2] = syncPull(2, "merged")
	f.hub.graphqlFail = true
	f.hk.rows = []integrations.UnreconciledChange{{ID: 1, Repo: "o/one", PRNumber: 2, State: "open"}}
	f.run(t)
	if writes := f.writes(t); len(writes) != 0 {
		t.Fatalf("merged write without a merge commit: %+v", writes)
	}
	if lines := f.logLines(t); len(lines) != 1 || lines[0]["errors"] != float64(1) {
		t.Fatalf("log %v", lines)
	}
}

// A run whose context ends stops before it advances the cursor: the rows it
// did not reach are the next run's first rows, and the row it was reading is
// retried from the retry set.
func TestChangeSyncTimeoutLeavesCursor(t *testing.T) {
	oldRows := changeSyncRows
	changeSyncRows = 3
	t.Cleanup(func() { changeSyncRows = oldRows })

	var rows []integrations.UnreconciledChange
	for i := int64(1); i <= 5; i++ {
		rows = append(rows, integrations.UnreconciledChange{ID: i, Repo: "o/one", PRNumber: int(10 + i), State: "open"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls []int
	s := &changeSync{
		rows: func(context.Context, int64, int) (integrations.UnreconciledPage, error) {
			return integrations.UnreconciledPage{Changes: rows}, nil
		},
		pull: func(pctx context.Context, repo string, n int) (github.Pull, error) {
			calls = append(calls, n)
			if len(calls) == 2 {
				cancel()
				return github.Pull{}, pctx.Err()
			}
			return github.Pull{State: "open"}, nil
		},
		record: func(context.Context, string, github.Pull) {},
		repos:  map[string]bool{"o/one": true},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s.run(ctx)
	if s.after != 2 {
		t.Fatalf("after %d, want 2", s.after)
	}
	s.run(context.Background())
	want := []int{11, 12, 12, 13, 14, 15}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", calls, want)
	}
}

// On a wrap the cursor drops back to the last row the run visited, so the
// rotation keeps advancing instead of re-checking the same first rows.
func TestChangeSyncWrapAdvancesCursor(t *testing.T) {
	oldRows := changeSyncRows
	changeSyncRows = 2
	t.Cleanup(func() { changeSyncRows = oldRows })

	f := newSyncFixture(t)
	for i := 1; i <= 5; i++ {
		f.hub.pulls[30+i] = syncPull(30+i, "open")
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: 30 + i, State: "open"})
	}
	f.sync.after = 5
	for range 4 {
		f.run(t)
	}
	want := []int{31, 32, 33, 34, 35, 31, 32}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
}

// A row whose PR read fails is read again on the very next run, even when the
// list is longer than one run's window.
func TestChangeSyncRetriesFailedRowNextRun(t *testing.T) {
	oldRows := changeSyncRows
	changeSyncRows = 2
	t.Cleanup(func() { changeSyncRows = oldRows })

	f := newSyncFixture(t)
	for i := 1; i <= 5; i++ {
		if i != 1 {
			f.hub.pulls[20+i] = syncPull(20+i, "open")
		}
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: 20 + i, State: "open"})
	}
	f.run(t)
	f.hub.pulls[21] = syncPull(21, "open")
	f.run(t)
	want := []int{21, 22, 21, 23, 24}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
}

// The retry set is bounded: past the bound the oldest unfinished rows wait
// for the rotation like any other.
func TestChangeSyncRetrySetBounded(t *testing.T) {
	oldRetries := changeSyncRetries
	changeSyncRetries = 2
	t.Cleanup(func() { changeSyncRetries = oldRetries })

	f := newSyncFixture(t)
	for i := 1; i <= 4; i++ {
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: 20 + i, State: "open"})
	}
	f.run(t)
	if len(f.sync.retry) != 2 || len(f.sync.retryOrder) != 2 {
		t.Fatalf("retry %v order %v, want 2 ids", f.sync.retry, f.sync.retryOrder)
	}
	for _, id := range []int64{1, 2} {
		if _, ok := f.sync.retry[id]; ok {
			t.Fatalf("retry keeps evicted id %d: %v", id, f.sync.retryOrder)
		}
	}
	for _, id := range []int64{3, 4} {
		if _, ok := f.sync.retry[id]; !ok {
			t.Fatalf("retry lost id %d: %v", id, f.sync.retryOrder)
		}
	}
}

// When the retry pass's time bound is spent, the window still makes progress
// and the unreached retries stay queued.
func TestChangeSyncRetryTimeKeepsWindowProgress(t *testing.T) {
	oldRows, oldTime := changeSyncRows, changeSyncRetryTime
	changeSyncRows, changeSyncRetryTime = 2, -time.Second
	t.Cleanup(func() { changeSyncRows, changeSyncRetryTime = oldRows, oldTime })

	f := newSyncFixture(t)
	for i := 1; i <= 4; i++ {
		if i != 1 {
			f.hub.pulls[40+i] = syncPull(40+i, "open")
		}
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: 40 + i, State: "open"})
	}
	f.run(t) // window=2: pull 41 fails (no hub entry), pull 42 open
	f.hub.pulls[41] = syncPull(41, "open")
	f.run(t) // retry budget spent: window rows 3-4 only
	want := []int{41, 42, 43, 44}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
	if _, ok := f.sync.retry[1]; !ok {
		t.Fatalf("retry lost id 1: %v", f.sync.retryOrder)
	}
}

// pagedRows is a changeSync.rows over all that honors after_id and limit and
// answers next_after_id, like a paging hk.
func pagedRows(all []integrations.UnreconciledChange) func(context.Context, int64, int) (integrations.UnreconciledPage, error) {
	return func(_ context.Context, after int64, limit int) (integrations.UnreconciledPage, error) {
		var page []integrations.UnreconciledChange
		for _, r := range all {
			if r.ID > after {
				page = append(page, r)
			}
		}
		out := integrations.UnreconciledPage{Paged: true, Changes: page}
		if limit > 0 && len(page) > limit {
			page = page[:limit]
			out.Changes = page
			next := page[len(page)-1].ID
			out.NextAfterID = &next
		}
		return out, nil
	}
}

// A paging hk lets one run read past the first page: successive runs reach
// every row, and the cursor wraps to the start after the list's end.
func TestChangeSyncPagesForward(t *testing.T) {
	oldRows, oldScan := changeSyncRows, changeSyncScan
	changeSyncRows, changeSyncScan = 4, 3
	t.Cleanup(func() { changeSyncRows, changeSyncScan = oldRows, oldScan })

	f := newSyncFixture(t)
	f.hk.paged = true
	for i := 1; i <= 7; i++ {
		f.hub.pulls[i] = syncPull(i, "open")
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: i, State: "open"})
	}
	for range 3 {
		f.run(t)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
	f.hk.mu.Lock()
	afters := append([]string(nil), f.hk.afters...)
	f.hk.mu.Unlock()
	if len(afters) < 2 || afters[0] != "0" || afters[1] != "3" {
		t.Fatalf("after_ids %v", afters)
	}
}

// A paged run whose context ends stops before it advances the cursor: the
// rows it did not reach are the next run's first rows, and the row it was
// reading is retried from the retry set.
func TestChangeSyncPagedTimeoutLeavesCursor(t *testing.T) {
	oldRows, oldScan := changeSyncRows, changeSyncScan
	changeSyncRows, changeSyncScan = 3, 2
	t.Cleanup(func() { changeSyncRows, changeSyncScan = oldRows, oldScan })

	var all []integrations.UnreconciledChange
	for i := int64(1); i <= 5; i++ {
		all = append(all, integrations.UnreconciledChange{ID: i, Repo: "o/one", PRNumber: int(10 + i), State: "open"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls []int
	s := &changeSync{
		rows: pagedRows(all),
		pull: func(pctx context.Context, repo string, n int) (github.Pull, error) {
			calls = append(calls, n)
			if len(calls) == 2 {
				cancel()
				return github.Pull{}, pctx.Err()
			}
			return github.Pull{State: "open"}, nil
		},
		record: func(context.Context, string, github.Pull) {},
		repos:  map[string]bool{"o/one": true},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s.run(ctx)
	if s.after != 2 {
		t.Fatalf("after %d, want 2", s.after)
	}
	s.run(context.Background())
	want := []int{11, 12, 12, 13, 14, 15}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", calls, want)
	}
}

// cancelAfter wraps pull so the cancelAt'th call cancels the run's context
// once the read has answered, leaving the run's failure count untouched.
func cancelAfter(pull func(context.Context, string, int) (github.Pull, error), cancelAt int, cancel context.CancelFunc) func(context.Context, string, int) (github.Pull, error) {
	calls := 0
	return func(ctx context.Context, repo string, n int) (github.Pull, error) {
		calls++
		p, e := pull(ctx, repo, n)
		if calls == cancelAt {
			cancel()
		}
		return p, e
	}
}

// unreconciledReads copies the after_id of each unreconciled read the fake hk
// received.
func (h *syncHK) unreconciledReads() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.afters...)
}

// A context that ends during the retry pass stops the run before any window
// read: the fake hk sees no request after it ended, the run logs no failure,
// and the next run starts at the first row the cursor did not reach.
func TestChangeSyncTimeoutDuringRetryMakesNoRead(t *testing.T) {
	f := newSyncFixture(t)
	f.hk.paged = true
	for i := 1; i <= 6; i++ {
		f.hub.pulls[i] = syncPull(i, "open")
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: i, State: "open"})
	}
	f.sync.after = 2
	f.sync.retry = map[int64]struct{}{5: {}}
	f.sync.retryOrder = []int64{5}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sync.pull = cancelAfter(f.sync.pull, 1, cancel)
	f.sync.run(ctx)

	if got := f.hk.unreconciledReads(); fmt.Sprint(got) != fmt.Sprint([]string{"4"}) {
		t.Fatalf("unreconciled reads %v, want [4]", got)
	}
	if lines := f.logLines(t); len(lines) != 1 || lines[0]["errors"] != float64(0) {
		t.Fatalf("log %v", lines)
	}
	if f.sync.after != 2 {
		t.Fatalf("after %d, want 2", f.sync.after)
	}
	f.run(t)
	if got := f.pulls(); got[0] != 5 || got[1] != 3 {
		t.Fatalf("pull reads %v, want 5 then 3", got)
	}
	if got := f.hk.unreconciledReads(); len(got) != 2 || got[1] != "2" {
		t.Fatalf("unreconciled reads %v, want a second read from 2", got)
	}
}

// A context that ends exactly at a page boundary stops the run before the
// next page's read: the fake hk sees no request after it ended, the run logs
// no failure, and the next run starts at the first unchecked row.
func TestChangeSyncTimeoutAtPageBoundaryMakesNoRead(t *testing.T) {
	oldRows, oldScan := changeSyncRows, changeSyncScan
	changeSyncRows, changeSyncScan = 10, 2
	t.Cleanup(func() { changeSyncRows, changeSyncScan = oldRows, oldScan })

	f := newSyncFixture(t)
	f.hk.paged = true
	for i := 1; i <= 4; i++ {
		f.hub.pulls[i] = syncPull(i, "open")
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: i, State: "open"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sync.pull = cancelAfter(f.sync.pull, 2, cancel)
	f.sync.run(ctx)

	if got := f.hk.unreconciledReads(); fmt.Sprint(got) != fmt.Sprint([]string{"0"}) {
		t.Fatalf("unreconciled reads %v, want [0]", got)
	}
	if lines := f.logLines(t); len(lines) != 1 || lines[0]["errors"] != float64(0) {
		t.Fatalf("log %v", lines)
	}
	if f.sync.after != 2 {
		t.Fatalf("after %d, want 2", f.sync.after)
	}
	f.run(t)
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint([]int{1, 2, 3, 4}) {
		t.Fatalf("pull reads %v, want [1 2 3 4]", got)
	}
	if got := f.hk.unreconciledReads(); len(got) != 2 || got[1] != "2" {
		t.Fatalf("unreconciled reads %v, want a second read from 2", got)
	}
}

// A retry row that sits behind the cursor is checked first on the next run:
// the retry pass reads from just before the smallest retry id.
func TestChangeSyncRetryBehindCursor(t *testing.T) {
	oldRows, oldScan := changeSyncRows, changeSyncScan
	changeSyncRows, changeSyncScan = 2, 3
	t.Cleanup(func() { changeSyncRows, changeSyncScan = oldRows, oldScan })

	f := newSyncFixture(t)
	f.hk.paged = true
	for i := 1; i <= 5; i++ {
		if i != 1 {
			f.hub.pulls[i] = syncPull(i, "open")
		}
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: i, State: "open"})
	}
	f.run(t) // pull 1 fails and joins the retry set; the window checks 1-2
	f.hub.pulls[1] = syncPull(1, "open")
	f.run(t) // the retry pass reads row 1 first, then the window rows 3-4
	want := []int{1, 2, 1, 3, 4}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
}

// An hk answer without next_after_id gives the one-page behaviour: one read
// per run and the cursor rotating inside the page.
func TestChangeSyncUnpagedReadsOnePage(t *testing.T) {
	oldRows := changeSyncRows
	changeSyncRows = 2
	t.Cleanup(func() { changeSyncRows = oldRows })

	f := newSyncFixture(t)
	for i := 1; i <= 5; i++ {
		f.hub.pulls[i] = syncPull(i, "open")
		f.hk.rows = append(f.hk.rows, integrations.UnreconciledChange{ID: int64(i), Repo: "o/one", PRNumber: i, State: "open"})
	}
	for range 4 {
		f.run(t)
	}
	want := []int{1, 2, 3, 4, 5, 1, 2}
	if got := f.pulls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pull reads %v, want %v", got, want)
	}
	f.hk.mu.Lock()
	reads := len(f.hk.limits)
	f.hk.mu.Unlock()
	if reads != 4 {
		t.Fatalf("%d unreconciled reads, want one per run", reads)
	}
}

// The sync runs with the handoffkeep sink and a change-state writer, hourly
// unless configured, and "off" stops it.
func TestChangeSyncEnabled(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sink   config.Sink
		writer bool
		want   time.Duration
		on     bool
	}{
		{"default", config.Sink{Type: "handoffkeep"}, true, config.DefaultReconcileEvery, true},
		{"configured", config.Sink{Type: "handoffkeep", ReconcileEvery: "15m"}, true, 15 * time.Minute, true},
		{"off", config.Sink{Type: "handoffkeep", ReconcileEvery: "off"}, true, 0, false},
		{"no writer", config.Sink{Type: "handoffkeep"}, false, 0, false},
		{"other sink", config.Sink{Type: "noop"}, true, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			every, on := changeSyncEnabled(config.Config{Sink: tc.sink}, tc.writer)
			if on != tc.on || every != tc.want {
				t.Fatalf("%v %v, want %v %v", every, on, tc.want, tc.on)
			}
		})
	}
}
