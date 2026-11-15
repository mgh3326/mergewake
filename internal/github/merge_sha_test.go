package github_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/mergewake/internal/changestate"
	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

const mergeSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

const mergedAt = "2026-11-06T09:40:49Z"

type stateWrites struct {
	mu     sync.Mutex
	states []map[string]any
}

func (w *stateWrites) Queue(_ string, _ int, name string, s changestate.Section) {
	if name != "state" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.states = append(w.states, s.Value.(map[string]any))
}
func (w *stateWrites) Async(f func())             { f() }
func (w *stateWrites) Counts() changestate.Counts { return changestate.Counts{} }
func (w *stateWrites) Wait(context.Context)       {}
func (w *stateWrites) all() []map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]map[string]any(nil), w.states...)
}

func mergedBody(sha string) string {
	field := ""
	if sha != "" {
		field = fmt.Sprintf(`"merge_commit_sha":%q,`, sha)
	}
	return fmt.Sprintf(`{"number":7,"state":"closed","merged":true,"merged_at":%q,%s"title":"t","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"},"user":{"login":"a"}}`, mergedAt, field, testSHA)
}

// graphQLMergeCommit is GitHub's answer to the merge commit query.
func graphQLMergeCommit(sha, at string) string {
	mergedAt := "null"
	if at != "" {
		mergedAt = fmt.Sprintf("%q", at)
	}
	commit := "null"
	if sha != "" {
		commit = fmt.Sprintf(`{"oid":%q}`, sha)
	}
	return fmt.Sprintf(`{"data":{"repository":{"pullRequest":{"mergedAt":%s,"mergeCommit":%s}}}}`, mergedAt, commit)
}

// mergeAPI answers the single-PR REST read and the merge commit query, and
// counts both.
type mergeAPI struct {
	pull    string
	graph   string
	pulls   atomic.Int32
	graphql atomic.Int32
}

func (m *mergeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == "POST" && r.URL.Path == "/graphql":
		m.graphql.Add(1)
		fmt.Fprint(w, m.graph)
	default:
		m.pulls.Add(1)
		fmt.Fprint(w, m.pull)
	}
}

func requireMergedWrite(t *testing.T, v map[string]any) {
	t.Helper()
	if v["state"] != "merged" || v["merge_sha"] != mergeSHA || v["merged_at"] != mergedAt {
		t.Fatalf("merged state write %v", v)
	}
}

// (a) A merged PR body without merge_commit_sha resolves the field through
// GraphQL, and the state write carries it.
func TestMergedPullResolvesMergeCommitFromGraphQL(t *testing.T) {
	m := &mergeAPI{pull: mergedBody(""), graph: graphQLMergeCommit(mergeSHA, mergedAt)}
	c := changeAPI(t, m.ServeHTTP)
	c.Stderr = io.Discard
	rec := &stateWrites{}
	c.Changes = rec
	p, e := c.Pull(context.Background(), "o/r", 7)
	if e != nil {
		t.Fatal(e)
	}
	if p.MergeCommitSHA != mergeSHA || m.graphql.Load() != 1 {
		t.Fatalf("sha %q with %d GraphQL calls", p.MergeCommitSHA, m.graphql.Load())
	}
	writes := rec.all()
	if len(writes) != 1 {
		t.Fatalf("writes=%v", writes)
	}
	requireMergedWrite(t, writes[0])
	if got := c.ChangeCounts().Skipped; got != 0 {
		t.Fatalf("skipped=%d", got)
	}
}

// (b) When the body also lacks merged_at, the same query supplies it.
func TestMergedPullWithoutMergedAtResolvesBoth(t *testing.T) {
	const body = `{"number":7,"state":"closed","merged":true,"title":"t","head":{"sha":"h"},"base":{"sha":"b1","ref":"main"},"user":{"login":"a"}}`
	m := &mergeAPI{pull: body, graph: graphQLMergeCommit(mergeSHA, mergedAt)}
	c := changeAPI(t, m.ServeHTTP)
	c.Stderr = io.Discard
	rec := &stateWrites{}
	c.Changes = rec
	p, e := c.Pull(context.Background(), "o/r", 7)
	if e != nil {
		t.Fatal(e)
	}
	if p.MergeCommitSHA != mergeSHA || p.MergedAt == nil || *p.MergedAt != mergedAt {
		t.Fatalf("pull %+v", p)
	}
	if writes := rec.all(); len(writes) != 1 {
		t.Fatalf("writes=%v", writes)
	} else {
		requireMergedWrite(t, writes[0])
	}
}

// (c) A GraphQL error is the old "unknown": no sha, no state write, and the
// skip is counted.
func TestMergedPullGraphQLErrorWritesNothing(t *testing.T) {
	m := &mergeAPI{pull: mergedBody(""), graph: `{"errors":[{"message":"unavailable"}]}`}
	c := changeAPI(t, m.ServeHTTP)
	c.Stderr = io.Discard
	rec := &stateWrites{}
	c.Changes = rec
	p, e := c.Pull(context.Background(), "o/r", 7)
	if e != nil || !p.Merged || p.MergeCommitSHA != "" {
		t.Fatalf("pull %+v err %v", p, e)
	}
	if len(rec.all()) != 0 {
		t.Fatalf("writes=%v", rec.all())
	}
	if got := c.ChangeCounts().Skipped; got != 1 {
		t.Fatalf("skipped=%d", got)
	}
}

// (d) A merged pull's merge commit never changes: the second read of the same
// PR makes no second GraphQL call.
func TestSecondPullMakesNoSecondGraphQLCall(t *testing.T) {
	m := &mergeAPI{pull: mergedBody(""), graph: graphQLMergeCommit(mergeSHA, mergedAt)}
	c := changeAPI(t, m.ServeHTTP)
	c.Stderr = io.Discard
	for range 2 {
		if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
			t.Fatal(e)
		}
	}
	if m.graphql.Load() != 1 || m.pulls.Load() != 2 {
		t.Fatalf("graphql=%d pulls=%d", m.graphql.Load(), m.pulls.Load())
	}
}

// (e) An unmerged PR makes no GraphQL call.
func TestUnmergedPullMakesNoGraphQLCall(t *testing.T) {
	m := &mergeAPI{pull: fmt.Sprintf(`{"number":7,"state":"closed","merged":false,"merged_at":null,"title":"t","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"},"user":{"login":"a"}}`, testSHA)}
	c := changeAPI(t, m.ServeHTTP)
	c.Stderr = io.Discard
	rec := &stateWrites{}
	c.Changes = rec
	if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
		t.Fatal(e)
	}
	if m.graphql.Load() != 0 {
		t.Fatalf("graphql=%d", m.graphql.Load())
	}
	writes := rec.all()
	if len(writes) != 1 || writes[0]["state"] != "closed" {
		t.Fatalf("writes=%v", writes)
	}
	for _, k := range []string{"merge_sha", "merged_at"} {
		if _, ok := writes[0][k]; ok {
			t.Fatalf("closed write carries %s: %v", k, writes[0])
		}
	}
	if got := c.ChangeCounts().Skipped; got != 0 {
		t.Fatalf("skipped=%d", got)
	}
}

// (f) A merged pull_request delivery writes merge_sha and merged_at from its own
// payload, with no further GitHub read.
func TestWebhookMergedPayloadWritesMergeFields(t *testing.T) {
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected GitHub read %s", r.URL.Path)
	})
	rec := &stateWrites{}
	c.Changes = rec
	c.Webhook = webhookstate.Open(t.TempDir())
	body := fmt.Sprintf(`{"action":"closed","pull_request":{"number":7,"state":"closed","merged":true,"merged_at":%q,"merge_commit_sha":%q,"updated_at":"2026-11-06T09:40:50Z","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"}}}`, mergedAt, mergeSHA, testSHA)
	c.WebhookDelivery("o/r", "pull_request", "d-merged", []byte(body))
	writes := rec.all()
	if len(writes) != 1 {
		t.Fatalf("writes=%v", writes)
	}
	requireMergedWrite(t, writes[0])
	if writes[0]["source"] != "webhook" || writes[0]["delivery_id"] != "d-merged" {
		t.Fatalf("write %v", writes[0])
	}
}

// (g) A stale cached body replayed on a 304 has no merge_commit_sha; every
// state write still carries one, and the query is made once.
func TestStaleCachedBodyReplayNeverErasesMergeSHA(t *testing.T) {
	var conditional atomic.Int32
	m := &mergeAPI{graph: graphQLMergeCommit(mergeSHA, mergedAt)}
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/graphql" {
			m.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("If-None-Match") == `"e1"` {
			conditional.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"e1"`)
		fmt.Fprint(w, mergedBody(""))
	})
	c.Polling = true
	c.Stderr = io.Discard
	rec := &stateWrites{}
	c.Changes = rec
	for range 3 {
		if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
			t.Fatal(e)
		}
	}
	if conditional.Load() == 0 {
		t.Fatal("no ETag replay exercised")
	}
	writes := rec.all()
	if len(writes) != 3 {
		t.Fatalf("writes=%v", writes)
	}
	for _, v := range writes {
		requireMergedWrite(t, v)
	}
	if m.graphql.Load() != 1 {
		t.Fatalf("graphql=%d", m.graphql.Load())
	}
}

// (h) The resolution failure is reported once per pull, not once per read.
func TestMergeCommitFailureLoggedOnce(t *testing.T) {
	var logs strings.Builder
	m := &mergeAPI{pull: mergedBody(""), graph: `{"errors":[{"message":"unavailable"}]}`}
	c := changeAPI(t, m.ServeHTTP)
	c.Stderr = &logs
	for range 2 {
		if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
			t.Fatal(e)
		}
	}
	if n := strings.Count(logs.String(), "merge commit of o/r#7 unresolved"); n != 1 {
		t.Fatalf("logged %d times: %q", n, logs.String())
	}
}
