package github_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/mergewake/internal/changestate"
	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

const mergeSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

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
	return fmt.Sprintf(`{"number":7,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z",%s"title":"t","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"},"user":{"login":"a"}}`, field, testSHA)
}

func requireMergedWrite(t *testing.T, v map[string]any) {
	t.Helper()
	if v["state"] != "merged" || v["merge_sha"] != mergeSHA || v["merged_at"] != "2026-11-06T09:40:49Z" {
		t.Fatalf("merged state write %v", v)
	}
}

// (a) A merged PR body without merge_commit_sha is read once more, past the
// cache, and the state write carries merge_sha and merged_at.
func TestMergedPullWithoutSHAIsReadFreshBeforeWriting(t *testing.T) {
	var gets atomic.Int32
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		if gets.Add(1) == 1 {
			fmt.Fprint(w, mergedBody(""))
			return
		}
		if r.Header.Get("If-None-Match") != "" {
			t.Errorf("fresh read sent If-None-Match %q", r.Header.Get("If-None-Match"))
		}
		fmt.Fprint(w, mergedBody(mergeSHA))
	})
	rec := &stateWrites{}
	c.Changes = rec
	if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
		t.Fatal(e)
	}
	writes := rec.all()
	if gets.Load() != 2 || len(writes) != 1 {
		t.Fatalf("gets=%d writes=%v", gets.Load(), writes)
	}
	requireMergedWrite(t, writes[0])
	if got := c.ChangeCounts().Skipped; got != 0 {
		t.Fatalf("skipped=%d", got)
	}
}

// (b) GitHub still lacks merge_commit_sha on the fresh read: no state write, and
// the skip is counted.
func TestMergedPullStillWithoutSHAWritesNothing(t *testing.T) {
	var gets atomic.Int32
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		gets.Add(1)
		fmt.Fprint(w, mergedBody(""))
	})
	rec := &stateWrites{}
	c.Changes = rec
	p, e := c.Pull(context.Background(), "o/r", 7)
	if e != nil || !p.Merged || p.MergeCommitSHA != "" {
		t.Fatalf("pull %+v err %v", p, e)
	}
	if gets.Load() != 2 || len(rec.all()) != 0 {
		t.Fatalf("gets=%d writes=%v", gets.Load(), rec.all())
	}
	if got := c.ChangeCounts().Skipped; got != 1 {
		t.Fatalf("skipped=%d", got)
	}
}

// (c) A merged pull_request delivery writes merge_sha and merged_at from its own
// payload, with no further GitHub read.
func TestWebhookMergedPayloadWritesMergeFields(t *testing.T) {
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected GitHub read %s", r.URL.Path)
	})
	rec := &stateWrites{}
	c.Changes = rec
	c.Webhook = webhookstate.Open(t.TempDir())
	body := fmt.Sprintf(`{"action":"closed","pull_request":{"number":7,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z","merge_commit_sha":%q,"updated_at":"2026-11-06T09:40:50Z","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"}}}`, mergeSHA, testSHA)
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

// (d) A closed, unmerged PR is written as state=closed with no merge fields and
// without a second read.
func TestClosedUnmergedPullWritesNoMergeFields(t *testing.T) {
	var gets atomic.Int32
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		gets.Add(1)
		fmt.Fprintf(w, `{"number":7,"state":"closed","merged":false,"merged_at":null,"merge_commit_sha":null,"title":"t","head":{"sha":%q,"ref":"topic"},"base":{"sha":"b1","ref":"main"},"user":{"login":"a"}}`, testSHA)
	})
	rec := &stateWrites{}
	c.Changes = rec
	if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
		t.Fatal(e)
	}
	writes := rec.all()
	if gets.Load() != 1 || len(writes) != 1 || writes[0]["state"] != "closed" {
		t.Fatalf("gets=%d writes=%v", gets.Load(), writes)
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

// (e) The hk 3296 record of a merged PR (state merged, merged_at set, merge_sha
// null, source poll, observed an hour later) came from a conditional poll read
// that GitHub answered 304 and the client answered with the cached body that had
// no merge_commit_sha; writePull stamped that body with a new observed_at, and hk
// keeps the newest observation per section. Reproduce: the body cached for the
// ETag lacks the sha, every conditional read replays it, and only an
// unconditional read gets the sha. Every state write must still carry it.
func TestStaleCachedBodyReplayNeverErasesMergeSHA(t *testing.T) {
	var unconditional, conditional atomic.Int32
	c := changeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"e1"` {
			conditional.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if unconditional.Add(1) == 1 {
			w.Header().Set("ETag", `"e1"`)
			fmt.Fprint(w, mergedBody(""))
			return
		}
		w.Header().Set("ETag", `"e2"`)
		fmt.Fprint(w, mergedBody(mergeSHA))
	})
	c.Polling = true
	rec := &stateWrites{}
	c.Changes = rec
	for range 3 {
		if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
			t.Fatal(e)
		}
	}
	writes := rec.all()
	if len(writes) != 3 {
		t.Fatalf("writes=%v", writes)
	}
	for _, v := range writes {
		requireMergedWrite(t, v)
	}
}
