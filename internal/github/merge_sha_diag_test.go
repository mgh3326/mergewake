package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// mergeSHALogs is a client whose stderr lines land in the returned buffer.
func mergeSHALogs(t *testing.T, h http.HandlerFunc) (*Client, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	c := api(t, h)
	c.Stderr = &logs
	return c, &logs
}

// logLines decodes the JSON lines written so far.
func logLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var v map[string]any
		if e := json.Unmarshal([]byte(line), &v); e != nil {
			t.Fatalf("log line %q: %v", line, e)
		}
		out = append(out, v)
	}
	return out
}

func requireLogKeys(t *testing.T, got any, want ...string) {
	t.Helper()
	v, _ := got.([]any)
	var keys []string
	for _, k := range v {
		keys = append(keys, fmt.Sprint(k))
	}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("keys %v, want %v", keys, want)
	}
}

// A merged PR whose body has no merge_commit_sha key logs one line naming the
// absent key and the body's top-level keys.
func TestMergeSHAMissingLogsAbsentKey(t *testing.T) {
	const body = `{"number":7,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z","title":"t"}`
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
	if _, e := c.Pull(context.Background(), "o/r", 7); e != nil {
		t.Fatal(e)
	}
	lines := logLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("lines=%v", lines)
	}
	l := lines[0]
	if l["level"] != "INFO" || l["msg"] != "merge_sha_missing" || l["repo"] != "o/r" || l["pr"] != float64(7) ||
		l["route"] != "single" || l["credential"] != "user" || l["source"] != "http 200" || l["status"] != float64(http.StatusOK) {
		t.Fatalf("line %v", l)
	}
	if l["key_present"] != false || l["value_null"] != false {
		t.Fatalf("key facts %v", l)
	}
	requireLogKeys(t, l["keys"], "merged", "merged_at", "number", "state", "title")
}

// A body that carries the key with a null value says so.
func TestMergeSHAMissingLogsNullValue(t *testing.T) {
	const body = `{"number":8,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z","merge_commit_sha":null}`
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
	if _, e := c.Pull(context.Background(), "o/r", 8); e != nil {
		t.Fatal(e)
	}
	lines := logLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("lines=%v", lines)
	}
	l := lines[0]
	if l["key_present"] != true || l["value_null"] != true {
		t.Fatalf("key facts %v", l)
	}
	requireLogKeys(t, l["keys"], "merge_commit_sha", "merged", "merged_at", "number", "state")
}

// A merged body that carries the sha logs nothing.
func TestMergeSHAMissingNormalBodyLogsNothing(t *testing.T) {
	body := fmt.Sprintf(`{"number":9,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z","merge_commit_sha":%q}`, testSHA)
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
	if _, e := c.Pull(context.Background(), "o/r", 9); e != nil {
		t.Fatal(e)
	}
	if lines := logLines(t, logs); len(lines) != 0 {
		t.Fatalf("lines=%v", lines)
	}
}

// The line is per PR per client: a second read of the same PR logs nothing more.
func TestMergeSHAMissingLogsOncePerPR(t *testing.T) {
	const body = `{"number":10,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z"}`
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
	for range 2 {
		if _, e := c.Pull(context.Background(), "o/r", 10); e != nil {
			t.Fatal(e)
		}
	}
	if lines := logLines(t, logs); len(lines) != 1 {
		t.Fatalf("lines=%v", lines)
	}
}

// A merged list item without the sha logs with its own top-level keys, not the
// page array's.
func TestMergeSHAMissingLogsListItems(t *testing.T) {
	const page = `[{"number":11,"state":"closed","merged":true,"merged_at":"2026-11-13T10:00:00Z","updated_at":"2026-11-13T10:01:00Z","base":{"ref":"main"},"merge_commit_sha":null}]`
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/pulls" {
			t.Errorf("path %s", r.URL.Path)
		}
		fmt.Fprint(w, page)
	})
	since := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	got, e := c.MergedPulls(context.Background(), "o/r", "main", since)
	if e != nil || len(got) != 1 {
		t.Fatalf("pulls=%v err %v", got, e)
	}
	lines := logLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("lines=%v", lines)
	}
	l := lines[0]
	if l["route"] != "list" || l["pr"] != float64(11) || l["source"] != "http 200" || l["status"] != float64(http.StatusOK) {
		t.Fatalf("line %v", l)
	}
	if l["key_present"] != true || l["value_null"] != true || l["refetch"] != "not_tried" {
		t.Fatalf("key facts %v", l)
	}
	requireLogKeys(t, l["keys"], "base", "merge_commit_sha", "merged", "merged_at", "number", "state", "updated_at")
}

// The provenance record names a live body and the ETag replay of it.
func TestDecodedBodyRecordsNotModifiedReplay(t *testing.T) {
	var conditional bool
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"e1"` {
			conditional = true
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"e1"`)
		fmt.Fprint(w, `{"number":7}`)
	})
	ctx, body := withDecodedBody(context.Background())
	var v map[string]any
	if e := c.json(ctx, "GET", "/repos/o/r/pulls/7", nil, &v); e != nil {
		t.Fatal(e)
	}
	if source, status, credential, _ := body.snapshot(); source != "http 200" || status != http.StatusOK || credential != "user" {
		t.Fatalf("%s %d %s", source, status, credential)
	}
	if e := c.json(ctx, "GET", "/repos/o/r/pulls/7", nil, &v); e != nil {
		t.Fatal(e)
	}
	if source, status, _, _ := body.snapshot(); !conditional || source != sourceNotModified || status != http.StatusNotModified {
		t.Fatalf("conditional=%v source=%s status=%d", conditional, source, status)
	}
}

func TestMergeSHAFacts(t *testing.T) {
	f := factsOf([]byte(`{"merge_commit_sha":null,"b":1,"a":2}`))
	if !f.KeyPresent || !f.ValueNull || strings.Join(f.Keys, ",") != "a,b,merge_commit_sha" {
		t.Fatalf("facts %+v", f)
	}
	if f := factsOf([]byte(`{"merge_commit_sha":"abc"}`)); !f.KeyPresent || f.ValueNull {
		t.Fatalf("facts %+v", f)
	}
	if f := factsOf([]byte(`[]`)); f.KeyPresent || f.ValueNull || len(f.Keys) != 0 {
		t.Fatalf("array facts %+v", f)
	}
}

// A merged pull whose cached body, replayed on a 304, lacks merge_commit_sha
// logs that first body (the 304 replay) with refetch filled when the fresh
// re-read supplies the sha.
func TestMergeSHAMissingLogsFirstBodyBeforeRefetch(t *testing.T) {
	const cached = `{"number":12,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z"}`
	const fresh = `{"number":12,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z","merge_commit_sha":"cccccccccccccccccccccccccccccccccccccccc"}`
	var reads int
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) {
		reads++
		switch {
		case reads == 1:
			w.Header().Set("ETag", `"e1"`)
			fmt.Fprint(w, cached)
		case reads == 2:
			if r.Header.Get("If-None-Match") != `"e1"` {
				t.Errorf("conditional read sent %q", r.Header.Get("If-None-Match"))
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			fmt.Fprint(w, fresh)
		}
	})
	var seed map[string]any
	if e := c.json(context.Background(), "GET", "/repos/o/r/pulls/12", nil, &seed); e != nil {
		t.Fatal(e)
	}
	p, e := c.Pull(context.Background(), "o/r", 12)
	if e != nil {
		t.Fatal(e)
	}
	if p.MergeCommitSHA == "" {
		t.Fatalf("fresh re-read did not supply the sha: %+v", p)
	}
	lines := logLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("lines=%v", lines)
	}
	l := lines[0]
	if l["source"] != sourceNotModified || l["status"] != float64(http.StatusNotModified) || l["credential"] != "user" {
		t.Fatalf("line %v", l)
	}
	if l["key_present"] != false || l["refetch"] != "filled" {
		t.Fatalf("line %v", l)
	}
	requireLogKeys(t, l["keys"], "merged", "merged_at", "number", "state")
}

// A merged pull whose first body and fresh re-read both lack merge_commit_sha
// logs refetch still_missing.
func TestMergeSHAMissingLogsRefetchStillMissing(t *testing.T) {
	const body = `{"number":13,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z"}`
	c, logs := mergeSHALogs(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
	if _, e := c.Pull(context.Background(), "o/r", 13); e != nil {
		t.Fatal(e)
	}
	lines := logLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("lines=%v", lines)
	}
	if l := lines[0]; l["source"] != "http 200" || l["refetch"] != "still_missing" {
		t.Fatalf("line %v", l)
	}
}
