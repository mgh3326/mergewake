package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/changestate"
)

// writerRec records the state sections written.
type writerRec struct {
	values []map[string]any
}

func (w *writerRec) Queue(_ string, _ int, name string, s changestate.Section) {
	if name == "state" {
		w.values = append(w.values, s.Value.(map[string]any))
	}
}
func (w *writerRec) Async(f func())             { f() }
func (w *writerRec) Counts() changestate.Counts { return changestate.Counts{} }
func (w *writerRec) Wait(context.Context)       {}

// The change-state writer supplies merge_sha and merged_at from GraphQL when
// the record it was handed (a list body, a webhook payload) lacks them.
func TestWritePullResolvesMergeCommit(t *testing.T) {
	const sha = "cccccccccccccccccccccccccccccccccccccccc"
	var calls int
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/graphql" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		calls++
		fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"mergedAt":"2026-11-06T09:40:49Z","mergeCommit":{"oid":%q}}}}}`, sha)
	})
	c.Stderr = io.Discard
	rec := &writerRec{}
	c.Changes = rec
	p := Pull{Number: 7, State: "closed", Merged: true, Head: Ref{SHA: "h"}, Base: Ref{Ref: "main"}}
	c.writePull(context.Background(), "o/r", p, time.Now().UTC(), "poll", "")
	if calls != 1 || len(rec.values) != 1 {
		t.Fatalf("calls=%d writes=%v", calls, rec.values)
	}
	v := rec.values[0]
	if v["merge_sha"] != sha || v["merged_at"] != "2026-11-06T09:40:49Z" || v["state"] != "merged" {
		t.Fatalf("write %v", v)
	}
}

// A merged PR read from the per-commit list carries no merge_commit_sha; the
// watch role needs it to bracket a red main, so the list resolves it.
func TestPRsForCommitResolvesMergeCommit(t *testing.T) {
	const sha = "dddddddddddddddddddddddddddddddddddddddd"
	var graphqlCalls int
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/graphql":
			graphqlCalls++
			fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"mergedAt":"2026-11-06T09:40:49Z","mergeCommit":{"oid":%q}}}}}`, sha)
		case strings.HasSuffix(r.URL.Path, "/commits/"+testSHA+"/pulls"):
			fmt.Fprintf(w, `[{"number":7,"state":"closed","merged":true,"merged_at":"2026-11-06T09:40:49Z","base":{"ref":"main"}}]`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	})
	c.Stderr = io.Discard
	pulls, e := c.PRsForCommit(context.Background(), "o/r", testSHA)
	if e != nil || len(pulls) != 1 {
		t.Fatalf("pulls=%v err=%v", pulls, e)
	}
	if pulls[0].MergeCommitSHA != sha || graphqlCalls != 1 {
		t.Fatalf("sha %q with %d queries", pulls[0].MergeCommitSHA, graphqlCalls)
	}
}
