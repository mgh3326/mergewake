package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

type latencyFake struct {
	pulls  []github.Pull
	passes map[int][]github.ReviewedPass
}

func (f latencyFake) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return f.pulls, nil
}
func (f latencyFake) ReviewHistory(_ context.Context, _ string, n int, _ string) ([]github.ReviewedPass, error) {
	return f.passes[n], nil
}
func TestReviewLatencyFixture(t *testing.T) {
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	first := start.Add(time.Hour)
	last := start.Add(3 * time.Hour)
	merged := start.Add(4 * time.Hour)
	ms := merged.Format(time.RFC3339)
	dir := t.TempDir()
	var lines []byte
	for _, n := range []int{1, 2} {
		b, _ := json.Marshal(outcomes.Event{Version: 1, Kind: "outcome", At: start, Outcome: &outcomes.Outcome{Repo: "o/r", PR: n, Head: "abc"}})
		lines = append(lines, append(b, '\n')...)
	}
	if e := os.WriteFile(filepath.Join(dir, "outcomes.jsonl"), lines, 0600); e != nil {
		t.Fatal(e)
	}
	p := func(n int) github.Pull { return github.Pull{Number: n, Head: github.Ref{SHA: "abc"}, MergedAt: &ms} }
	f := latencyFake{pulls: []github.Pull{p(1), p(2)}, passes: map[int][]github.ReviewedPass{1: {{Head: "abc", Marked: true, SubmittedAt: &first}, {Head: "abc", Marked: true, SubmittedAt: &last}}}}
	r, e := ReviewLatency(context.Background(), dir, []config.Repo{{Name: "o/r", Branch: "main"}}, "bot", start.Add(-time.Hour), f)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("rows: %+v", r.Rows)
	}
	row := r.Rows[0]
	for label, pair := range map[string]struct{ got, want *time.Time }{"first seen": {row.FirstSeen, &start}, "first review": {row.FirstReview, &first}, "last review": {row.LastReview, &last}, "merged": {row.Merged, &merged}} {
		if pair.got == nil || !pair.got.Equal(*pair.want) {
			t.Errorf("%s = %v, want %v", label, pair.got, pair.want)
		}
	}
	for label, pair := range map[string]struct {
		got  *float64
		want float64
	}{"wait": {row.WaitSeconds, 3600}, "review": {row.ReviewSeconds, 7200}, "done": {row.DoneToMergeSeconds, 3600}, "total": {row.TotalSeconds, 14400}} {
		if pair.got == nil || *pair.got != pair.want {
			t.Errorf("%s = %v, want %v", label, pair.got, pair.want)
		}
	}
	missing := r.Rows[1]
	if missing.WaitSeconds == nil || *missing.WaitSeconds != 14400 || !missing.WaitCensored || missing.ReviewSeconds != nil {
		t.Errorf("no review: %+v", missing)
	}
}
func TestReviewLatencyPercentiles(t *testing.T) {
	var rows []LatencyRow
	for i := 1; i <= 10; i++ {
		v := float64(i)
		rows = append(rows, LatencyRow{Repo: "o/r", TotalSeconds: &v})
	}
	r := summarize(rows)
	s := r.Repos[0].Total
	if s.Count != 10 || *s.P50 != 5 || *s.P90 != 9 || *s.Max != 10 {
		t.Fatalf("stats: %+v", s)
	}
	if *r.Slowest[0].TotalSeconds != 10 {
		t.Fatal("slowest order")
	}
}
func TestReviewLatencyJSON(t *testing.T) {
	b, e := json.Marshal(summarize([]LatencyRow{{Repo: "o/r", PR: 1}}))
	if e != nil {
		t.Fatal(e)
	}
	var v map[string]json.RawMessage
	if e = json.Unmarshal(b, &v); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"repos", "slowest", "rows"} {
		if _, ok := v[key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
	var rows []map[string]any
	if e = json.Unmarshal(v["rows"], &rows); e != nil {
		t.Fatal(e)
	}
	if rows[0]["first_seen"] != nil || rows[0]["review_seconds"] != nil || rows[0]["repo"] != "o/r" {
		t.Fatalf("row: %s", v["rows"])
	}
}
