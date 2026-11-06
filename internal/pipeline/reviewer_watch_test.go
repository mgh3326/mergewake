package pipeline

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// The reviewer watch reads the recorded reviewer jobs and their hk records. It
// holds no GitHub client at all: with GH nil, any GitHub call would panic.
func TestReviewerWatchReadsRecordsAndHKWithoutGitHub(t *testing.T) {
	s, _, r := panewireSetup(t, &panewireRunner{})
	s.GH = nil
	x := &panewireExecutor{s: s, repo: r}
	x.pr.Number = 7
	if got := s.RecordedReviewerJobs(r.Name); len(got) != 0 {
		t.Fatalf("no records yet, got %+v", got)
	}
	if e := x.saveReviewerJob(reviewerJob{PR: 7, Head: shaA, Pass: 2, Label: "review-7-p2", Args: []string{"x"}}); e != nil {
		t.Fatal(e)
	}
	want := []daemon.WatchedJob{{Repo: r.Name, PR: 7, Pass: 2, Label: "review-7-p2"}}
	if got := s.RecordedReviewerJobs(r.Name); len(got) != 1 || got[0] != want[0] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	sink := s.Sink.(*taskJobSink)
	if _, e := s.ReviewerJobView(context.Background(), "review-7-p2"); !IsJobNotFound(e) {
		t.Fatalf("missing record: err=%v", e)
	}
	result, _ := json.Marshal(map[string]any{"findings": []any{}})
	sink.jobs["review-7-p2"] = integrations.JobRecord{State: "completed", Result: result}
	v, e := s.ReviewerJobView(context.Background(), "review-7-p2")
	if e != nil || v.State != "completed" || !v.HasResult || !v.Terminal() {
		t.Fatalf("view=%+v err=%v", v, e)
	}
}
