package pipeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestQuotaReplacementFollowAndCollect(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	next := old.Label + "-q1"
	f.runner.sink.jobs[old.Label] = integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=p replaced_by=" + next}
	f.runner.sink.jobs[next] = integrations.JobRecord{State: "running", Host: "replacement-host", PaneID: "replacement-pane"}
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	got := readReviewerJob(t, f.s, f.r, 1)
	if got.Label != next || got.LeaseSHA == "" || !f.g.refs[claimRef(7, shaA, 1)] {
		t.Fatalf("old=%+v got=%+v", old, got)
	}
	placement, ok := spawnPlacement(got.Spawned)
	if !ok || placement.Host != "replacement-host" || placement.PaneID != "replacement-pane" {
		t.Fatalf("placement=%+v", placement)
	}
	if len(auditRows(t, f.s, f.r, "review-reviewer-replaced")) != 1 || len(auditRows(t, f.s, f.r, "review-quota-stopped")) != 0 {
		t.Fatal("wrong audits")
	}
	if _, err := os.Stat(filepath.Join(f.s.state(f.r.Name), "review-retry-7.json")); !os.IsNotExist(err) {
		t.Fatalf("retry state: %v", err)
	}
	if len(f.v.got) != 2 || f.v.got[1].label != next {
		t.Fatalf("timers: %+v", f.v.got)
	}
	f.finish(next)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if f.g.postCount != 1 || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("posts=%d", f.g.postCount)
	}
}

func TestQuotaReplacementChain(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1).Label
	q1, q2 := old+"-q1", old+"-q2"
	f.runner.sink.jobs[old] = integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=p replaced_by=" + q1}
	f.runner.sink.jobs[q1] = integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=p replaced_by=" + q2}
	f.finish(q2)
	for _, want := range []string{q1, q2} {
		if err := f.review(t); err != nil {
			t.Fatal(err)
		}
		if got := readReviewerJob(t, f.s, f.r, 1).Label; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	}
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if f.g.postCount != 1 {
		t.Fatalf("posts=%d", f.g.postCount)
	}
}

func TestQuotaReplacementMissingRevisitsSoon(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1).Label
	f.runner.sink.missing = nil
	f.runner.sink.jobs[old] = integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=p replaced_by=" + old + "-q1"}
	for i := 0; i < 2; i++ {
		if err := f.review(t); err != nil {
			t.Fatal(err)
		}
		if got := readReviewerJob(t, f.s, f.r, 1).Label; got != old {
			t.Fatal(got)
		}
		if v := f.v.got[len(f.v.got)-1]; v.label != old || !v.at.Equal(f.c.t.Add(10*time.Second)) {
			t.Fatalf("timer=%+v", v)
		}
	}
	if len(auditRows(t, f.s, f.r, "review-quota-stopped")) != 0 {
		t.Fatal("retry audit")
	}
}

func TestQuotaReplacementInvalidTreatedAsAbsent(t *testing.T) {
	_, _, replacement, ok := parseQuotaStop(integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=p replaced_by=../bad"})
	if !ok || replacement != "" {
		t.Fatalf("replacement=%q ok=%v", replacement, ok)
	}
}
