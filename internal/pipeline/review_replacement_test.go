package pipeline

import (
	"encoding/json"
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
	f.runner.sink.jobs[next] = integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`)}
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if f.g.postCount != 1 || len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "A concrete bug" || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("posts=%d", f.g.postCount)
	}
}

// A replacement's budget starts at the handoff, not at the old reviewer's
// landing. Forty minutes after the old landing (25m review.timeout), the
// replacement has only been running 20 minutes, so it is not lost.
func TestQuotaReplacementBudgetRestartsAtHandoff(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	if old.Started == nil {
		t.Fatal("reviewer landed without a start")
	}
	next := old.Label + "-q1"
	f.c.t = old.Started.Add(20 * time.Minute)
	f.runner.sink.jobs[old.Label] = integrations.JobRecord{State: "lost", EndReason: "quota_stop pool=p replaced_by=" + next}
	f.runner.sink.jobs[next] = integrations.JobRecord{State: "running", Host: "replacement-host", PaneID: "replacement-pane"}
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	got := readReviewerJob(t, f.s, f.r, 1)
	if got.Label != next || got.Started != nil || !got.Recorded.Equal(f.c.t) {
		t.Fatalf("replacement record=%+v", got)
	}
	f.c.t = f.c.t.Add(20 * time.Minute)
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if n := len(auditRows(t, f.s, f.r, "review-result-lost")); n != 0 {
		t.Fatalf("replacement lost on the old reviewer's budget: %d lost rows", n)
	}
	if still := readReviewerJob(t, f.s, f.r, 1); still.Label != next {
		t.Fatalf("record=%+v", still)
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
	// A carried chain is not a loss: no row counts a lost result, nothing was
	// given up, and mergewake spawned no replacement of its own.
	if rows := auditRows(t, f.s, f.r, "review-result-lost"); len(rows) != 0 {
		t.Fatalf("a carried chain counted a lost result: %v", rows)
	}
	if rows := auditRows(t, f.s, f.r, "review-quota-given-up"); len(rows) != 0 {
		t.Fatalf("a carried chain was counted given up: %v", rows)
	}
	if len(f.runner.spawns) != 1 {
		t.Fatalf("a carried chain spawned a replacement: %v", spawnExclusions(f.runner.spawns))
	}
	carried := auditRows(t, f.s, f.r, "review-reviewer-replaced")
	if len(carried) != 2 || carried[0]["pool"] != "p" || carried[1]["pool"] != "p" {
		t.Fatalf("carried rows = %v, want one per hop", carried)
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
