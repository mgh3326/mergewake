package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// landingDelay is a panewire whose spawn places the reviewer d after the spawn
// call began: the clock moves while the spawn call places the pane.
type landingDelay struct {
	integrations.Runner
	c *clock
	d time.Duration
}

func (r landingDelay) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" {
		r.c.t = r.c.t.Add(r.d)
	}
	return r.Runner.Run(ctx, command, args, input, dir)
}

// landedReview is asyncReview with a 15 minute review.timeout, whose reviewer
// lands d after its spawn begins. The record is written at 08:00.
func landedReview(t *testing.T, d time.Duration) *asyncFixture {
	t.Helper()
	f := asyncReview(t)
	f.s.Config.Review.Timeout = "15m"
	f.s.Runner = landingDelay{Runner: f.runner, c: f.c, d: d}
	return f
}

// clearStarted drops the landing from the saved record, as a record written
// before the field existed would read.
func clearStarted(t *testing.T, f *asyncFixture) {
	t.Helper()
	rj := readReviewerJob(t, f.s, f.r, 1)
	rj.Started = nil
	if err := fsutil.WriteJSON(f.s.reviewerJobPath(f.r.Name, 7, 1), rj); err != nil {
		t.Fatal(err)
	}
}

// The budget starts when the reviewer lands, not when its record is written: a
// reviewer that landed 4 minutes after its record is still inside a 15 minute
// budget at 16 minutes, and its timer comes 15 minutes after the landing.
func TestReviewBudgetStartsWhenReviewerLands(t *testing.T) {
	f := landedReview(t, 4*time.Minute)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	rj := readReviewerJob(t, f.s, f.r, 1)
	landed := rj.Recorded.Add(4 * time.Minute)
	if rj.Started == nil || !rj.Started.Equal(landed) {
		t.Fatalf("started %v, want %v", rj.Started, landed)
	}
	timer := landed.Add(15*time.Minute + revisitMargin)
	if len(f.v.got) != 1 || f.v.got[0].label != rj.Label || !f.v.got[0].at.Equal(timer) {
		t.Fatalf("revisits %v, want one at %v", f.v.got, timer)
	}

	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	f.c.t = rj.Recorded.Add(16 * time.Minute)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(f.runner.spawns) != 1 || len(auditRows(t, f.s, f.r, "review-result-lost")) != 0 {
		t.Fatalf("a reviewer inside its budget was replaced: spawns %v", f.runner.spawns)
	}

	f.c.t = timer
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	rows := auditRows(t, f.s, f.r, "review-result-lost")
	if len(rows) != 1 || len(f.runner.spawns) != 2 {
		t.Fatalf("lost rows %d, spawns %v", len(rows), f.runner.spawns)
	}
	if rows[0]["budget_from"] != "started" || rows[0]["started"] == nil || rows[0]["recorded"] == nil {
		t.Fatalf("audit row %v does not name the landing as the budget start", rows[0])
	}
}

// A record with no landing seen keeps the record-time budget: it is lost at
// Recorded + review.timeout, and its timer comes from the record.
func TestReviewBudgetWithoutLandingUsesRecord(t *testing.T) {
	f := landedReview(t, 4*time.Minute)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	clearStarted(t, f)
	rec := readReviewerJob(t, f.s, f.r, 1).Recorded
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()

	f.c.t = rec.Add(14 * time.Minute)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(auditRows(t, f.s, f.r, "review-result-lost")) != 0 {
		t.Fatal("a reviewer lost before its record budget ended")
	}
	if last := f.v.got[len(f.v.got)-1]; !last.at.Equal(rec.Add(15*time.Minute + revisitMargin)) {
		t.Fatalf("timer %v, want the record budget %v", last.at, rec.Add(15*time.Minute+revisitMargin))
	}

	f.c.t = rec.Add(15 * time.Minute)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	rows := auditRows(t, f.s, f.r, "review-result-lost")
	if len(rows) != 1 || len(f.runner.spawns) != 2 {
		t.Fatalf("lost rows %d, spawns %v", len(rows), f.runner.spawns)
	}
	if rows[0]["budget_from"] != "recorded" || rows[0]["started"] != nil {
		t.Fatalf("audit row %v does not name the record as the budget start", rows[0])
	}
}

// A spawn that never landed, with no job behind its record, is lost once its
// record budget ends: the bound is Recorded + review.timeout.
func TestReviewNeverLandedSpawnLostAtRecordedBudget(t *testing.T) {
	f := landedReview(t, 0)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	clearStarted(t, f)
	rec := readReviewerJob(t, f.s, f.r, 1).Recorded
	f.runner.sink.missing = nil
	f.c.t = rec.Add(15 * time.Minute)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	rows := auditRows(t, f.s, f.r, "review-result-lost")
	if len(rows) != 1 || rows[0]["budget_from"] != "recorded" {
		t.Fatalf("lost rows %v", rows)
	}
}
