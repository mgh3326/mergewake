package pipeline

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// unreadableFixture is an async review whose reviewer was placed on another
// host and whose hk job read then starts failing.
type unreadableFixture struct {
	*asyncFixture
	old   reviewerJob
	timer time.Time
	hub   *fakeReapHub
}

func unreadableReviewer(t *testing.T, remote string) *unreadableFixture {
	t.Helper()
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	hub := budgetPane(t, f.s, f.runner.panewireRunner, old, "done")
	hub.jobResult = json.RawMessage(remote)
	f.runner.sink.jobErr = errors.New("hk: HTTP 502 bad gateway")
	return &unreadableFixture{f, old, f.v.got[0].at, hub}
}

// again runs the next evaluation at now, with the lease still held.
func (f *unreadableFixture) again(t *testing.T, now time.Time) error {
	t.Helper()
	f.c.t = now
	f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	return f.review(t)
}

const (
	remoteFound    = `{"found":true,"findings":{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]},"report":{"present":true,"last_line":"done"}}`
	remoteNotFound = `{"found":false}`
)

// (a) hk cannot be read but the result sits on the reviewer's host: it is
// collected from there and nothing is replaced.
func TestReviewerUnreadableHkResultOnHostIsCollected(t *testing.T) {
	f := unreadableReviewer(t, remoteFound)
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	calls := f.hub.ops("job.result")
	if len(calls) != 1 || calls[0]["job"] != f.old.Label {
		t.Fatalf("job.result calls=%v", calls)
	}
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "A concrete bug" || len(f.runner.spawns) != 1 || len(f.runner.closes) != 0 {
		t.Fatalf("posted=%+v spawns=%d closes=%v", f.g.postedFindings, len(f.runner.spawns), f.runner.closes)
	}
	if got := len(auditRows(t, f.s, f.r, "review-result-lost")); got != 0 {
		t.Fatalf("replaced: %d lost rows", got)
	}
	reviewerJobGone(t, f.s, f.r, 1)
}

// (a, disk) The result file of a local reviewer is collected the same way.
func TestReviewerUnreadableHkResultOnDiskIsCollected(t *testing.T) {
	f := unreadableReviewer(t, remoteNotFound)
	findings := `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`
	dir := filepath.Join(f.s.Config.Review.Panewire.Jobs, f.old.Label)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(findings), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(f.g.postedFindings) != 1 || len(f.runner.spawns) != 1 || len(auditRows(t, f.s, f.r, "review-result-lost")) != 0 {
		t.Fatalf("posted=%+v spawns=%d", f.g.postedFindings, len(f.runner.spawns))
	}
}

// (b) Inside the budget with nothing found the pass is pending, and a revisit
// timer brings the PR back at the budget, not the sweep.
func TestReviewerUnreadableHkWithinBudgetSchedulesRevisit(t *testing.T) {
	f := unreadableReviewer(t, remoteNotFound)
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(f.v.got) != 2 || f.v.got[1].label != f.old.Label || !f.v.got[1].at.Equal(f.timer) {
		t.Fatalf("revisits=%v, want a second timer at %v", f.v.got, f.timer)
	}
	if len(f.runner.spawns) != 1 || len(f.runner.closes) != 0 || f.g.postCount != 0 || readReviewerJob(t, f.s, f.r, 1).Label != f.old.Label {
		t.Fatalf("spawns=%d closes=%v posts=%d", len(f.runner.spawns), f.runner.closes, f.g.postCount)
	}
}

// (c) Past the budget, still unreadable, nothing found: the pass is replaced
// once, and when the replacement is lost too the lease is released with the
// record, so a fresh pass starts at once.
func TestReviewerUnreadableHkPastBudgetIsReplacedOnceThenReleased(t *testing.T) {
	f := unreadableReviewer(t, remoteNotFound)
	if err := f.again(t, f.timer); err != nil {
		t.Fatal(err)
	}
	rows := auditRows(t, f.s, f.r, "review-result-lost")
	if len(rows) != 1 || rows[0]["collect_error"] != "reviewer job unreadable in hk past the review deadline" || rows[0]["label"] != f.old.Label {
		t.Fatalf("lost rows=%v", rows)
	}
	fresh := readReviewerJob(t, f.s, f.r, 1)
	if len(f.runner.spawns) != 2 || fresh.Label == f.old.Label || fresh.Replacements != 1 || len(f.runner.closes) != 0 {
		t.Fatalf("spawns=%d fresh=%+v closes=%v", len(f.runner.spawns), fresh, f.runner.closes)
	}
	if got := len(auditRows(t, f.s, f.r, "review-lease-released")); got != 0 {
		t.Fatalf("lease released while the replacement runs: %d rows", got)
	}
	budgetPane(t, f.s, f.runner.panewireRunner, fresh, "done")
	err := f.again(t, fresh.Recorded.Add(26*time.Minute))
	if !errors.Is(err, ErrReviewUnavailable) || len(f.runner.spawns) != 2 {
		t.Fatalf("err=%v spawns=%d", err, len(f.runner.spawns))
	}
	released := auditRows(t, f.s, f.r, "review-lease-released")
	if len(released) != 1 || released[0]["reason"] != "reviewer-lost" {
		t.Fatalf("released=%v", released)
	}
	reviewerJobGone(t, f.s, f.r, 1)
	if _, active, _ := f.g.ReviewLease(f.ctx, f.r.Name, claimRef(7, shaA, 1), time.Hour); active {
		t.Fatal("lease still active after the record was dropped")
	}
}

// (d) An unreadable or stalled pass does not renew its lease; a readable live
// one does.
func TestReviewerUnreadableOrStalledPassDoesNotRenewLease(t *testing.T) {
	ref := claimRef(7, shaA, 1)
	f := unreadableReviewer(t, remoteNotFound)
	before := f.g.leaseIDs[ref]
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := f.g.leaseIDs[ref]; got != before {
		t.Fatalf("unreadable pass renewed its lease: %q -> %q", before, got)
	}

	s := asyncReview(t)
	if err := s.review(t); err != nil {
		t.Fatal(err)
	}
	s.s.Config.Review.Panewire.StaleAfter = "10m"
	old := readReviewerJob(t, s.s, s.r, 1)
	s.c.t = old.Recorded.Add(time.Minute)
	s.g.leaseTimes[ref] = time.Now()
	before = s.g.leaseIDs[ref]
	if err := s.review(t); err != nil {
		t.Fatal(err)
	}
	if got := s.g.leaseIDs[ref]; got == before {
		t.Fatalf("a readable live pass did not renew its lease: %q", got)
	}
	renewed := s.g.leaseIDs[ref]
	s.c.t = old.Recorded.Add(11 * time.Minute)
	s.g.leaseTimes[ref] = time.Now()
	err := s.review(t)
	if !errors.Is(err, ErrReviewUnavailable) || len(auditRows(t, s.s, s.r, "reviewer-stale")) != 1 {
		t.Fatalf("err=%v stale rows=%v", err, auditRows(t, s.s, s.r, "reviewer-stale"))
	}
	if got := s.g.leaseIDs[ref]; got != renewed {
		t.Fatalf("stalled pass renewed its lease: %q -> %q", renewed, got)
	}
}

// (e) The read error is logged once per label and error text, with the label
// and the reviewer's host; a different error text logs again.
func TestReviewerUnreadableHkErrorLoggedOncePerText(t *testing.T) {
	f := unreadableReviewer(t, remoteNotFound)
	f.out.Reset()
	for i := 1; i <= 3; i++ {
		if err := f.again(t, f.old.Recorded.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	out := f.out.String()
	if n := strings.Count(out, `"state":"hk-job-unreadable"`); n != 1 {
		t.Fatalf("logged %d times:\n%s", n, out)
	}
	if !strings.Contains(out, `"reviewer":"`+f.old.Label+`"`) || !strings.Contains(out, `"host":"node-a"`) || !strings.Contains(out, "502 bad gateway") {
		t.Fatalf("log line lacks label, host or error:\n%s", out)
	}
	f.runner.sink.jobErr = errors.New("hk: connection refused")
	if err := f.again(t, f.old.Recorded.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(f.out.String(), `"state":"hk-job-unreadable"`); n != 2 {
		t.Fatalf("a new error text logged %d lines in total", n)
	}
}

// (f) A job with no hk record at all keeps its path: replaced past the
// budget with its own cause, renewed inside it, and never logged as an
// unreadable read.
func TestReviewerUnreadableHkNotFoundPathUnchanged(t *testing.T) {
	ref := claimRef(7, shaA, 1)
	f := unreadableReviewer(t, remoteNotFound)
	f.runner.sink.jobErr = nil
	f.runner.sink.jobs = nil
	f.runner.sink.missing = nil
	before := f.g.leaseIDs[ref]
	f.out.Reset()
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil && !errors.Is(err, errPassPending) {
		t.Fatal(err)
	}
	if got := f.g.leaseIDs[ref]; got == before {
		t.Fatal("a pass with no job record stopped renewing its lease")
	}
	if err := f.again(t, f.timer); err != nil {
		t.Fatal(err)
	}
	rows := auditRows(t, f.s, f.r, "review-result-lost")
	if len(rows) != 1 || rows[0]["collect_error"] != "reviewer job record missing past the review deadline" || len(f.runner.spawns) < 2 {
		t.Fatalf("lost rows=%v spawns=%d", rows, len(f.runner.spawns))
	}
	if strings.Contains(f.out.String(), "hk-job-unreadable") || len(f.hub.ops("job.result")) != 0 {
		t.Fatalf("not-found path took the unreadable branch:\n%s", f.out)
	}
}
