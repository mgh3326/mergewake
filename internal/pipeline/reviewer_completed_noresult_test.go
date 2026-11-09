package pipeline

import (
	"encoding/json"

	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// completedNoResult is the #306 shape: a resumed reviewer whose hk job is
// completed without a result (a watcher completion beat `panewire done
// --result`), no local job dir, and its findings.json readable only on the
// host it ran on.
type completedNoResult struct {
	*asyncFixture
	old reviewerJob
	hub *fakeReapHub
}

func completedWithoutResult(t *testing.T, hostResult string) *completedNoResult {
	t.Helper()
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	hub := budgetPane(t, f.s, f.runner.panewireRunner, old, "done")
	hub.jobResult = json.RawMessage(hostResult)
	f.runner.sink.missing = nil
	f.runner.sink.jobs[old.Label] = integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
	if err := os.RemoveAll(filepath.Join(f.s.Config.Review.Panewire.Jobs, old.Label)); err != nil {
		t.Fatal(err)
	}
	f.out.Reset()
	return &completedNoResult{f, old, hub}
}

// again runs the next evaluation at now, with the lease still held.
func (c *completedNoResult) again(t *testing.T, now time.Time) error {
	t.Helper()
	c.c.t = now
	c.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	return c.review(t)
}

// The result of a completed job that lost it is read from the host it ran on
// and posted: the evaluation neither replaces the reviewer nor fails.
func TestCompletedReviewerWithoutResultReadsItsHost(t *testing.T) {
	f := completedWithoutResult(t, remoteFound)
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatalf("review: %v", err)
	}
	calls := f.hub.ops("job.result")
	if len(calls) != 1 || calls[0]["job"] != f.old.Label {
		t.Fatalf("host reads=%v, want one for %s", calls, f.old.Label)
	}
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "A concrete bug" {
		t.Fatalf("posted=%+v", f.g.postedFindings)
	}
	if len(f.runner.spawns) != 1 || len(auditRows(t, f.s, f.r, "review-result-lost")) != 0 {
		t.Fatalf("spawns=%d lost rows=%d", len(f.runner.spawns), len(auditRows(t, f.s, f.r, "review-result-lost")))
	}
	reviewerJobGone(t, f.s, f.r, 1)
}

// A completed job whose result cannot be read from its host either is lost:
// one replacement, and a log line naming the label, the host and the reason.
func TestCompletedReviewerWithoutResultIsLostWhenHostReadFails(t *testing.T) {
	f := completedWithoutResult(t, remoteNotFound)
	// The replacement is placed and runs on, so this evaluation is pending.
	// Review prints review-pending and returns nil for it.
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatalf("review: %v", err)
	}
	calls := f.hub.ops("job.result")
	if len(calls) != 1 || calls[0]["job"] != f.old.Label {
		t.Fatalf("host reads=%v, want one for %s", calls, f.old.Label)
	}
	if len(f.runner.spawns) != 2 || arg(f.runner.spawns[1], "-l") == f.old.Label {
		t.Fatalf("spawns=%v, want one replacement", f.runner.spawns)
	}
	if got := len(auditRows(t, f.s, f.r, "review-result-lost")); got != 1 {
		t.Fatalf("lost rows=%d, want one", got)
	}
	out := f.out.String()
	if n := strings.Count(out, `"state":"result-lost"`); n != 1 {
		t.Fatalf("logged %d result-lost lines:\n%s", n, out)
	}
	if !strings.Contains(out, `"reviewer":"`+f.old.Label+`"`) || !strings.Contains(out, `"host":"node-a"`) || !strings.Contains(out, "not_found") {
		t.Fatalf("log line lacks the label, host or reason:\n%s", out)
	}
}

// A completed job that carries a result is collected from it, as before: the
// host is never read.
func TestCompletedReviewerWithResultIsUnchanged(t *testing.T) {
	f := completedWithoutResult(t, remoteFound)
	f.runner.sink.jobs[f.old.Label] = integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", OwnerLane: "owner",
		Result: json.RawMessage(`{"findings":[{"path":"tool","line":2,"body":"A result bug","severity":"high"}]}`)}
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if calls := f.hub.ops("job.result"); len(calls) != 0 {
		t.Fatalf("host read for a record with a result: %v", calls)
	}
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "A result bug" {
		t.Fatalf("posted=%+v", f.g.postedFindings)
	}
	if len(f.runner.spawns) != 1 || len(auditRows(t, f.s, f.r, "review-result-lost")) != 0 {
		t.Fatalf("spawns=%d lost=%d", len(f.runner.spawns), len(auditRows(t, f.s, f.r, "review-result-lost")))
	}
}

// A completed job without a result whose findings.json sits in the local job
// dir, and whose host has none, lost its result: the local file is not read.
// The resumed pass is replaced once, as for any lost result, and the file's
// findings are never posted.
func TestCompletedReviewerWithoutResultLocalFileIsIgnored(t *testing.T) {
	f := completedWithoutResult(t, remoteNotFound)
	dir := filepath.Join(f.s.Config.Review.Panewire.Jobs, f.old.Label)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	findings := `{"findings":[{"path":"tool","line":3,"body":"A local bug","severity":"high"}]}`
	if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(findings), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, p := range f.g.postedFindings {
		if p.Body == "A local bug" {
			t.Fatalf("the local file was posted: %+v", f.g.postedFindings)
		}
	}
	if len(f.runner.spawns) != 2 || arg(f.runner.spawns[1], "-l") == f.old.Label {
		t.Fatalf("spawns=%v, want one replacement", f.runner.spawns)
	}
	if rows := auditRows(t, f.s, f.r, "review-result-lost"); len(rows) != 1 || rows[0]["label"] != f.old.Label {
		t.Fatalf("lost rows=%v", rows)
	}
}
