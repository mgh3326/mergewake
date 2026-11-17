package pipeline

import (
	"slices"
	"strconv"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// placedRecord is a recorded asynchronous request whose hub job exists, as the
// hub creates it when it accepts the request, with the hk job record job. A job
// that names no host or pane has not landed.
func placedRecord(t *testing.T, job integrations.JobRecord) (*asyncFixture, reviewerJob) {
	t.Helper()
	f, rj := recordedRequest(t)
	f.runner.sink.missing = &job
	return f, rj
}

// retryableStatus answers the hub's status of asyncRequestID as retryable with
// rc, the state the live refusal at landing left behind.
func retryableStatus(t *testing.T, f *asyncFixture, rj reviewerJob, rc int) *spawnStatusReads {
	t.Helper()
	return &spawnStatusReads{
		next: f.runner, replay: rj.Label,
		exits:   map[string]int{asyncRequestID: rc},
		outputs: map[string]string{asyncRequestID: `{"request":"` + asyncRequestID + `","state":"retryable","rc":` + strconv.Itoa(rc) + `}`},
	}
}

// (h) A placed reviewer that has not landed, whose hub request was refused at
// landing with rc 88, fails the pass at once: no pane will ever come back to
// bring the PR back, and the failure is no lost result.
func TestAsyncUnlandedPlacedRequestEndedFailsAtOnce(t *testing.T) {
	f, rj := placedRecord(t, integrations.JobRecord{State: "queued"})
	status := retryableStatus(t, f, rj, panewireAuthUnavailable)
	f.s.Runner = status
	if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err == nil {
		t.Fatal("an ended request must fail the pass")
	}
	if !slices.Equal(status.reads, []string{asyncRequestID}) || status.replays != 0 {
		t.Fatalf("status reads %v replays %d, want one read and no replay", status.reads, status.replays)
	}
	if len(f.runner.spawns) != 1 {
		t.Fatalf("spawns %d, want no replacement in the failing evaluation", len(f.runner.spawns))
	}
	dead := auditRows(t, f.s, f.r, "reviewer-spawn-dead")
	if len(dead) != 1 || dead[0]["label"] != rj.Label || dead[0]["exit"] != float64(panewireAuthUnavailable) {
		t.Fatalf("reviewer-spawn-dead rows %v, want the request ended with exit %d", dead, panewireAuthUnavailable)
	}
	if lost := auditRows(t, f.s, f.r, "review-result-lost"); len(lost) != 0 {
		t.Fatalf("review-result-lost rows %v, want none: an ended request is no lost result", lost)
	}
	reviewerJobGone(t, f.s, f.r, 1)
}

// (i) The same unlanded record whose hub request is parked on a landing timeout
// (rc 124) stays pending, as a request the hub did not end does.
func TestAsyncUnlandedPlacedRequestLandingTimeoutStaysPending(t *testing.T) {
	f, rj := placedRecord(t, integrations.JobRecord{State: "queued"})
	status := retryableStatus(t, f, rj, panewireQuotaTimeout)
	f.s.Runner = status
	if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
		t.Fatalf("a request the hub did not end stays pending: %v", err)
	}
	if !slices.Equal(status.reads, []string{asyncRequestID}) || status.replays != 0 || len(f.runner.spawns) != 1 {
		t.Fatalf("status reads %v replays %d spawns %d, want one read and nothing else", status.reads, status.replays, len(f.runner.spawns))
	}
	if got := readReviewerJob(t, f.s, f.r, 1).Label; got != rj.Label {
		t.Fatalf("record label %s, want the kept %s", got, rj.Label)
	}
	if rows := auditRows(t, f.s, f.r, "reviewer-spawn-dead"); len(rows) != 0 {
		t.Fatalf("reviewer-spawn-dead rows %v", rows)
	}
}

// (j) A landed reviewer never reads the hub status, even when the status would
// read as ended: its request is no longer the one that decides its result.
func TestAsyncLandedPlacedRecordNeverReadsTheHub(t *testing.T) {
	f, rj := placedRecord(t, integrations.JobRecord{State: "running", Host: "node-a", PaneID: "p1"})
	status := retryableStatus(t, f, rj, panewireAuthUnavailable)
	f.s.Runner = status
	if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
		t.Fatalf("a landed reviewer stays pending: %v", err)
	}
	if len(status.reads) != 0 {
		t.Fatalf("status reads %v, want none for a landed reviewer", status.reads)
	}
	if len(f.runner.spawns) != 1 {
		t.Fatalf("spawns %d, want the landed reviewer kept", len(f.runner.spawns))
	}
	if rows := auditRows(t, f.s, f.r, "reviewer-spawn-dead"); len(rows) != 0 {
		t.Fatalf("reviewer-spawn-dead rows %v", rows)
	}
}

// (k) A status read that cannot answer keeps an unlanded placed reviewer
// pending, with no failure counted and its record kept.
func TestAsyncUnlandedPlacedRequestUnreadableStatusStaysPending(t *testing.T) {
	for name, empty := range map[string]bool{"unparsable": false, "empty": true} {
		t.Run(name, func(t *testing.T) {
			f, rj := placedRecord(t, integrations.JobRecord{State: "queued"})
			status := &spawnStatusReads{
				next: f.runner, replay: rj.Label,
				exits:   map[string]int{asyncRequestID: 70},
				outputs: map[string]string{asyncRequestID: "not json"},
				empty:   map[string]bool{asyncRequestID: empty},
			}
			f.s.Runner = status
			if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
				t.Fatalf("an unreadable status stays pending: %v", err)
			}
			if !slices.Equal(status.reads, []string{asyncRequestID}) || status.replays != 0 || len(f.runner.spawns) != 1 {
				t.Fatalf("status reads %v replays %d spawns %d, want one read and nothing else", status.reads, status.replays, len(f.runner.spawns))
			}
			if got := readReviewerJob(t, f.s, f.r, 1).Label; got != rj.Label {
				t.Fatalf("record label %s, want the kept %s", got, rj.Label)
			}
			if rows := auditRows(t, f.s, f.r, "reviewer-spawn-dead"); len(rows) != 0 {
				t.Fatalf("reviewer-spawn-dead rows %v", rows)
			}
		})
	}
}
