package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// Items 4 and 6: a job_unknown close backs off, and the job's terminal evidence is
// read again on the passes inside that backoff, not only once it has passed.
func TestReapReviewerBackedOffCloseSettlesWhenNodeReportsEnd(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
	}

	// The node now reports the job ended, while the close is still backing off.
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":true,"terminal_event":"job.completed"}`)
	f.passAfter(t, 30*time.Minute+10*time.Second)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closed again inside its backoff: closes=%d: %v", n, rowSummary(f.rows(t)))
	}
	rows := f.resultRows(t, "closed")
	if len(rows) != 1 || rows[0].Reason != "already closed (terminal event job.completed)" {
		t.Fatalf("closed rows: %v", rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
	}
}

// Item 6, first claim: a transient job.result failure after a job_unknown
// refusal is not final. Once the node answers again, a later pass settles the
// job and removes its checkout.
func TestReapReviewerTransientJobResultFailureRecoversOnLaterPass(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	f.hub.jobResultError = "node unreachable"
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	if errs := f.resultRows(t, "error"); len(errs) != 0 {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 0 {
		t.Fatalf("worktree.remove=%d while the node is unreachable: %v", n, rowSummary(f.rows(t)))
	}

	f.hub.jobResultError = ""
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":true,"terminal_event":"job.completed"}`)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.resultRows(t, "closed")); n != 1 {
		t.Fatalf("closed rows: %v", rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d after recovery: %v", n, rowSummary(f.rows(t)))
	}
}

// Item 5 pin: on the hub transport a terminal kind other than job.completed
// (job.revoked, job.reaped, job.lost) settles the close, as the node reports it.
func TestReapReviewerHubTerminalKindsAllSettle(t *testing.T) {
	for _, kind := range []string{"job.revoked", "job.reaped", "job.lost"} {
		t.Run(kind, func(t *testing.T) {
			f := newStuckJobFix(t, 7*time.Hour)
			f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false,"terminal_event":"` + kind + `"}`)
			refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
			f.passAfter(t, 0)
			f.clearPanes()
			f.passAfter(t, 30*time.Minute)
			if n := len(f.jobCloses()); n != 1 {
				t.Fatalf("job closes=%d: %v", n, rowSummary(f.rows(t)))
			}
			if rows := f.resultRows(t, "closed"); len(rows) != 1 || rows[0].Reason != "already closed (job_unknown, terminal event "+kind+")" {
				t.Fatalf("closed rows: %v", rowSummary(f.rows(t)))
			}
			if n := len(f.hub.ops("worktree.remove")); n != 1 {
				t.Fatalf("worktree.remove=%d: %v", n, rowSummary(f.rows(t)))
			}
		})
	}
}

// Item 1: a restart between the closed receipt and the worktree cleanup leaves
// only the pane stream recorded. The remote checkout is still owed, so the
// label is visited and its worktree removed.
func TestReapReviewerClosedReceiptBeforeCleanupStillRemovesWorktree(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.setJob(func(j *integrations.JobRecord) { j.State = "completed" })
	f.clearPanes()
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Host: "node-a", Result: "closed", Reason: reviewerAlreadyClosed})
	f.passAfter(t, 30*time.Minute)
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d, want the stranded checkout removed: %v", n, rowSummary(f.rows(t)))
	}
}

// Item 2: a local reviewer whose reap host names no jobs path (the documented
// default) has its ledger under the reviewer jobs root, and that ledger
// settles the job.
func TestReapReviewerLocalLedgerUnderReviewerJobsRoot(t *testing.T) {
	f := newReviewerFix(t)
	jobsRoot := t.TempDir()
	f.s.Config.Review.Panewire.Jobs = jobsRoot
	label := reviewerLabelFor(12, time.Hour)
	events := filepath.Join(jobsRoot, label, "events")
	if err := os.MkdirAll(events, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00001-job.completed.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	local := reap.Host{Name: "local"}
	hs := &reapHost{host: local, exec: reap.Exec{Runner: osRunner{}, Host: local}}
	w := &reviewerSweep{s: f.s}
	if got := w.terminalEvent(context.Background(), hs, label); got != "job.completed" {
		t.Fatalf("terminalEvent=%q, want the ledger under the reviewer jobs root", got)
	}
}

// osRunner runs the commands a local ledger read needs, for tests that read a
// real events directory.
type osRunner struct{}

func (osRunner) Run(ctx context.Context, name string, args []string, dir, stdin string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	return cmd.Output()
}

// Item 6, third claim: a label past the 24-hour checkout TTL is released by the
// aged cleanup, and a close still backing off does not hold its checkout.
func TestReapReviewerAgedLabelCleanupIsNotHeldByCloseBackoff(t *testing.T) {
	f := newStuckJobFix(t, 25*time.Hour)
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f.reviewerFix, jobUnknownStderr(f.label))
	f.passAfter(t, 0)
	f.clearPanes()
	f.passAfter(t, 30*time.Minute)
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 1 {
		t.Fatalf("job closes=%d, want no repeat inside the backoff: %v", n, rowSummary(f.rows(t)))
	}
	if n := len(f.hub.ops("worktree.remove")); n != 1 {
		t.Fatalf("worktree.remove=%d, want the aged checkout released: %v", n, rowSummary(f.rows(t)))
	}
}
