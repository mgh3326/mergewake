package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// A worker stage that its two-thirds share cuts, followed by a reviewer sweep,
// orphan scan and archive that all finish inside the last third, is a pass that
// completed: the share cut is local to the stage, not the pass's budget stop.
func TestWorkerShareCutIsNotThePassStop(t *testing.T) {
	f := budgetWorkerFix(t, 24, "3s", 100*time.Millisecond)
	err := f.reap(t, 0, Options{})
	if got := len(f.s.Sink.(*slowJobSink).read); got == 24 {
		t.Fatalf("the worker stage read every candidate: its share never cut it, so the test proves nothing")
	}
	if errors.Is(err, ErrReapRepoBudget) {
		t.Fatalf("a pass whose worker share was cut and whose later stages finished returned a budget stop: %v", err)
	}
}

// A job read the worker stage's share cut is not cached for the pass: the
// archive stage, which runs later on the pass's own budget, reads the record.
func TestShareCutJobReadIsNotReplayedToLaterStage(t *testing.T) {
	jobs := &budgetedJobs{}
	records := newPassJobRecords(jobs)
	deadline := time.Now().Add(10 * time.Second)
	passCtx, cancelPass := context.WithDeadlineCause(context.Background(), deadline, ErrReapRepoBudget)
	defer cancelPass()
	passCtx = context.WithValue(passCtx, reapBudgetKey{}, reapBudget{deadline: deadline})

	stage, cancelStage := reapStageShare(passCtx, 1, 1000)
	defer cancelStage()
	<-stage.Done()
	if _, err := records.job(stage, "j1"); !errors.Is(err, ErrReapRepoBudget) {
		t.Fatalf("the share-cut read returned %v, want a budget cut", err)
	}
	rec, err := records.job(passCtx, "j1")
	if err != nil {
		t.Fatalf("the later stage replayed the share cut instead of reading the record: %v", err)
	}
	if rec.State != "done" {
		t.Fatalf("later stage record = %+v, want state done", rec)
	}
}

// budgetedJobs answers a job read with its record, or with the budget cause
// when the read's context has already ended.
type budgetedJobs struct{}

func (budgetedJobs) TaskJob(context.Context, string) (string, string, error) { return "", "", nil }

func (budgetedJobs) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if err := ctx.Err(); err != nil {
		return integrations.JobRecord{}, fmt.Errorf("%w: hk job lookup: %v", ErrReapRepoBudget, err)
	}
	return integrations.JobRecord{State: "done", Label: id}, nil
}

// A worker cursor file that cannot be decoded is reported once, and the stage
// that reports it rewrites the file, so the pass after it has no cursor error.
func TestCorruptWorkerCursorIsRepaired(t *testing.T) {
	f := budgetWorkerFix(t, 3, "", 0)
	path := filepath.Join(f.s.state(testRepo.Name), "worker-worktree-cursor.json")
	writeCorrupt(t, path)

	if err := f.reap(t, 0, Options{}); err == nil || !strings.Contains(err.Error(), "worker worktrees: resume cursor") {
		t.Fatalf("the unreadable worker cursor was not reported on its pass: %v", err)
	}
	if !decodes(path, &workerCursor{}) {
		t.Fatalf("the worker stage did not rewrite the unreadable cursor %s", path)
	}
	if err := f.reap(t, 0, Options{}); err != nil && strings.Contains(err.Error(), "worker worktrees: resume cursor") {
		t.Fatalf("the next pass still reports the cursor: %v", err)
	}
}

// The reviewer sweep cursor is repaired the same way.
func TestCorruptReviewerCursorIsRepaired(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	path := filepath.Join(f.s.state(testRepo.Name), "reviewer-sweep-cursor.json")
	writeCorrupt(t, path)

	if err := f.reap(t, 0, Options{}); err == nil || !strings.Contains(err.Error(), "reviewer reap: sweep cursor") {
		t.Fatalf("the unreadable reviewer cursor was not reported on its pass: %v", err)
	}
	if !decodes(path, &reviewerCursor{}) {
		t.Fatalf("the reviewer sweep did not rewrite the unreadable cursor %s", path)
	}
	if err := f.reap(t, 0, Options{}); err != nil && strings.Contains(err.Error(), "reviewer reap: sweep cursor") {
		t.Fatalf("the next pass still reports the cursor: %v", err)
	}
}

func TestWorkerCursorIOErrorPreservesFile(t *testing.T) {
	f := budgetWorkerFix(t, 3, "", 0)
	path := filepath.Join(f.s.state(testRepo.Name), "worker-worktree-cursor.json")
	if err := os.Symlink(path, path); err != nil {
		t.Fatal(err)
	}
	err := f.reap(t, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "worker worktrees: resume cursor") {
		t.Fatalf("worker cursor I/O error was not reported: %v", err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("worker cursor path changed after I/O error: info=%v err=%v", info, err)
	}
}

func TestReviewerCursorIOErrorPreservesFile(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	path := filepath.Join(f.s.state(testRepo.Name), "reviewer-sweep-cursor.json")
	if err := os.Symlink(path, path); err != nil {
		t.Fatal(err)
	}
	err := f.reap(t, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "reviewer reap: sweep cursor") {
		t.Fatalf("reviewer cursor I/O error was not reported: %v", err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("reviewer cursor path changed after I/O error: info=%v err=%v", info, err)
	}
}

func writeCorrupt(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// decodes reports whether the file at path now holds valid JSON for v.
func decodes(path string, v any) bool {
	b, err := os.ReadFile(path)
	return err == nil && json.Unmarshal(b, v) == nil
}
