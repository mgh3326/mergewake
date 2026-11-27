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

// A decode-error cursor sets w.unreadable, so a completed stage clears it to the
// zero cursor even though no position was read. Only the !w.unreadable term in
// workerPass.finished reaches that write: without it the zero cursor returns
// early and the corrupt file survives the stage.
func TestWorkerCursorDecodeErrorIsClearedOnCompletedStage(t *testing.T) {
	f := budgetWorkerFix(t, 3, "", 0)
	path := filepath.Join(f.s.state(testRepo.Name), "worker-worktree-cursor.json")
	writeCorrupt(t, path)

	if err := f.reap(t, 0, Options{}); err == nil || !strings.Contains(err.Error(), "worker worktrees: resume cursor") {
		t.Fatalf("the decode-error worker cursor was not reported on its pass: %v", err)
	}
	var got workerCursor
	if !decodes(path, &got) || got != (workerCursor{}) {
		t.Fatalf("the completed stage did not clear the decode-error cursor %s to the zero cursor: %+v", path, got)
	}
	if err := f.reap(t, 0, Options{}); err != nil && strings.Contains(err.Error(), "worker worktrees: resume cursor") {
		t.Fatalf("the pass after a completed stage still reports the cursor: %v", err)
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

// The stage completes after the cursor read fails with an I/O error (ELOOP from
// the self-referencing symlink, not a decode error). w.unreadable stays false and
// the cursor is the zero value, so in workerPass.finished the !w.unreadable term
// is evaluated (it is true) and the symlink survives the early return. Removing
// that term would not change the result here, so this test does not detect its
// removal; TestWorkerCursorDecodeErrorIsClearedOnCompletedStage covers it. This
// test fails if the whole early return is removed, or if the I/O error is
// classified as unreadable (reap_worker.go), since finished then writes over the path.
func TestWorkerCursorIOErrorPreservesFileOnCompletedStage(t *testing.T) {
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
		t.Fatalf("worker cursor path changed after I/O error on a completed stage: info=%v err=%v", info, err)
	}
}

// The budget cuts the worker stage mid-order, so stopAt would rewrite the
// cursor over the unreadable file. Preservation is what keeps the symlink.
func TestWorkerCursorIOErrorPreservesFile(t *testing.T) {
	f := budgetWorkerFix(t, 24, "600ms", 50*time.Millisecond)
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
	if got := len(f.s.Sink.(*slowJobSink).read); got == 0 || got == 24 {
		t.Fatalf("worker stage did not run and cut mid-stage: read %d candidates", got)
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

// The self-referencing symlink makes the durable cursor read fail with ELOOP.
// The budget then cuts the worker candidate loop, exercising stopAt. Earlier
// tests did not reach that write path: they completed with no candidates, so
// finished saw an empty cursor and had nothing to save.
func TestWorkerCursorIOErrorPreservesFileOnBudgetCut(t *testing.T) {
	f := budgetWorkerFix(t, 24, "600ms", 50*time.Millisecond)
	path := filepath.Join(f.s.state(testRepo.Name), "worker-worktree-cursor.json")
	if err := os.Symlink(path, path); err != nil {
		t.Fatal(err)
	}
	_ = f.reap(t, 0, Options{})
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("worker cursor path changed after I/O error and budget cut: info=%v err=%v", info, err)
	}
	if got := len(f.s.Sink.(*slowJobSink).read); got == 0 || got == 24 {
		t.Fatalf("worker stage did not run and cut mid-stage: read %d candidates", got)
	}
}

// A slow job lookup makes the real reviewer sweep hit its repository budget
// between labels, where stopAt would otherwise replace the unreadable cursor.
func TestReviewerCursorIOErrorPreservesFileOnBudgetCut(t *testing.T) {
	f := newReviewerFix(t)
	for i := 12; i < 32; i++ {
		f.addReviewer(reviewerLabelFor(i, time.Hour), fmt.Sprintf("w%d:p5", i))
	}
	const budget = "2s"
	f.s.Config.Reap.RepoBudget = budget
	slow := &slowJobSink{jobSink: f.sink, step: 500 * time.Millisecond, read: map[string]bool{}}
	f.s.Sink = slow
	path := filepath.Join(f.s.state(testRepo.Name), "reviewer-sweep-cursor.json")
	if err := os.Symlink(path, path); err != nil {
		t.Fatal(err)
	}
	_ = f.reap(t, 0, Options{})
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("reviewer cursor path changed after I/O error and budget cut: info=%v err=%v", info, err)
	}
	if len(slow.read) < 2 || len(slow.read) == 20 {
		t.Fatalf("reviewer stage did not run and cut mid-stage: read %d labels", len(slow.read))
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
