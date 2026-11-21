package pipeline

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// jobGiveUp is panewire's hk record for a reviewer it gave up on: a lost job
// whose end_reason is "quota_stop pool=<pool> reset_at=<RFC3339>" with no
// successor token (panewire hk 5690).
func jobGiveUp(pool string, resetAt time.Time) integrations.JobRecord {
	reason := "quota_stop"
	if pool != "" {
		reason += " pool=" + pool
	}
	if !resetAt.IsZero() {
		reason += " reset_at=" + resetAt.Format(time.RFC3339)
	}
	return integrations.JobRecord{State: "lost", Host: "node-a", PaneID: "p1", EndReason: reason}
}

// A give-up (quota_stop with no replaced_by) is a loss mergewake replaces at
// once, leaving the stopped pool out of the replacement's placement, instead of
// waiting hours for that pool's reset (hk 6023).
func TestQuotaGiveUpIsReplacedNowOnAnotherPool(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	reset := f.c.t.Add(3 * time.Hour)
	f.runner.sink.jobs[old.Label] = jobGiveUp("devin-free", reset)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	if len(f.runner.spawns) != 2 {
		t.Fatalf("spawns=%d, want one replacement", len(f.runner.spawns))
	}
	if got := arg(f.runner.spawns[1], "--exclude-pool"); got != "devin-free" {
		t.Fatalf("replacement --exclude-pool = %q, want the stopped pool", got)
	}
	next := readReviewerJob(t, f.s, f.r, 1)
	if next.Label == old.Label || next.Replacements != 1 {
		t.Fatalf("replacement record = %+v", next)
	}
	if !slices.ContainsFunc(next.Replaced, func(r replacedReviewer) bool { return r.Label == old.Label && r.Pool == "devin-free" }) {
		t.Fatalf("the given-up pool is not excluded for later spawns: %+v", next.Replaced)
	}
	rows := auditRows(t, f.s, f.r, "review-quota-given-up")
	if len(rows) != 1 || rows[0]["label"] != old.Label || rows[0]["pool"] != "devin-free" || rows[0]["reset_at"] != reset.Format(time.RFC3339) || rows[0]["replacements"] != float64(0) {
		t.Fatalf("give-up audit = %v", rows)
	}
	if lost := auditRows(t, f.s, f.r, "review-result-lost"); len(lost) != 1 || lost[0]["label"] != old.Label {
		t.Fatalf("the give-up was not counted as a loss: %v", lost)
	}
	if rows := auditRows(t, f.s, f.r, "review-quota-stopped"); len(rows) != 0 {
		t.Fatalf("the give-up waited instead of replacing: %v", rows)
	}
	if f.g.postCount != 0 {
		t.Fatalf("posts=%d", f.g.postCount)
	}
}

// The pass has one replacement: a give-up on the replacement is accounted as
// spent, exactly as a second lost result is, and no third reviewer spawns.
func TestQuotaGiveUpAfterSpentReplacementIsNotReplaced(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	f.runner.sink.jobs[old.Label] = jobGiveUp("devin-free", f.c.t.Add(time.Hour))
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	replacement := readReviewerJob(t, f.s, f.r, 1)
	if replacement.Replacements != 1 {
		t.Fatalf("replacement record = %+v", replacement)
	}
	f.runner.sink.jobs[replacement.Label] = jobGiveUp("codex", f.c.t.Add(time.Hour))
	err := f.review(t)
	if !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want review-unavailable after the spent replacement", err)
	}
	if len(f.runner.spawns) != 2 {
		t.Fatalf("spawns=%d, want no second replacement: %v", len(f.runner.spawns), spawnExclusions(f.runner.spawns))
	}
	if rows := auditRows(t, f.s, f.r, "review-quota-given-up"); len(rows) != 2 {
		t.Fatalf("give-up rows = %v, want both give-ups recorded", rows)
	}
	if rows := auditRows(t, f.s, f.r, "review-result-lost"); len(rows) != 2 {
		t.Fatalf("lost rows = %v, want both give-ups counted", rows)
	}
	reviewerJobGone(t, f.s, f.r, 1)
}

// A give-up whose replacement finds no placement candidate at all is not
// dropped: it waits for the pool's reset as a quota stop always did, and the
// next run starts a fresh reviewer with its one replacement unspent.
func TestQuotaGiveUpWithoutAPlacementCandidateWaitsForReset(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	reset := f.c.t.Add(time.Hour)
	f.runner.sink.jobs[old.Label] = jobGiveUp("devin-free", reset)
	f.runner.noCandidateAlways = true
	var stopped *reviewQuotaStopped
	if err := f.review(t); !errors.As(err, &stopped) || stopped.pool != "devin-free" || !stopped.resetAt.Equal(reset) {
		t.Fatalf("err=%v, want the quota wait on the stopped pool", err)
	}
	if len(f.runner.spawns) != 3 || arg(f.runner.spawns[1], "--exclude-pool") != "devin-free" || slices.Contains(f.runner.spawns[2], "--exclude-pool") {
		t.Fatalf("spawns exclusions=%q, want the refused replacement and its retry without the exclusion", spawnExclusions(f.runner.spawns))
	}
	reviewerJobGone(t, f.s, f.r, 1)
	rows := auditRows(t, f.s, f.r, "review-quota-stopped")
	if len(rows) != 1 || rows[0]["label"] != old.Label || rows[0]["pool"] != "devin-free" || rows[0]["reset_at"] != reset.Format(time.RFC3339) {
		t.Fatalf("quota-stopped audit = %v", rows)
	}
	if given := auditRows(t, f.s, f.r, "review-quota-given-up"); len(given) != 1 {
		t.Fatalf("give-up rows = %v", given)
	}
	// After the reset the pass spawns a fresh reviewer; its replacement budget
	// is unspent, and the stopped pool is still left out.
	f.c.t = reset
	f.runner.noCandidateAlways = false
	f.runner.sink.missing = nil
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	fresh := readReviewerJob(t, f.s, f.r, 1)
	if len(f.runner.spawns) != 4 || fresh.Replacements != 0 {
		t.Fatalf("spawns=%d fresh=%+v", len(f.runner.spawns), fresh)
	}
	if got := arg(f.runner.spawns[3], "--exclude-pool"); got != "devin-free" {
		t.Fatalf("fresh spawn --exclude-pool = %q, want the stopped pool kept out", got)
	}
}

// A give-up whose replacement fails before closeReplaced runs still gets the
// given-up pane's cleanup: the cleanup stays on for every path (hk 6027).
func TestQuotaGiveUpCleanupRunsWhenReplacementFails(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	f.runner.sink.jobs[old.Label] = jobGiveUp("devin-free", f.c.t.Add(3*time.Hour))
	// A directory where the replaced-reviewers state file goes makes it neither
	// readable nor writable, so the replacement returns before closeReplaced.
	if err := os.MkdirAll(f.s.replacedReviewersPath(f.r.Name, 7, 1), 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.review(t); err == nil {
		t.Fatal("the review passed with unusable replaced-reviewers state")
	}
	rows := auditRows(t, f.s, f.r, "reviewer-cleanup")
	if !slices.ContainsFunc(rows, func(row map[string]any) bool { return row["label"] == old.Label }) {
		t.Fatalf("the given-up pane's cleanup did not run: %v", rows)
	}
}

// A give-up's replacement governs only its own spawn. Once that spawn is
// placed, a later spawn of the same pass is not held for the stopped pool's
// reset: the replacement's replacement, dead on its replay, is refused without
// a placement candidate and must not write a quota stop (hk 6027).
func TestGiveUpReplacementDoesNotGovernLaterSpawns(t *testing.T) {
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	// The give-up's pass, as the quota case leaves it: its record is dropped
	// as replaceLostReviewer does, and the replacement spawns on the same plan.
	stopped := &reviewQuotaStopped{label: old.Label, pool: "devin-free", resetAt: f.c.t.Add(time.Hour)}
	if err := f.s.dropReviewerJob(f.r.Name, 7, 1); err != nil {
		t.Fatal(err)
	}
	// A deadline with time to spare: a dead replacement without one is deferred
	// to the next run instead of spawning here.
	ctx, cancel := context.WithTimeout(f.ctx, time.Hour)
	defer cancel()
	x := &panewireExecutor{s: f.s, repo: f.r, pr: f.g.pull}
	plan := &passPlan{pass: 1, lens: "default", givenUp: stopped}
	lease := func() (string, bool, error) { return f.g.pull.Head.SHA, true, nil }
	if _, err := x.runPass(ctx, plan, lease); !errors.Is(err, errPassPending) {
		t.Fatalf("replacement err=%v, want it placed and pending", err)
	}
	if plan.givenUp != nil {
		t.Fatalf("the give-up still governs the pass after its replacement was placed")
	}
	// The replacement dies on replay, and the pass spawns once more with no
	// placement candidate left.
	dead := readReviewerJob(t, f.s, f.r, 1)
	f.runner.noCandidateAlways = true
	_, err := x.replaceDeadSpawn(ctx, plan, dead, 74, "active job refusal", f.g.pull.Head.SHA)
	if len(f.runner.spawns) != 3 {
		t.Fatalf("spawns=%d, want the later spawn to run: err=%v", len(f.runner.spawns), err)
	}
	var quota *reviewQuotaStopped
	if errors.As(err, &quota) {
		t.Fatalf("a later spawn waited for the give-up's pool reset: %v", err)
	}
	if rows := auditRows(t, f.s, f.r, "review-quota-stopped"); len(rows) != 0 {
		t.Fatalf("quota-stopped rows for a later spawn: %v", rows)
	}
}
