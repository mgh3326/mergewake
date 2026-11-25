package pipeline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// A revert attempt written by the watch path belongs to watch's
// settlePendingRevert. For a closed PR that reads back merged, the orphan sweep
// leaves the file byte-identical and makes no PR read, no task event, no hook
// call and no MERGED notice.
func TestOrphanSweepLeavesWatchRevertAttempt(t *testing.T) {
	s, u, n, r := unknownMergeSetup(t, 500)
	if e := s.mergeOnce(context.Background(), r, 7, shaA, shaB, mergeRoleWatch); !github.IsUnknownOutcome(e) {
		t.Fatalf("mergeOnce = %v, want an unknown outcome that keeps the attempt", e)
	}
	before, e := os.ReadFile(s.mergeAttemptFile(r.Name, 7))
	if e != nil {
		t.Fatal(e)
	}
	at := "2026-11-24T23:54:48Z"
	u.fakeGH.pull.State, u.fakeGH.pull.Merged, u.fakeGH.pull.MergedAt = "closed", true, &at
	pulls := u.pullCalls
	hooks := 0
	ctx := WithMergeHook(context.Background(), func(string, int) { hooks++ })
	if e := s.SettleOrphanedMergeAttempts(ctx, r, map[int]bool{}, Options{}); e != nil {
		t.Fatal(e)
	}
	after, e := os.ReadFile(s.mergeAttemptFile(r.Name, 7))
	if e != nil {
		t.Fatalf("watch attempt was cleared: %v", e)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("watch attempt changed:\nbefore %s\nafter  %s", before, after)
	}
	if u.pullCalls != pulls {
		t.Fatalf("orphan sweep read PR 7 %d times, want none", u.pullCalls-pulls)
	}
	if hooks != 0 || n.count != 0 {
		t.Fatalf("hook calls %d, MERGED notices %d; want none", hooks, n.count)
	}
}

func TestOrphanSweepLeavesLegacyRecoveryBranchAttempt(t *testing.T) {
	s, g, _, r := unknownMergeSetup(t, 500)
	g.pull.Head.Ref = github.RecoveryBranch
	if err := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 7), mergeAttempt{PR: 7, Head: shaA, Base: shaB, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.mergeAttemptFile(r.Name, 7))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SettleOrphanedMergeAttempts(context.Background(), r, map[int]bool{}, Options{}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(s.mergeAttemptFile(r.Name, 7))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("legacy recovery attempt changed: %v", err)
	}
}

// An orphan whose post-merge step fails once is finished on the next sweep.
// The failure is injected after the task event and hook (the pull inside
// finishMerge), so the first sweep sends the event and hook and then errors.
// The retry must not send them again, must send the MERGED notice once, and
// must clear the attempt. The attempt has no Role, so it is a PR attempt.
func TestOrphanFinishRetriedOnNextSweepPostsOnce(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	n := s.Notifier.(*fakeNotifier)
	r.Lane = "example-lane"
	g.pull.Title = "Add feature"
	withMergeSHA(g)
	mergedAt := time.Now().UTC().Format(time.RFC3339)
	g.pull.State, g.pull.Merged, g.pull.MergedAt = "closed", true, &mergedAt
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 7), mergeAttempt{PR: 7, Head: shaA, Base: shaB, At: time.Now()}); e != nil {
		t.Fatal(e)
	}
	hooks := 0
	ctx := WithMergeHook(context.Background(), func(string, int) {
		hooks++
		if hooks == 1 {
			g.pullErr = errors.New("transient GitHub read failure")
		}
	})
	if e := s.SettleOrphanedMergeAttempts(ctx, r, map[int]bool{}, Options{}); e == nil {
		t.Fatal("first sweep returned nil, want the injected post-merge failure")
	}
	if got := hk.received(); len(got) != 1 || hooks != 1 {
		t.Fatalf("after failed sweep: task events %d, hook calls %d; want 1 and 1", len(got), hooks)
	}
	if n.count != 0 {
		t.Fatalf("after failed sweep: MERGED notices %d, want 0", n.count)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); e != nil {
		t.Fatalf("after failed sweep the attempt must stay for the retry: %v", e)
	}

	g.pullErr = nil
	if e := s.SettleOrphanedMergeAttempts(ctx, r, map[int]bool{}, Options{}); e != nil {
		t.Fatalf("retry sweep: %v", e)
	}
	if got := hk.received(); len(got) != 1 {
		t.Fatalf("retry posted task events=%+v, want still one", got)
	}
	if hooks != 1 {
		t.Fatalf("retry hook calls %d, want still one", hooks)
	}
	if n.count != 1 {
		t.Fatalf("retry MERGED notices %d, want one", n.count)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); !os.IsNotExist(e) {
		t.Fatalf("attempt still pending after the retry: %v", e)
	}

	if e := s.SettleOrphanedMergeAttempts(ctx, r, map[int]bool{}, Options{}); e != nil {
		t.Fatalf("third sweep: %v", e)
	}
	if len(hk.received()) != 1 || hooks != 1 || n.count != 1 {
		t.Fatalf("third sweep repeated a step: events %d, hooks %d, notices %d", len(hk.received()), hooks, n.count)
	}
}
