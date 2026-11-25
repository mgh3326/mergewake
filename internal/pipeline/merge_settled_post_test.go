package pipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A pending unknown-outcome attempt whose PR now reads back merged runs the
// normal post-merge steps exactly once: one merge task event, one merge hook
// call and one MERGED notice. A second Land on the same PR adds none of
// those: the attempt is gone and the closed PR never merges again.
func TestLandSettledMergeRunsPostMergeOnce(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	n := s.Notifier.(*fakeNotifier)
	r.Lane = "repo-design"
	g.pull.Title = "Add feature"
	withMergeSHA(g)
	mergedAt := time.Now().UTC().Format(time.RFC3339)
	g.pull.State, g.pull.Merged, g.pull.MergedAt = "closed", true, &mergedAt
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 7), mergeAttempt{PR: 7, Head: shaA, Base: shaB, At: time.Now()}); e != nil {
		t.Fatal(e)
	}
	hooks := 0
	ctx := WithMergeHook(context.Background(), func(repo string, pr int) {
		if repo != r.Name || pr != 7 {
			t.Errorf("hook got %s#%d, want %s#7", repo, pr, r.Name)
		}
		hooks++
	})
	if e := s.Land(ctx, r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if got := hk.received(); len(got) != 1 || got[0].Source != integrations.SourceMerge {
		t.Fatalf("task events=%+v", got)
	}
	if hooks != 1 {
		t.Fatalf("hook calls %d, want 1", hooks)
	}
	if n.count != 1 {
		t.Fatalf("MERGED notices=%+v", n.events)
	}
	// A second Land on the same PR adds none of those.
	_ = s.Land(ctx, r, 7, Options{})
	if got := hk.received(); len(got) != 1 {
		t.Fatalf("second land posted task events=%+v", got)
	}
	if hooks != 1 {
		t.Fatalf("second land hook calls %d, want 1", hooks)
	}
	if n.count != 1 {
		t.Fatalf("second land MERGED notices=%+v", n.events)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); !os.IsNotExist(e) {
		t.Fatalf("attempt still pending: %v", e)
	}
}

// A dry run that settles a merged PR stays read-only: no task event, no hook
// call, no MERGED notice, and the attempt file is left for the real pass.
func TestLandSettledMergeDryRunIsReadOnly(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	n := s.Notifier.(*fakeNotifier)
	r.Lane = "repo-design"
	withMergeSHA(g)
	mergedAt := time.Now().UTC().Format(time.RFC3339)
	g.pull.State, g.pull.Merged, g.pull.MergedAt = "closed", true, &mergedAt
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 7), mergeAttempt{PR: 7, Head: shaA, Base: shaB, At: time.Now()}); e != nil {
		t.Fatal(e)
	}
	hooks := 0
	ctx := WithMergeHook(context.Background(), func(string, int) { hooks++ })
	if e := s.Land(ctx, r, 7, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if got := hk.received(); len(got) != 0 {
		t.Fatalf("dry run posted task events=%+v", got)
	}
	if hooks != 0 {
		t.Fatalf("dry run hook calls %d, want 0", hooks)
	}
	if n.count != 0 {
		t.Fatalf("dry run MERGED notices=%+v", n.events)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); e != nil {
		t.Fatalf("dry run dropped the attempt: %v", e)
	}
}
