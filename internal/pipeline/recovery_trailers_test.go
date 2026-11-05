package pipeline

import (
	"context"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestRecoveryMarkerBranchGate(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Recovery: abc123\n"
	for _, tc := range []struct {
		branch  string
		blocked bool
	}{{github.RecoveryBranch, false}, {"feature", true}} {
		t.Run(tc.branch, func(t *testing.T) {
			f.g.pull.Head.Ref = tc.branch
			b, blocked := f.s.reviewBlockFor(context.Background(), f.r, f.g.pull)
			if blocked != tc.blocked || (blocked && b.reason != blockMissingTrailer) {
				t.Fatalf("block = %+v, %v", b, blocked)
			}
		})
	}
}

func TestRecoveryTrailerPrecedence(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Task: 42\nMergewake-Grade: B\n"
	f.g.messages = map[string]string{f.g.pull.Head.SHA: "Mergewake-Task: 43\nMergewake-Grade: C\nMergewake-Tier: T1\nMergewake-Notify-Lane: team\n"}
	got, err := f.s.recoveryTrailers(context.Background(), f.r, f.g.pull)
	if err != nil {
		t.Fatal(err)
	}
	want := "Mergewake-Task: 42\nMergewake-Grade: B\nMergewake-Tier: T1\nMergewake-Notify-Lane: team"
	if got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
	f.g.pull.Body = got
	f.g.pull.Head.Ref = github.RecoveryBranch
	if b, blocked := f.s.reviewBlockFor(context.Background(), f.r, f.g.pull); blocked {
		t.Fatalf("blocked: %+v", b)
	}
}

func TestWatchPassesRecoveryTrailers(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.head = shaC
	g.parents = map[string]string{shaC: shaB, shaB: shaA}
	g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green}
	merged := "2026-01-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.MergeCommitSHA = shaB
	g.pull.Body = "Mergewake-Task: 42\nMergewake-Grade: B\nMergewake-Tier: T1"
	if err := s.Watch(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.revertCount != 1 || g.revertTrailers != g.pull.Body {
		t.Fatalf("reverts %d, trailers %q", g.revertCount, g.revertTrailers)
	}
}
