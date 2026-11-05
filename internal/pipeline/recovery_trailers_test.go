package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestRecoveryMarkerBranchGate(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Recovery: abc123\n"
	f.g.pull.Head.Repo.FullName = f.r.Name
	if err := f.s.recordRevert(f.r.Name, 123, "https://github.com/example/project/pull/7"); err != nil {
		t.Fatal(err)
	}
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

func TestRecoveryMarkerForkBlocked(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Head.Ref = github.RecoveryBranch
	f.g.pull.Head.Repo.FullName = "fork/project"
	f.g.pull.Body = "Mergewake-Recovery: abc123\n"
	b, blocked := f.s.reviewBlockFor(context.Background(), f.r, f.g.pull)
	if !blocked || b.reason != blockMissingTrailer {
		t.Fatalf("block = %+v, %v", b, blocked)
	}
}

func TestRecoveryTrailersCompleteBodyAvoidsCommitLookup(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Task: 42\nMergewake-Grade: B\nMergewake-Tier: T1\nMergewake-Notify-Lane: team"
	f.g.commitErr = map[string]error{f.g.pull.Head.SHA: errors.New("unavailable")}
	got, err := f.s.recoveryTrailers(context.Background(), f.r, f.g.pull)
	if err != nil || got != f.g.pull.Body {
		t.Fatalf("got %q, error %v", got, err)
	}
}

func TestRecoveryMarkerUnrecordedBlocked(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Head.Ref = github.RecoveryBranch
	f.g.pull.Head.Repo.FullName = f.r.Name
	f.g.pull.Body = "Mergewake-Recovery: abc123\n"
	b, blocked := f.s.reviewBlockFor(context.Background(), f.r, f.g.pull)
	if !blocked || b.reason != blockMissingTrailer {
		t.Fatalf("block = %+v, %v", b, blocked)
	}
}
