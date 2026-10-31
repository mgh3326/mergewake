package pipeline

import (
	"testing"

	"github.com/mgh3326/mergewake/internal/reap"
)

func TestWorktreeRemoveVerdicts(t *testing.T) {
	cases := []struct{ outcome, result, reason string }{
		{"removed", "removed", ""},
		{"worktree-gone", "skipped", "worktree-gone"},
		{"worktree-changed", "skipped", "worktree-changed"},
		{"outside-work-root", "skipped", "outside-work-root"},
		{"unregistered", "skipped", "unregistered"},
		{"locked", "skipped", "locked"},
		{"detached", "skipped", "detached"},
		{"status-check-failed", "skipped", "status-check-failed"},
		{"dirty", "skipped", "dirty"},
		{"ignored-files", "skipped", "ignored-files:" + filesSig([]string{".env"})},
		{"unmerged-or-unpushed", "skipped", "unmerged-or-unpushed"},
		{"pane-in-use", "skipped", "pane-in-use"},
		{"pane-scan-failed", "skipped", "pane-scan-failed"},
		{"busy", "skipped", "busy"},
		{"git-failed", "skipped", "git-failed"},
		{"git_too_old", "skipped", "git_too_old"},
		{"future-outcome", "skipped", "cleanup-unknown:future-outcome"},
	}
	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			gotResult, gotReason := worktreeRemoveVerdict(reap.WorktreeRemoveResult{Outcome: tc.outcome, Files: []string{".env"}, RetryAfterS: 2})
			if gotResult != tc.result || gotReason != tc.reason {
				t.Fatalf("got %q/%q, want %q/%q", gotResult, gotReason, tc.result, tc.reason)
			}
		})
	}
}
