package pipeline

import (
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestJobOtherRepo(t *testing.T) {
	for _, tt := range []struct {
		name string
		wt   *integrations.JobWorktree
		want string
	}{
		{"no worktree names no repo", nil, ""},
		{"unparseable origin names no repo", &integrations.JobWorktree{Origin: "not a remote"}, ""},
		{"this repo", &integrations.JobWorktree{Origin: "git@github.com:Example/Project.git"}, ""},
		{"another repo", &integrations.JobWorktree{Origin: "https://github.com/example/other.git"}, "example/other"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := jobOtherRepo(integrations.JobRecord{Worktree: tt.wt}, testRepo.Name); got != tt.want {
				t.Fatalf("jobOtherRepo = %q, want %q", got, tt.want)
			}
		})
	}
}
