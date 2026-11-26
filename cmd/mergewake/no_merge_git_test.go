package main

import (
	"context"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// noMergeGitHistoryRunner gives fixtures with synthetic PRs a valid empty
// merge history while forwarding non-Git work to the fixture's runner.
type noMergeGitHistoryRunner struct {
	next integrations.Runner
}

func (r noMergeGitHistoryRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" {
		gitArgs := args
		if len(gitArgs) >= 2 && gitArgs[0] == "-C" {
			gitArgs = gitArgs[2:]
		}
		switch {
		case len(gitArgs) >= 2 && gitArgs[0] == "rev-parse" && gitArgs[1] == "--git-dir":
			return []byte(".git\n"), nil
		case len(gitArgs) >= 2 && gitArgs[0] == "rev-parse" && gitArgs[1] == "--is-shallow-repository":
			return []byte("false\n"), nil
		case len(gitArgs) > 0 && gitArgs[0] == "cat-file":
			return nil, nil
		case len(gitArgs) > 0 && gitArgs[0] == "fetch":
			return nil, nil
		case len(gitArgs) > 0 && gitArgs[0] == "merge-base":
			if len(gitArgs) > 1 {
				return []byte(gitArgs[1] + "\n"), nil
			}
		case len(gitArgs) > 0 && gitArgs[0] == "rev-list":
			return nil, nil
		}
	}
	return r.next.Run(ctx, command, args, input, dir)
}
