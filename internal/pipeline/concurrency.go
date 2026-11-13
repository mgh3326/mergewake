package pipeline

import (
	"context"
	"sync"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// The daemon reviews different PRs of one repository at once (see
// daemon.pr.review_parallel), and its roles share one Service. These locks
// guard what those goroutines share beyond the GitHub client, which guards its
// own caches.

// printMu keeps each Print one whole line on Out, whatever writer Out is.
var printMu sync.Mutex

// appendMu keeps each JSONL record (decisions, reap and archive logs) one
// whole line.
var appendMu sync.Mutex

func appendLine(name string, line []byte) error {
	appendMu.Lock()
	defer appendMu.Unlock()
	return fsutil.AppendLine(name, line)
}

// pathLocks holds one mutex per state file or checkout that is read, changed
// and written back, so two goroutines never lose each other's update.
var pathLocks sync.Map // string → *sync.Mutex

// lockPath locks name in this process and returns the unlock.
func lockPath(name string) func() {
	v, _ := pathLocks.LoadOrStore(name, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

// gitIn runs git in the repo checkout at root. Concurrent reviews share that
// checkout: a fetch updates the same remote-tracking refs and a worktree add
// or remove edits the same worktree list, and git fails a second writer on
// its ref and index locks. Those commands, and an update-ref that deletes a
// remote-tracking ref, take root's lock; reads do not.
func (s *Service) gitIn(ctx context.Context, root string, args ...string) ([]byte, error) {
	if len(args) > 0 && (args[0] == "fetch" || args[0] == "worktree" || args[0] == "update-ref") {
		defer lockPath("git:" + root)()
	}
	return s.Runner.Run(ctx, "git", append([]string{"-C", root}, args...), "", "")
}
