// Package changestate defines the observation sink shared by GitHub and hk.
package changestate

import (
	"context"
	"time"
)

type Section struct {
	At    time.Time
	Value any
}

// Skipped counts merged state writes withheld because GitHub gave no
// merge_commit_sha or merged_at even to the GraphQL merge commit resolver.
// Unchanged counts sections not written because their value is what the
// process last wrote for the change (observation times aside).
type Counts struct{ Writes, Stale, Rejected, Unavailable, Skipped, Unchanged uint64 }
type Writer interface {
	Queue(repo string, pr int, name string, s Section)
	Async(func())
	Counts() Counts
	Wait(context.Context)
}
