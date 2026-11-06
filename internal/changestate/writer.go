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
// merge_commit_sha or merged_at even on a read past the cache.
type Counts struct{ Writes, Stale, Rejected, Unavailable, Skipped uint64 }
type Writer interface {
	Queue(repo string, pr int, name string, s Section)
	Async(func())
	Counts() Counts
	Wait(context.Context)
}
