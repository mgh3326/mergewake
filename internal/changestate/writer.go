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
type Counts struct{ Writes, Stale, Rejected, Unavailable uint64 }
type Writer interface {
	Queue(repo string, pr int, name string, s Section)
	Async(func())
	Counts() Counts
	Wait(context.Context)
}
