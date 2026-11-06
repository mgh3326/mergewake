package pipeline

import (
	"context"
	"sync/atomic"
)

type attemptKey struct{}

// reviewAttempt records what one review run did that RetryReview must account
// for. The passes of a head run concurrently, so it is atomic.
type reviewAttempt struct{ placed atomic.Bool }

// withAttempt returns a context carrying a fresh attempt record and the record.
func withAttempt(ctx context.Context) (context.Context, *reviewAttempt) {
	a := &reviewAttempt{}
	return context.WithValue(ctx, attemptKey{}, a), a
}

// attemptOf is nil without RetryReview around the run; its methods accept that.
func attemptOf(ctx context.Context) *reviewAttempt {
	a, _ := ctx.Value(attemptKey{}).(*reviewAttempt)
	return a
}

// reviewerPlaced notes that this run placed a reviewer that no earlier run
// had: a new pass, a replaced one, or one whose lease and record were gone.
func (a *reviewAttempt) reviewerPlaced() {
	if a != nil {
		a.placed.Store(true)
	}
}
