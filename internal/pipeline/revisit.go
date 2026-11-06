package pipeline

import (
	"context"
	"time"
)

// Revisit is how an evaluation that places a reviewer gets its PR back. It
// says a reviewer-finished event queues the PR again (the daemon's reviewer
// watch), so the pass places its reviewer and returns without waiting. It also
// queues the PR at `at`, past the reviewer's deadline, for the pass that
// replaces a reviewer that never finished.
type Revisit func(repo string, pr int, label string, at time.Time)

type revisitKey struct{}

// WithRevisit returns a context whose review passes do not wait for their
// reviewers and ask fn for a timer. A context without one keeps a short
// bounded wait (reviewerPassWait), so a manual `mergewake review` still
// collects a reviewer that finishes fast.
func WithRevisit(ctx context.Context, fn Revisit) context.Context {
	return context.WithValue(ctx, revisitKey{}, fn)
}

func revisitOf(ctx context.Context) Revisit {
	fn, _ := ctx.Value(revisitKey{}).(Revisit)
	return fn
}
