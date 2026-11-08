package pipeline

import "context"

// UrgentHook is told that watch just made a PR urgent: the fix for a red main.
// The daemon uses it to re-rank that PR if its review is already queued.
type UrgentHook func(repo string, pr int)

type urgentHookKey struct{}

// WithUrgentHook returns a context whose Watch calls fn once the urgent mark of
// a fix PR is written. A context without a hook marks as before.
func WithUrgentHook(ctx context.Context, fn UrgentHook) context.Context {
	return context.WithValue(ctx, urgentHookKey{}, fn)
}

func fixMarkedUrgent(ctx context.Context, repo string, pr int) {
	if fn, ok := ctx.Value(urgentHookKey{}).(UrgentHook); ok && fn != nil {
		fn(repo, pr)
	}
}
