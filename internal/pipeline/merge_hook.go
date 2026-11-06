package pipeline

import "context"

// MergeHook is told that the pr role just merged a PR. The daemon uses it to
// wake the reap role, so the merged PR's worker and reviewers are reaped within
// minutes instead of at the next poll.
type MergeHook func(repo string, pr int)

type mergeHookKey struct{}

// WithMergeHook returns a context whose Land calls fn once per PR it merges,
// after the merge landed. A context without a hook merges as before.
func WithMergeHook(ctx context.Context, fn MergeHook) context.Context {
	return context.WithValue(ctx, mergeHookKey{}, fn)
}

// mergeLanded runs the hook of ctx, if any. Land reaches it through
// postTaskEvent, which it calls exactly once right after a successful merge.
func mergeLanded(ctx context.Context, repo string, pr int) {
	if fn, ok := ctx.Value(mergeHookKey{}).(MergeHook); ok && fn != nil {
		fn(repo, pr)
	}
}
