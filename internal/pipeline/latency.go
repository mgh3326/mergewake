package pipeline

import (
	"context"
	"log/slog"
	"time"
)

// HeadLatency is what an evaluation knows about when mergewake met the PR head
// it reviews. The review pass that has its reviewer spawn accepted logs one
// `review-start latency` line from it.
type HeadLatency struct {
	Log  *slog.Logger
	Repo string
	PR   int
	Head string
	// Seen is when the head was first seen and SeenBy says by what: "webhook" or
	// "eval". FirstEval is the start of the first evaluation that read the head.
	Seen      time.Time
	SeenBy    string
	FirstEval time.Time
	// EvalStart is the start of this evaluation; Evals counts the evaluations
	// of the head so far, this one included.
	EvalStart time.Time
	Evals     int
}

type headLatencyKey struct{}

// WithHeadLatency returns a context that carries h for the review passes.
func WithHeadLatency(ctx context.Context, h *HeadLatency) context.Context {
	return context.WithValue(ctx, headLatencyKey{}, h)
}

func headLatencyOf(ctx context.Context) *HeadLatency {
	h, _ := ctx.Value(headLatencyKey{}).(*HeadLatency)
	return h
}

// spawnAccepted logs the head-to-reviewer latency of pass, whose spawn was
// accepted at time at.
func (h *HeadLatency) spawnAccepted(pass int, at time.Time) {
	if h == nil || h.Log == nil {
		return
	}
	ms := func(from, to time.Time) int64 { return max(to.Sub(from), 0).Milliseconds() }
	h.Log.Info("review-start latency", "repo", h.Repo, "pr", h.PR, "head", h.Head, "pass", pass,
		"first_seen", h.Seen.UTC().Format(time.RFC3339Nano), "first_seen_by", h.SeenBy, "evals", h.Evals,
		"seen_to_first_eval_ms", ms(h.Seen, h.FirstEval),
		"first_eval_to_eval_ms", ms(h.FirstEval, h.EvalStart),
		"eval_to_accepted_ms", ms(h.EvalStart, at),
		"total_ms", ms(h.Seen, at))
}
