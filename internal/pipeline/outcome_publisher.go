package pipeline

import (
	"context"
	"log/slog"
	"time"
)

// OutcomePublisher publishes the outcomes in the ledger's outbox from one
// background loop, so no pass that reviews or merges a PR waits for a publish.
// It runs when nudged (Land and the closed-row reconcile nudge it) and on a
// timer, which also finds rows written by another process.
type OutcomePublisher struct {
	s     *Service
	repos []string
	// Interval is the timer.
	Interval time.Duration
	nudge    chan struct{}
}

// NewOutcomePublisher is a publisher of the outcomes of repos, in order.
func (s *Service) NewOutcomePublisher(repos []string, interval time.Duration) *OutcomePublisher {
	return &OutcomePublisher{s: s, repos: repos, Interval: interval, nudge: make(chan struct{}, 1)}
}

// Nudge asks for a run. It never blocks, and a burst of nudges is one run.
func (p *OutcomePublisher) Nudge() {
	select {
	case p.nudge <- struct{}{}:
	default:
	}
}

// Run publishes at start, then on every nudge and timer tick, until ctx ends.
// A publish in flight ends with ctx.
func (p *OutcomePublisher) Run(ctx context.Context) {
	timer := time.NewTimer(p.Interval)
	defer timer.Stop()
	for {
		p.once(ctx)
		select {
		case <-ctx.Done():
			return
		case <-p.nudge:
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(p.Interval)
	}
}

// once is one run over every repository. Each publish is bounded on its own
// (see outcomes.Ledger.Flush) and ends with ctx. The run has no deadline of its
// own: a shared one would let slow early outcomes starve the later ones, run
// after run. A publication failure is audited by FlushOutcomes and stays in the
// outbox; any other error is logged.
func (p *OutcomePublisher) once(ctx context.Context) {
	for _, repo := range p.repos {
		if err := p.s.FlushOutcomes(ctx, repo); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("outcome publisher run failed", "repo", repo, "error", err)
		}
	}
}
