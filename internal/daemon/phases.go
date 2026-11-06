package daemon

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Phase is one stretch of a PR evaluation. An evaluation is in exactly one
// phase at a time, so the phases add up to the evaluation's own time.
type Phase int

const (
	// PhaseRead is the evaluation's first reads: the PR, the bot policy and the
	// skip plan.
	PhaseRead Phase = iota + 1
	// PhaseObserve records the PR's outcomes.
	PhaseObserve
	// PhaseReview is everything from the review claim check to the end of the
	// review call, spawn included, but not the wait for the settle lock.
	PhaseReview
	// PhaseLockWait waits for the repo's settle lock.
	PhaseLockWait
	// PhaseSettle holds that lock: findings, notice and land.
	PhaseSettle
	numPhases
)

// Phases records where one evaluation spends its time. The queue creates one
// per evaluation, hands it to Run in Work, and logs it with the `pr evaluated`
// line. Its methods accept a nil receiver, so code that also runs outside an
// evaluation records nothing.
//
// The top-level phases are disjoint. Gate and Spawn are nested: the time
// inside the merge gate (it runs in the settle phase) and inside the panewire
// spawn call (it runs in the review phase).
type Phases struct {
	now func() time.Time

	mu       sync.Mutex
	cur      Phase
	since    time.Time
	took     [numPhases]time.Duration
	gate     time.Duration
	spawn    time.Duration
	settling []string
}

func newPhases(now func() time.Time) *Phases { return &Phases{now: now} }

// Now is the clock of the evaluation.
func (p *Phases) Now() time.Time {
	if p == nil || p.now == nil {
		return time.Now()
	}
	return p.now()
}

// Enter ends the current phase and starts ph.
func (p *Phases) Enter(ph Phase) {
	if p == nil {
		return
	}
	at := p.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur != 0 {
		p.took[p.cur] += at.Sub(p.since)
	}
	p.cur, p.since = ph, at
}

// End ends the current phase. The queue calls it when Run returns.
func (p *Phases) End() { p.Enter(0) }

// Gate times one run of the merge gate; call the result when it ends. The time
// of several runs adds up.
func (p *Phases) Gate() (stop func()) {
	if p == nil {
		return func() {}
	}
	start := p.Now()
	return func() {
		d := p.Now().Sub(start)
		p.mu.Lock()
		p.gate += d
		p.mu.Unlock()
	}
}

// Spawned records how long one review pass spent in its panewire spawn calls.
// The passes of a head run at once, so the longest counts.
func (p *Phases) Spawned(d time.Duration) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.spawn = max(p.spawn, d)
	p.mu.Unlock()
}

// Settling records why the evaluation held a verdict or a review back while
// GitHub's evidence settled, such as `head_moving` or `files_settling`.
func (p *Phases) Settling(reason string) {
	if p == nil || reason == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.settling {
		if r == reason {
			return
		}
	}
	p.settling = append(p.settling, reason)
}

// attrs are the fields the `pr evaluated` line gains. `settling` is present only
// when a reason was recorded.
func (p *Phases) attrs() []any {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []any{
		"read_ms", p.took[PhaseRead].Milliseconds(),
		"observe_ms", p.took[PhaseObserve].Milliseconds(),
		"review_ms", p.took[PhaseReview].Milliseconds(),
		"spawn_ms", p.spawn.Milliseconds(),
		"lock_wait_ms", p.took[PhaseLockWait].Milliseconds(),
		"settle_ms", p.took[PhaseSettle].Milliseconds(),
		"gate_ms", p.gate.Milliseconds(),
	}
	if len(p.settling) > 0 {
		out = append(out, "settling", strings.Join(p.settling, "+"))
	}
	return out
}

type phasesKey struct{}

// WithPhases returns a context that carries p.
func WithPhases(ctx context.Context, p *Phases) context.Context {
	return context.WithValue(ctx, phasesKey{}, p)
}

// PhasesOf returns the evaluation's phases, or nil outside an evaluation.
func PhasesOf(ctx context.Context) *Phases {
	p, _ := ctx.Value(phasesKey{}).(*Phases)
	return p
}
