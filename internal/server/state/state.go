// Package state owns deterministic PR transitions. It performs no external work.
package state

import "errors"

type Stage string

const (
	Observing      Stage = "observing"
	Draft          Stage = "draft"
	WaitingCI      Stage = "waiting_ci"
	ReviewQueued   Stage = "review_queued"
	Reviewing      Stage = "reviewing"
	AwaitingAuthor Stage = "awaiting_author"
	BlockedAuthor  Stage = "blocked_author"
	GatePending    Stage = "gate_pending"
	Ready          Stage = "ready"
	Merged         Stage = "merged"
	Closed         Stage = "closed"
	SystemBlocked  Stage = "system_blocked"
)

type Snapshot struct {
	Head, Base          string
	Generation, Version uint64
	Stage               Stage
	FixRounds, Failures int
	Reviewed            bool
}

type EventKind string

const (
	Observe         EventKind = "observe"
	MarkDraft       EventKind = "draft"
	WaitCI          EventKind = "wait_ci"
	QueueReview     EventKind = "queue_review"
	StartReview     EventKind = "start_review"
	ReviewPassed    EventKind = "review_passed"
	Findings        EventKind = "findings"
	FixPatch        EventKind = "fix_patch"
	EquivalentPatch EventKind = "equivalent_patch"
	GatePassed      EventKind = "gate_passed"
	GateFailed      EventKind = "gate_failed"
	ConfirmMerged   EventKind = "merged"
	ConfirmClosed   EventKind = "closed"
	Resume          EventKind = "resume"
	Failure         EventKind = "failure"
	Recovered       EventKind = "recovered"
)

// Events are validated facts from trusted adapters, never webhook instructions.
// Authorization of Resume and evidence for GatePassed belong to those adapters.
type Event struct {
	Kind       EventKind
	Head, Base string
	Generation uint64
}

type EffectKind string

const (
	Refresh      EffectKind = "refresh"
	Review       EffectKind = "review"
	Gate         EffectKind = "gate"
	NotifyAuthor EffectKind = "notify_author"
	Escalate     EffectKind = "escalate"
)

type Transition struct {
	Next    Snapshot
	Effects []EffectKind
}

var ErrTransition = errors.New("invalid PR transition")
var ErrStale = errors.New("stale PR generation")

func (s Snapshot) Valid() bool {
	if s == (Snapshot{}) {
		return true
	}
	if s.Head == "" || s.Base == "" || s.Generation == 0 || s.Version == 0 || s.FixRounds < 0 || s.FixRounds > 2 || s.Failures < 0 || s.Failures > 3 || (!s.Reviewed && s.FixRounds > 0) {
		return false
	}
	switch s.Stage {
	case Observing, Draft, WaitingCI, ReviewQueued, Reviewing, AwaitingAuthor, BlockedAuthor, GatePending, Ready, Merged, Closed, SystemBlocked:
		return true
	}
	return false
}

func Reduce(s Snapshot, e Event) (Transition, error) {
	if !s.Valid() {
		return Transition{}, ErrTransition
	}
	n := s
	var effects []EffectKind
	if e.Kind == Observe {
		if e.Head == "" || e.Base == "" || s.Stage == Merged || s.Stage == Closed {
			return Transition{}, ErrTransition
		}
		if e.Head == s.Head && e.Base == s.Base {
			return Transition{Next: s}, nil
		}
		n.Head, n.Base = e.Head, e.Base
		n.Generation++
		// Observation cannot erase exhausted fix cycles or operational failures.
		if s.Stage != BlockedAuthor && s.Stage != SystemBlocked {
			n.Stage = Observing
			effects = []EffectKind{Refresh}
		}
	} else {
		if s.Generation == 0 || e.Generation != s.Generation || e.Head != s.Head {
			return Transition{}, ErrStale
		}
		if s.Stage == Merged || s.Stage == Closed {
			return Transition{}, ErrTransition
		}
		switch e.Kind {
		case ConfirmClosed:
			n.Stage = Closed
		case ConfirmMerged:
			// GitHub can confirm an external merge regardless of the local stage.
			n.Stage = Merged
		case Resume:
			if s.Stage != BlockedAuthor && s.Stage != SystemBlocked {
				return Transition{}, ErrTransition
			}
			n.Stage, n.FixRounds, n.Failures = Observing, 0, 0
			n.Generation++
			n.Reviewed = false
			effects = []EffectKind{Refresh}
		case Failure:
			if s.Stage == SystemBlocked {
				return Transition{Next: s}, nil
			}
			n.Failures++
			if n.Failures >= 3 {
				n.Stage = SystemBlocked
				effects = []EffectKind{Escalate}
			}
		case Recovered:
			if s.Stage == SystemBlocked {
				return Transition{}, ErrTransition
			}
			n.Failures = 0
		default:
			if s.Stage == BlockedAuthor || s.Stage == SystemBlocked {
				return Transition{}, ErrTransition
			}
			switch e.Kind {
			case MarkDraft:
				if s.Stage != Observing {
					return Transition{}, ErrTransition
				}
				n.Stage = Draft
			case WaitCI:
				if s.Stage != Observing && s.Stage != GatePending {
					return Transition{}, ErrTransition
				}
				n.Stage = WaitingCI
			case QueueReview, FixPatch:
				if s.Stage != Observing && s.Stage != WaitingCI && s.Stage != Draft {
					if e.Kind != FixPatch || s.Stage != AwaitingAuthor {
						return Transition{}, ErrTransition
					}
				}
				if e.Kind == FixPatch && !s.Reviewed {
					return Transition{}, ErrTransition
				}
				if s.Reviewed {
					if s.FixRounds >= 2 {
						n.Stage = BlockedAuthor
						effects = []EffectKind{NotifyAuthor}
						break
					}
					n.FixRounds++
				}
				n.Stage = ReviewQueued
				effects = []EffectKind{Review}
			case StartReview:
				if s.Stage != ReviewQueued {
					return Transition{}, ErrTransition
				}
				n.Stage = Reviewing
			case ReviewPassed:
				if s.Stage != Reviewing {
					return Transition{}, ErrTransition
				}
				n.Stage, n.Failures = GatePending, 0
				n.Reviewed = true
				effects = []EffectKind{Gate}
			case Findings:
				if s.Stage != Reviewing {
					return Transition{}, ErrTransition
				}
				n.Stage, n.Failures = AwaitingAuthor, 0
				n.Reviewed = true
				if s.FixRounds >= 2 {
					n.Stage = BlockedAuthor
				}
				effects = []EffectKind{NotifyAuthor}
			case EquivalentPatch:
				if s.Stage != Observing && s.Stage != AwaitingAuthor {
					return Transition{}, ErrTransition
				}
				n.Stage = GatePending
				effects = []EffectKind{Gate}
			case GatePassed:
				if s.Stage != GatePending && s.Stage != WaitingCI {
					return Transition{}, ErrTransition
				}
				n.Stage, n.Failures = Ready, 0
				effects = []EffectKind{NotifyAuthor}
			case GateFailed:
				if s.Stage != GatePending && s.Stage != Ready {
					return Transition{}, ErrTransition
				}
				n.Stage = AwaitingAuthor
				effects = []EffectKind{NotifyAuthor}
			default:
				return Transition{}, ErrTransition
			}
		}
	}
	if n == s && len(effects) == 0 {
		return Transition{Next: s}, nil
	}
	n.Version++
	return Transition{Next: n, Effects: effects}, nil
}
