package daemon

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// A reviewer job that finishes between two pr passes used to wait for the next
// pass before mergewake posted its result. The watch reads the hk record of
// every live reviewer job and wakes the pr role the moment one is done. It reads
// only hk, never GitHub, so how often it looks costs no rate limit.
const (
	// ReviewerWatchEvery is how often the watch reads the live job records.
	ReviewerWatchEvery = 30 * time.Second
	// reviewerWatchMax bounds the records read per tick.
	reviewerWatchMax = 50
	// reviewerReadTimeout bounds one hk read, so a hung hk delays the tick, not the role.
	reviewerReadTimeout = 5 * time.Second
	// reviewerNotFoundFirst is the wait after a job's record is first found
	// missing. A spawn lands soon after its record is saved, so the record is
	// read again within a tick or two of the miss.
	reviewerNotFoundFirst = 15 * time.Second
	// reviewerNotFoundEvery caps the wait for a job whose record stays missing,
	// which doubles on each miss from reviewerNotFoundFirst.
	reviewerNotFoundEvery = 5 * time.Minute
)

// WatchedJob is a live reviewer job the pr role recorded: the PR whose pass
// spawned it and its hk job label.
type WatchedJob struct {
	Repo  string
	PR    int
	Pass  int
	Label string
}

// JobView is what the watch needs of an hk job record.
type JobView struct {
	State     string
	HasResult bool
}

// Terminal reports a job that will do no more work: completed, escalated or lost.
func (v JobView) Terminal() bool {
	return v.State == "completed" || v.State == "escalated" || v.State == "lost"
}

// ReviewerWatch wakes the pr role when a live reviewer job finishes.
type ReviewerWatch struct {
	// Jobs lists the live reviewer jobs; Read reads one hk job record. An error
	// from Read is an hk outage unless NotFound says the record does not exist yet.
	Jobs     func() []WatchedJob
	Read     func(ctx context.Context, label string) (JobView, error)
	NotFound func(error) bool
	// Dirty is marked for the job's PR before the wake, so the pass does not
	// skip it as unchanged. nil marks nothing.
	Dirty *Dirty
	// Enqueue evaluates the job's PR at once, when set.
	Enqueue func(repo string, pr int)
	// Wake starts the pr role; Run sets it when nil.
	Wake func()
	Log  *slog.Logger
	// Every is the tick; zero means ReviewerWatchEvery. Tests shorten it.
	Every time.Duration
	// now is the clock; nil means time.Now. Tests set it.
	now func() time.Time

	// woken holds the jobs already reported, so a finished job whose record
	// outlives a pass wakes the role once, not every tick.
	woken map[string]bool
	// missing holds each job whose record was found missing: when, and how long
	// until it is read again.
	missing map[string]notFound
	outage  bool
}

// notFound is a job's missing record: when the read found it missing, and the
// wait before the record is read again.
type notFound struct {
	at   time.Time
	wait time.Duration
}

func (w *ReviewerWatch) clock() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// Run ticks until ctx ends.
func (w *ReviewerWatch) Run(ctx context.Context) {
	every := w.Every
	if every <= 0 {
		every = ReviewerWatchEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.Tick(ctx)
		}
	}
}

// Tick reads each live job once, at most reviewerWatchMax of them, and wakes
// the role once for each job that has finished since the last tick. hk
// trouble ends the tick and is logged once per outage; it never reaches the role.
func (w *ReviewerWatch) Tick(ctx context.Context) {
	log := w.Log
	if log == nil {
		log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	jobs := w.Jobs()
	if len(jobs) > reviewerWatchMax {
		jobs = jobs[:reviewerWatchMax]
	}
	live := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		live[j.Label] = true
	}
	for label := range w.woken {
		if !live[label] {
			delete(w.woken, label)
		}
	}
	for label := range w.missing {
		if !live[label] {
			delete(w.missing, label)
		}
	}
	for _, j := range jobs {
		if w.woken[j.Label] {
			continue
		}
		if seen, ok := w.missing[j.Label]; ok && w.clock().Sub(seen.at) < seen.wait {
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, reviewerReadTimeout)
		v, err := w.Read(readCtx, j.Label)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if w.NotFound != nil && w.NotFound(err) {
				// The wait starts when the read returns: earlier reads in this
				// tick can take seconds each, and the miss is what the wait follows.
				wait := reviewerNotFoundFirst
				if prev, ok := w.missing[j.Label]; ok {
					wait = min(2*prev.wait, reviewerNotFoundEvery)
				}
				if w.missing == nil {
					w.missing = map[string]notFound{}
				}
				w.missing[j.Label] = notFound{at: w.clock(), wait: wait}
				continue
			}
			if !w.outage {
				w.outage = true
				log.Warn("reviewer watch: hk unreadable; interval and webhook wakes continue", "repo", j.Repo, "pr", j.PR, "label", j.Label, "error", err)
			}
			return
		}
		w.outage = false
		delete(w.missing, j.Label)
		if !v.Terminal() && !v.HasResult {
			continue
		}
		if w.woken == nil {
			w.woken = map[string]bool{}
		}
		w.woken[j.Label] = true
		log.Info("reviewer watch: job finished", "repo", j.Repo, "pr", j.PR, "label", j.Label, "state", v.State)
		if w.Dirty != nil {
			w.Dirty.Mark(j.Repo, j.PR, "")
		}
		if w.Enqueue != nil {
			w.Enqueue(j.Repo, j.PR)
		}
		if w.Wake != nil {
			w.Wake()
		}
	}
}
