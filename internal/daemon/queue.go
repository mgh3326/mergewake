package daemon

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// Source names what put a PR on the queue.
type Source string

const (
	// SourceWebhook is a verified delivery that names the PR.
	SourceWebhook Source = "webhook"
	// SourceReviewer is a reviewer job of the PR that finished.
	SourceReviewer Source = "reviewer"
	// SourceBase is a push to the repository's base branch.
	SourceBase Source = "base"
	// SourceStatus is a fresh status read that found the PR changed.
	SourceStatus Source = "status"
	// SourceTimer is a reviewer's deadline: the PR is evaluated once the
	// reviewer is past its timeout, so a reviewer that never finishes is replaced.
	SourceTimer Source = "timer"
	// SourceSweep is the periodic pass, which repairs events that never arrived.
	SourceSweep Source = "sweep"
)

// PRKey names one PR.
type PRKey struct {
	Repo string
	PR   int
}

// Priority orders waiting items: the lowest Rank starts first, ties in arrival
// order. Urgent items start at once past Cap, up to Extra of them per repo.
type Priority struct {
	Rank   int
	Urgent bool
}

// Work is one evaluation the queue hands to Run.
type Work struct {
	Key PRKey
	// Sources are what queued the item, in arrival order, without repeats.
	Sources []Source
	// Waited is the time from the event to the start of the evaluation.
	Waited time.Duration
	// Known says Priority and Detail came from Lookup. An item that started
	// without waiting for a free slot never needed one.
	Known    bool
	Priority Priority
	Detail   any
	// Expired says the lookup was still out after Grace: the item is ranked as
	// one without a priority, and asking again would only wait again.
	Expired bool
	// Over says the item started past Cap as an urgent one.
	Over bool
}

// Queue evaluates one PR per event. Items are (repo, PR); an item that is
// queued already absorbs a repeat, and an item that is being evaluated runs
// once more afterwards, so an event is never lost and a PR is never evaluated
// twice at once. Different PRs run at once, at most Cap per repo when Cap is
// set. It holds no GitHub state: Run does the work.
type Queue struct {
	// Cap bounds the evaluations of one repo that run at once; zero is no
	// bound. Extra is how many Urgent items of one repo may run past Cap.
	Cap, Extra int
	// Run evaluates one PR. It must return when ctx ends.
	Run func(ctx context.Context, w Work)
	// Lookup resolves the priority of an item that has to wait for a slot. It
	// never blocks the queue past Grace: an item whose lookup is out then ranks
	// as one without a priority. nil ranks every item the same.
	Lookup func(ctx context.Context, k PRKey) (Priority, any)
	// Grace is how long a choice between waiting items waits for lookups; zero
	// means DefaultPriorityGrace.
	Grace time.Duration
	// Now is the clock for waits; nil means time.Now.
	Now func() time.Time
	// After runs f once d has passed and returns a stop func; nil means
	// time.AfterFunc. Tests replace it with a clock they move.
	After func(d time.Duration, f func()) (stop func() bool)
	Log   *slog.Logger

	mu      sync.Mutex
	changed *sync.Cond
	ctx     context.Context
	wg      sync.WaitGroup
	seq     uint64
	entries map[PRKey]*entry
	repos   map[string]*repoState
	// open is each repo's known open PRs and their heads: what a push to the
	// base branch enqueues, and how an event that names only a commit finds its PR.
	open map[string]map[int]string
	// timers are the pending EnqueueAt calls. They live only in memory: a
	// restart repeats what they would have found through the sweep.
	timers map[timerKey]*timer
}

type timer struct {
	gen  uint64
	stop func() bool
}

type timerKey struct {
	key PRKey
	id  string
}

// DefaultPriorityGrace is how long ordering waits for priority lookups.
const DefaultPriorityGrace = 2 * time.Second

type entryState int

const (
	queued entryState = iota
	running
)

type prioState int

const (
	prioUnknown prioState = iota
	prioPending
	prioKnown
	prioExpired
)

type entry struct {
	key     PRKey
	state   entryState
	seq     uint64
	at      time.Time
	sources []Source
	// again says an event arrived while the item ran; againAt and againSources
	// become the next queued item.
	again        bool
	againAt      time.Time
	againSources []Source

	prio   prioState
	gen    uint64
	pri    Priority
	detail any
	timer  *time.Timer
	over   bool
}

type repoState struct {
	slots   int // running within Cap
	extra   int // running past Cap
	waitMax time.Duration
}

// Stats is what the queue holds for one repo.
type Stats struct {
	// Depth is the items waiting for a slot; Running the evaluations in progress.
	Depth, Running int
	// WaitMax is the longest wait of an item that started since the last Take.
	WaitMax time.Duration
}

func (q *Queue) now() time.Time {
	if q.Now != nil {
		return q.Now()
	}
	return time.Now()
}

func (q *Queue) logger() *slog.Logger {
	if q.Log != nil {
		return q.Log
	}
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// init is called with mu held.
func (q *Queue) init() {
	if q.entries == nil {
		q.entries = map[PRKey]*entry{}
		q.repos = map[string]*repoState{}
		q.open = map[string]map[int]string{}
		q.timers = map[timerKey]*timer{}
		q.changed = sync.NewCond(&q.mu)
	}
}

// Start lets the queue run items until ctx ends. Items queued before Start
// wait for it. A cancelled ctx starts nothing further; Wait returns once the
// running evaluations have.
func (q *Queue) Start(ctx context.Context) {
	q.mu.Lock()
	q.init()
	q.ctx = ctx
	q.pump()
	q.mu.Unlock()
	context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.changed.Broadcast()
		q.mu.Unlock()
	})
}

// Wait blocks until nothing is running and nothing is waiting, or, once the
// queue's context has ended, until nothing is running. A queue never started
// has nothing to wait for.
func (q *Queue) Wait() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	for q.ctx != nil && !q.idle() {
		q.changed.Wait()
	}
}

// idle is called with mu held.
func (q *Queue) idle() bool {
	if q.ctx.Err() == nil {
		return len(q.entries) == 0
	}
	for _, e := range q.entries {
		if e.state == running {
			return false
		}
	}
	return true
}

// Enqueue queues each PR of one repo for src, then starts what can run.
func (q *Queue) Enqueue(src Source, keys ...PRKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	for _, k := range keys {
		q.add(src, k)
	}
	q.pump()
}

// add is called with mu held.
func (q *Queue) add(src Source, k PRKey) {
	now := q.now()
	e, ok := q.entries[k]
	if !ok {
		q.seq++
		q.entries[k] = &entry{key: k, state: queued, seq: q.seq, at: now, sources: []Source{src}}
		return
	}
	if e.state == queued {
		if !slices.Contains(e.sources, src) {
			e.sources = append(e.sources, src)
		}
		return
	}
	if !e.again {
		e.again, e.againAt = true, now
	}
	if !slices.Contains(e.againSources, src) {
		e.againSources = append(e.againSources, src)
	}
}

// EnqueueNamed queues the PRs an event names: PR number when it is positive,
// else every known open PR whose head is sha. A PR named with its head is
// remembered under it.
func (q *Queue) EnqueueNamed(src Source, repo string, number int, sha string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	switch {
	case number > 0:
		if sha != "" {
			q.learn(repo, number, sha)
		}
		q.add(src, PRKey{repo, number})
	case sha != "":
		for n, head := range q.open[repo] {
			if head == sha {
				q.add(src, PRKey{repo, n})
			}
		}
	}
	q.pump()
}

// EnqueueAt queues PR number of repo for src at time at. id names the timer: a
// later call with the same repo, PR and id replaces it. A timer is not an
// item: it holds nothing until it fires, and Wait does not wait for it.
func (q *Queue) EnqueueAt(src Source, repo string, number int, id string, at time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	tk := timerKey{PRKey{repo, number}, id}
	if old := q.timers[tk]; old != nil {
		old.stop()
	}
	after := q.After
	if after == nil {
		after = func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }
	}
	q.seq++
	t := &timer{gen: q.seq}
	q.timers[tk] = t
	t.stop = after(max(at.Sub(q.now()), 0), func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		// A replaced timer's callback may still be in flight: only the
		// current one queues the PR.
		if cur := q.timers[tk]; cur == nil || cur.gen != t.gen {
			return
		}
		delete(q.timers, tk)
		q.add(src, tk.key)
		q.pump()
	})
}

// CancelTimer drops the timer id of PR number of repo, so it never queues the
// PR. It does nothing when there is none, or when it already fired.
func (q *Queue) CancelTimer(repo string, number int, id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	q.cancel(timerKey{PRKey{repo, number}, id})
}

// TimerIDs lists the ids of the timers PR number of repo has pending.
func (q *Queue) TimerIDs(repo string, number int) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	var ids []string
	for tk := range q.timers {
		if tk.key == (PRKey{repo, number}) {
			ids = append(ids, tk.id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (q *Queue) cancel(tk timerKey) {
	if t := q.timers[tk]; t != nil {
		t.stop()
		delete(q.timers, tk)
	}
}

// cancelPR drops every timer of PR number of repo.
func (q *Queue) cancelPR(repo string, number int) {
	for tk := range q.timers {
		if tk.key == (PRKey{repo, number}) {
			q.cancel(tk)
		}
	}
}

// EnqueueRepo queues every known open PR of repo.
func (q *Queue) EnqueueRepo(src Source, repo string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	for n := range q.open[repo] {
		q.add(src, PRKey{repo, n})
	}
	q.pump()
}

// SetOpen replaces the known open PRs of repo with their heads.
func (q *Queue) SetOpen(repo string, heads map[int]string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	for tk := range q.timers {
		if _, ok := heads[tk.key.PR]; tk.key.Repo == repo && !ok {
			q.cancel(tk)
		}
	}
	q.open[repo] = heads
}

// Learn records that PR n of repo is open at head sha.
func (q *Queue) Learn(repo string, n int, sha string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	q.learn(repo, n, sha)
}

func (q *Queue) learn(repo string, n int, sha string) {
	if q.open[repo] == nil {
		q.open[repo] = map[int]string{}
	}
	q.open[repo][n] = sha
}

// Forget drops PR n of repo from the known open PRs, and its timers with it:
// a PR that closed or merged is not queued by a timer that was set before.
func (q *Queue) Forget(repo string, n int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	q.cancelPR(repo, n)
	delete(q.open[repo], n)
}

// Take reports repo's queue and starts a new window for WaitMax; an empty
// repo reports every repo, with the longest WaitMax, and starts new windows
// for all of them.
func (q *Queue) Take(repo string) Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.init()
	var out Stats
	for _, e := range q.entries {
		if repo != "" && e.key.Repo != repo {
			continue
		}
		if e.state == queued {
			out.Depth++
		} else {
			out.Running++
		}
	}
	for name, st := range q.repos {
		if repo != "" && name != repo {
			continue
		}
		out.WaitMax = max(out.WaitMax, st.waitMax)
		st.waitMax = 0
	}
	return out
}

func (q *Queue) repo(name string) *repoState {
	st := q.repos[name]
	if st == nil {
		st = &repoState{}
		q.repos[name] = st
	}
	return st
}

// pump starts every item that may run. It is called with mu held, after
// anything that can free a slot or add an item.
//
// An item starts at once when a slot is free for it. When more items wait than
// slots are free the order matters: the priorities of the waiting items are
// looked up, and nothing but an item known to be urgent starts until each
// lookup has answered or Grace has run out. Then the waiting items start in
// rank order into the free slots, and urgent ones past Cap.
func (q *Queue) pump() {
	if q.ctx == nil || q.ctx.Err() != nil {
		return
	}
	waiting := map[string][]*entry{}
	for _, e := range q.entries {
		if e.state == queued {
			waiting[e.key.Repo] = append(waiting[e.key.Repo], e)
		}
	}
	for repo, list := range waiting {
		slices.SortFunc(list, func(a, b *entry) int { return int(a.seq) - int(b.seq) })
		st := q.repo(repo)
		if q.Cap <= 0 || len(list) <= q.Cap-st.slots {
			for _, e := range list {
				q.start(e, false)
			}
			continue
		}
		q.order(list, st)
	}
}

// order starts what it can of list, a repo's waiting items, when they outnumber
// the free slots.
func (q *Queue) order(list []*entry, st *repoState) {
	unresolved := false
	for _, e := range list {
		if q.Lookup == nil {
			break
		}
		if e.prio == prioUnknown {
			q.lookup(e)
		}
		unresolved = unresolved || e.prio == prioPending
	}
	rank := func(e *entry) int {
		if e.prio == prioKnown && e.pri.Rank > 0 {
			return e.pri.Rank
		}
		return int(^uint(0) >> 1)
	}
	slices.SortStableFunc(list, func(a, b *entry) int {
		if ra, rb := rank(a), rank(b); ra != rb {
			if ra < rb {
				return -1
			}
			return 1
		}
		return int(a.seq) - int(b.seq)
	})
	for _, e := range list {
		urgent := e.prio == prioKnown && e.pri.Urgent
		switch {
		case unresolved && !urgent:
		case st.slots < q.Cap:
			q.start(e, false)
		case urgent && st.extra < q.Extra:
			q.start(e, true)
		}
	}
}

// lookup asks for e's priority without blocking the queue. Called with mu held.
func (q *Queue) lookup(e *entry) {
	e.prio = prioPending
	gen, ctx := e.gen, q.ctx
	grace := q.Grace
	if grace <= 0 {
		grace = DefaultPriorityGrace
	}
	e.timer = time.AfterFunc(grace, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if cur := q.entries[e.key]; cur == e && e.gen == gen && e.state == queued && e.prio == prioPending {
			e.prio = prioExpired
			q.pump()
		}
	})
	go func() {
		p, detail := q.Lookup(ctx, e.key)
		q.mu.Lock()
		defer q.mu.Unlock()
		if cur := q.entries[e.key]; cur != e || e.gen != gen || e.state != queued || e.prio != prioPending {
			return
		}
		e.timer.Stop()
		e.prio, e.pri, e.detail = prioKnown, p, detail
		q.pump()
	}()
}

// start runs e. Called with mu held.
func (q *Queue) start(e *entry, over bool) {
	st := q.repo(e.key.Repo)
	if over {
		st.extra++
	} else {
		st.slots++
	}
	if e.timer != nil {
		e.timer.Stop()
	}
	e.state, e.over = running, over
	waited := q.now().Sub(e.at)
	st.waitMax = max(st.waitMax, waited)
	w := Work{Key: e.key, Sources: slices.Clone(e.sources), Waited: waited, Known: e.prio == prioKnown, Priority: e.pri, Detail: e.detail, Expired: e.prio == prioExpired, Over: over}
	ctx := q.ctx
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		began := q.now()
		q.Run(ctx, w)
		q.finish(e, w, q.now().Sub(began))
	}()
}

func (q *Queue) finish(e *entry, w Work, took time.Duration) {
	srcs := make([]string, len(w.Sources))
	for i, s := range w.Sources {
		srcs[i] = string(s)
	}
	q.logger().Info("pr evaluated", "repo", e.key.Repo, "pr", e.key.PR, "source", strings.Join(srcs, "+"), "queue_wait_ms", w.Waited.Milliseconds(), "eval_ms", took.Milliseconds())
	q.mu.Lock()
	defer q.mu.Unlock()
	st := q.repo(e.key.Repo)
	if e.over {
		st.extra--
	} else {
		st.slots--
	}
	if e.again {
		q.seq++
		e.state, e.seq, e.at, e.sources = queued, q.seq, e.againAt, e.againSources
		e.again, e.againAt, e.againSources = false, time.Time{}, nil
		e.prio, e.pri, e.detail, e.over = prioUnknown, Priority{}, nil, false
		e.gen++
	} else {
		delete(q.entries, e.key)
	}
	q.changed.Broadcast()
	q.pump()
}
