package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
	"github.com/mgh3326/mergewake/internal/prstatus"
	"github.com/mgh3326/mergewake/internal/tracing"
)

// prEngine is the pr role. It evaluates one PR when something about it
// happens: a delivery, a finished reviewer job, a push to the base branch, a
// fresh status read. Each of those puts the PR on the queue. The sweep, which
// the role still runs every pr.interval, only puts the open PRs on it, so it
// repairs events that never arrived and no longer decides how soon a PR is
// looked at.
//
// One evaluation is observe, then review, then settle, as the pass used to run
// it for each PR. It never waits for another PR, except at the merge: Land
// takes the repo's merge lock from its last recheck to the end of the merge,
// so each merge sees the base the previous one left. Findings and the verdict
// notice of two PRs of one repo go out at once.
type prEngine struct {
	s     *pipeline.Service
	gh    *github.Client
	o     pipeline.Options
	repos map[string]config.Repo
	// skips is nil without the webhook receiver: nothing is skipped then.
	skips   *prSkips
	log     *slog.Logger
	onMerge pipeline.MergeHook
	q       *daemon.Queue
	// seen remembers when each PR head was first seen, for the
	// `review-start latency` line.
	seen *daemon.HeadSeen
	// revisit is set when the reviewer watch can see reviewers finish: the
	// pr role then starts reviewers without waiting for them.
	revisit pipeline.Revisit

	mu sync.Mutex
	// open is each repo's open PRs as this engine knows them, for the
	// closed-row reconcile (see openSet).
	open     map[string]*openSet
	sweepMu  sync.Mutex
	sweeping map[string]*prSweepState
	// since is what happened per repo after the last pass line.
	since map[string]*passCounts
	errs  map[string][]error
	// taken is the queue state each repo's last sweep saw.
	taken        map[string]daemon.Stats
	evalRequests int
	// evalCounts is the requests the evaluations made since the last "github
	// requests" line, broken down by route like the sweep's.
	evalCounts *github.RequestCounts
}

type prSweepState struct {
	started time.Time
	mu      sync.Mutex
	step    string
}

type prSweepStateKey struct{}

func (s *prSweepState) setStep(step string) {
	s.mu.Lock()
	s.step = step
	s.mu.Unlock()
}

func (s *prSweepState) currentStep() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.step
}

func setPRSweepStep(ctx context.Context, step string) {
	if state, ok := ctx.Value(prSweepStateKey{}).(*prSweepState); ok {
		state.setStep(step)
	}
}

// passCounts is what the evaluations of one repo did since the last pass line.
type passCounts struct {
	skipped, evaluated int
	reasons            map[string]int
	evaluatedPRs       []string
	reviewing, peak    int
}

func newPREngine(s *pipeline.Service, gh *github.Client, o pipeline.Options, repos []config.Repo, skips *prSkips, log *slog.Logger, onMerge pipeline.MergeHook) *prEngine {
	e := &prEngine{s: s, gh: gh, o: o, repos: map[string]config.Repo{}, skips: skips, log: log, onMerge: onMerge,
		sweeping: map[string]*prSweepState{}, open: map[string]*openSet{}, since: map[string]*passCounts{}, errs: map[string][]error{}, taken: map[string]daemon.Stats{},
		evalCounts: github.NewRequestCounts("pr")}
	for _, r := range repos {
		e.repos[r.Name] = r
	}
	// Different PRs of a repo run at once, at most review_parallel when it is
	// set, else all of them; a priority-1 PR also starts past the bound, up to
	// urgentExtra more at once.
	e.seen = daemon.OpenHeadSeen(s.Config.StateDir)
	e.seen.Log = log
	e.q = &daemon.Queue{Cap: s.Config.Daemon.PR.Parallel(), Extra: urgentExtra, Run: e.run, Lookup: e.lookup, Grace: reviewPriorityGrace, Seen: e.seen, Log: log}
	// The reviewer watch reads hk job records. A sink that cannot read them
	// never reports a finished reviewer, so the passes keep a bounded wait.
	if watchesReviewers(s.Sink) {
		e.revisit = func(repo string, pr int, label string, at time.Time) {
			e.q.EnqueueAt(daemon.SourceTimer, repo, pr, label, at)
		}
	}
	return e
}

// watchesReviewers reports whether the reviewer watch can see a reviewer of
// sink finish: it reads hk job records. A var so tests can keep the bounded
// wait over a fake sink that answers the job records of its fake panes.
var watchesReviewers = func(sink integrations.Sink) bool {
	_, ok := sink.(integrations.JobReader)
	return ok
}

// urgentExtra is how many priority-1 evaluations of one repo may run past an
// explicit review_parallel at once.
const urgentExtra = 2

// reviewPriorityGrace is how long a choice between waiting PRs waits for their
// priority lookups. A lookup still out then is dropped and its PR is ordered
// as one without a priority, so a slow hk delays no other PR beyond this. A
// var so tests can shorten it.
var reviewPriorityGrace = 2 * time.Second

// prSweepTimeout bounds how long one repository can hold a PR pass. Tests
// shorten it to exercise the timeout and still-running paths.
var prSweepTimeout = 5 * time.Minute

func (e *prEngine) counts(repo string) *passCounts {
	if e.since[repo] == nil {
		e.since[repo] = &passCounts{reasons: map[string]int{}}
	}
	return e.since[repo]
}

// lookup is the queue's priority lookup for a PR that has to wait for a slot.
func (e *prEngine) lookup(ctx context.Context, k daemon.PRKey) (daemon.Priority, any) {
	r, ok := e.repos[k.Repo]
	if !ok {
		return daemon.Priority{}, nil
	}
	pr, err := e.gh.Pull(ctx, r.Name, k.PR)
	if err != nil {
		p := pipeline.ReviewPriority{Source: "error"}
		return daemon.Priority{Rank: p.Rank()}, p
	}
	p := e.s.ReviewPriority(ctx, r, pr)
	return daemon.Priority{Rank: p.Rank(), Urgent: p.Urgent()}, p
}

// run is one queued evaluation. Its errors wait for the repo's next sweep to
// return them, so the role's failure count, backoff and health stay what the
// pass made them.
func (e *prEngine) run(ctx context.Context, w daemon.Work) {
	r, ok := e.repos[w.Key.Repo]
	if !ok {
		return
	}
	ctx = daemon.WithPhases(ctx, w.Phases)
	w.Phases.Enter(daemon.PhaseRead)
	ctx, transaction := tracing.Pass(ctx, "pr")
	defer tracing.Finish(transaction)
	ctx, requests := github.ContinueRequests(ctx, "pr")
	ctx = pipeline.WithMergeHook(ctx, e.onMerge)
	ctx, repoSpan := tracing.Span(ctx, "mergewake.repo")
	tracing.Tag(repoSpan, "repo", r.Name)
	defer tracing.Finish(repoSpan)
	errs, res := e.evaluate(ctx, r, w)
	total := 0
	for _, c := range requests.Snapshot() {
		total += c.Count
	}
	tracing.Data(transaction, "github_requests", total)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.evalRequests += total
	e.evalCounts.Add(requests)
	if ctx.Err() != nil {
		return
	}
	e.errs[r.Name] = append(e.errs[r.Name], errs...)
	if res.gone {
		return
	}
	c := e.counts(r.Name)
	if res.skipped {
		c.skipped++
		return
	}
	c.evaluated++
	for _, reason := range res.reasons {
		c.reasons[reason]++
	}
	if len(res.reasons) > 0 {
		c.evaluatedPRs = append(c.evaluatedPRs, fmt.Sprintf("%d:%s", w.Key.PR, strings.Join(res.reasons, "+")))
	}
}

// evalResult is what one evaluation did, for the pass line.
type evalResult struct {
	// gone: the PR is closed, merged, or not against this repo's base branch.
	gone bool
	// skipped: nothing about the PR changed since its last clean evaluation.
	skipped bool
	reasons []string
}

// freshRead reports whether an evaluation must read past the poll windows and
// caches. Only check deliveries say the PR itself did not change: their result
// is the new fact, when the check ran on the head the queue knows for the PR.
// Any other source says GitHub changed the PR, as it always did.
func (e *prEngine) freshRead(w daemon.Work) bool {
	onlyChecks := len(w.Sources) > 0
	for _, s := range w.Sources {
		if s != daemon.SourceCheck {
			onlyChecks = false
		}
	}
	if onlyChecks {
		head := e.q.Head(w.Key.Repo, w.Key.PR)
		return w.CheckSHA == "" || head == "" || w.CheckSHA != head
	}
	return slices.ContainsFunc(w.Sources, func(s daemon.Source) bool { return s != daemon.SourceSweep })
}

// evaluate is the single-PR path: read the PR, then observe it, review it and
// settle it as one PR of the old pass was.
func (e *prEngine) evaluate(ctx context.Context, r config.Repo, w daemon.Work) (errs []error, res evalResult) {
	n := w.Key.PR
	evalStart := w.Phases.Now()
	readCtx := ctx
	if e.freshRead(w) {
		readCtx = github.FreshEvidence(ctx)
	}
	pr, err := e.gh.Pull(readCtx, r.Name, n)
	if err != nil {
		if errors.Is(err, github.ErrPollSkipped) {
			res.gone = true
			return nil, res
		}
		return []error{fmt.Errorf("PR %d: %w", n, err)}, evalResult{gone: true}
	}
	if pr.State != "open" || pr.Merged || (r.Branch != "" && pr.Base.Ref != r.Branch) {
		e.q.Forget(r.Name, n)
		e.openOf(r.Name).forget(n)
		res.gone = true
		return nil, res
	}
	e.q.Learn(r.Name, n, pr.Head.SHA)
	e.openOf(r.Name).learn(n)
	// The head is met here at the latest; a delivery that named it set an
	// earlier time.
	rec := e.seen.Evaluating(r.Name, n, pr.Head.SHA, evalStart)
	ctx = pipeline.WithHeadLatency(ctx, &pipeline.HeadLatency{Log: e.log, Repo: r.Name, PR: n, Head: pr.Head.SHA,
		Seen: rec.Seen, SeenBy: rec.SeenBy, FirstEval: rec.FirstEval, EvalStart: evalStart, Evals: rec.Evals})
	if len(skipBotPulls([]github.Pull{pr}, e.s.Config.Daemon.PR.SkipBots)) == 0 {
		res.gone = true
		return errs, res
	}
	// A conflicting head gets its one ESC here, before the unchanged skip and
	// whatever the draft, review or settle below does: a held, quota-stopped or
	// pending review never reaches NotifyPR. The ESC changes nothing below.
	if err := e.s.EscalateConflict(ctx, r, pr, e.o); err != nil {
		errs = append(errs, fmt.Errorf("PR %d conflict ESC: %w", n, err))
	}
	prCtx, prSpan := tracing.Span(ctx, "mergewake.pr")
	tracing.Data(prSpan, "pr", n)
	defer tracing.Finish(prSpan)
	// A PR whose ETags, head, base and webhooks are as its last clean
	// evaluation left them keeps that verdict: nothing below touches it.
	var plan skipPlan
	if e.skips != nil {
		base, baseErr := e.gh.Head(readCtx, r.Name, r.Branch)
		plan = e.skips.plan(ctx, e.gh, r.Name, n, base, baseErr)
		// A fresh status read that found the PR changed asked for a full
		// evaluation, whatever the validators say.
		if plan.skip && prstatus.DirtyPRs(e.s.Config.StateDir, r.Name)[n] {
			plan.skip, plan.reasons = false, []string{"status"}
		}
	}
	if plan.skip {
		res.skipped = true
		return errs, res
	}
	res.reasons = plan.reasons
	opts := e.o
	w.Phases.Enter(daemon.PhaseObserve)
	observeCtx, observeSpan := tracing.Span(prCtx, "mergewake.observe")
	observationErr := e.s.ObservePR(observeCtx, r, pr, e.o)
	tracing.Finish(observeSpan)
	opts.Observed = observationErr == nil
	failed := observationErr != nil
	if observationErr != nil {
		errs = append(errs, fmt.Errorf("PR %d outcomes: %w", n, observationErr))
	}
	if ctx.Err() != nil {
		return append(errs, ctx.Err()), res
	}
	settled := false
	switch {
	case pr.Draft:
		settled = true
	default:
		var reviewErrs []error
		var clean, reviewed bool
		reviewCtx, endPass := e.trackTimers(prCtx, r.Name, n)
		reviewErrs, clean, settled, reviewed = e.review(reviewCtx, r, pr, opts, w)
		endPass(reviewed)
		errs = append(errs, reviewErrs...)
		failed = failed || !clean
	}
	// Only a settled PR is remembered: a draft, or a head whose review is
	// done. One still being reviewed is evaluated again.
	e.skips.record(r.Name, n, plan, !failed && settled)
	return errs, res
}

// trackTimers is the context for one PR's review pass, and the call that ends
// it. A reviewer that is still running asks for a deadline timer through the
// context, and the pass asks again each time it finds the reviewer still
// running. A pass that collected, dropped or replaced a reviewer asks for
// nothing, so the timers it did not ask for belong to reviewers that are gone:
// endPass(reviewed) cancels them. A pass that did not review (held,
// quota-stopped, or failed) cancels none, since its reviewers may still be
// running.
func (e *prEngine) trackTimers(ctx context.Context, repo string, pr int) (context.Context, func(reviewed bool)) {
	if e.revisit == nil {
		return ctx, func(bool) {}
	}
	var mu sync.Mutex
	armed := map[string]bool{}
	ctx = pipeline.WithRevisit(ctx, func(repo string, pr int, label string, at time.Time) {
		mu.Lock()
		armed[label] = true
		mu.Unlock()
		e.revisit(repo, pr, label, at)
	})
	return ctx, func(reviewed bool) {
		if !reviewed {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for _, id := range e.q.TimerIDs(repo, pr) {
			if !armed[id] {
				e.q.CancelTimer(repo, pr, id)
			}
		}
	}
}

// review is a PR's review and settle. A head whose review is done is settled
// first, then its evidence is checked; any other is reviewed, then settled.
// handled reports the first. reviewed is false when the review itself failed
// or did not run. A head whose only outcome was a held refusal, a quota stop or
// a pending pass is not settled, whatever its marker says: the next evaluation
// must retry once the hold clears, the pool resets or the reviewer finishes.
func (e *prEngine) review(ctx context.Context, r config.Repo, pr github.Pull, o pipeline.Options, w daemon.Work) (errs []error, clean, handled, reviewed bool) {
	ph := daemon.PhasesOf(ctx)
	ph.Enter(daemon.PhaseReview)
	// An unreadable or poll-skipped marker is not done: the PR is reviewed.
	if done, err := e.gh.ReviewClaimed(ctx, r.Name, pr.Number, pr.Head.SHA+"-done"); err == nil && done {
		handled = true
		ph.Enter(daemon.PhaseSettle)
		errs = settlePR(ctx, e.s, r, pr.Number, o)
		ph.Enter(daemon.PhaseReview)
	} else {
		// A PR that never had to wait for a slot has not been looked up yet.
		priority, ok := w.Detail.(pipeline.ReviewPriority)
		switch {
		case w.Expired:
			priority = pipeline.ReviewPriority{Source: "timeout"}
		case !w.Known || !ok:
			priority = e.s.ReviewPriority(ctx, r, pr)
		}
		if err := e.s.AuditReviewStart(r, pr, priority, e.o, w.Over); err != nil {
			errs = append(errs, err)
		}
	}
	e.mu.Lock()
	c := e.counts(r.Name)
	c.reviewing++
	c.peak = max(c.peak, c.reviewing)
	e.mu.Unlock()
	reviewErrs, ok, reviewed, unreviewed := reviewPR(ctx, e.s, r, pr, o, !handled)
	e.mu.Lock()
	e.counts(r.Name).reviewing--
	e.mu.Unlock()
	return append(errs, reviewErrs...), ok && len(errs) == 0, handled && !unreviewed, reviewed
}

// sweep is one pass over repo r: it puts the open PRs on the queue and
// returns the errors the evaluations since the last sweep ended with. It does
// not wait for them.
func (e *prEngine) sweep(ctx context.Context, r config.Repo) error {
	setPRSweepStep(ctx, "open_pulls")
	e.q.BeginList(r.Name)
	e.openOf(r.Name).beginList()
	prs, err := e.gh.OpenPulls(ctx, r.Name, r.Branch)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var errs []error
	seen := map[int]bool{}
	heads := map[int]string{}
	for _, pr := range prs {
		seen[pr.Number] = true
		heads[pr.Number] = pr.Head.SHA
	}
	e.q.SetOpen(r.Name, heads)
	e.openOf(r.Name).replace(heads)
	prs = skipBotPulls(prs, e.s.Config.Daemon.PR.SkipBots)
	// PRs a fresh status read marked go first.
	prs = dirtyFirst(prs, prstatus.DirtyPRs(e.s.Config.StateDir, r.Name))
	setPRSweepStep(ctx, "feed")
	e.s.FeedPRs(ctx, r, prs, e.o)
	if err := ctx.Err(); err != nil {
		return err
	}
	keys := make([]daemon.PRKey, len(prs))
	for i, pr := range prs {
		keys[i] = daemon.PRKey{Repo: r.Name, PR: pr.Number}
	}
	e.q.Enqueue(daemon.SourceSweep, keys...)
	e.skips.forget(r.Name, seen)
	errs = append(errs, e.takeErrors(r.Name)...)
	return errors.Join(errs...)
}

// openSet is the open PRs of one repo for the closed-row reconcile. It is the
// queue's set, kept here because the queue offers no snapshot: the sweep replaces
// it, and each evaluation adds or drops its PR. A PR learned while a sweep's list
// was being read stays in the set the list replaces, so a PR that opened after
// the list was read is never taken for a closed one.
type openSet struct {
	mu     sync.Mutex
	swept  bool
	open   map[int]bool
	recent map[int]bool
	// gone is the PRs an evaluation saw closed since beginList: a list read
	// before they closed must not bring them back.
	gone map[int]bool
}

func (e *prEngine) openOf(repo string) *openSet {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.open[repo] == nil {
		e.open[repo] = &openSet{open: map[int]bool{}, recent: map[int]bool{}, gone: map[int]bool{}}
	}
	return e.open[repo]
}

// beginList starts a sweep's list read: PRs learned from now on survive replace,
// and PRs seen closed from now on stay out of it.
func (o *openSet) beginList() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recent, o.gone = map[int]bool{}, map[int]bool{}
}

func (o *openSet) learn(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.open[n], o.recent[n] = true, true
	delete(o.gone, n)
}

func (o *openSet) forget(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.open, n)
	delete(o.recent, n)
	o.gone[n] = true
}

// replace sets the open PRs to a listed set, less those seen closed since
// beginList, plus those learned since.
func (o *openSet) replace(heads map[int]string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	next := map[int]bool{}
	for n := range heads {
		if !o.gone[n] {
			next[n] = true
		}
	}
	for n := range o.recent {
		next[n] = true
	}
	o.open, o.swept = next, true
}

// openPRs is the open set the closed-row reconcile needs. isOpen reads the
// engine's set when it is called, once per row, so a PR an evaluation learns
// after the reconcile began is still open to it: a row is written only after
// its PR is learned. The reconcile makes no GitHub request or ledger read for
// it. known is false until repo's first sweep.
func (e *prEngine) openPRs(repo string) (isOpen func(pr int) bool, known bool) {
	o := e.openOf(repo)
	o.mu.Lock()
	swept := o.swept
	o.mu.Unlock()
	if !swept {
		return nil, false
	}
	return func(pr int) bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.open[pr]
	}, true
}

// sweepBounded keeps one repository's in-flight sweep registered until the
// worker really returns, even if its deadline lets this pass move on.
func (e *prEngine) sweepBounded(ctx context.Context, r config.Repo) error {
	e.sweepMu.Lock()
	if state := e.sweeping[r.Name]; state != nil {
		age := time.Since(state.started)
		step := state.currentStep()
		e.sweepMu.Unlock()
		if e.log != nil {
			e.log.Warn("pr sweep still running", "repo", r.Name, "step", step, "age", age.String())
		}
		return nil
	}
	state := &prSweepState{started: time.Now(), step: "open_pulls"}
	e.sweeping[r.Name] = state
	e.sweepMu.Unlock()

	sweepCtx, cancel := context.WithTimeout(ctx, prSweepTimeout)
	done := make(chan error, 1)
	go func() {
		err := e.sweep(context.WithValue(sweepCtx, prSweepStateKey{}, state), r)
		e.sweepMu.Lock()
		if e.sweeping[r.Name] == state {
			delete(e.sweeping, r.Name)
		}
		e.sweepMu.Unlock()
		done <- err
		cancel()
	}()

	select {
	case err := <-done:
		return err
	case <-sweepCtx.Done():
		select {
		case err := <-done:
			return err
		default:
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("pr sweep deadline exceeded during %s: %w", state.currentStep(), context.DeadlineExceeded)
	}
}

func (e *prEngine) takeErrors(repo string) []error {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.errs[repo]
	delete(e.errs, repo)
	return out
}

// passLine logs what the repo's evaluations did since the last line, and the
// queue as it is, and starts the next window. The role calls it after each
// sweep.
func (e *prEngine) passLine(repo string) {
	st := e.q.Take(repo)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.taken[repo] = st
	c := e.counts(repo)
	overdue := pipeline.TakeMergeOverdue(repo)
	// The next window starts with the reviews still running.
	defer func() { *c = passCounts{reasons: map[string]int{}, reviewing: c.reviewing, peak: c.reviewing} }()
	if e.skips == nil || e.skips.Log == nil {
		return
	}
	slices.Sort(c.evaluatedPRs)
	e.skips.Log.Info("pr pass", "repo", repo, "prs_skipped", c.skipped, "prs_evaluated", c.evaluated, "evaluated_reasons", c.reasons, "evaluated", c.evaluatedPRs[:min(len(c.evaluatedPRs), 30)], "reviews_concurrent_max", c.peak, "queue_depth", st.Depth, "queue_wait_max", st.WaitMax.String(), "merge_overdue", overdue)
}

// queueTotals is what the sweeps of all repos last saw of the queue, and the
// requests the evaluations since the last call made, by route.
func (e *prEngine) queueTotals() (depth int, waitMax time.Duration, evalRequests int, routes []github.RequestCount) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.taken {
		depth += st.Depth
		waitMax = max(waitMax, st.WaitMax)
	}
	evalRequests, e.evalRequests = e.evalRequests, 0
	routes, e.evalCounts = e.evalCounts.Snapshot(), github.NewRequestCounts("pr")
	return depth, waitMax, evalRequests, routes
}
