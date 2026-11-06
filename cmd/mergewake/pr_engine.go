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
// it for each PR. It never waits for another PR. Lands stay one at a time per
// repo, under the repo's settling lock: each must see the base the previous
// land left.
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
	// revisit is set when the reviewer watch can see reviewers finish: the
	// pr role then starts reviewers without waiting for them.
	revisit pipeline.Revisit

	mu       sync.Mutex
	settling map[string]*sync.Mutex
	// since is what happened per repo after the last pass line.
	since map[string]*passCounts
	errs  map[string][]error
	// taken is the queue state each repo's last sweep saw.
	taken        map[string]daemon.Stats
	evalRequests int
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
		settling: map[string]*sync.Mutex{}, since: map[string]*passCounts{}, errs: map[string][]error{}, taken: map[string]daemon.Stats{}}
	for _, r := range repos {
		e.repos[r.Name] = r
	}
	// Different PRs of a repo run at once, at most review_parallel when it is
	// set, else all of them; a priority-1 PR also starts past the bound, up to
	// urgentExtra more at once.
	e.q = &daemon.Queue{Cap: s.Config.Daemon.PR.Parallel(), Extra: urgentExtra, Run: e.run, Lookup: e.lookup, Grace: reviewPriorityGrace, Log: log}
	// The reviewer watch reads hk job records. A sink that cannot read them
	// never reports a finished reviewer, so the passes keep a bounded wait.
	if _, ok := s.Sink.(integrations.JobReader); ok {
		e.revisit = func(repo string, pr int, label string, at time.Time) {
			e.q.EnqueueAt(daemon.SourceTimer, repo, pr, label, at)
		}
	}
	return e
}

// urgentExtra is how many priority-1 evaluations of one repo may run past an
// explicit review_parallel at once.
const urgentExtra = 2

// reviewPriorityGrace is how long a choice between waiting PRs waits for their
// priority lookups. A lookup still out then is dropped and its PR is ordered
// as one without a priority, so a slow hk delays no other PR beyond this. A
// var so tests can shorten it.
var reviewPriorityGrace = 2 * time.Second

func (e *prEngine) repoLock(repo string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.settling[repo] == nil {
		e.settling[repo] = &sync.Mutex{}
	}
	return e.settling[repo]
}

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
	ctx, transaction := tracing.Pass(ctx, "pr")
	defer tracing.Finish(transaction)
	ctx, requests := github.ContinueRequests(ctx, "pr")
	ctx = pipeline.WithMergeHook(ctx, e.onMerge)
	if e.revisit != nil {
		ctx = pipeline.WithRevisit(ctx, e.revisit)
	}
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

// evaluate is the single-PR path: read the PR, then observe it, review it and
// settle it as one PR of the old pass was.
func (e *prEngine) evaluate(ctx context.Context, r config.Repo, w daemon.Work) (errs []error, res evalResult) {
	n := w.Key.PR
	// An event says GitHub changed: read it past the poll windows and caches.
	readCtx := ctx
	if slices.ContainsFunc(w.Sources, func(s daemon.Source) bool { return s != daemon.SourceSweep }) {
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
		res.gone = true
		return nil, res
	}
	e.q.Learn(r.Name, n, pr.Head.SHA)
	// A policy that lists dependency bots keeps their PRs out of the drop and
	// out of review: they are settled by Land's bot-patch gate alone. An
	// unreadable policy lists none, so the bots stay dropped as before.
	var listed []string
	if pr.User.Type == "Bot" {
		var err error
		if listed, err = e.s.BotUpdateAuthors(ctx, r); err != nil {
			errs = append(errs, fmt.Errorf("bot_updates policy: %w", err))
		}
	}
	if len(skipBotPulls([]github.Pull{pr}, e.s.Config.Daemon.PR.SkipBots, listed...)) == 0 {
		res.gone = true
		return errs, res
	}
	prCtx, prSpan := tracing.Span(ctx, "mergewake.pr")
	tracing.Data(prSpan, "pr", n)
	defer tracing.Finish(prSpan)
	// A PR whose ETags, head, base and webhooks are as its last clean
	// evaluation left them keeps that verdict: nothing below touches it.
	var plan skipPlan
	if e.skips != nil && !isListedBot(pr, listed) {
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
	case isListedBot(pr, listed):
		errs = append(errs, botLand(prCtx, e.s, r, n, opts)...)
	default:
		var reviewErrs []error
		var clean bool
		reviewErrs, clean, settled = e.review(prCtx, r, pr, opts, w)
		errs = append(errs, reviewErrs...)
		failed = failed || !clean
	}
	// Only a settled PR is remembered: a draft, or a head whose review is
	// done. One still being reviewed is evaluated again.
	e.skips.record(r.Name, n, plan, !failed && settled)
	return errs, res
}

// review is a PR's review and settle. A head whose review is done is settled
// first, then its evidence is checked; any other is reviewed, then settled.
// handled reports the first.
func (e *prEngine) review(ctx context.Context, r config.Repo, pr github.Pull, o pipeline.Options, w daemon.Work) (errs []error, clean, handled bool) {
	lock := e.repoLock(r.Name)
	// An unreadable or poll-skipped marker is not done: the PR is reviewed.
	if done, err := e.gh.ReviewClaimed(ctx, r.Name, pr.Number, pr.Head.SHA+"-done"); err == nil && done {
		handled = true
		lock.Lock()
		errs = settlePR(ctx, e.s, r, pr.Number, o)
		lock.Unlock()
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
	reviewErrs, ok := reviewPR(ctx, e.s, r, pr, o, !handled, lock)
	e.mu.Lock()
	e.counts(r.Name).reviewing--
	e.mu.Unlock()
	return append(errs, reviewErrs...), ok && len(errs) == 0, handled
}

// sweep is one pass over repo r: it puts the open PRs on the queue and
// returns the errors the evaluations since the last sweep ended with. It does
// not wait for them.
func (e *prEngine) sweep(ctx context.Context, r config.Repo) error {
	prs, err := e.gh.OpenPulls(ctx, r.Name, r.Branch)
	if err != nil {
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
	var listed []string
	if hasBotPull(prs) {
		var err error
		if listed, err = e.s.BotUpdateAuthors(ctx, r); err != nil {
			errs = append(errs, fmt.Errorf("bot_updates policy: %w", err))
		}
	}
	prs = skipBotPulls(prs, e.s.Config.Daemon.PR.SkipBots, listed...)
	// PRs a fresh status read marked go first.
	prs = dirtyFirst(prs, prstatus.DirtyPRs(e.s.Config.StateDir, r.Name))
	e.s.FeedPRs(ctx, r, prs, e.o)
	keys := make([]daemon.PRKey, len(prs))
	for i, pr := range prs {
		keys[i] = daemon.PRKey{Repo: r.Name, PR: pr.Number}
	}
	e.q.Enqueue(daemon.SourceSweep, keys...)
	e.skips.forget(r.Name, seen)
	errs = append(errs, e.s.ReconcileOutcomes(ctx, r, seen, e.o))
	errs = append(errs, e.takeErrors(r.Name)...)
	return errors.Join(errs...)
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
	// The next window starts with the reviews still running.
	defer func() { *c = passCounts{reasons: map[string]int{}, reviewing: c.reviewing, peak: c.reviewing} }()
	if e.skips == nil || e.skips.Log == nil {
		return
	}
	slices.Sort(c.evaluatedPRs)
	e.skips.Log.Info("pr pass", "repo", repo, "prs_skipped", c.skipped, "prs_evaluated", c.evaluated, "evaluated_reasons", c.reasons, "evaluated", c.evaluatedPRs[:min(len(c.evaluatedPRs), 30)], "reviews_concurrent_max", c.peak, "queue_depth", st.Depth, "queue_wait_max", st.WaitMax.String())
}

// queueTotals is what the sweeps of all repos last saw of the queue, and the
// requests the evaluations since the last call made.
func (e *prEngine) queueTotals() (depth int, waitMax time.Duration, evalRequests int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.taken {
		depth += st.Depth
		waitMax = max(waitMax, st.WaitMax)
	}
	evalRequests, e.evalRequests = e.evalRequests, 0
	return depth, waitMax, evalRequests
}
