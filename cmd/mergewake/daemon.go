package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/crashreport"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/github/webhookstate"
	"github.com/mgh3326/mergewake/internal/pipeline"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/prstatus"
	"github.com/mgh3326/mergewake/internal/tracing"
)

type lockedWriter struct {
	mu  sync.Mutex
	out io.Writer
}

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.out.Write(b)
}

// runDaemon runs the daemon with crash output armed: a fatal panic in any of
// its goroutines reaches Sentry on the next start (see crashreport.Run).
func runDaemon(ctx context.Context, s *pipeline.Service, gh *github.Client, repo string, o pipeline.Options, stderr io.Writer) error {
	ctx, stop := crashreport.StartContext(ctx, s.Config.StateDir, "mergewake", version, stderr)
	err := runDaemonLoops(ctx, s, gh, repo, o, stderr)
	stop()
	return err
}

func runDaemonLoops(ctx context.Context, s *pipeline.Service, gh *github.Client, repo string, o pipeline.Options, stderr io.Writer) error {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	loops := []daemon.Loop{}
	// d is assigned below, before any loop runs; a pr pass that merges a PR
	// wakes the reap role through it.
	var d daemon.Daemon
	// Skipping unchanged PRs needs the webhook receiver: its per-PR marks are
	// what tells a quiet PR from one whose review thread was just resolved.
	var dirty *daemon.Dirty
	var skips *prSkips
	if s.Config.Daemon.Webhook.Enabled {
		dirty = &daemon.Dirty{}
		skips = &prSkips{Dirty: dirty, Log: log}
	}
	// Each role owns one goroutine. The pr role runs its repositories at once;
	// the other roles visit them in config order.
	for _, entry := range []struct {
		role string
		cfg  config.Loop
	}{{"pr", s.Config.Daemon.PR.Loop}, {"watch", s.Config.Daemon.Watch}, {"release", s.Config.Daemon.Release}, {"rollout", s.Config.Daemon.Rollout},
		{"reap", config.Loop{Enabled: s.Config.Reap.Enabled, Interval: s.Config.PollInterval}}} {
		if !entry.cfg.Enabled {
			continue
		}
		interval, err := config.Duration(entry.cfg.Interval)
		if err != nil {
			return err
		}
		role := entry.role
		var requests []github.RequestCount
		loops = append(loops, daemon.Loop{Role: role, Interval: interval, Requests: func() any { return requests }, Run: func(ctx context.Context) error {
			ctx, transaction := tracing.Pass(ctx, role)
			defer tracing.Finish(transaction)
			ctx, counts := github.CountRequests(ctx, role)
			if role == "pr" {
				ctx = pipeline.WithMergeHook(ctx, func(string, int) { d.Trigger("reap") })
			}
			defer func() {
				requests = counts.Snapshot()
				total := 0
				for _, request := range requests {
					total += request.Count
				}
				tracing.Data(transaction, "github_requests", total)
				cache := counts.CacheStats()
				hk := gh.HKReviewStats()
				log.Info("github requests", "role", role, "total", total, "cache_hits", cache.Hits, "cache_misses", cache.Misses, "memo_hits", cache.MemoHits, "not_modified", cache.NotModified, "by_credential", counts.ByCredential(), "rate_remaining", gh.RateRemaining(), "hk_review_hits", hk.Hits, "hk_review_misses", hk.Misses, "hk_review_mismatches", hk.Mismatches, "routes", requests[:min(len(requests), 10)])
			}()
			var repos []config.Repo
			for _, r := range s.Config.Repos {
				if repo == "" || r.Name == repo {
					repos = append(repos, r)
				}
			}
			// The pr role runs every repository at once: a pass is as long as
			// its slowest repository, not their sum, so no repository starves
			// behind the others when the daemon restarts often.
			return forRepos(ctx, repos, role == "pr", func(ctx context.Context, r config.Repo) error {
				// Release and rollout work from the install inventory. A
				// repository without one has nothing to publish or install, so
				// the pass decides that from config and reads nothing.
				if (role == "release" || role == "rollout") && len(r.Install) == 0 {
					log.Debug("skip repository without install inventory", "role", role, "repo", r.Name)
					return nil
				}
				repoCtx, repoSpan := tracing.Span(ctx, "mergewake.repo")
				tracing.Tag(repoSpan, "repo", r.Name)
				var err error
				func() {
					defer tracing.Finish(repoSpan)
					ctx := repoCtx
					switch role {
					case "pr":
						err = pollPRsSkipping(ctx, s, gh, r, o, skips)
					case "watch":
						checkCtx, span := tracing.Span(ctx, "mergewake.watch")
						err = s.Watch(checkCtx, r, o)
						tracing.Finish(span)
					case "release":
						head, e := gh.Head(ctx, r.Name, r.Branch)
						if e != nil {
							err = e
							break
						}
						p, e := s.Policy(ctx, r, head)
						if e != nil {
							err = e
							break
						}
						ci, e := gh.Check(ctx, r.Name, head, p.RequiredChecks)
						if e != nil {
							err = e
							break
						}
						if ci == github.Green {
							err = s.Release(ctx, r, head, o)
						}
					case "rollout":
						err = s.Rollout(ctx, r, o)
					case "reap":
						sweepCtx, span := tracing.Span(ctx, "mergewake.reap")
						err = s.Reap(sweepCtx, r, 0, o)
						tracing.Finish(span)
					}
				}()
				return err
			})
		}})
	}
	if len(s.Config.Repos) == 0 {
		return errors.New("daemon requires configured repositories")
	}
	d = daemon.Daemon{StateDir: s.Config.StateDir, Host: s.Config.Host, Log: log, RateWait: gh.RateWait}
	if s.Config.Discord.Enabled {
		s.Feed = pipeline.NewFeed(s.Config, log)
		s.Feed.Now = s.Now
		feedCtx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Feed.Run(feedCtx)
		}()
		defer func() {
			stop()
			<-done
		}()
	}
	if wc := s.Config.Daemon.Webhook; wc.Enabled {
		repos := map[string]bool{}
		for _, r := range s.Config.Repos {
			repos[r.Name] = true
		}
		secret := os.Getenv(wc.SecretEnv)
		if secret == "" {
			return errors.New("daemon.webhook.secret_env is unset or empty")
		}
		state := webhookstate.Open(s.Config.StateDir)
		state.Log = log
		gh.Webhook = state
		d.Webhook = &daemon.Webhook{Listen: wc.Listen, Path: wc.Path, Secret: []byte(secret), Repos: repos, Invalidate: gh.InvalidateEvidence, Dirty: dirty, State: state,
			Status: prstatus.Handler{StateDir: s.Config.StateDir, Fresh: gh.PRFresh, Invalidate: gh.InvalidateEvidence,
				Wake: func() { d.Trigger("pr") }}}
		gap, err := config.Duration(wc.MinGap)
		if err != nil {
			return err
		}
		d.WakeGap = gap
	}
	// A finished reviewer job wakes the pr role without waiting for the
	// webhook gap; the watch reads hk job records only.
	d.Reviewers = &daemon.ReviewerWatch{
		Dirty: dirty,
		Jobs: func() []daemon.WatchedJob {
			var jobs []daemon.WatchedJob
			for _, r := range s.Config.Repos {
				if repo == "" || r.Name == repo {
					jobs = append(jobs, s.RecordedReviewerJobs(r.Name)...)
				}
			}
			return jobs
		},
		Read:     s.ReviewerJobView,
		NotFound: pipeline.IsJobNotFound,
	}
	if url := os.Getenv("MERGEWAKE_HEALTHCHECK_URL"); url != "" {
		d.Healthy = healthcheck(url)
	}
	return d.Run(ctx, loops)
}

// forRepos runs fn for each repository and joins the errors in config order,
// each prefixed with its repository. A cancelled ctx starts no further
// repository. With parallel, every repository runs in its own goroutine and
// the call returns when all of them have.
func forRepos(ctx context.Context, repos []config.Repo, parallel bool, fn func(context.Context, config.Repo) error) error {
	errs := make([]error, len(repos))
	var running sync.WaitGroup
	for i, r := range repos {
		if ctx.Err() != nil {
			break
		}
		run := func() {
			if err := fn(ctx, r); err != nil {
				errs[i] = fmt.Errorf("%s: %w", r.Name, err)
			}
		}
		if !parallel {
			run()
			continue
		}
		running.Add(1)
		go func() {
			defer running.Done()
			run()
		}()
	}
	running.Wait()
	return errors.Join(errs...)
}

// healthcheck returns the fail-open ping run after a clean loop. The request
// is a bare GET with a bounded timeout; errors never embed the URL, which is a
// secret (forge-able ping endpoint) that must not reach the daemon logs.
func healthcheck(endpoint string) func(context.Context) error {
	client := &http.Client{Timeout: 5 * time.Second}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return errors.New("invalid healthcheck URL")
		}
		resp, err := client.Do(req)
		if err != nil {
			var ue *url.Error
			if errors.As(err, &ue) {
				return ue.Err
			}
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("healthcheck ping: HTTP %d", resp.StatusCode)
		}
		return nil
	}
}

// dirtyFirst moves the PRs a fresh status read marked for re-evaluation to the
// front, keeping the order inside each group, so any priority order the list
// already has still decides among them.
func dirtyFirst(prs []github.Pull, dirty map[int]bool) []github.Pull {
	if len(dirty) == 0 {
		return prs
	}
	out := make([]github.Pull, 0, len(prs))
	for _, pr := range prs {
		if dirty[pr.Number] {
			out = append(out, pr)
		}
	}
	for _, pr := range prs {
		if !dirty[pr.Number] {
			out = append(out, pr)
		}
	}
	return out
}

func pollPRs(ctx context.Context, s *pipeline.Service, gh *github.Client, r config.Repo, o pipeline.Options) error {
	return pollPRsSkipping(ctx, s, gh, r, o, nil)
}

// pollPRsSkipping is pollPRs that leaves PRs alone when skips finds them
// unchanged since their last clean evaluation; nil evaluates every PR.
func pollPRsSkipping(ctx context.Context, s *pipeline.Service, gh *github.Client, r config.Repo, o pipeline.Options, skips *prSkips) error {
	prs, err := gh.OpenPulls(ctx, r.Name, r.Branch)
	if err != nil {
		return err
	}
	var errs []error
	seen := map[int]bool{}
	for _, pr := range prs {
		seen[pr.Number] = true
	}
	// A policy that lists dependency bots keeps their PRs out of the drop and
	// out of review: they are settled by Land's bot-patch gate alone. An
	// unreadable policy lists none, so the bots stay dropped as before.
	var listed []string
	if hasBotPull(prs) {
		var err error
		if listed, err = s.BotUpdateAuthors(ctx, r); err != nil {
			errs = append(errs, fmt.Errorf("bot_updates policy: %w", err))
		}
	}
	prs = skipBotPulls(prs, s.Config.Daemon.PR.SkipBots, listed...)
	marked := prstatus.DirtyPRs(s.Config.StateDir, r.Name)
	prs = dirtyFirst(prs, marked)
	// Before the per-PR work: a review can hold this pass for minutes.
	s.FeedPRs(ctx, r, prs, o)
	// A review waits for its reviewer pane, so one slow review used to hold
	// the land decision of every PR behind it for the whole pass. PRs whose
	// head already carries the review done marker are delivered, notified and
	// landed first; reviews run after that.
	options := make([]pipeline.Options, len(prs))
	handled := make([]bool, len(prs))
	prContexts := make([]context.Context, len(prs))
	for i, pr := range prs {
		prCtx, span := tracing.Span(ctx, "mergewake.pr")
		tracing.Data(span, "pr", pr.Number)
		prContexts[i] = prCtx
		defer tracing.Finish(span)
	}
	// A PR whose ETags, head, base and webhooks are as its last clean
	// evaluation left them keeps that verdict: nothing below touches it.
	plans := make([]skipPlan, len(prs))
	if skips != nil {
		base, baseErr := gh.Head(ctx, r.Name, r.Branch)
		for i, pr := range prs {
			if !isListedBot(pr, listed) {
				plans[i] = skips.plan(ctx, gh, r.Name, pr.Number, base, baseErr)
				// A fresh status read that found the PR changed asked for a
				// full evaluation, whatever the validators say.
				if marked[pr.Number] && plans[i].skip {
					plans[i].skip, plans[i].reasons = false, []string{"status"}
				}
			}
		}
	}
	failed := make([]bool, len(prs))
	for i, pr := range prs {
		if plans[i].skip {
			continue
		}
		observeCtx, observeSpan := tracing.Span(prContexts[i], "mergewake.observe")
		observationErr := s.ObservePR(observeCtx, r, pr, o)
		tracing.Finish(observeSpan)
		options[i] = o
		options[i].Observed = observationErr == nil
		if e := observationErr; e != nil {
			errs = append(errs, fmt.Errorf("PR %d outcomes: %w", pr.Number, e))
			failed[i] = true
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if pr.Draft {
			continue
		}
		if isListedBot(pr, listed) {
			errs = append(errs, botLand(prContexts[i], s, r, pr.Number, options[i])...)
			continue
		}
		// An unreadable or poll-skipped marker is not done: the PR keeps
		// today's order below.
		if done, e := gh.ReviewClaimed(ctx, r.Name, pr.Number, pr.Head.SHA+"-done"); e != nil || !done {
			continue
		}
		handled[i] = true
		settleErrs := settlePR(prContexts[i], s, r, pr.Number, options[i])
		failed[i] = failed[i] || len(settleErrs) > 0
		errs = append(errs, settleErrs...)
	}
	// Reviews of different PRs run at once, at most review_parallel of them:
	// each waits for its reviewer pane. One PR's review and settle stay in
	// order. Settles run one at a time: they share the repo's delivery,
	// notify and ready ledgers, and each land must see the base the previous
	// land left, as in the sequential order. They start in hk priority order
	// (ties in list order); a priority-1 review also starts past the bound,
	// up to urgentExtra more at once.
	results := make([][]error, len(prs))
	slots := make(chan struct{}, s.Config.Daemon.PR.Parallel())
	extra := make(chan struct{}, urgentExtra)
	var settling sync.Mutex
	var running sync.WaitGroup
	reviewable := make([]int, 0, len(prs))
	for i, pr := range prs {
		if !pr.Draft && !isListedBot(pr, listed) && !plans[i].skip {
			reviewable = append(reviewable, i)
		}
	}
	priorities := reviewPriorities(ctx, s, r, prs, reviewable, handled)
	slices.SortStableFunc(reviewable, func(a, b int) int { return cmp.Compare(priorities[a].Rank(), priorities[b].Rank()) })
	for _, i := range reviewable {
		pr := prs[i]
		// A cancelled pass starts no further PR and returns once the running
		// ones have. A free review slot is taken first; a priority-1 review
		// with none free takes an extra one rather than wait.
		release := slots
		var urgent chan struct{}
		if priorities[i].Urgent() {
			urgent = extra
		}
		select {
		case slots <- struct{}{}:
		default:
			select {
			case slots <- struct{}{}:
			case urgent <- struct{}{}:
				release = extra
			case <-ctx.Done():
			}
		}
		if ctx.Err() != nil {
			running.Wait()
			return ctx.Err()
		}
		if !handled[i] {
			if e := s.AuditReviewStart(r, pr, priorities[i], o, release == extra); e != nil {
				errs = append(errs, e)
			}
		}
		running.Add(1)
		go func() {
			defer running.Done()
			defer func() { <-release }()
			var clean bool
			results[i], clean = reviewPR(prContexts[i], s, r, pr, options[i], !handled[i], &settling)
			failed[i] = failed[i] || !clean
		}()
	}
	running.Wait()
	for _, e := range results {
		errs = append(errs, e...)
	}
	numbers := make([]int, len(prs))
	for i, pr := range prs {
		numbers[i] = pr.Number
		// Only a settled PR is remembered: a draft, or a head whose review is
		// done. One still being reviewed is evaluated every pass, as before.
		skips.record(r.Name, pr.Number, plans[i], !failed[i] && (pr.Draft || handled[i]))
	}
	skips.forget(r.Name, seen)
	skips.report(r.Name, numbers, plans)
	errs = append(errs, s.ReconcileOutcomes(ctx, r, seen, o))
	return errors.Join(errs...)
}

// urgentExtra is how many priority-1 reviews of one repo may run past
// review_parallel at once.
const urgentExtra = 2

// reviewPriorityGrace is how long a pass waits for the priority lookups before
// it starts reviews. A lookup still out then is dropped and its PR is ordered
// as one without a priority, so a slow hk delays no other PR beyond this. A
// var so tests can shorten it.
var reviewPriorityGrace = 2 * time.Second

// reviewPriorities resolves, at once and at most once per PR, the priority of
// every PR in reviewable whose head still needs a review, and waits for them
// for at most reviewPriorityGrace. Lookups never fail the pass: a PR without a
// priority keeps its place in list order.
func reviewPriorities(ctx context.Context, s *pipeline.Service, r config.Repo, prs []github.Pull, reviewable []int, handled []bool) []pipeline.ReviewPriority {
	out := make([]pipeline.ReviewPriority, len(prs))
	// Each lookup reports on its own buffered channel, so one that finishes
	// after the grace never writes into out.
	answers := map[int]chan pipeline.ReviewPriority{}
	for _, i := range reviewable {
		if handled[i] {
			continue
		}
		ch := make(chan pipeline.ReviewPriority, 1)
		answers[i] = ch
		go func() { ch <- s.ReviewPriority(ctx, r, prs[i]) }()
	}
	grace := time.NewTimer(reviewPriorityGrace)
	defer grace.Stop()
	expired := false
	for _, i := range reviewable {
		ch, ok := answers[i]
		if !ok {
			continue
		}
		out[i] = pipeline.ReviewPriority{Source: "timeout"}
		if expired {
			select {
			case out[i] = <-ch:
			default:
			}
			continue
		}
		select {
		case out[i] = <-ch:
		case <-grace.C:
			expired = true
			select {
			case out[i] = <-ch:
			default:
			}
		case <-ctx.Done():
			expired = true
		}
	}
	return out
}

// reviewPR runs one PR's review, then settles it under settling unless phase
// one already settled its reviewed head. A review the retry reran there is
// settled next pass.
func reviewPR(ctx context.Context, s *pipeline.Service, r config.Repo, pr github.Pull, o pipeline.Options, settle bool, settling *sync.Mutex) (errs []error, clean bool) {
	clean = true
	reviewCtx, reviewSpan := tracing.Span(ctx, "mergewake.review")
	e := s.RetryReview(reviewCtx, r, pr, o)
	tracing.Finish(reviewSpan)
	if e != nil {
		clean = false
		if errors.Is(e, pipeline.ErrReviewUnavailable) {
			if printErr := s.Print(map[string]any{"action": "review-unavailable", "repo": r.Name, "pr": pr.Number, "reason": e.Error()}); printErr != nil {
				errs = append(errs, printErr)
			}
		} else {
			errs = append(errs, fmt.Errorf("PR %d review: %w", pr.Number, e))
		}
	}
	if !settle {
		return errs, clean
	}
	settling.Lock()
	defer settling.Unlock()
	settleErrs := settlePR(ctx, s, r, pr.Number, o)
	return append(errs, settleErrs...), clean && len(settleErrs) == 0
}

// settlePR delivers findings to the author lane before the verdict, so the
// author fixes them without the desk relaying anything, then notifies the
// verdict and lands the PR.
func settlePR(ctx context.Context, s *pipeline.Service, r config.Repo, n int, o pipeline.Options) []error {
	ctx, span := tracing.Span(ctx, "mergewake.settle")
	defer tracing.Finish(span)
	var errs []error
	deliverCtx, deliver := tracing.Span(ctx, "mergewake.deliver")
	e := s.DeliverFindings(deliverCtx, r, n, o)
	tracing.Finish(deliver)
	if e != nil {
		errs = append(errs, fmt.Errorf("PR %d findings: %w", n, e))
	}
	notifyCtx, notify := tracing.Span(ctx, "mergewake.notify")
	e = s.NotifyPR(notifyCtx, r, n, o)
	tracing.Finish(notify)
	if e != nil {
		errs = append(errs, fmt.Errorf("PR %d notify: %w", n, e))
	}
	landCtx, land := tracing.Span(ctx, "mergewake.land")
	e = s.Land(landCtx, r, n, o)
	tracing.Finish(land)
	if e != nil {
		errs = append(errs, fmt.Errorf("PR %d land: %w", n, e))
	}
	return errs
}

// skipBotPulls drops PRs opened by bot accounts when skip is set, except those
// by a bot in listed; they stay in the open set for outcome reconciliation.
func skipBotPulls(prs []github.Pull, skip bool, listed ...string) []github.Pull {
	if !skip {
		return prs
	}
	out := make([]github.Pull, 0, len(prs))
	for _, pr := range prs {
		if pr.User.Type != "Bot" || isListedBot(pr, listed) {
			out = append(out, pr)
		}
	}
	return out
}

func hasBotPull(prs []github.Pull) bool {
	for _, pr := range prs {
		if pr.User.Type == "Bot" {
			return true
		}
	}
	return false
}

// isListedBot reports whether pr is by a bot the policy lists. It is the same
// test Land's gate selection applies.
func isListedBot(pr github.Pull, listed []string) bool {
	return (&policy.BotUpdates{Authors: listed}).Lists(pr.User.Login, pr.User.Type)
}

// botLand settles a listed bot's PR: Land decides it with the bot-patch gate,
// merging it or telling the desk why not. It delivers no findings, sends no
// verdict notice and starts no review.
func botLand(ctx context.Context, s *pipeline.Service, r config.Repo, n int, o pipeline.Options) []error {
	ctx, span := tracing.Span(ctx, "mergewake.settle")
	defer tracing.Finish(span)
	landCtx, land := tracing.Span(ctx, "mergewake.land")
	e := s.Land(landCtx, r, n, o)
	tracing.Finish(land)
	if e != nil {
		return []error{fmt.Errorf("PR %d land: %w", n, e)}
	}
	return nil
}
