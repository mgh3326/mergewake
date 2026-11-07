package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
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
	var repos []config.Repo
	for _, r := range s.Config.Repos {
		if repo == "" || r.Name == repo {
			repos = append(repos, r)
		}
	}
	// The pr role evaluates one PR per event through a queue; its loop is the
	// sweep that puts every open PR on it. The queue exists only while the role
	// is enabled, so no event evaluates a PR for a daemon that does not run it.
	var engine *prEngine
	if s.Config.Daemon.PR.Loop.Enabled {
		engine = newPREngine(s, gh, o, repos, skips, log, func(string, int) { d.Trigger("reap") })
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
			defer func() {
				requests = counts.Snapshot()
				total := 0
				for _, request := range requests {
					total += request.Count
				}
				tracing.Data(transaction, "github_requests", total)
				cache := counts.CacheStats()
				state := gh.ChangeCounts()
				reviews := gh.HKReviewStats()
				// The sweep's own requests are the ones above; the evaluations it
				// queued run on their own and report what they cost here.
				var queue []any
				if role == "pr" {
					depth, wait, evaluated := engine.queueTotals()
					queue = []any{"queue_depth", depth, "queue_wait_max", wait.String(), "eval_requests", evaluated}
				}
				log.Info("github requests", append(queue, "hk_state_writes", state.Writes, "hk_state_stale", state.Stale, "hk_state_rejected", state.Rejected, "hk_state_unavailable", state.Unavailable, "hk_state_merged_skipped", state.Skipped, "role", role, "total", total, "cache_hits", cache.Hits, "cache_misses", cache.Misses, "memo_hits", cache.MemoHits, "not_modified", cache.NotModified, "by_credential", counts.ByCredential(), "rate_remaining", gh.RateRemaining(), "gate_wait_ms", counts.GateWaitMax().Milliseconds(), "hk_review_hits", reviews.Hits, "hk_review_misses", reviews.Misses, "hk_review_mismatches", reviews.Mismatches, "routes", requests[:min(len(requests), 10)])...)
			}()
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
						err = engine.sweep(ctx, r)
						engine.passLine(r.Name)
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
		known := map[string]bool{}
		for _, r := range s.Config.Repos {
			known[r.Name] = true
		}
		secret := os.Getenv(wc.SecretEnv)
		if secret == "" {
			return errors.New("daemon.webhook.secret_env is unset or empty")
		}
		state := webhookstate.Open(s.Config.StateDir)
		state.Log = log
		gh.Webhook = state
		status := prstatus.Handler{StateDir: s.Config.StateDir, Fresh: gh.PRFresh, Invalidate: gh.InvalidateEvidence,
			Wake: func() { d.Trigger("pr") }}
		d.Webhook = &daemon.Webhook{Listen: wc.Listen, Path: wc.Path, Secret: []byte(secret), Repos: known, Invalidate: gh.InvalidateEvidence, Dirty: dirty, State: state, Applied: gh.WebhookDelivery}
		if engine != nil {
			branches := map[string]string{}
			for _, r := range repos {
				branches[r.Name] = r.Branch
			}
			d.Webhook.Queue, d.Webhook.Branches = engine.q, branches
			status.Enqueue = func(repo string, n int) { engine.q.EnqueueNamed(daemon.SourceStatus, repo, n, "") }
		}
		d.Webhook.Status = status
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
	if engine != nil {
		d.Reviewers.Enqueue = func(repo string, pr int) { engine.q.EnqueueNamed(daemon.SourceReviewer, repo, pr, "") }
	}
	if url := os.Getenv("MERGEWAKE_HEALTHCHECK_URL"); url != "" {
		d.Healthy = healthcheck(url)
	}
	// The queue runs until the daemon stops, and the daemon waits for the
	// evaluations in flight, as a pass used to wait for its reviews.
	if engine != nil {
		queueCtx, stopQueue := context.WithCancel(ctx)
		engine.q.Start(queueCtx)
		defer func() {
			stopQueue()
			engine.q.Wait()
		}()
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

// reviewPR runs one PR's review, then settles it under settling unless the
// caller already settled its reviewed head. A review the retry reran there is
// settled by the next evaluation of the PR.
func reviewPR(ctx context.Context, s *pipeline.Service, r config.Repo, pr github.Pull, o pipeline.Options, settle bool, settling *sync.Mutex) (errs []error, clean, reviewed, unreviewed bool) {
	clean = true
	reviewCtx, reviewSpan := tracing.Span(ctx, "mergewake.review")
	e := s.RetryReview(reviewCtx, r, pr, o)
	tracing.Finish(reviewSpan)
	// A review that failed for any reason but an unavailable reviewer may not
	// have reached its reviewers. A held or quota-stopped review produced no
	// result: it is no failure, it counts as no review, and the head must not
	// be settled.
	unreviewed = errors.Is(e, pipeline.ErrReviewHeld) || errors.Is(e, pipeline.ErrReviewQuotaStopped)
	reviewed = e == nil || errors.Is(e, pipeline.ErrReviewUnavailable)
	if e != nil {
		switch {
		case unreviewed:
			// No failure and nothing to settle: the reviewer-held or
			// review-quota-stopped audit row already records it.
		case errors.Is(e, pipeline.ErrReviewUnavailable):
			clean = false
			if printErr := s.Print(map[string]any{"action": "review-unavailable", "repo": r.Name, "pr": pr.Number, "reason": e.Error()}); printErr != nil {
				errs = append(errs, printErr)
			}
		default:
			clean = false
			errs = append(errs, fmt.Errorf("PR %d review: %w", pr.Number, e))
		}
	}
	// A held or quota-stopped review posts nothing, so it must not settle
	// either: running DeliverFindings, NotifyPR or Land for it would report and
	// act on a review that did not happen.
	if !settle || unreviewed {
		return errs, clean, reviewed, unreviewed
	}
	ph := daemon.PhasesOf(ctx)
	ph.Enter(daemon.PhaseLockWait)
	settling.Lock()
	defer settling.Unlock()
	ph.Enter(daemon.PhaseSettle)
	settleErrs := settlePR(ctx, s, r, pr.Number, o)
	return append(errs, settleErrs...), clean && len(settleErrs) == 0, reviewed, unreviewed
}

// settlePR delivers findings to the author lane before the verdict, so the
// author fixes them without the desk relaying anything, then notifies the
// verdict and lands the PR.
func settlePR(ctx context.Context, s *pipeline.Service, r config.Repo, n int, o pipeline.Options) []error {
	ctx, span := tracing.Span(ctx, "mergewake.settle")
	defer tracing.Finish(span)
	var errs []error
	// FINDINGS and READY for this PR share one author-pane scan.
	ctx = pipeline.WithAuthorScan(ctx)
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
	daemon.PhasesOf(ctx).Enter(daemon.PhaseSettle)
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
