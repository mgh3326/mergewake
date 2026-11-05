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
	"github.com/mgh3326/mergewake/internal/pipeline"
	"github.com/mgh3326/mergewake/internal/policy"
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
	// Each role owns one goroutine; repositories within it run in config order.
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
				log.Info("github requests", "role", role, "total", total, "routes", requests[:min(len(requests), 10)])
			}()
			var errs []error
			for _, r := range s.Config.Repos {
				if repo != "" && r.Name != repo {
					continue
				}
				if ctx.Err() != nil {
					break
				}
				repoCtx, repoSpan := tracing.Span(ctx, "mergewake.repo")
				tracing.Tag(repoSpan, "repo", r.Name)
				var err error
				func() {
					defer tracing.Finish(repoSpan)
					ctx := repoCtx
					switch role {
					case "pr":
						err = pollPRs(ctx, s, gh, r, o)
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
				if err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", r.Name, err))
				}
			}
			return errors.Join(errs...)
		}})
	}
	if len(s.Config.Repos) == 0 {
		return errors.New("daemon requires configured repositories")
	}
	d := daemon.Daemon{StateDir: s.Config.StateDir, Host: s.Config.Host, Log: log, RateWait: gh.RateWait}
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
		d.Webhook = &daemon.Webhook{Listen: wc.Listen, Path: wc.Path, Secret: []byte(secret), Repos: repos, Invalidate: gh.InvalidateEvidence}
		gap, err := config.Duration(wc.MinGap)
		if err != nil {
			return err
		}
		d.WakeGap = gap
	}
	if url := os.Getenv("MERGEWAKE_HEALTHCHECK_URL"); url != "" {
		d.Healthy = healthcheck(url)
	}
	return d.Run(ctx, loops)
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
func pollPRs(ctx context.Context, s *pipeline.Service, gh *github.Client, r config.Repo, o pipeline.Options) error {
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
	for i, pr := range prs {
		observeCtx, observeSpan := tracing.Span(prContexts[i], "mergewake.observe")
		observationErr := s.ObservePR(observeCtx, r, pr, o)
		tracing.Finish(observeSpan)
		options[i] = o
		options[i].Observed = observationErr == nil
		if e := observationErr; e != nil {
			errs = append(errs, fmt.Errorf("PR %d outcomes: %w", pr.Number, e))
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
		errs = append(errs, settlePR(prContexts[i], s, r, pr.Number, options[i])...)
	}
	// Reviews of different PRs run at once, at most review_parallel of them:
	// each waits for its reviewer pane. One PR's review and settle stay in
	// order. Settles run one at a time: they share the repo's delivery,
	// notify and ready ledgers, and each land must see the base the previous
	// land left, as in the sequential order.
	results := make([][]error, len(prs))
	slots := make(chan struct{}, s.Config.Daemon.PR.Parallel())
	var settling sync.Mutex
	var running sync.WaitGroup
	for i, pr := range prs {
		if pr.Draft || isListedBot(pr, listed) {
			continue
		}
		// A cancelled pass starts no further PR and returns once the running
		// ones have.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			running.Wait()
			return ctx.Err()
		}
		running.Add(1)
		go func() {
			defer running.Done()
			defer func() { <-slots }()
			results[i] = reviewPR(prContexts[i], s, r, pr, options[i], !handled[i], &settling)
		}()
	}
	running.Wait()
	for _, e := range results {
		errs = append(errs, e...)
	}
	errs = append(errs, s.ReconcileOutcomes(ctx, r, seen, o))
	return errors.Join(errs...)
}

// reviewPR runs one PR's review, then settles it under settling unless phase
// one already settled its reviewed head. A review the retry reran there is
// settled next pass.
func reviewPR(ctx context.Context, s *pipeline.Service, r config.Repo, pr github.Pull, o pipeline.Options, settle bool, settling *sync.Mutex) []error {
	var errs []error
	reviewCtx, reviewSpan := tracing.Span(ctx, "mergewake.review")
	e := s.RetryReview(reviewCtx, r, pr, o)
	tracing.Finish(reviewSpan)
	if e != nil {
		if errors.Is(e, pipeline.ErrReviewUnavailable) {
			if printErr := s.Print(map[string]any{"action": "review-unavailable", "repo": r.Name, "pr": pr.Number, "reason": e.Error()}); printErr != nil {
				errs = append(errs, printErr)
			}
		} else {
			errs = append(errs, fmt.Errorf("PR %d review: %w", pr.Number, e))
		}
	}
	if !settle {
		return errs
	}
	settling.Lock()
	defer settling.Unlock()
	return append(errs, settlePR(ctx, s, r, pr.Number, o)...)
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
