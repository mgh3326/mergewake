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
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/pipeline"
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

func runDaemon(ctx context.Context, s *pipeline.Service, gh *github.Client, repo string, o pipeline.Options, stderr io.Writer) error {
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
		loops = append(loops, daemon.Loop{Role: role, Interval: interval, Run: func(ctx context.Context) error {
			var errs []error
			for _, r := range s.Config.Repos {
				if repo != "" && r.Name != repo {
					continue
				}
				if ctx.Err() != nil {
					break
				}
				var err error
				switch role {
				case "pr":
					err = pollPRs(ctx, s, gh, r, o)
				case "watch":
					err = s.Watch(ctx, r, o)
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
					err = s.Reap(ctx, r, 0, o)
				}
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
	d := daemon.Daemon{StateDir: s.Config.StateDir, Host: s.Config.Host, Log: slog.New(slog.NewJSONHandler(stderr, nil)), RateWait: gh.RateWait}
	if wc := s.Config.Daemon.Webhook; wc.Enabled {
		repos := map[string]bool{}
		for _, r := range s.Config.Repos {
			repos[r.Name] = true
		}
		secret := os.Getenv(wc.SecretEnv)
		if secret == "" {
			return errors.New("daemon.webhook.secret_env is unset or empty")
		}
		d.Webhook = &daemon.Webhook{Listen: wc.Listen, Path: wc.Path, Secret: []byte(secret), Repos: repos}
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
	prs = skipBotPulls(prs, s.Config.Daemon.PR.SkipBots)
	for _, pr := range prs {
		observationErr := s.ObservePR(ctx, r, pr, o)
		childOptions := o
		childOptions.Observed = observationErr == nil
		if e := observationErr; e != nil {
			errs = append(errs, fmt.Errorf("PR %d outcomes: %w", pr.Number, e))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if pr.Draft {
			continue
		}
		// A failed retained claim is not a retry signal; only the -done marker
		// means all required passes finished. Landing stays independent so
		// advisory review failures do not block routine policy decisions.
		claimed, e := gh.ReviewClaimed(ctx, r.Name, pr.Number, pr.Head.SHA+"-done")
		if errors.Is(e, github.ErrPollSkipped) {
			// The marker probe sits inside its poll interval with nothing
			// cached; every per-PR step rechecks next round.
			continue
		}
		if e == nil && !claimed {
			e = s.Review(ctx, r, pr.Number, "", childOptions)
		}
		if e != nil {
			if errors.Is(e, pipeline.ErrReviewUnavailable) {
				if printErr := s.Print(map[string]any{"action": "review-unavailable", "repo": r.Name, "pr": pr.Number, "reason": e.Error()}); printErr != nil {
					errs = append(errs, printErr)
				}
			} else {
				errs = append(errs, fmt.Errorf("PR %d review: %w", pr.Number, e))
			}
		}
		if e := s.NotifyPR(ctx, r, pr.Number, childOptions); e != nil {
			errs = append(errs, fmt.Errorf("PR %d notify: %w", pr.Number, e))
		}
		if e = s.Land(ctx, r, pr.Number, childOptions); e != nil {
			errs = append(errs, fmt.Errorf("PR %d land: %w", pr.Number, e))
		}
	}
	// The alarm is the pr role's own heartbeat: it fires when this loop is
	// alive yet has begun no review of a ready head, which a healthcheck ping
	// alone cannot tell from a healthy idle loop.
	errs = append(errs, s.CheckStalls(ctx, r, prs, o))
	errs = append(errs, s.ReconcileOutcomes(ctx, r, seen, o))
	return errors.Join(errs...)
}

// skipBotPulls drops PRs opened by bot accounts when skip is set; they stay in
// the open set for outcome reconciliation.
func skipBotPulls(prs []github.Pull, skip bool) []github.Pull {
	if !skip {
		return prs
	}
	out := make([]github.Pull, 0, len(prs))
	for _, pr := range prs {
		if pr.User.Type != "Bot" {
			out = append(out, pr)
		}
	}
	return out
}
