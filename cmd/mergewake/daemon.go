package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

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
	}{{"pr", s.Config.Daemon.PR}, {"watch", s.Config.Daemon.Watch}, {"release", s.Config.Daemon.Release}, {"rollout", s.Config.Daemon.Rollout}} {
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
	return d.Run(ctx, loops)
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
		if e = s.Land(ctx, r, pr.Number, childOptions); e != nil {
			errs = append(errs, fmt.Errorf("PR %d land: %w", pr.Number, e))
		}
	}
	errs = append(errs, s.ReconcileOutcomes(ctx, r, seen, o))
	return errors.Join(errs...)
}
