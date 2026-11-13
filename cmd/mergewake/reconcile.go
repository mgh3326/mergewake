package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// reconcileTimeout bounds one repository's closed-row reconcile run; the retry
// wait is how soon a run that had no open set to work from is tried again.
// Vars so tests can shorten them.
var (
	reconcileTimeout   = 10 * time.Minute
	reconcileRetryWait = 5 * time.Second
)

// runReconcileLoop is the closed-row outcome reconcile's own loop. It revisits
// the closed PRs whose outcome rows are due (each at most hourly, see
// pipeline.ReconcileOutcomes) every interval, one run per repository at once,
// and returns when ctx ends. It runs beside the pr role, never inside its sweep
// or an evaluation, so a slow run delays neither. Publishing the outcomes it
// records is the outcome publisher's job.
func runReconcileLoop(ctx context.Context, s *pipeline.Service, openPRs func(repo string) (map[int]bool, bool), repos []config.Repo, interval time.Duration, o pipeline.Options, log *slog.Logger) {
	for ctx.Err() == nil {
		wait := interval
		_ = forRepos(ctx, repos, true, func(ctx context.Context, r config.Repo) error {
			if ran, _ := reconcileClosed(ctx, s, openPRs, r, o, log); !ran {
				wait = min(wait, reconcileRetryWait)
			}
			return nil
		})
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// reconcileClosed is one bounded run of the reconcile for r. ran is false when
// the run had to wait: before r's first sweep no row can be told from an open
// PR. A run logs one line, with its error.
func reconcileClosed(ctx context.Context, s *pipeline.Service, openPRs func(repo string) (map[int]bool, bool), r config.Repo, o pipeline.Options, log *slog.Logger) (ran bool, err error) {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	started := time.Now()
	open, known := openPRs(r.Name)
	if !known {
		log.Info("outcome reconcile run", "repo", r.Name, "skipped", "no open set yet")
		return false, nil
	}
	err = s.ReconcileOutcomes(ctx, r, open, o)
	attrs := []any{"repo", r.Name, "elapsed_ms", time.Since(started).Milliseconds(), "open_prs", len(open), "timed_out", errors.Is(ctx.Err(), context.DeadlineExceeded)}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	log.Info("outcome reconcile run", attrs...)
	return true, err
}
