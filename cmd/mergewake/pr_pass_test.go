package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// pollPRs is one sweep of r that waits for every evaluation it queued, the
// shape of the pass the pr role used to be: it returns the errors of both.
func pollPRs(ctx context.Context, s *pipeline.Service, gh *github.Client, r config.Repo, o pipeline.Options) error {
	return pollPRsSkipping(ctx, s, gh, r, o, nil)
}

func pollPRsSkipping(ctx context.Context, s *pipeline.Service, gh *github.Client, r config.Repo, o pipeline.Options, skips *prSkips) error {
	var log *slog.Logger
	if skips != nil {
		log = skips.Log
	}
	e := newPREngine(s, gh, o, []config.Repo{r}, skips, log, nil)
	e.q.Start(ctx)
	err := e.sweep(ctx, r)
	e.q.Wait()
	e.passLine(r.Name)
	return errors.Join(err, errors.Join(e.takeErrors(r.Name)...), ctx.Err())
}
