package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// The unreconciled-row sync's bounds, vars so tests can shorten them.
var (
	// changeSyncScan is the page size requested from hk: the endpoint's
	// maximum, since it pages by limit only. One page is the window a run
	// rotates over.
	changeSyncScan = 1000
	// changeSyncRows bounds the PRs one run checks, so a large backlog cannot
	// make a run outlast its interval.
	changeSyncRows = 500
	// changeSyncTimeout bounds one run; a run that hits it leaves the rest of
	// its rows to the next one.
	changeSyncTimeout = 10 * time.Minute
)

// changeSync checks hk's unreconciled PR rows against GitHub and records the
// merged and closed ones back, so a PR merged outside mergewake's pass no
// longer holds its hk tasks open.
type changeSync struct {
	rows   func(context.Context, int) ([]integrations.UnreconciledChange, error)
	pull   func(context.Context, string, int) (github.Pull, error)
	record func(context.Context, string, github.Pull)
	repos  map[string]bool
	log    *slog.Logger
	// after is the id of the last row the previous run checked. The endpoint
	// takes no after_id, so a run starts there in the rows it reads and wraps
	// at the end: rows that stay open at the head cannot starve the rest.
	after int64
}

// newChangeSync builds the sync over the daemon's GitHub client and the repos
// this run may operate on.
func newChangeSync(gh *github.Client, repos []config.Repo, log *slog.Logger) *changeSync {
	known := make(map[string]bool, len(repos))
	for _, r := range repos {
		known[r.Name] = true
	}
	return &changeSync{
		rows: integrations.Handoffkeep{}.UnreconciledChanges,
		pull: gh.ReadPull,
		record: func(ctx context.Context, repo string, p github.Pull) {
			gh.RecordPull(ctx, repo, p, time.Now().UTC())
		},
		repos: known,
		log:   log,
	}
}

// changeSyncEnabled reports whether the daemon starts the unreconciled-row
// sync, and how often. It needs the handoffkeep sink, whose change state the
// sync writes, and a change-state writer; sink.reconcile_every "off" turns it
// off, and unset means config.DefaultReconcileEvery.
func changeSyncEnabled(c config.Config, writer bool) (time.Duration, bool) {
	if c.Sink.Type != "handoffkeep" || !writer {
		return 0, false
	}
	return c.Sink.ReconcileInterval()
}

// runChangeSyncLoop is the sync's own loop, beside the pr role's: it runs one
// bounded sync every interval and returns when ctx ends.
func runChangeSyncLoop(ctx context.Context, sync *changeSync, every time.Duration) {
	for ctx.Err() == nil {
		runCtx, cancel := context.WithTimeout(ctx, changeSyncTimeout)
		// One run is one pass, so its reads are memoized for the run only and
		// a later run asks again. The ETag cache still answers across runs.
		sync.run(github.NewPass(runCtx))
		cancel()
		select {
		case <-ctx.Done():
		case <-time.After(every):
		}
	}
}

// run is one bounded sync. It logs one line, with the rows it saw and what it
// wrote, and its error when the hk read failed.
func (s *changeSync) run(ctx context.Context) {
	if s.log == nil {
		s.log = slog.Default()
	}
	started := time.Now()
	var merged, closed, open, other, failures int
	rows, readErr := s.rows(ctx, changeSyncScan)
	if readErr != nil {
		failures++
	}
	start := changeSyncStart(rows, s.after)
	for i := start; i < len(rows) && i-start < changeSyncRows; i++ {
		r := rows[i]
		s.after = r.ID
		if !s.repos[r.Repo] {
			other++
			continue
		}
		p, e := s.pull(ctx, r.Repo, r.PRNumber)
		if e != nil {
			failures++
			continue
		}
		switch {
		case p.Merged || p.MergedAt != nil:
			if p.MergeCommitSHA == "" || p.MergedAt == nil {
				// The writer makes no merged write without both, so the row
				// stays unreconciled for the next run.
				failures++
				continue
			}
			s.record(ctx, r.Repo, p)
			merged++
		case p.State == "closed":
			s.record(ctx, r.Repo, p)
			closed++
		default:
			open++
		}
	}
	attrs := []any{"rows", len(rows), "merged", merged, "closed", closed, "open", open, "other_repo", other, "errors", failures, "elapsed_ms", time.Since(started).Milliseconds()}
	if readErr != nil {
		attrs = append(attrs, "error", readErr.Error())
	}
	s.log.Info("hk change sync run", attrs...)
}

// changeSyncStart is the index of the first row whose id is after the last id
// the previous run checked; the rows are id-ordered, so a run that reached the
// end starts over.
func changeSyncStart(rows []integrations.UnreconciledChange, after int64) int {
	for i, r := range rows {
		if r.ID > after {
			return i
		}
	}
	return 0
}
