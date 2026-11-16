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
	// changeSyncRetries bounds the rows a run leaves unfinished that the next
	// run reads first; past it a row waits for the rotation like any other.
	changeSyncRetries = 100
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
	// at the end: rows that stay open at the head cannot starve the rest. A
	// run whose context ends stops before it advances after, so the rows it
	// did not reach are the next run's first rows.
	after int64
	// retry holds the ids of the rows the last run left unfinished — a failed
	// PR read, or a merged row with no merge commit yet — so the next run
	// reads them before the cursor's window. retryOrder keeps the same ids,
	// oldest first, for the bound's eviction.
	retry      map[int64]struct{}
	retryOrder []int64
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
	count := func(kind string) {
		switch kind {
		case "merged":
			merged++
		case "closed":
			closed++
		case "open":
			open++
		case "other":
			other++
		default:
			failures++
		}
	}
	// retried marks the rows this run already checked from the retry set, so
	// the window pass does not read them twice; failed collects the rows this
	// run leaves unfinished for the next run's retry set.
	retried := make(map[int64]bool, len(s.retry))
	var failed []int64
	visit := func(r integrations.UnreconciledChange) {
		kind, retry := s.checkRow(ctx, r)
		count(kind)
		retried[r.ID] = true
		if retry {
			failed = append(failed, r.ID)
		}
	}

	// Rows the last run left unfinished are read first, wherever the cursor
	// sits; the cursor may pass them, the retry set brings them back.
	for i := range rows {
		if ctx.Err() != nil {
			break
		}
		r := rows[i]
		if _, ok := s.retry[r.ID]; ok {
			visit(r)
		}
	}

	start := changeSyncStart(rows, s.after)
	for i := start; i < len(rows) && i-start < changeSyncRows; i++ {
		if ctx.Err() != nil {
			break
		}
		r := rows[i]
		if r.ID > s.after {
			s.after = r.ID
		}
		if retried[r.ID] {
			continue
		}
		visit(r)
	}

	// The next run's retry set: ids the page did not show stay queued, this
	// run's failures join them oldest-first, bounded by changeSyncRetries.
	next := make([]int64, 0, len(s.retryOrder)+len(failed))
	nextSet := make(map[int64]struct{}, cap(next))
	for _, id := range s.retryOrder {
		if retried[id] {
			continue
		}
		next = append(next, id)
		nextSet[id] = struct{}{}
	}
	for _, id := range failed {
		if _, ok := nextSet[id]; !ok {
			next = append(next, id)
			nextSet[id] = struct{}{}
		}
	}
	if len(next) > changeSyncRetries {
		next = next[len(next)-changeSyncRetries:]
		nextSet = make(map[int64]struct{}, len(next))
		for _, id := range next {
			nextSet[id] = struct{}{}
		}
	}
	s.retryOrder, s.retry = next, nextSet

	attrs := []any{"rows", len(rows), "merged", merged, "closed", closed, "open", open, "other_repo", other, "errors", failures, "elapsed_ms", time.Since(started).Milliseconds()}
	if readErr != nil {
		attrs = append(attrs, "error", readErr.Error())
	}
	s.log.Info("hk change sync run", attrs...)
}

// checkRow reads one row's PR and records the write when the PR reconciles.
// It reports what the row counted as and whether it stays unreconciled for
// the next run's retry set.
func (s *changeSync) checkRow(ctx context.Context, r integrations.UnreconciledChange) (string, bool) {
	if !s.repos[r.Repo] {
		return "other", false
	}
	p, e := s.pull(ctx, r.Repo, r.PRNumber)
	if e != nil {
		return "failed", true
	}
	switch {
	case p.Merged || p.MergedAt != nil:
		if p.MergeCommitSHA == "" || p.MergedAt == nil {
			// The writer makes no merged write without both, so the row
			// stays unreconciled for the next run.
			return "failed", true
		}
		s.record(ctx, r.Repo, p)
		return "merged", false
	case p.State == "closed":
		s.record(ctx, r.Repo, p)
		return "closed", false
	default:
		return "open", false
	}
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
