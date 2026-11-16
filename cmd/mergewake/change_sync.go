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
	// changeSyncScan is the page size requested from hk.
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
	// changeSyncRetryTime bounds how much of one run the retry pass may take,
	// so slow retries cannot starve the cursor's window; retries it does not
	// reach stay queued for the next run.
	changeSyncRetryTime = 5 * time.Minute
)

// changeSync checks hk's unreconciled PR rows against GitHub and records the
// merged and closed ones back, so a PR merged outside mergewake's pass no
// longer holds its hk tasks open.
type changeSync struct {
	rows   func(context.Context, int64, int) (integrations.UnreconciledPage, error)
	pull   func(context.Context, string, int) (github.Pull, error)
	record func(context.Context, string, github.Pull)
	repos  map[string]bool
	log    *slog.Logger
	// after is the id a run's window starts after: the last row the previous
	// run reached. hk pages the unreconciled rows by after_id, so a run reads
	// forward from it until it has checked changeSyncRows rows or the list
	// ends; at the end the cursor goes back to 0 and the rotation covers the
	// whole list. An hk without paging ignores after_id and answers one page
	// without next_after_id: the run then rotates inside that page as it
	// always did. A run whose context ends stops before it advances after,
	// so the rows it did not reach are the next run's first rows.
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
	// retried marks the rows this run already checked, so a later pass does
	// not read them twice; failed collects the rows this run leaves
	// unfinished for the next run's retry set.
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

	// The first read starts just before the smallest retry id, so the retry
	// pass finds a retry row wherever it sits; the largest bounds how far
	// the scan must go before every retry id is behind it.
	firstAfter := s.after
	var minRetry, maxRetry int64
	if len(s.retryOrder) > 0 {
		minRetry, maxRetry = s.retryOrder[0], s.retryOrder[0]
		for _, id := range s.retryOrder[1:] {
			if id < minRetry {
				minRetry = id
			}
			if id > maxRetry {
				maxRetry = id
			}
		}
		firstAfter = minRetry - 1
	}

	var seen int
	page, readErr := s.rows(ctx, firstAfter, changeSyncScan)
	if readErr != nil {
		failures++
	}

	// retryScanned reports that the retry pass covered every retry id, so
	// one the list no longer has leaves the set.
	retryScanned := false
	if readErr == nil && page.Paged {
		retryUntil := started.Add(changeSyncRetryTime)
		if len(s.retryOrder) > 0 {
			var e error
			retryScanned, e = s.scanRetries(ctx, page, maxRetry, retryUntil, &seen, visit)
			if e != nil {
				if readErr == nil {
					readErr = e
				}
				failures++
			}
			page = integrations.UnreconciledPage{}
		}
		var first *integrations.UnreconciledPage
		if len(s.retryOrder) == 0 {
			// The read above already started at the cursor.
			first = &page
		}
		if e := s.scanWindow(ctx, first, &seen, retried, visit); e != nil {
			if readErr == nil {
				readErr = e
			}
			failures++
		}
	} else {
		// An hk without after_id paging ignores the parameter and answers
		// the first page with no next_after_id: the run keeps the one-page
		// rotation it always had.
		rows := page.Changes
		seen = len(rows)
		retryUntil := started.Add(changeSyncRetryTime)
		for i := range rows {
			if ctx.Err() != nil || time.Now().After(retryUntil) {
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
			s.after = r.ID
			if retried[r.ID] {
				continue
			}
			visit(r)
		}
	}

	// The next run's retry set: ids the scan did not resolve stay queued,
	// this run's failures join them oldest-first, bounded by
	// changeSyncRetries.
	next := make([]int64, 0, len(s.retryOrder)+len(failed))
	nextSet := make(map[int64]struct{}, cap(next))
	for _, id := range s.retryOrder {
		if retried[id] || retryScanned {
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

	attrs := []any{"rows", seen, "merged", merged, "closed", closed, "open", open, "other_repo", other, "errors", failures, "elapsed_ms", time.Since(started).Milliseconds()}
	if readErr != nil {
		attrs = append(attrs, "error", readErr.Error())
	}
	s.log.Info("hk change sync run", attrs...)
}

// scanRetries pages forward from first, visiting the rows still in the retry
// set, until the scan passes the largest retry id or the list ends. It is
// bounded by until and ctx, and makes no read once ctx has ended; it reports
// whether it covered every retry id — an id the list no longer has then
// leaves the set. A page read error is returned for the run's log.
func (s *changeSync) scanRetries(ctx context.Context, first integrations.UnreconciledPage, maxRetry int64, until time.Time, seen *int, visit func(integrations.UnreconciledChange)) (bool, error) {
	page := first
	for {
		*seen += len(page.Changes)
		bounded := false
		for _, r := range page.Changes {
			if ctx.Err() != nil || time.Now().After(until) {
				bounded = true
				break
			}
			if _, ok := s.retry[r.ID]; ok {
				visit(r)
			}
		}
		if bounded {
			return false, nil
		}
		if len(page.Changes) == 0 {
			return page.NextAfterID == nil, nil
		}
		last := page.Changes[len(page.Changes)-1].ID
		if last >= maxRetry || page.NextAfterID == nil {
			return true, nil
		}
		if *page.NextAfterID < last {
			return false, nil
		}
		if ctx.Err() != nil {
			return false, nil
		}
		next, e := s.rows(ctx, *page.NextAfterID, changeSyncScan)
		if e != nil {
			return false, e
		}
		if !next.Paged {
			return false, nil
		}
		page = next
	}
}

// scanWindow pages forward from the cursor, visiting up to changeSyncRows
// rows and advancing s.after over each row it reaches. It stops at the
// list's end — where the cursor goes back to 0 for the next run — at ctx's
// end, or at the row bound, and makes no read once ctx has ended. first,
// when non-nil, is the page the run's first read already fetched from the
// cursor.
func (s *changeSync) scanWindow(ctx context.Context, first *integrations.UnreconciledPage, seen *int, retried map[int64]bool, visit func(integrations.UnreconciledChange)) error {
	after := s.after
	page := first
	examined := 0
	for {
		if page == nil {
			if ctx.Err() != nil {
				return nil
			}
			p, e := s.rows(ctx, after, changeSyncScan)
			if e != nil {
				return e
			}
			if !p.Paged {
				return nil
			}
			page = &p
		}
		*seen += len(page.Changes)
		stop := false
		for _, r := range page.Changes {
			if ctx.Err() != nil {
				stop = true
				break
			}
			s.after = r.ID
			examined++
			if retried[r.ID] {
				continue
			}
			visit(r)
			if examined >= changeSyncRows {
				stop = true
				break
			}
		}
		if stop {
			return nil
		}
		if len(page.Changes) == 0 || page.NextAfterID == nil {
			s.after = 0
			return nil
		}
		last := page.Changes[len(page.Changes)-1].ID
		if *page.NextAfterID < last {
			return nil
		}
		after = *page.NextAfterID
		page = nil
	}
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
// end starts over. It serves the one-page rotation an hk without after_id
// paging still gets.
func changeSyncStart(rows []integrations.UnreconciledChange, after int64) int {
	for i, r := range rows {
		if r.ID > after {
			return i
		}
	}
	return 0
}
