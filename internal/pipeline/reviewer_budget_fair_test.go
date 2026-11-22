package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// cutAfterSink is the job sink of a pass whose budget covers budget reviewer
// labels: once budget job records have been read, the pass's context is
// cancelled, which is what the repo budget's deadline does between two labels.
type cutAfterSink struct {
	*jobSink
	budget int
	reads  int
	cancel context.CancelFunc
}

func (s *cutAfterSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	rec, err := s.jobSink.Job(ctx, id)
	if strings.HasPrefix(id, "mwr-") {
		s.reads++
		if s.reads == s.budget && s.cancel != nil {
			s.cancel()
		}
	}
	return rec, err
}

// A budget that covers fewer labels than the sweep finds must still reach
// every label. Four reviewer labels stay open on every pass (their job is still
// claimed) and sort first; the budget covers three of them per pass. Two
// completed checkouts that are ready to remove sort behind them, so a fixed
// visit order never reaches them. Both must be removed within a bounded number
// of passes.
func TestReapReviewerBudgetCutReachesEveryLabel(t *testing.T) {
	f := newReviewerFix(t)
	f.s.Config.Hosts[0].WorkRoot = "/work"
	const pending = "pane still present; panewire closes it and a later sweep cleans up"
	var rows []reapReceipt
	for pr := 1; pr <= 4; pr++ {
		label := reviewerLabelFor(pr, time.Hour)
		f.sink.recs[label] = integrations.JobRecord{State: "claimed", Role: "reviewer", Label: label, Host: "node-a"}
		rows = append(rows, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: pr, Host: "node-a",
			Label: label, Worktree: label + reviewerCheckoutSuffix, Result: "pending-cleanup", Reason: pending})
	}
	var ready []string
	for pr := 7; pr <= 8; pr++ {
		label := reviewerLabelFor(pr, time.Hour)
		f.hub.detached["/work/"+label+reviewerCheckoutSuffix] = true
		f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a"}
		rows = append(rows, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: pr, Host: "node-a",
			Label: label, Worktree: label + reviewerCheckoutSuffix, Result: "pending-cleanup", Reason: pending})
		ready = append(ready, label)
	}
	seedReapRows(t, f.s, testRepo.Name, rows...)

	sink := &cutAfterSink{jobSink: f.sink, budget: 3}
	f.s.Sink = sink
	const passes = 6
	for pass := 1; pass <= passes; pass++ {
		ctx, cancel := context.WithCancel(context.Background())
		sink.cancel, sink.reads = cancel, 0
		_ = f.s.Reap(ctx, testRepo, 0, Options{})
		cancel()
		if reachedAll(t, f, ready) {
			return
		}
	}
	t.Fatalf("labels %v not all removed in %d budget-cut passes; rows: %v", ready, passes, rowSummary(f.rows(t)))
}

// reachedAll reports whether every label has a removed or already-gone row.
func reachedAll(t *testing.T, f *reviewerFix, labels []string) bool {
	t.Helper()
	done := map[string]bool{}
	for _, v := range f.rows(t) {
		if v.Result == "removed" || v.Result == "skipped" && v.Reason == reviewerAlreadyGone {
			done[v.Label] = true
		}
	}
	for _, l := range labels {
		if !done[l] {
			return false
		}
	}
	return true
}
