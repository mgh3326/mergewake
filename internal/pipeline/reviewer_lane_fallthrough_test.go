package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// laneDown resumes the interrupted reviewer with the findings lane failing
// the way an unauthorised hub read does.
func laneDown(t *testing.T) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock, reviewerJob) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Review.Panewire.Lane = "review-desk"
	runner.inboxErr = errors.New("inbox: hub returned HTTP 401")
	old := readReviewerJob(t, s, r, 1)
	return s, g, r, runner, c, old
}

func laneAudit(t *testing.T, s *Service, rows []map[string]any) {
	t.Helper()
	if len(rows) != 1 || rows[0]["class"] != "inbox_unauthorized" || rows[0]["label"] == "" || !strings.Contains(rows[0]["error"].(string), "HTTP 401") {
		t.Fatalf("lane audit=%v", rows)
	}
}

// (a) A job claimed but never placed, past the budget, is replaced although
// the findings lane cannot be read; the lane error is not the pass error.
func TestReviewerLaneErrorNeverPlacedJobIsReplaced(t *testing.T) {
	s, g, repo, runner, c, old := laneDown(t)
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "claimed", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	pastBudget(g, c)
	err := s.Review(context.Background(), repo, 7, "", Options{})
	if err != nil && strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("lane error surfaced as pass error: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
		t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
	}
	lost := auditRows(t, s, repo, "review-result-lost")
	if len(lost) != 1 || !strings.Contains(lost[0]["collect_error"].(string), "never placed") {
		t.Fatalf("lost audit=%v", lost)
	}
	if rows := auditRows(t, s, repo, "reviewer-lane-unreadable"); len(rows) < 1 || rows[0]["class"] != "inbox_unauthorized" {
		t.Fatalf("lane audit=%v", rows)
	}
}

// (b) The job result is still collected when the lane cannot be read.
func TestReviewerLaneErrorJobResultCollected(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	hub := budgetPane(t, s, runner, old, "done")
	hub.jobResult = json.RawMessage(`{"found":true,"findings":{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}}`)
	if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 1 || len(g.postedFindings) != 1 || g.postedFindings[0].Body != "A concrete bug" {
		t.Fatalf("closes=%v spawns=%d posted=%+v", runner.closes, len(runner.spawns), g.postedFindings)
	}
	laneAudit(t, s, auditRows(t, s, repo, "reviewer-lane-unreadable"))
}

// (c) A finished pane's local findings file is read when the lane cannot be.
func TestReviewerLaneErrorLocalFileCollected(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	budgetPane(t, s, runner, old, "done")
	dir := filepath.Join(s.Config.Review.Panewire.Jobs, old.Label)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	local := `{"findings":[{"path":"tool","line":1,"body":"Found locally","severity":"high"}]}`
	if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(local), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 1 || len(g.postedFindings) != 1 || g.postedFindings[0].Body != "Found locally" {
		t.Fatalf("closes=%v spawns=%d posted=%+v", runner.closes, len(runner.spawns), g.postedFindings)
	}
	laneAudit(t, s, auditRows(t, s, repo, "reviewer-lane-unreadable"))
}

// (d) Inside the budget a running job is re-attached, never replaced; the
// lane is not read at all until the pane finishes or the budget is spent.
func TestReviewerLaneErrorWithinBudgetStaysPending(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	budgetPane(t, s, runner, old, "working")
	runner.exits = []int{panewireWaitTimeout}
	err := s.Review(context.Background(), repo, 7, "", Options{})
	if err != nil && !errors.Is(err, errPassPending) {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || g.postCount != 0 || (err != nil && strings.Contains(err.Error(), "HTTP 401")) {
		t.Fatalf("err=%v closes=%v posts=%d", err, runner.closes, g.postCount)
	}
	if got := readReviewerJob(t, s, repo, 1); got.Label != old.Label {
		t.Fatalf("record=%+v", got)
	}
}

// (d') A finished pane with no result inside the budget is retried, not replaced, when
// the lane fails and the result read is transient.
func TestReviewerLaneErrorFinishedPaneTransientResultStaysPending(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	hub := budgetPane(t, s, runner, old, "done")
	hub.jobResultError = "temporarily_unavailable"
	err := s.Review(context.Background(), repo, 7, "", Options{})
	if err != nil && !errors.Is(err, errPassPending) {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || g.postCount != 0 {
		t.Fatalf("err=%v closes=%v posts=%d", err, runner.closes, g.postCount)
	}
}

// cancelOnInbox cancels the pass while the findings lane is being read.
type cancelOnInbox struct {
	*panewireRunner
	cancel context.CancelFunc
}

func (r cancelOnInbox) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "inbox" && args[1] == "list" {
		r.cancel()
		return nil, ctx.Err()
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

// A cancelled context while the lane is read is returned, not treated as a
// lane outage: a local findings file is not collected after cancellation.
func TestReviewerLaneErrorCancellationPropagates(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	budgetPane(t, s, runner, old, "done")
	dir := filepath.Join(s.Config.Review.Panewire.Jobs, old.Label)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	local := `{"findings":[{"path":"tool","line":1,"body":"Found locally","severity":"high"}]}`
	if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(local), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Runner = cancelOnInbox{runner, cancel}
	err := s.Review(ctx, repo, 7, "", Options{})
	if err == nil || !errors.Is(err, context.Canceled) || len(g.postedFindings) != 0 || g.postCount != 0 || len(runner.closes) != 0 {
		t.Fatalf("err=%v posted=%+v posts=%d closes=%v", err, g.postedFindings, g.postCount, runner.closes)
	}
	if rows := auditRows(t, s, repo, "reviewer-lane-unreadable"); len(rows) != 0 {
		t.Fatalf("cancellation audited as a lane error: %v", rows)
	}
}
