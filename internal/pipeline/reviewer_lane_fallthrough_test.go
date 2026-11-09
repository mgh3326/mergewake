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

// laneDown resumes the interrupted reviewer with a findings lane configured
// that would fail the way an unauthorised hub read does, were it read.
func laneDown(t *testing.T) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock, reviewerJob) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Review.Panewire.Lane = "review-desk"
	runner.inboxErr = errors.New("inbox: hub returned HTTP 401")
	old := readReviewerJob(t, s, r, 1)
	return s, g, r, runner, c, old
}

// laneUnread checks that no findings lane was listed and none was audited.
func laneUnread(t *testing.T, s *Service, r config.Repo, runner *panewireRunner) {
	t.Helper()
	if runner.looks != 0 || len(runner.acks) != 0 {
		t.Fatalf("findings lane read: looks=%d acks=%v", runner.looks, runner.acks)
	}
	if rows := auditRows(t, s, r, "reviewer-lane-unreadable"); len(rows) != 0 {
		t.Fatalf("lane audit=%v", rows)
	}
}

// (a) A job claimed but never placed, past the budget, is replaced; the
// configured findings lane is never read.
func TestReviewerLaneErrorNeverPlacedJobIsReplaced(t *testing.T) {
	s, g, repo, runner, c, old := laneDown(t)
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "claimed", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	pastBudget(g, c)
	if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
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
	laneUnread(t, s, repo, runner)
}

// (b) The job result on the reviewer's host is collected; the lane is not read.
func TestReviewerLaneErrorJobResultCollected(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	hub := budgetPane(t, s, runner, old, "done")
	hub.jobResult = json.RawMessage(`{"found":true,"findings":{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}}`)
	if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	collectedClosedOnce(t, runner, old.Label)
	if len(runner.spawns) != 1 || len(g.postedFindings) != 1 || g.postedFindings[0].Body != "A concrete bug" {
		t.Fatalf("spawns=%d posted=%+v", len(runner.spawns), g.postedFindings)
	}
	laneUnread(t, s, repo, runner)
}

// (c) A finished pane's local findings file is not read, lane or no lane: with
// nothing on its host the reviewer is lost and replaced, and the file's
// findings are never posted.
func TestReviewerLaneErrorLocalFileIgnored(t *testing.T) {
	s, g, repo, runner, _, old := laneDown(t)
	budgetPane(t, s, runner, old, "done")
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
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
	for _, f := range g.postedFindings {
		if f.Body == "Found locally" {
			t.Fatalf("the local file was posted: %+v", g.postedFindings)
		}
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || len(auditRows(t, s, repo, "review-result-lost")) != 1 {
		t.Fatalf("spawns=%v lost=%v", runner.spawns, auditRows(t, s, repo, "review-result-lost"))
	}
	laneUnread(t, s, repo, runner)
}

// (d) Inside the budget a running job is re-attached, never replaced; the
// lane is not read.
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
	laneUnread(t, s, repo, runner)
}

// (d') A finished pane with no result inside the budget is retried, not
// replaced, when the result read is transient.
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
