package pipeline

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// lostReviewerSetup resumes a reviewer whose hk record is the given job, past
// the review deadline, so the pass takes the lost path. The replacement's job
// is answered with a finished review.
func lostReviewerSetup(t *testing.T, job integrations.JobRecord) (*Service, *fakeGH, config.Repo, *panewireRunner, reviewerJob) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	runner.sink.jobs[old.Label] = job
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	pastBudget(g, c)
	pastBudget(g, c)
	return s, g, r, runner, old
}

func replacedOnce(t *testing.T, s *Service, g *fakeGH, r config.Repo, runner *panewireRunner, old reviewerJob) {
	t.Helper()
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
		t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
	}
	if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 1 {
		t.Fatalf("lost audit=%v", rows)
	}
}

// A lost reviewer whose hk record names no owner lane cannot be closed from
// this host; the replacement still runs and the skipped close is audited.
func TestLostReviewerWithoutOwnerLaneIsReplaced(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	if len(runner.closes) != 0 {
		t.Fatalf("closes=%v", runner.closes)
	}
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 1 || rows[0]["label"] != old.Label || rows[0]["head"] != shaA || rows[0]["reason"] != "no owner lane" {
		t.Fatalf("skipped audit=%v", rows)
	}
}

// A close that fails (the job's events live on another host) does not stop
// the replacement; the failure is audited.
func TestLostReviewerCloseFailureIsReplaced(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	runner.closeErrs = []error{panewireExit(1)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	closedOnce(t, runner, old.Label)
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 1 || rows[0]["label"] != old.Label || rows[0]["head"] != shaA || rows[0]["reason"] == "" || rows[0]["reason"] == "no owner lane" {
		t.Fatalf("skipped audit=%v", rows)
	}
	reviewerJobGone(t, s, r, 1)
}

// With no owner lane in hk the close falls back to the --owner of the spawn
// arguments recorded for the job, then to the configured owner.
func TestLostReviewerCloseFallsBackToSpawnOwner(t *testing.T) {
	for _, tt := range []struct {
		name      string
		recorded  string
		configure string
		lane      string
	}{
		{"recorded args", "args-owner", "cfg-owner", "args-owner"},
		{"config", "", "cfg-owner", "cfg-owner"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed"})
			s.Config.Review.Panewire.Owner = tt.configure
			if tt.recorded != "" {
				old.Args = append(slices.Clone(old.Args), "--owner", tt.recorded)
				if err := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, 1), old); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			replacedOnce(t, s, g, r, runner, old)
			if len(runner.closes) != 1 || runner.closes[0][2] != old.Label || arg(runner.closes[0], "--lane") != tt.lane || arg(runner.closes[0], "--outcome") != "abandoned" {
				t.Fatalf("closes=%v, want --lane %s", runner.closes, tt.lane)
			}
			if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 0 {
				t.Fatalf("skipped audit=%v", rows)
			}
		})
	}
}

// A record left by an older head whose job names no owner lane cannot be
// closed here; the pass goes on, the record is dropped and the skip audited.
func TestReviewerHeadMovedWithoutOwnerLaneIsDropped(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 1 || g.postCount != 1 {
		t.Fatalf("closes=%v spawns=%d posts=%d", runner.closes, len(runner.spawns), g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
	reviewerJobGone(t, s, r, 2)
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 2 || rows[0]["label"] != labels[0] || rows[1]["label"] != labels[1] || rows[0]["head"] != shaB || rows[0]["reason"] != "no owner lane" {
		t.Fatalf("skipped audit=%v", rows)
	}
	if rows := auditRows(t, s, r, "reviewer-head-moved"); len(rows) != 0 {
		t.Fatalf("head-moved audit=%v", rows)
	}
	if !strings.HasPrefix(arg(runner.spawns[0], "-l"), "mwr-7-"+shaA[:7]+"-p1-") {
		t.Fatalf("spawn %v", runner.spawns[0])
	}
}
