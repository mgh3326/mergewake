package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// collectedClosedOnce checks the one close of a reviewer job whose findings
// mergewake collected: the owner lane completes it.
func collectedClosedOnce(t *testing.T, runner *panewireRunner, label string) {
	t.Helper()
	want := []string{"job", "close", label, "--lane", "owner", "--outcome", "completed", "--reason", "result collected by mergewake"}
	if len(runner.closes) != 1 || !slices.Equal(runner.closes[0], want) {
		t.Fatalf("closes=%v, want one %v", runner.closes, want)
	}
}

// fileCollected is a resumed reviewer whose hk job is in a given state and
// whose findings are only in its job-dir file: the fallback collection path.
type fileCollected struct {
	s      *Service
	g      *fakeGH
	r      config.Repo
	runner *panewireRunner
	old    reviewerJob
}

func collectFromFile(t *testing.T, state string) fileCollected {
	t.Helper()
	s, g, r, runner, _ := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: state, OwnerLane: "owner"}
	dir := filepath.Join(s.Config.Review.Panewire.Jobs, old.Label)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	findings := `{"findings":[{"path":"tool","line":1,"body":"Found locally","severity":"high"}]}`
	if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(findings), 0o600); err != nil {
		t.Fatal(err)
	}
	return fileCollected{s, g, r, runner, old}
}

func (f fileCollected) review(t *testing.T) {
	t.Helper()
	if err := f.s.Review(context.Background(), f.r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "Found locally" {
		t.Fatalf("posted=%+v", f.g.postedFindings)
	}
}

// (a) Findings collected from the job-dir file while the hk job is still
// spawned close that job as completed, once.
func TestCollectedFindingsCloseSpawnedJob(t *testing.T) {
	f := collectFromFile(t, "spawned")
	f.review(t)
	collectedClosedOnce(t, f.runner, f.old.Label)
	if rows := auditRows(t, f.s, f.r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("skipped audit=%v", rows)
	}
}

// (b) A job that already ended is not closed again.
func TestCollectedFindingsLeaveCompletedJobAlone(t *testing.T) {
	f := collectFromFile(t, "completed")
	f.review(t)
	if len(f.runner.closes) != 0 {
		t.Fatalf("closes=%v", f.runner.closes)
	}
}

// (c) A close that fails is audited and the pass still posts its findings.
func TestCollectedFindingsCloseFailureIsAudited(t *testing.T) {
	f := collectFromFile(t, "spawned")
	f.runner.closeErrs = []error{panewireExit(1)}
	f.review(t)
	collectedClosedOnce(t, f.runner, f.old.Label)
	rows := auditRows(t, f.s, f.r, "reviewer-close-skipped")
	if len(rows) != 1 || rows[0]["label"] != f.old.Label || rows[0]["reason"] == "" {
		t.Fatalf("skipped audit=%v", rows)
	}
}

// (d) The lost-replacement and moved-head closes keep the abandoned outcome.
func TestLostAndMovedHeadClosesStayAbandoned(t *testing.T) {
	t.Run("lost replacement", func(t *testing.T) {
		s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
		if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
			t.Fatal(err)
		}
		replacedOnce(t, s, g, r, runner, old)
		closedOnce(t, runner, old.Label)
	})
	t.Run("moved head", func(t *testing.T) {
		runner := &panewireRunner{family: "openai"}
		s, _, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
		if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
			t.Fatal(err)
		}
		movedHeadClosed(t, runner, labels...)
	})
}
