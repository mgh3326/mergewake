package pipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// multiRepoGH has merged pulls for the fixture's repository only, so a second
// repository of the same pass has nothing to reap but its own host scan.
type multiRepoGH struct{ stateGH }

func (g multiRepoGH) MergedPulls(ctx context.Context, repo, branch string, since time.Time) ([]github.Pull, error) {
	if repo != testRepo.Name {
		return nil, nil
	}
	return g.stateGH.MergedPulls(ctx, repo, branch, since)
}

// A reviewer whose pane was done at the pass's host scan and started work
// before the sweep reaches its job is not abandoned: the close re-reads the pane.
// The scan is shared with an earlier repository of the same pass.
func TestReapPassReviewerPaneBusyAfterSharedScanKeepsJob(t *testing.T) {
	f := newEndedFix(t, "spawned")
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &multiRepoGH{stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}}
	f.allowJobClose()
	f.setJob(func(j *integrations.JobRecord) { j.OwnerLane = "lane-x" })
	f.now = reviewerNow
	ctx := WithReapPassHosts(context.Background())
	if err := f.s.Reap(ctx, config.Repo{Name: "example/other", Branch: "dev"}, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	f.setPane("w1:p5", "agent_status", "working")
	if err := f.s.Reap(ctx, testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()); n != 0 {
		t.Fatalf("job closed for a pane that started work after the shared scan: %d: %v", n, rowSummary(f.rows(t)))
	}
}

// A job directory a shell opened in after the pass's host scan is not archived:
// the archive re-reads the host's panes before it deletes the directory.
func TestReapPassArchiveRechecksPanesBeforeDelete(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	ctx := WithReapPassHosts(context.Background())
	// The first pass sights the ended job and scans the host with no pane in it.
	if err := f.s.Reap(ctx, testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	// A shell opens in the job directory after that scan, and the sighting ages out.
	f.run.script.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[{"pane_id":"w1:p9","agent_status":"working","cwd":"` + dir + `"}]}}`
	f.run.script.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[{"pane_id":"w1:p9","cwd":"` + dir + `"}]}}`
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(ctx, testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("job dir a pane now uses was archived: %v; rows=%+v", err, f.rows(t))
	}
	for _, r := range f.rows(t) {
		if r.Result == "archived" {
			t.Fatalf("job archived while a pane used it: %+v", r)
		}
	}
}
