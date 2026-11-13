package pipeline

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// refusingLabel answers exit for every spawn of one label and runs every other
// spawn through next: the hub never starts one request, and the fresh reviewers
// that replace it still run.
type refusingLabel struct {
	next    integrations.Runner
	label   string
	exit    int
	delay   time.Duration // spent before a refusal answers
	refused int           // spawns of label answered with exit
}

func (x *refusingLabel) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if x.label != "" && arg(args, "-l") == x.label && len(args) > 0 && args[0] == "spawn" {
		time.Sleep(x.delay)
		x.refused++
		return nil, panewireExit(x.exit)
	}
	return x.next.Run(ctx, command, args, input, dir)
}

// deadReplayRig is an interrupted reviewer whose hk record is gone: the hub
// never started its request. Every replay of that request answers exit; the
// fresh reviewers the pass spawns after it run. The returned refusingLabel
// counts the replays; runner.spawns counts the spawns that ran.
func deadReplayRig(t *testing.T, exit int) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock, *refusingLabel) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	runner.sink.missing = nil
	runner.findings = `{"findings":[]}`
	refuse := &refusingLabel{next: s.Runner, label: readReviewerJob(t, s, r, 1).Label, exit: exit}
	s.Runner = refuse
	return s, g, r, runner, c, refuse
}

// (a) A replay that answers a terminal exit for a request the hub never started
// drops the record and spawns a fresh reviewer for the same head and pass under
// a new label, in the same evaluation. The audit names both labels.
func TestReviewerDeadReplayIsDroppedAndRespawned(t *testing.T) {
	s, g, r, runner, _, refuse := deadReplayRig(t, panewireQuotaTimeout)
	old := readReviewerJob(t, s, r, 1).Label
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("review: %v", err)
	}
	if refuse.refused != 1 {
		t.Fatalf("replays of the recorded request=%d, want 1", refuse.refused)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns=%d, want the first spawn and the fresh replacement", len(runner.spawns))
	}
	fresh := arg(runner.spawns[1], "-l")
	if fresh == old || !strings.HasPrefix(fresh, "mwr-7-"+shaA[:7]+"-p1-") {
		t.Fatalf("fresh label %s must differ from recorded %s", fresh, old)
	}
	if g.postCount != 1 {
		t.Fatalf("posts=%d, want the fresh reviewer's findings posted", g.postCount)
	}
	dead := auditRows(t, s, r, "reviewer-spawn-dead")
	if len(dead) != 1 || dead[0]["label"] != old || dead[0]["exit"] != float64(panewireQuotaTimeout) {
		t.Fatalf("reviewer-spawn-dead rows %v", dead)
	}
	replaced := auditRows(t, s, r, "reviewer-spawn-replaced")
	if len(replaced) != 1 || replaced[0]["old_label"] != old || replaced[0]["label"] != fresh {
		t.Fatalf("reviewer-spawn-replaced rows %v, want %s replaced by %s", replaced, old, fresh)
	}
	reviewerJobGone(t, s, r, 1)
}

// (b) Replays of a dead request add no failure of their own: five evaluations
// against the lost request leave review-retry at most one failure.
func TestReviewerDeadReplayCountsAtMostOneFailure(t *testing.T) {
	s, g, r, _, c, _ := deadReplayRig(t, panewireQuotaTimeout)
	// A short backoff and small steps: every evaluation runs the pass, and the
	// record stays young enough to be replayed rather than declared lost.
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1s", Maximum: "1s", Failures: 10}
	worst := 0
	for i := 0; i < 5; i++ {
		c.t = c.t.Add(2 * time.Second)
		expireLease(g)
		_ = s.RetryReview(context.Background(), r, g.pull, Options{})
		var st reviewRetryState
		if ok, err := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), &st); err == nil && ok {
			worst = max(worst, st.Failures)
		}
	}
	if worst > 1 {
		t.Fatalf("review-retry failures reached %d over five evaluations, want at most 1", worst)
	}
}

// (d) When the review deadline leaves too little time for a replacement to wait
// for its pane, the dead record still goes and the pass is pending: the
// replacement spawns on the next evaluation under a new label.
func TestReviewerDeadReplayWithoutTimeRespawnsNextRun(t *testing.T) {
	s, g, r, runner, c, refuse := deadReplayRig(t, panewireQuotaTimeout)
	refuse.delay = 1200 * time.Millisecond
	s.Config.Review.Timeout = "2s"
	old := readReviewerJob(t, s, r, 1).Label
	// The pass is pending, which Review reports as no failure.
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("review err=%v, want the pass pending without a failure", err)
	}
	if refuse.refused != 1 || len(runner.spawns) != 1 {
		t.Fatalf("replays=%d spawns=%d, want the replay only", refuse.refused, len(runner.spawns))
	}
	reviewerJobGone(t, s, r, 1)
	if rows := auditRows(t, s, r, "reviewer-spawn-dead"); len(rows) != 1 || rows[0]["label"] != old {
		t.Fatalf("reviewer-spawn-dead rows %v", rows)
	}
	refuse.delay = 0
	c.t = c.t.Add(time.Second) // a later tick: the replacement's label differs from the dead one's second
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("next evaluation: %v", err)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old || g.postCount != 1 {
		t.Fatalf("next evaluation spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
}

// (c) A replay that answers an exit which may still land keeps its record and
// spawns nothing new, as today. The list holds the answers that mean a pane may
// exist, the held reservation, and the quota waits that keep the request queued.
func TestReviewerReplayThatMayLandKeepsRecord(t *testing.T) {
	for name, exit := range map[string]int{
		"wait timeout":       panewireWaitTimeout,
		"brief unconfirmed":  76,
		"lane unregistered":  82,
		"held reservation":   panewireHeld,
		"quota wait":         86,
		"quota escalate":     87,
		"quota unknown":      89,
		"unclassified error": 1,
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r, runner, _, refuse := deadReplayRig(t, exit)
			before := readReviewerJob(t, s, r, 1).Label
			_ = s.Review(context.Background(), r, 7, "", Options{})
			if refuse.refused != 1 || len(runner.spawns) != 1 {
				t.Fatalf("replays=%d spawns=%d, want the replay only and no fresh spawn", refuse.refused, len(runner.spawns))
			}
			if got := readReviewerJob(t, s, r, 1).Label; got != before {
				t.Fatalf("record label %s, want kept %s", got, before)
			}
			if rows := auditRows(t, s, r, "reviewer-spawn-dead"); len(rows) != 0 {
				t.Fatalf("reviewer-spawn-dead rows %v, want none", rows)
			}
			if g.postCount != 0 {
				t.Fatalf("posts=%d", g.postCount)
			}
		})
	}
}
