package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// collectedClosedOnce checks the one close of a reviewer job whose findings
// mergewake collected: the owner lane completes it.
func collectedClosedOnce(t *testing.T, runner *panewireRunner, label string) {
	t.Helper()
	want := []string{"job", "close", label, "--lane", "owner", "--outcome", "completed", "--reason", "result collected by mergewake"}
	if len(runner.closes) != 1 || len(runner.closes[0]) < len(want) || !slices.Equal(runner.closes[0][:len(want)], want) {
		t.Fatalf("closes=%v, want one %v", runner.closes, want)
	}
	// Past the close itself only the reap.hub credential flags may follow.
	if rest := runner.closes[0][len(want):]; len(rest) > 0 && rest[0] != "--hub-url" {
		t.Fatalf("close carries %v after %v", rest, want)
	}
}

// nodeCollected is a resumed reviewer whose pane is done, whose hk job is in a
// given state, and whose findings are on its host: a nonterminal job's result
// is read there (job.result), a completed one's from its record.
type nodeCollected struct {
	s      *Service
	g      *fakeGH
	r      config.Repo
	runner *panewireRunner
	old    reviewerJob
}

func collectFromNode(t *testing.T, state string) nodeCollected {
	t.Helper()
	s, g, r, runner, _ := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := budgetPane(t, s, runner, old, "done")
	findings := `{"findings":[{"path":"tool","line":1,"body":"Found on the host","severity":"high"}]}`
	hub.jobResult = json.RawMessage(`{"found":true,"findings":` + findings + `}`)
	job := integrations.JobRecord{State: state, OwnerLane: "owner", Host: "node-a", PaneID: "p1"}
	if terminalJob(state) {
		job.Result = json.RawMessage(findings)
	}
	runner.sink.jobs[old.Label] = job
	return nodeCollected{s, g, r, runner, old}
}

func (f nodeCollected) review(t *testing.T) {
	t.Helper()
	if err := f.s.Review(context.Background(), f.r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "Found on the host" {
		t.Fatalf("posted=%+v", f.g.postedFindings)
	}
}

// (a) Findings collected from the reviewer's host while the hk job is still
// spawned close that job as completed, once.
func TestCollectedFindingsCloseSpawnedJob(t *testing.T) {
	f := collectFromNode(t, "spawned")
	f.review(t)
	collectedClosedOnce(t, f.runner, f.old.Label)
	if rows := auditRows(t, f.s, f.r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("skipped audit=%v", rows)
	}
}

// (b) A job that already ended, its result on its record, is not closed again.
func TestCollectedFindingsLeaveCompletedJobAlone(t *testing.T) {
	f := collectFromNode(t, "completed")
	f.review(t)
	if len(f.runner.closes) != 0 {
		t.Fatalf("closes=%v", f.runner.closes)
	}
}

// (c) A close that fails is audited and the pass still posts its findings.
func TestCollectedFindingsCloseFailureIsAudited(t *testing.T) {
	f := collectFromNode(t, "spawned")
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

// auditOut records the JSON lines printed to it and fails the one write whose
// text carries fail, so a single audit write can be made to fail while the rest
// of the pass keeps writing.
type auditOut struct {
	mu   sync.Mutex
	fail string
	text strings.Builder
}

func (w *auditOut) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail != "" && strings.Contains(string(p), w.fail) {
		return 0, errors.New("audit sink unavailable")
	}
	return w.text.Write(p)
}

func (w *auditOut) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.text.String()
}

// (e) A close whose command fails and whose audit write fails too never
// aborts the pass: the collected findings are still posted, and the failure is
// logged once with the reviewer's label and a reason.
func TestCollectedCloseFailureAndDeadAuditKeepFindings(t *testing.T) {
	f := collectFromNode(t, "spawned")
	out := &auditOut{fail: "reviewer-close-skipped"}
	f.s.Out = out
	f.runner.closeErrs = []error{panewireExit(1)}
	f.review(t)
	collectedClosedOnce(t, f.runner, f.old.Label)
	lines := out.String()
	if !strings.Contains(lines, `"state":"close-failed"`) || !strings.Contains(lines, f.old.Label) || !strings.Contains(lines, "audit sink unavailable") {
		t.Fatalf("close failure not logged with its label and reason: %s", lines)
	}
}

// (f) A review context that is already past its deadline does not skip the
// collected close: it runs on reportContext instead.
func TestCollectedCloseRunsPastReviewDeadline(t *testing.T) {
	f := collectFromNode(t, "spawned")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancel()
	x := &panewireExecutor{s: f.s, repo: f.r, pr: f.g.pull}
	if err := x.closeCollectedJob(ctx, f.old, f.runner.sink.jobs[f.old.Label]); err != nil {
		t.Fatalf("close on a spent review context: %v", err)
	}
	collectedClosedOnce(t, f.runner, f.old.Label)
}

// (g) The success path is unchanged: one completed close and no failure line.
func TestCollectedCloseSuccessUnchanged(t *testing.T) {
	f := collectFromNode(t, "spawned")
	out := &auditOut{}
	f.s.Out = out
	f.review(t)
	collectedClosedOnce(t, f.runner, f.old.Label)
	if lines := out.String(); strings.Contains(lines, "close-failed") {
		t.Fatalf("unexpected close failure: %s", lines)
	}
	if rows := auditRows(t, f.s, f.r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("skipped audit=%v", rows)
	}
}
