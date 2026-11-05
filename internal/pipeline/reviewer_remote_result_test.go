package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestReviewerRemoteResult(t *testing.T) {
	for _, tc := range []struct {
		name, status, result, outcome string
		nodeError                     string
		budget, recovered             bool
	}{
		{name: "finished", status: "done", result: `{"found":true,"findings":{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]},"report":{"present":true,"last_line":"done"}}`, outcome: "found", recovered: true},
		{name: "missing", status: "done", result: `{"found":true,"findings":null,"findings_error":"missing"}`, outcome: "missing"},
		{name: "old_node", status: "done", nodeError: "unknown_op", outcome: "error"},
		{name: "past_budget", status: "working", result: `{"found":true,"findings":{"findings":[]}}`, outcome: "found", budget: true, recovered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, repo, runner, c := interruptedReviewer(t)
			old := readReviewerJob(t, s, repo, 1)
			hub := budgetPane(t, s, runner, old, tc.status)
			hub.jobResult = json.RawMessage(tc.result)
			hub.jobResultError = tc.nodeError
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			if tc.budget || tc.name == "old_node" {
				pastBudget(g, c)
				if tc.name == "old_node" {
					pastBudget(g, c)
				}
			} else {
				g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
			}
			if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			calls := hub.ops("job.result")
			if len(calls) != 1 || calls[0]["job"] != old.Label {
				t.Fatalf("calls=%v", calls)
			}
			rows := auditRows(t, s, repo, "reviewer-remote-result")
			if len(rows) != 1 || rows[0]["outcome"] != tc.outcome {
				t.Fatalf("audit=%v", rows)
			}
			if tc.name == "finished" && (len(g.postedFindings) != 1 || g.postedFindings[0].Body != "A concrete bug") {
				t.Fatalf("posted findings = %+v", g.postedFindings)
			}
			if tc.recovered {
				if len(runner.closes) != 0 || len(runner.spawns) != 1 || g.postCount != 1 {
					t.Fatalf("closes=%v spawns=%d posts=%d", runner.closes, len(runner.spawns), g.postCount)
				}
			} else {
				closedOnce(t, runner, old.Label)
				if len(runner.spawns) != 2 || g.postCount != 1 {
					t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
				}
			}
		})
	}
}

type oldLocalPanewire struct{ *panewireRunner }

func (r oldLocalPanewire) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "node" && args[1] == "job-result" {
		return nil, panewireExit(2)
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

func TestReviewerRemoteResultLocalOldCLI(t *testing.T) {
	runner := &panewireRunner{}
	s, _, repo := panewireSetup(t, runner)
	s.Runner = oldLocalPanewire{runner}
	x := panewireExecutor{s: s, repo: repo}
	if got, retry := x.remoteFindings(context.Background(), &passPlan{pass: 1}, integrations.JobRecord{Host: "local"}, "job"); got != nil || !retry {
		t.Fatalf("findings = %s", got)
	}
	rows := auditRows(t, s, repo, "reviewer-remote-result")
	if len(rows) != 1 || rows[0]["outcome"] != "error" {
		t.Fatalf("audit=%v", rows)
	}
}

func TestReviewerRemoteResultOperatorHold(t *testing.T) {
	for _, state := range []string{"escalated", "lost"} {
		t.Run(state, func(t *testing.T) {
			s, g, repo, runner, _ := interruptedReviewer(t)
			old := readReviewerJob(t, s, repo, 1)
			hub := budgetPane(t, s, runner, old, "done")
			hub.jobResult = json.RawMessage(`{"found":true,"findings":{"findings":[]}}`)
			rec := runner.sink.jobs[old.Label]
			rec.State = state
			runner.sink.jobs[old.Label] = rec
			g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
			err := s.Review(context.Background(), repo, 7, "", Options{})
			if err == nil || !strings.Contains(err.Error(), "reviewer job record "+state) || g.postCount != 0 || len(runner.closes) != 0 || len(runner.spawns) != 1 || len(hub.ops("job.result")) != 0 {
				t.Fatalf("err=%v posts=%d closes=%v spawns=%d calls=%v", err, g.postCount, runner.closes, len(runner.spawns), hub.ops("job.result"))
			}
		})
	}
}

type stalledLocalPanewire struct{ *panewireRunner }

func (r stalledLocalPanewire) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "node" && args[1] == "job-result" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}
func TestReviewerRemoteResultTimeoutPreservesBudget(t *testing.T) {
	runner := &panewireRunner{}
	s, _, repo := panewireSetup(t, runner)
	s.Runner = stalledLocalPanewire{runner}
	x := panewireExecutor{s: s, repo: repo}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if got, retry := x.remoteFindings(ctx, &passPlan{pass: 1}, integrations.JobRecord{Host: "local"}, "job"); got != nil || !retry || ctx.Err() != nil {
		t.Fatalf("findings=%s parent error=%v", got, ctx.Err())
	}
}

func TestReviewerRemoteResultTransientRetry(t *testing.T) {
	s, g, repo, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, repo, 1)
	hub := budgetPane(t, s, runner, old, "done")
	hub.jobResultError = "temporarily_unavailable"
	pastBudget(g, c)
	err := s.Review(context.Background(), repo, 7, "", Options{})
	if (err != nil && !errors.Is(err, errPassPending)) || len(runner.closes) != 0 || len(runner.spawns) != 1 || g.postCount != 0 {
		t.Fatalf("err=%v closes=%v spawns=%d posts=%d", err, runner.closes, len(runner.spawns), g.postCount)
	}
	if got := readReviewerJob(t, s, repo, 1); got.Label != old.Label {
		t.Fatalf("record=%+v", got)
	}
	pastBudget(g, c)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	closedOnce(t, runner, old.Label)
	if len(runner.spawns) != 2 || g.postCount != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
}
