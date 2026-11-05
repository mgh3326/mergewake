package pipeline

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

type remoteResultRunner struct {
	*panewireRunner
	result string
	err    error
	calls  [][]string
}

func (r *remoteResultRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) >= 2 && args[0] == "node" && args[1] == "job-result" {
		r.calls = append(r.calls, append([]string(nil), args...))
		return []byte(r.result), r.err
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

func TestReviewerRemoteResult(t *testing.T) {
	for _, tc := range []struct {
		name, status, result, outcome string
		err                           error
		budget, recovered             bool
	}{
		{name: "finished", status: "done", result: `{"found":true,"findings":{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]},"report":{"present":true,"last_line":"done"}}`, outcome: "found", recovered: true},
		{name: "missing", status: "done", result: `{"found":true,"findings":null,"findings_error":"missing"}`, outcome: "missing"},
		{name: "old_cli", status: "done", err: panewireExit(2), outcome: "error"},
		{name: "past_budget", status: "working", result: `{"found":true,"findings":{"findings":[]}}`, outcome: "found", budget: true, recovered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, repo, runner, c := interruptedReviewer(t)
			old := readReviewerJob(t, s, repo, 1)
			budgetPane(t, s, runner, old, tc.status)
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			rr := &remoteResultRunner{panewireRunner: runner, result: tc.result, err: tc.err}
			s.Runner = rr
			if tc.budget {
				pastBudget(g, c)
			} else {
				g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
			}
			if err := s.Review(context.Background(), repo, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(rr.calls) != 1 || arg(rr.calls[0], "--host") != "node-a" || arg(rr.calls[0], "--job") != old.Label {
				t.Fatalf("calls=%v", rr.calls)
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
