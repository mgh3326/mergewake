package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

// refusedReps is a reps CLI whose every add fails with a long, multibyte
// message ending in an invalid byte.
type refusedReps struct{ adds int }

const refusedText = "publish refused: "

func (r *refusedReps) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	if len(args) > 1 && args[1] == "add" {
		r.adds++
		return nil, errors.New(refusedText + strings.Repeat("가", 400) + "\xff")
	}
	return nil, nil
}

// finalRow records a final, attributed outcome for PR n that the flush will
// try to publish.
func finalRow(t *testing.T, s *Service, repo config.Repo, n int) {
	t.Helper()
	if err := s.ledger().Update(repo.Name, n, func(v *outcomes.Outcome) {
		v.Final, v.Decision = true, "landed"
		v.Profile, v.Model, v.Effort, v.Grade, v.Tier = "example", "example-model", "high", "B", "T1"
	}); err != nil {
		t.Fatal(err)
	}
}

// An automatic flush whose publication fails does not fail the pass: it
// writes one bounded outcome-publish-error audit per call and keeps the
// outcome in the outbox; the strict Ledger.Flush still fails.
func TestFlushOutcomesAuditsPublishFailure(t *testing.T) {
	s, _, _, _, repo := setup(t)
	finalRow(t, s, repo, 7)
	r := &refusedReps{}
	s.Runner = r
	for call := 1; call <= 2; call++ {
		if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
			t.Fatalf("call %d: publication failure failed the pass: %v", call, err)
		}
		rows := auditRows(t, s, repo, "outcome-publish-error")
		if len(rows) != call {
			t.Fatalf("call %d: %d outcome-publish-error audits", call, len(rows))
		}
		row := rows[call-1]
		msg, _ := row["error"].(string)
		if row["repo"] != repo.Name || row["count"] != float64(1) || len(msg) > 512 || !utf8.ValidString(msg) || !strings.Contains(msg, outcomes.Key(repo.Name, 7)) || !strings.Contains(msg, refusedText) {
			t.Fatalf("call %d: audit repo=%v count=%v error bytes=%d", call, row["repo"], row["count"], len(msg))
		}
	}
	if r.adds != 2 {
		t.Fatalf("reps add ran %d times, want one per flush", r.adds)
	}
	if err := s.ledger().Flush(context.Background(), outcomes.CLI{Runner: r, Command: "pacelever"}, repo.Name); err == nil {
		t.Fatal("strict Ledger.Flush hid the publication failure")
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Published) != 0 {
		t.Fatalf("failed publication acknowledged: %v", state.Published)
	}
}

// pullFailGH cannot read a PR.
type pullFailGH struct{ *fakeGH }

func (pullFailGH) Pull(context.Context, string, int) (github.Pull, error) {
	return github.Pull{}, errors.New("pull read failed")
}

// A reconciliation error other than publication still fails the pass; the
// publication failure beside it is audited, not returned.
func TestReconcileOutcomesKeepsReadErrors(t *testing.T) {
	s, g, _, _, repo := setup(t)
	finalRow(t, s, repo, 7)
	s.GH = pullFailGH{g}
	s.Runner = &refusedReps{}
	err := s.ReconcileOutcomes(context.Background(), repo, map[int]bool{}, Options{Shadow: true})
	if err == nil || !strings.Contains(err.Error(), "pull read failed") {
		t.Fatalf("PR read failure lost: %v", err)
	}
	if strings.Contains(err.Error(), refusedText) {
		t.Fatal("publication failure returned with the pass error")
	}
	if rows := auditRows(t, s, repo, "outcome-publish-error"); len(rows) != 1 {
		t.Fatalf("%d outcome-publish-error audits", len(rows))
	}
}
