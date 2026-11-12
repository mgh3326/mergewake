package pipeline

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// commandRecorder is a reps CLI that records the command each call names and
// refuses every add, so the flushed outcome stays in the outbox.
type commandRecorder struct{ commands []string }

func (r *commandRecorder) Run(_ context.Context, command string, _ []string, _, _ string) ([]byte, error) {
	r.commands = append(r.commands, command)
	return nil, errors.New("publish refused")
}

// FlushOutcomes runs the outcomes command as loaded. Even with review.quota
// set to the retired tool, an empty outcomes.command never falls back to it.
func TestFlushOutcomesNeverRunsRetiredQuotaTool(t *testing.T) {
	s, _, _, _, repo := setup(t)
	finalRow(t, s, repo, 7)
	s.Config.Review.Quota = "scopefuel"
	s.Config.Outcomes.Command = ""
	r := &commandRecorder{}
	s.Runner = r
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	if len(r.commands) == 0 || slices.Contains(r.commands, "scopefuel") {
		t.Fatalf("outcomes commands %q", r.commands)
	}
}
