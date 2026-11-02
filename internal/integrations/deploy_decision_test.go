package integrations

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type decisionRunner struct {
	body  []byte
	calls [][]string
}

func (r *decisionRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, args)
	return r.body, nil
}
func TestDeployDecision(t *testing.T) {
	for _, tc := range []struct {
		name, status, question, option string
		human, expired, want           bool
	}{
		{"new", "", "", "", false, false, false},
		{"open", "open", "release", "", true, false, false},
		{"approved", "answered", "release", "A", true, false, true},
		{"rejected", "answered", "release", "B", true, false, false},
		{"withdrawn", "withdrawn", "release", "A", true, false, false},
		{"expired", "answered", "release", "A", true, true, false},
		{"other release", "answered", "old release", "A", true, false, false},
		{"not human", "answered", "release", "A", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := map[string]any{"id": "dr-1-1", "question": tc.question, "status": tc.status, "human_only": tc.human, "resolution": map[string]string{"kind": "answered", "option": tc.option}}
			if tc.expired {
				req["due_at"] = time.Now().Add(-time.Minute)
			}
			b, _ := json.Marshal(map[string]any{"refs": map[string]any{"decision_request": req}})
			r := &decisionRunner{body: b}
			h := Handoffkeep{Runner: r, Command: "hk"}
			got, e := h.DeployDecision(context.Background(), "1", "release")
			if e != nil || got != tc.want {
				t.Fatalf("got %v %v", got, e)
			}
			if tc.question != "release" || !tc.human {
				if len(r.calls) != 2 || r.calls[1][0] != "decisions" || r.calls[1][len(r.calls[1])-1] != "--human-only" {
					t.Fatalf("missing mirrored human request: %v", r.calls)
				}
			}
		})
	}
}
