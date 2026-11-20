package pipeline

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// The review audit row's seat names the reviewer that ran the pass: its launch
// profile and effort from the hk job record, with its quota pool beside it.
// The executor stays "panewire". A record that names no profile reads "unknown".
func TestReviewAuditSeatNamesProfileAndEffort(t *testing.T) {
	for name, tt := range map[string]struct {
		rec      integrations.JobRecord
		wantSeat string
		wantPool any
	}{
		"profile and effort": {
			rec:      integrations.JobRecord{State: "completed", Family: "openai", Profile: "codex-sol", Effort: "medium", Pool: "codex", Result: json.RawMessage(`{"findings":[]}`)},
			wantSeat: "codex-sol@medium",
			wantPool: "codex",
		},
		"no profile": {
			rec:      integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)},
			wantSeat: "unknown",
			wantPool: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai"}
			s, g, r := panewireSetup(t, runner)
			runner.sink.missing = &tt.rec
			if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
				t.Fatal(e)
			}
			if got := g.passes[shaA]; len(got) != 1 || got[0].Executor != "panewire" {
				t.Fatalf("passes=%+v", got)
			}
			reviews, _ := reviewRecords(t, s, r, "review")
			if len(reviews) != 1 || reviews[0]["executor"] != "panewire" || reviews[0]["seat"] != tt.wantSeat || reviews[0]["pool"] != tt.wantPool {
				t.Fatalf("review audit=%v, want seat=%q pool=%v", reviews, tt.wantSeat, tt.wantPool)
			}
		})
	}
}
