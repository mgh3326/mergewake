package pipeline

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/hktest"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type taskBodyDocSink struct{ *taskJobSink }

func (taskBodyDocSink) TaskBody(ctx context.Context, task string) (string, error) {
	return (integrations.Handoffkeep{}).TaskBody(ctx, task)
}

func TestReviewUsesLinkedTaskBodyDocumentAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failDocGet bool
	}{
		{name: "linked document"},
		{name: "document error records contract error", failDocGet: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := hktest.New(t)
			hk.SetTask(42, `{"id":42,"title":"task","state":"unstarted","body_doc":"design/task-42"}`)
			hk.SetDocument("design/task-42", "# Task\n\n## Acceptance\n\n- preserve the linked contract\n\n## Notes\n\nextra\n")
			if tc.failDocGet {
				hk.Fail("GET /v1/documents/{key}", http.StatusInternalServerError, "internal")
			}
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			s.Sink = taskBodyDocSink{runner.sink}
			if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(runner.spawns) != 1 {
				t.Fatalf("reviewer spawns = %d, want 1", len(runner.spawns))
			}
			rows := auditRows(t, s, r, "review")
			if len(rows) != 1 {
				t.Fatalf("review rows = %+v", rows)
			}
			if tc.failDocGet {
				if rows[0]["contract_error"] == nil || rows[0]["contract"] != nil {
					t.Fatalf("review row did not record contract read failure: %+v", rows[0])
				}
				return
			}
			if rows[0]["contract"] != "linked task 42" {
				t.Fatalf("review row contract = %v", rows[0]["contract"])
			}
			briefPath := arg(runner.spawns[0], "-p")
			brief, err := os.ReadFile(briefPath)
			if err != nil {
				t.Fatalf("read reviewer brief %q: %v", briefPath, err)
			}
			if !strings.Contains(string(brief), "## Acceptance\n\n- preserve the linked contract") || strings.Contains(string(brief), "## Notes") {
				t.Fatalf("reviewer brief did not carry only the linked Acceptance section:\n%s", filepath.Base(briefPath)+"\n"+string(brief))
			}
		})
	}
}
