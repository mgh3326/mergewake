package pipeline

import (
	"bytes"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A consult request names a family panewire accepts. The bound job's recorded
// family wins over the PR trailer, as it does for the review spawn; both are
// bounded by panewireBuilderFamily. The audit row records what was sent.
func TestConsultRequestSendsBoundedAuthorFamily(t *testing.T) {
	cases := []struct {
		name       string
		trailer    string
		jobFamily  string
		unbindJob  bool
		wantJob    bool
		wantFamily string
		wantSource string
	}{
		{
			name:       "job record cognition is bounded to other",
			jobFamily:  "cognition",
			wantJob:    true,
			wantFamily: "other",
			wantSource: "job",
		},
		{
			name:       "no bound job, trailer cognition is bounded to other",
			trailer:    "cognition",
			unbindJob:  true,
			wantFamily: "other",
			wantSource: "trailer",
		},
		{
			name:       "trailer and job record anthropic pass through",
			trailer:    "anthropic",
			jobFamily:  "anthropic",
			wantJob:    true,
			wantFamily: "anthropic",
			wantSource: "job",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, g, _, sink, run, hub, r, _ := hubConsultSetup(t, 3)
			g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
			if tc.trailer != "" {
				g.pull.Body += "Mergewake-Author-Family: " + tc.trailer + "\n"
			}
			if tc.unbindJob {
				delete(sink.jobs, "2481")
			} else {
				sink.recs["w-7"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-7", OwnerLane: "integrator", Family: tc.jobFamily}
			}
			hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}
			var out bytes.Buffer
			s.Out = &out

			deliver(t, s, r)

			if run.requestCount() != 1 {
				t.Fatalf("want 1 consult request, got %d: %v", run.requestCount(), run.requests)
			}
			args := run.lastRequest()
			if argvHas(args, "--author-job", "w-7") != tc.wantJob {
				t.Fatalf("--author-job bound=%v: %v", tc.wantJob, args)
			}
			if !argvHas(args, "--author-family", tc.wantFamily) {
				t.Fatalf("want --author-family %q: %v", tc.wantFamily, args)
			}
			rows := auditRecords(t, out.Bytes(), "consult-request")
			if len(rows) != 1 {
				t.Fatalf("consult-request rows=%d: %s", len(rows), out.String())
			}
			if rows[0]["author_family"] != tc.wantFamily || rows[0]["family_source"] != tc.wantSource {
				t.Fatalf("audit family=%v source=%v, want %q/%q", rows[0]["author_family"], rows[0]["family_source"], tc.wantFamily, tc.wantSource)
			}
		})
	}
}
