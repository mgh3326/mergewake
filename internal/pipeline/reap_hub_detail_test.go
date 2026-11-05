package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A node that fails job.done answers HTTP 200 with error and detail; the
// job-done receipt reason carries both, not "missing result".
func TestReapJobDoneReceiptReadsHubErrorDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"request_id": "cmd-1", "machine": "node-a", "op": "job.done", "error": "done_failed", "detail": "job record unreadable"})
	}))
	t.Cleanup(server.Close)
	hs := &reapHost{ops: reap.HubHost{Hub: &reap.Hub{URL: server.URL, Client: "svc-mergewake", Token: "svc-token"}, Machine: "node-a"}}
	var got []reapReceipt
	(&Service{}).reapJobDone(context.Background(), reapReceipt{Job: "lead-a", Saved: "/jobs/lead-a/pane-screen.txt"}, hs, integrations.JobRecord{State: "spawned"}, true, func(v reapReceipt) { got = append(got, v) })
	if len(got) != 1 || got[0].Action != "job-done" || got[0].Result != "error" || got[0].Reason != "hub 200 done_failed: job record unreadable" {
		t.Fatalf("receipts: %+v", got)
	}
}
