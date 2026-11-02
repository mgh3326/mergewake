package reap

import (
	"context"
	"net/http"
	"testing"
)

func TestHubComposerFailSafe(t *testing.T) {
	for _, test := range []struct {
		outcome, composer, want string
		status                  int
	}{
		{"read", "empty", ComposerEmpty, 200}, {"read", "has_text", ComposerText, 200},
		{"read", "unknown", ComposerUnknown, 200}, {"read", "", ComposerUnknown, 200},
		{"read", "bogus", ComposerUnknown, 200}, {"pane-gone", "empty", ComposerUnknown, 200},
		{"", "empty", ComposerUnknown, 200}, {"read", "empty", ComposerUnknown, 502},
	} {
		hub, _ := fakeHub(t, func(r hubRequest) (int, any) {
			if r.body["op"] != "pane.read" {
				t.Errorf("op=%v", r.body)
			}
			args := r.body["args"].(map[string]any)
			if args["pane_id"] != "w1:p1" || args["name"] != "job-a" || args["cwd"] != "/work/repo" || args["tail_lines"] != float64(0) {
				t.Errorf("args=%v", args)
			}
			return test.status, map[string]any{"result": map[string]any{"outcome": test.outcome, "composer": test.composer}}
		})
		got, err := (HubHost{Hub: hub, Machine: "node-a"}).Composer(context.Background(), Pane{ID: "w1:p1", Name: "job-a", Cwd: "/work/repo"})
		if got != test.want || (err != nil) != (test.status != http.StatusOK) {
			t.Fatalf("%+v: got=%s err=%v", test, got, err)
		}
	}
}

func TestHubJobDoneOutcomes(t *testing.T) {
	for _, outcome := range []string{"done", "already-done", "not-a-job", "report-invalid", "", "bogus"} {
		hub, _ := fakeHub(t, func(r hubRequest) (int, any) {
			args := r.body["args"].(map[string]any)
			if r.body["op"] != "job.done" || args["job"] != "job-a" || args["report"] != "/jobs/job-a/report.md" {
				t.Errorf("request=%v", r.body)
			}
			return 200, map[string]any{"result": map[string]any{"outcome": outcome}}
		})
		err := (HubHost{Hub: hub, Machine: "node-a"}).JobDone(context.Background(), "job-a", "/jobs/job-a/report.md")
		if (err == nil) != (outcome == "done" || outcome == "already-done") {
			t.Fatalf("%s: %v", outcome, err)
		}
	}
	for _, status := range []int{403, 502} {
		hub, _ := fakeHub(t, func(hubRequest) (int, any) { return status, map[string]any{"error": "done_failed"} })
		if err := (HubHost{Hub: hub, Machine: "node-a"}).JobDone(context.Background(), "job-a", "/jobs/job-a/report.md"); err == nil {
			t.Fatalf("status %d accepted", status)
		}
	}
}

func TestHubJobDoneOmitsEmptyReport(t *testing.T) {
	hub, _ := fakeHub(t, func(r hubRequest) (int, any) {
		args := r.body["args"].(map[string]any)
		if r.body["op"] != "job.done" || args["job"] != "job-a" || len(args) != 1 {
			t.Errorf("request=%v", r.body)
		}
		return 200, map[string]any{"result": map[string]any{"outcome": "done"}}
	})
	if err := (HubHost{Hub: hub, Machine: "node-a"}).JobDone(context.Background(), "job-a", ""); err != nil {
		t.Fatal(err)
	}
}
