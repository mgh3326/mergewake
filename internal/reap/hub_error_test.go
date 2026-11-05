package reap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func jobDoneErr(t *testing.T, hub *Hub) error {
	t.Helper()
	err := (HubHost{Hub: hub, Machine: "node-a"}).JobDone(context.Background(), "job-a", "/jobs/job-a/report.md")
	if err == nil {
		t.Fatal("JobDone accepted the answer")
	}
	return err
}

func hubAnswering(status int, body any) func(*testing.T) *Hub {
	return func(t *testing.T) *Hub {
		hub, _ := fakeHub(t, func(hubRequest) (int, any) { return status, body })
		return hub
	}
}

// (a)-(e): the hub's error and detail reach the caller whatever the status.
func TestHubCommandErrorMessages(t *testing.T) {
	for _, test := range []struct {
		name string
		hub  func(*testing.T) *Hub
		want string
	}{
		{"200 error with detail", hubAnswering(200, map[string]any{"request_id": "r", "op": "job.done", "error": "done_failed", "detail": "job record unreadable"}), "hub 200 done_failed: job record unreadable"},
		{"200 error without detail", hubAnswering(200, map[string]any{"error": "done_failed"}), "hub 200 done_failed"},
		{"200 error beside a result", hubAnswering(200, map[string]any{"error": "done_failed", "detail": "x", "result": map[string]any{"outcome": "done"}}), "hub 200 done_failed: x"},
		{"200 neither error nor result", hubAnswering(200, map[string]any{"op": "job.done"}), "hub job.done: missing result"},
		{"504 node_timeout", hubAnswering(504, map[string]any{"error": "node_timeout"}), "hub 504 node_timeout"},
		{"503 node_lost", hubAnswering(503, map[string]any{"error": "node_lost"}), "hub 503 node_lost"},
		{"502 detail", hubAnswering(502, map[string]any{"error": "node_unavailable", "detail": "no route"}), "hub 502 node_unavailable: no route"},
		{"502 non-JSON body", func(t *testing.T) *Hub {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("<html>bad gateway</html>"))
			}))
			t.Cleanup(server.Close)
			return &Hub{URL: server.URL, Client: "svc-mergewake", Token: "svc-token"}
		}, "hub 502 unexpected-response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := jobDoneErr(t, test.hub(t)).Error(); got != test.want {
				t.Fatalf("got %q want %q", got, test.want)
			}
		})
	}
}

func TestHubCommandAuthErrorSurvivesDetail(t *testing.T) {
	for _, status := range []int{401, 403} {
		err := jobDoneErr(t, hubAnswering(status, map[string]any{"error": "scope_forbidden", "detail": "needs job.done"})(t))
		if !errors.Is(err, ErrHubAuth) {
			t.Fatalf("%d: %v", status, err)
		}
	}
	if err := jobDoneErr(t, hubAnswering(200, map[string]any{"error": "scope_forbidden"})(t)); errors.Is(err, ErrHubAuth) {
		t.Fatalf("a 200 answer read as an auth error: %v", err)
	}
}

func TestHubErrorDetailIsBoundedAndRedacted(t *testing.T) {
	secret := "ghp_" + strings.Repeat("a1B2c3D4e5", 4)
	err := jobDoneErr(t, hubAnswering(200, map[string]any{"error": "done_failed", "detail": "token " + secret + " leaked"})(t))
	if strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "[redacted]") {
		t.Fatalf("not redacted: %q", err)
	}
	long := strings.Repeat("가", 200) // 600 bytes
	err = jobDoneErr(t, hubAnswering(200, map[string]any{"error": "done_failed", "detail": long})(t))
	detail := strings.TrimPrefix(err.Error(), "hub 200 done_failed: ")
	if len(detail) == 0 || len(detail) > 240 || strings.ContainsRune(detail, '�') || !strings.HasPrefix(long, detail) {
		t.Fatalf("detail %d bytes: %q", len(detail), detail)
	}
}
