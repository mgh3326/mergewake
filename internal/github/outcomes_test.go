package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFirstCICannotBeReplacedByRerunOrNewHead(t *testing.T) {
	for _, conclusion := range []string{"failure", "success", "pending"} {
		t.Run(conclusion, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/actions/runs"):
					fmt.Fprint(w, `{"workflow_runs":[{"id":99,"workflow_id":1,"created_at":"2026-01-02T00:00:00Z","pull_requests":[{"number":7}]},{"id":10,"workflow_id":1,"created_at":"2026-01-01T00:00:00Z","pull_requests":[{"number":7}]}]}`)
				case strings.Contains(r.URL.Path, "/runs/10/attempts/1/jobs"):
					status := "completed"
					if conclusion == "pending" {
						status = "in_progress"
					}
					fmt.Fprintf(w, `{"jobs":[{"name":"ci-ok","status":%q,"conclusion":%q}]}`, status, conclusion)
				default:
					t.Errorf("read newer run instead of initial attempt: %s", r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			c := New("")
			c.BaseURL = server.URL
			v, err := c.FirstPRCI(context.Background(), "example/project", Pull{Number: 7}, []string{"ci-ok"})
			if err != nil {
				t.Fatal(err)
			}
			if conclusion == "pending" {
				if v != nil {
					t.Fatal(*v)
				}
			} else if v == nil || *v != (conclusion == "success") {
				t.Fatal(v)
			}
		})
	}
}
