package integrations

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestHKRouteTemplates(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{http.MethodGet, "/v1/tasks/42", "GET /v1/tasks/{id}"},
		{http.MethodGet, "/v1/jobs/job-5441-forwarder", "GET /v1/jobs/{id}"},
		{http.MethodPost, "/v1/tasks/42/comments", "POST /v1/tasks/{id}/comments"},
		{http.MethodPost, "/v1/tasks/42/reopen", "POST /v1/tasks/{id}/reopen"},
		{http.MethodPost, "/v1/tasks/42/decision-request", "POST /v1/tasks/{id}/decision-request"},
		{http.MethodPost, "/v1/changes/batch", "POST /v1/changes/batch"},
		{http.MethodPut, "/v1/changes/pr-123-abc", "PUT /v1/changes/{id}"},
		{http.MethodGet, "/v1/nodes/machine-7/command", "GET /v1/nodes/{id}/command"},
		{http.MethodGet, "/v1/documents/handoff/plan/42", "GET /v1/documents/{id}"},
		{http.MethodGet, "/v1/changes/unreconciled", "GET /v1/changes/unreconciled"},
	}
	for _, c := range cases {
		req := &http.Request{Method: c.method, URL: &url.URL{Path: c.path, RawQuery: "limit=5&after_id=9"}}
		if got := hkRoute(req); got != c.want {
			t.Errorf("hkRoute(%s %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// Many distinct ids must not grow the route counts: the key set is bounded by
// the route templates, and the serialized counts stay well under the 4 KiB the
// daemon's journal keeps per line.
func TestHKRouteCountsBoundedByTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	client := countedHK(&http.Client{})

	before := len(HKRouteCounts())
	send := func(method, path string) {
		req, err := http.NewRequest(method, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	for i := 0; i < 2000; i++ {
		send(http.MethodGet, fmt.Sprintf("/v1/tasks/%d", i))
		send(http.MethodGet, fmt.Sprintf("/v1/jobs/job-%d-forwarder", i))
		send(http.MethodPost, fmt.Sprintf("/v1/tasks/%d/comments?ts=%d", i, i))
		send(http.MethodPut, fmt.Sprintf("/v1/changes/pr-%d", i))
		send(http.MethodGet, fmt.Sprintf("/v1/documents/handoff/plan-%d/part", i))
	}
	send(http.MethodPost, "/v1/changes/batch")

	counts := HKRouteCounts()
	if added := len(counts) - before; added > 6 {
		t.Errorf("distinct ids added %d route keys, want at most 6 templates", added)
	}
	line, err := json.Marshal(counts)
	if err != nil {
		t.Fatal(err)
	}
	if len(line) >= 4096 {
		t.Errorf("route counts serialize to %d bytes, want under 4096", len(line))
	}
	want := map[string]int{
		"GET /v1/tasks/{id}":           2000,
		"POST /v1/tasks/{id}/comments": 2000,
		"PUT /v1/changes/{id}":         2000,
		"GET /v1/documents/{id}":       2000,
		"GET /v1/jobs/{id}":            2000,
		"POST /v1/changes/batch":       1,
	}
	got := map[string]int{}
	for _, c := range counts {
		got[c.Route] = c.Count
	}
	for route, n := range want {
		if got[route] < n {
			t.Errorf("route %q counted %d, want at least %d", route, got[route], n)
		}
	}
}
