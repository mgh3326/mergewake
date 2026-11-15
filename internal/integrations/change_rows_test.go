package integrations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The read sends the limit, decodes the rows oldest first, and reports a
// missing endpoint (404), another failure, and an unconfigured host.
func TestUnreconciledChanges(t *testing.T) {
	var gotPath, gotLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotLimit = r.URL.Path, r.URL.Query().Get("limit")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changes":[{"id":7,"repo":"o/r","pr_number":11,"state":"open"},{"id":9,"repo":"o/s","pr_number":2,"state":"merged"}]}`))
	}))
	defer srv.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
	rows, e := Handoffkeep{}.UnreconciledChanges(context.Background(), 500)
	if e != nil || len(rows) != 2 {
		t.Fatalf("%+v %v", rows, e)
	}
	if rows[0] != (UnreconciledChange{ID: 7, Repo: "o/r", PRNumber: 11, State: "open"}) {
		t.Fatalf("first row %+v", rows[0])
	}
	if rows[1].ID != 9 || rows[1].State != "merged" {
		t.Fatalf("second row %+v", rows[1])
	}
	if gotPath != "/v1/changes/unreconciled" || gotLimit != "500" {
		t.Fatalf("path %q limit %q", gotPath, gotLimit)
	}

	for name, status := range map[string]int{"missing endpoint": http.StatusNotFound, "server error": http.StatusBadGateway} {
		t.Run(name, func(t *testing.T) {
			fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer fail.Close()
			t.Setenv("HANDOFFKEEP_URL", fail.URL)
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
			if _, e := (Handoffkeep{}).UnreconciledChanges(context.Background(), 10); e == nil {
				t.Fatal("hk failure hidden")
			}
		})
	}

	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	if _, e := (Handoffkeep{}).UnreconciledChanges(context.Background(), 10); e == nil {
		t.Fatal("unconfigured endpoint is not an error")
	}
}
