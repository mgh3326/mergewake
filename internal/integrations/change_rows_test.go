package integrations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The read sends the limit and after_id, decodes the rows oldest first, and
// reports a missing endpoint (404), another failure, and an unconfigured
// host.
func TestUnreconciledChanges(t *testing.T) {
	var gotPath, gotLimit, gotAfter string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotLimit, gotAfter = r.URL.Path, r.URL.Query().Get("limit"), r.URL.Query().Get("after_id")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changes":[{"id":7,"repo":"o/r","pr_number":11,"state":"open"},{"id":9,"repo":"o/s","pr_number":2,"state":"merged"}]}`))
	}))
	defer srv.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
	page, e := Handoffkeep{}.UnreconciledChanges(context.Background(), 0, 500)
	if e != nil || len(page.Changes) != 2 {
		t.Fatalf("%+v %v", page, e)
	}
	if page.Changes[0] != (UnreconciledChange{ID: 7, Repo: "o/r", PRNumber: 11, State: "open"}) {
		t.Fatalf("first row %+v", page.Changes[0])
	}
	if page.Changes[1].ID != 9 || page.Changes[1].State != "merged" {
		t.Fatalf("second row %+v", page.Changes[1])
	}
	if gotPath != "/v1/changes/unreconciled" || gotLimit != "500" || gotAfter != "0" {
		t.Fatalf("path %q limit %q after_id %q", gotPath, gotLimit, gotAfter)
	}
	// No next_after_id key: an hk without after_id paging.
	if page.Paged || page.NextAfterID != nil {
		t.Fatalf("page %+v", page)
	}

	for name, status := range map[string]int{"missing endpoint": http.StatusNotFound, "server error": http.StatusBadGateway} {
		t.Run(name, func(t *testing.T) {
			fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer fail.Close()
			t.Setenv("HANDOFFKEEP_URL", fail.URL)
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
			if _, e := (Handoffkeep{}).UnreconciledChanges(context.Background(), 0, 10); e == nil {
				t.Fatal("hk failure hidden")
			}
		})
	}

	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	if _, e := (Handoffkeep{}).UnreconciledChanges(context.Background(), 0, 10); e == nil {
		t.Fatal("unconfigured endpoint is not an error")
	}
}

// A paging hk answers next_after_id with the following page's cursor, and
// null at the list's end; the page reports which it got.
func TestUnreconciledChangesPaging(t *testing.T) {
	for name, tc := range map[string]struct {
		body  string
		paged bool
		next  *int64
	}{
		"cursor": {`{"changes":[],"next_after_id":41}`, true, ptr(int64(41))},
		"end":    {`{"changes":[],"next_after_id":null}`, true, nil},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			t.Setenv("HOME", t.TempDir())
			t.Setenv("HANDOFFKEEP_URL", srv.URL)
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "tok")
			page, e := Handoffkeep{}.UnreconciledChanges(context.Background(), 40, 10)
			if e != nil {
				t.Fatal(e)
			}
			if page.Paged != tc.paged {
				t.Fatalf("paged %v, want %v", page.Paged, tc.paged)
			}
			if tc.next == nil {
				if page.NextAfterID != nil {
					t.Fatalf("next %v, want nil", *page.NextAfterID)
				}
			} else if page.NextAfterID == nil || *page.NextAfterID != *tc.next {
				t.Fatalf("next %v, want %d", page.NextAfterID, *tc.next)
			}
		})
	}
}

func ptr(v int64) *int64 { return &v }
