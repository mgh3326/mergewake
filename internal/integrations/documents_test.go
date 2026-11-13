package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// (e) DecisionDocument decodes the response's body field; a response without
// it still verifies with an empty text.
func TestDecisionDocumentBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/v1/documents/")
		switch key {
		case "decision/with-body":
			fmt.Fprint(w, `{"key":"decision/with-body","updated_at":"2026-11-10T00:00:00Z","body":"first line\nsecond line"}`)
		case "decision/no-body":
			fmt.Fprint(w, `{"key":"decision/no-body","updated_at":"2026-11-10T00:00:00Z"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")

	doc, err := Handoffkeep{}.DecisionDocument(context.Background(), "decision/with-body")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"body":"first line\nsecond line"`) {
		t.Fatalf("document body not carried: %s", raw)
	}

	doc, err = Handoffkeep{}.DecisionDocument(context.Background(), "decision/no-body")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Key != "decision/no-body" || doc.UpdatedAt == "" {
		t.Fatalf("no-body response did not verify: %+v", doc)
	}
}
