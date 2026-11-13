package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestDecisionProvenance(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing service credential")
		}
		key := strings.TrimPrefix(r.URL.Path, "/v1/documents/")
		if strings.HasSuffix(key, "missing") {
			http.NotFound(w, r)
			return
		}
		if strings.HasSuffix(key, "slow") {
			<-r.Context().Done()
			return
		}
		fmt.Fprintf(w, `{"key":%q,"updated_at":"2026-11-10T00:00:00Z","body":"SECRET DOCUMENT BODY"}`, key)
	}))
	defer server.Close()
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	body := "## Operator decisions\n- verified hk:doc decision/one\n- again hk:doc decision/one\n- missing hk:doc decision/missing\n- wrong hk:doc reports/other\n- plain claim\n"
	verified := resolveDecisions(context.Background(), body, integrations.Handoffkeep{})
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{contract: "scope", contractSource: "PR"}, "", verified)
	for _, want := range []string{"VERIFIED OPERATOR DECISIONS", "decision/one (updated_at: 2026-11-10T00:00:00Z):\n- verified", "document excerpt (20 of 20 bytes):\nSECRET DOCUMENT BODY", "CLAIMED DECISIONS", "- missing hk:doc decision/missing", "- wrong hk:doc reports/other", "- plain claim", "a regression this change introduces", "not a blanket waiver"} {
		if !strings.Contains(rc.text, want) {
			t.Errorf("missing %q in %s", want, rc.text)
		}
	}
	for _, bad := range []string{"Do not report " + "anything they decide", "OPERATOR DECISIONS (from the PR description, " + "verbatim"} {
		if strings.Contains(rc.text, bad) {
			t.Errorf("unexpected %q", bad)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["/v1/documents/decision/one"] != 1 || calls["/v1/documents/decision/missing"] != 1 || len(calls) != 2 {
		t.Fatal(calls)
	}
	if _, ok := verified["decision/missing"]; ok {
		t.Fatal("404 verified")
	}
}

func TestDecisionLookupLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/v1/documents/")
		fmt.Fprintf(w, `{"key":%q,"updated_at":"2026-11-10T00:00:00Z"}`, key)
	}))
	defer server.Close()
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	var body strings.Builder
	body.WriteString("## Operator decisions\n")
	for i := 0; i < 6; i++ {
		fmt.Fprintf(&body, "- item %d hk:doc decision/key%d\n", i, i)
	}
	verified := resolveDecisions(context.Background(), body.String(), integrations.Handoffkeep{})
	rc := buildRoundContext(body.String(), github.ReviewFeedback{}, roundPlan{}, "", verified)
	if len(verified) != 5 || !strings.Contains(rc.text[strings.Index(rc.text, "CLAIMED DECISIONS"):], "item 5 hk:doc decision/key5") {
		t.Fatal(verified, rc.text)
	}
}

func TestDecisionUnavailableAndNoSection(t *testing.T) {
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	body := "## Operator decisions\n- claim hk:doc decision/one"
	verified := resolveDecisions(context.Background(), body, integrations.Handoffkeep{})
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{}, "", verified)
	if len(verified) != 0 || strings.Contains(rc.text, "VERIFIED OPERATOR DECISIONS") || !strings.Contains(rc.text, "CLAIMED DECISIONS") {
		t.Fatal(rc.text)
	}
	if rc := buildRoundContext("no section hk:doc decision/one", github.ReviewFeedback{}, roundPlan{}, "", verified); rc.text != "" {
		t.Fatal(rc.text)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if got := resolveDecisions(ctx, body, integrations.Handoffkeep{}); len(got) != 0 {
		t.Fatal(got)
	}
}
