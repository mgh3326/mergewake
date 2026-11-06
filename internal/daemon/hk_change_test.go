package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/github/webhookstate"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestHKChangeWebhookDelivery(t *testing.T) {
	var mu sync.Mutex
	var batches []map[string]any
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		fmt.Fprint(w, `{"sections":{"state":{"applied":true}}}`)
	}))
	defer hk.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	var triggers []string
	d := webhookDaemon(&triggers)
	d.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	state := webhookstate.Open(t.TempDir())
	g := github.New("")
	g.Webhook = state
	g.Changes = integrations.NewChangeWriter(nil)
	d.Webhook.State = state
	d.Webhook.Applied = g.WebhookDelivery
	server := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer server.Close()
	at := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 50; i++ {
		body := fmt.Sprintf(`{"action":"synchronize","repository":{"full_name":"o/r"},"pull_request":{"number":7,"state":"open","head":{"sha":%q},"base":{"sha":%q,"ref":"main"},"updated_at":%q}}`, strings.Repeat("a", 40), strings.Repeat("b", 40), at.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano))
		resp := post(t, server.Client(), server.URL+"/github", "pull_request", fmt.Sprint(i), []byte(body), testSecret)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatal(resp.StatusCode)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g.Changes.Wait(ctx)
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches) > 3 || len(batches) == 0 {
		t.Fatalf("batches=%d", len(batches))
	}
	stateWrite := batches[len(batches)-1]["state"].(map[string]any)
	if stateWrite["source"] != "webhook" || stateWrite["delivery_id"] != "49" || stateWrite["observed_at"] != at.Add(49*time.Millisecond).Format(time.RFC3339Nano) {
		t.Fatalf("state=%v", stateWrite)
	}
}

func TestHKChangeUnavailableDoesNotFailDelivery(t *testing.T) {
	hk := httptest.NewServer(http.NotFoundHandler())
	defer hk.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	var triggered []string
	d := webhookDaemon(&triggered)
	d.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	st := webhookstate.Open(t.TempDir())
	g := github.New("")
	g.Webhook = st
	g.Changes = integrations.NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.Webhook.State = st
	d.Webhook.Applied = g.WebhookDelivery
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	body := fmt.Sprintf(`{"repository":{"full_name":"o/r"},"pull_request":{"number":7,"state":"open","head":{"sha":%q},"base":{"sha":%q,"ref":"main"},"updated_at":%q}}`, strings.Repeat("a", 40), strings.Repeat("b", 40), time.Now().UTC().Format(time.RFC3339))
	resp := post(t, srv.Client(), srv.URL+"/github", "pull_request", "one", []byte(body), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatal(resp.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g.Changes.Wait(ctx)
	if g.Changes.Counts().Unavailable != 1 {
		t.Fatal(g.Changes.Counts())
	}
}

func TestHKChangeWebhookChecksKeepOutOfOrderFacts(t *testing.T) {
	var mu sync.Mutex
	var batches []map[string]any
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		fmt.Fprint(w, `{"sections":{"checks":{"applied":true}}}`)
	}))
	defer hk.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	st := webhookstate.Open(t.TempDir())
	g := github.New("")
	g.Webhook = st
	g.Changes = integrations.NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	sha := strings.Repeat("a", 40)
	at := time.Now().UTC().Truncate(time.Second)
	for i, name := range []string{"newer", "older"} {
		stamp := at.Add(-time.Duration(i) * time.Minute)
		body := []byte(fmt.Sprintf(`{"check_run":{"id":%d,"name":%q,"head_sha":%q,"status":"completed","conclusion":"success","started_at":%q,"completed_at":%q,"check_suite":{"id":%d},"pull_requests":[{"number":7}]}}`, i+1, name, sha, stamp.Format(time.RFC3339), stamp.Format(time.RFC3339), i+1))
		applied, err := st.ApplyObserved("o/r", "check_run", fmt.Sprint(i), body)
		if err != nil || !applied {
			t.Fatalf("applied=%v err=%v", applied, err)
		}
		g.WebhookDelivery("o/r", "check_run", fmt.Sprint(i), body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g.Changes.Wait(ctx)
	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 1 {
		t.Fatalf("batches=%v", batches)
	}
	checks := batches[0]["checks"].(map[string]any)
	if checks["observed_at"] != at.Format(time.RFC3339) || len(checks["checks"].([]any)) != 2 {
		t.Fatalf("checks=%v", checks)
	}
	if _, ok := checks["delivery_id"]; ok {
		t.Fatalf("unexpected check delivery ID: %v", checks)
	}
}
