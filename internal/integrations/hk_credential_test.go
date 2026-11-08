package integrations

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

// hkCredentialKeys is every variable the hk HTTP credential could come from,
// the hk CLI's HANDOFFKEEP_TOKEN included, so a test starts from none of them.
var hkCredentialKeys = []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET"}

// clearHKEnv points HOME at an empty directory and empties every credential
// variable, so neither the host's environment nor its hk CLI config reaches
// the test.
func clearHKEnv(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, k := range hkCredentialKeys {
		t.Setenv(k, "")
	}
	return home
}

// writeCLIConfig writes a fake hk CLI config.env under home.
func writeCLIConfig(t *testing.T, home, body string) {
	t.Helper()
	dir := filepath.Join(home, ".config", "handoffkeep")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.env"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The hk CLI's own config.env is not a source: a URL and token present only
// there leave the endpoint unconfigured, whichever token key it holds.
func TestHKEndpointIgnoresCLIConfigEnv(t *testing.T) {
	home := clearHKEnv(t)
	writeCLIConfig(t, home, "HANDOFFKEEP_URL=https://hk.example.invalid\n"+
		"export HANDOFFKEEP_TOKEN=cli-config-token\n"+
		"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE=svc-config-token\n")
	if c, e := hkJobEndpoint(); !errors.Is(e, errEndpointUnconfigured) {
		t.Fatalf("config.env-only credential used: err=%v configured=%t", e, c != nil)
	}
	// With the URL in the environment, the token still is not read from the file.
	t.Setenv("HANDOFFKEEP_URL", "https://hk.example.invalid")
	if c, e := hkJobEndpoint(); !errors.Is(e, errEndpointUnconfigured) {
		t.Fatalf("config.env token used: err=%v configured=%t", e, c != nil)
	}
}

// The CF Access pair comes from the environment only: a pair in config.env
// adds no CF headers.
func TestHKEndpointCFPairFromEnvOnly(t *testing.T) {
	home := clearHKEnv(t)
	writeCLIConfig(t, home, "HANDOFFKEEP_CF_ACCESS_CLIENT_ID=cfg-id\nHANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=cfg-secret\n")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.example.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "svc")
	c, e := hkJobEndpoint()
	if e != nil {
		t.Fatalf("svc credential refused: %v", e)
	}
	if c.cfID != "" || c.cfSecret != "" {
		t.Fatal("CF Access pair read from config.env")
	}
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "env-id")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "env-secret")
	if c, e = hkJobEndpoint(); e != nil || c.cfID != "env-id" || c.cfSecret != "env-secret" {
		t.Fatalf("CF Access pair from env not used: err=%v", e)
	}
}

// The hk CLI's machine token HANDOFFKEEP_TOKEN is not mergewake's credential.
func TestHKEndpointIgnoresCLIToken(t *testing.T) {
	clearHKEnv(t)
	t.Setenv("HANDOFFKEEP_URL", "https://hk.example.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN", "cli-token")
	if c, e := hkJobEndpoint(); !errors.Is(e, errEndpointUnconfigured) {
		t.Fatalf("HANDOFFKEEP_TOKEN used: err=%v configured=%t", e, c != nil)
	}
}

// Every mergewake → hk HTTP path sends the svc-mergewake token, never the hk
// CLI's token set next to it.
func TestHKHTTPPathsSendSvcToken(t *testing.T) {
	var mu sync.Mutex
	auth := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.Method + " " + r.URL.Path
		mu.Lock()
		auth[path] = r.Header.Get("Authorization")
		mu.Unlock()
		switch path {
		case "GET /v1/jobs/job-1":
			_, _ = io.WriteString(w, `{"state":"completed"}`)
		case "GET /v1/changes":
			_, _ = io.WriteString(w, `{"changes":[]}`)
		case "POST /v1/changes/batch":
			// An hk without the batch route makes the writer fall back to
			// PUT /v1/changes/<section>.
			http.NotFound(w, r)
		case "PUT /v1/changes/state":
			_, _ = io.WriteString(w, `{"sections":{"state":{"applied":true}}}`)
		case "POST /v1/task-events":
			_, _ = io.WriteString(w, `{"applied":true,"state":"open","task":{"id":5}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	clearHKEnv(t)
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "cli-token")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "svc-token")

	ctx := context.Background()
	hk := Handoffkeep{}
	if _, e := hk.Job(ctx, "job-1"); e != nil {
		t.Fatalf("Job: %v", e)
	}
	if _, e := hk.ChangePriority(ctx, "https://github.com/example/project/pull/7"); e != nil {
		t.Fatalf("ChangePriority: %v", e)
	}
	mu.Lock()
	changesAfterPriority := auth["GET /v1/changes"]
	delete(auth, "GET /v1/changes")
	mu.Unlock()
	if _, e := hk.ReviewRecords(ctx, "example/project", 7); e != nil {
		t.Fatalf("ReviewRecords: %v", e)
	}
	w := NewChangeWriter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	at := time.Now().UTC()
	w.Queue("example/project", 7, "state", ChangeSection{At: at, Value: map[string]any{"observed_at": at}})
	drainChange(t, w)
	url := "https://github.com/example/project/pull/7"
	if _, e := hk.PostTaskEvent(ctx, TaskEvent{Source: SourceMerge, SourceRef: url, TaskID: 5, PR: url, At: &at}); e != nil {
		t.Fatalf("PostTaskEvent: %v", e)
	}

	mu.Lock()
	defer mu.Unlock()
	if changesAfterPriority != "Bearer svc-token" {
		t.Errorf("ChangePriority GET /v1/changes: got %q", changesAfterPriority)
	}
	for _, path := range []string{"GET /v1/jobs/job-1", "GET /v1/changes", "POST /v1/changes/batch", "PUT /v1/changes/state", "POST /v1/task-events"} {
		if got, ok := auth[path]; !ok {
			t.Errorf("%s: not requested", path)
		} else if got != "Bearer svc-token" {
			t.Errorf("%s: got %q", path, got)
		}
	}
}

// The startup check names each unset key of the svc credential and never a
// value; it is silent for another sink or a complete credential.
func TestHKEndpointCheckNamesMissingKeys(t *testing.T) {
	hk := config.Config{Sink: config.Sink{Type: "handoffkeep"}}
	for name, tc := range map[string]struct {
		env     map[string]string
		missing []string
		present []string
	}{
		"nothing set":   {missing: []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"}},
		"cli token":     {env: map[string]string{"HANDOFFKEEP_URL": "https://hk-url-value.invalid", "HANDOFFKEEP_TOKEN": "cli-token-value"}, missing: []string{"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"}, present: []string{"HANDOFFKEEP_URL"}},
		"url missing":   {env: map[string]string{"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE": "svc-token-value"}, missing: []string{"HANDOFFKEEP_URL"}, present: []string{"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"}},
		"blank token":   {env: map[string]string{"HANDOFFKEEP_URL": "https://hk-url-value.invalid", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE": "  "}, missing: []string{"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"}, present: []string{"HANDOFFKEEP_URL"}},
		"complete":      {env: map[string]string{"HANDOFFKEEP_URL": "https://hk-url-value.invalid", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE": "svc-token-value"}},
		"config.env":    {env: map[string]string{"config.env": "HANDOFFKEEP_URL=https://hk-url-value.invalid\nHANDOFFKEEP_TOKEN_SVC_MERGEWAKE=svc-token-value\n"}, missing: []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"}},
		"cf pair alone": {env: map[string]string{"HANDOFFKEEP_CF_ACCESS_CLIENT_ID": "cf-id-value", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET": "cf-secret-value"}, missing: []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE"}},
	} {
		t.Run(name, func(t *testing.T) {
			home := clearHKEnv(t)
			var values []string
			for k, v := range tc.env {
				if k == "config.env" {
					writeCLIConfig(t, home, v)
					values = append(values, "hk-url-value", "svc-token-value")
					continue
				}
				t.Setenv(k, v)
				if strings.TrimSpace(v) != "" {
					values = append(values, v)
				}
			}
			e := HKEndpointCheck(hk)
			if len(tc.missing) == 0 {
				if e != nil {
					t.Fatalf("complete credential reported: %v", e)
				}
				return
			}
			if !errors.Is(e, errEndpointUnconfigured) {
				t.Fatalf("not an unconfigured error: %v", e)
			}
			msg := e.Error()
			for _, k := range tc.missing {
				if !strings.Contains(msg, k) {
					t.Errorf("missing key %s not named: %q", k, msg)
				}
			}
			for _, k := range tc.present {
				if strings.Contains(msg, k) {
					t.Errorf("set key %s named as missing: %q", k, msg)
				}
			}
			for _, v := range values {
				if strings.Contains(msg, v) {
					t.Errorf("error carries a value: %q", msg)
				}
			}
		})
	}
	clearHKEnv(t)
	for _, sink := range []string{"", "noop"} {
		if e := HKEndpointCheck(config.Config{Sink: config.Sink{Type: sink}}); e != nil {
			t.Fatalf("sink %q: %v", sink, e)
		}
	}
}
