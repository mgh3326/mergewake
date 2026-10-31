package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The record read sends the bearer credential and returns the record's
// projection; a 404 is ErrJobNotFound and a redirect is never followed.
func TestHKJobsGet(t *testing.T) {
	var auth, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, path = r.Header.Get("Authorization"), r.URL.Path
		json.NewEncoder(w).Encode(map[string]any{"state": "completed", "family": "openai", "result": map[string]any{"findings": []any{}}})
	}))
	defer srv.Close()
	c := &hkJobs{base: srv.URL, token: "tok"}
	rec, e := c.get(context.Background(), "job-1")
	if e != nil || rec.State != "completed" || rec.Family != "openai" || len(rec.Result) == 0 {
		t.Fatalf("%+v %v", rec, e)
	}
	if auth != "Bearer tok" || path != "/v1/jobs/job-1" {
		t.Fatalf("%q %q", auth, path)
	}

	srv404 := httptest.NewServer(http.NotFoundHandler())
	defer srv404.Close()
	c.base = srv404.URL
	if _, e = c.get(context.Background(), "job-1"); !errors.Is(e, ErrJobNotFound) {
		t.Fatal(e)
	}
}

// Job records go through the sink's credential convention: env wins,
// config.env fills; an http URL off loopback is refused. Values are never
// printed.
func TestHKJobEndpoint(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	for name, tc := range map[string]struct {
		env map[string]string
		err string
	}{
		"unconfigured":  {env: map[string]string{}, err: "unconfigured"},
		"http remote":   {env: map[string]string{"HANDOFFKEEP_URL": "http://hk.example.invalid", "HANDOFFKEEP_TOKEN": "t"}, err: "HTTPS"},
		"http loopback": {env: map[string]string{"HANDOFFKEEP_URL": "http://127.0.0.1:9", "HANDOFFKEEP_TOKEN": "t"}},
		"userinfo":      {env: map[string]string{"HANDOFFKEEP_URL": "https://u:p@hk.example.invalid", "HANDOFFKEEP_TOKEN": "t"}, err: "invalid"},
		"cf half pair":  {env: map[string]string{"HANDOFFKEEP_URL": "https://hk.example.invalid", "HANDOFFKEEP_TOKEN": "t", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID": "id"}, err: "pair"},
		"https ok":      {env: map[string]string{"HANDOFFKEEP_URL": "https://hk.example.invalid", "HANDOFFKEEP_TOKEN": "t"}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, k := range []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET"} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			c, e := hkJobEndpoint()
			if tc.err == "" {
				if e != nil || c.token != "t" {
					t.Fatalf("%v %+v", e, c)
				}
				return
			}
			if e == nil || !strings.Contains(e.Error(), tc.err) {
				t.Fatal(e)
			}
		})
	}
}
