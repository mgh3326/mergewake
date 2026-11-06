package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

const testSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type hookLog struct {
	mu     sync.Mutex
	paths  []string
	bodies []string
}

func (h *hookLog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var b bytes.Buffer
	_, _ = b.ReadFrom(r.Body)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.paths = append(h.paths, r.URL.Path)
	h.bodies = append(h.bodies, b.String())
	w.WriteHeader(http.StatusNoContent)
}

func (h *hookLog) snapshot() ([]string, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.paths...), append([]string(nil), h.bodies...)
}

func discordHost(t *testing.T, hook, extra string) string {
	t.Helper()
	dir := t.TempDir()
	env := filepath.Join(dir, "discord.env")
	if e := os.WriteFile(env, []byte("HOOK="+hook+"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	cfg := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf("version: 1\nhost: host-a\nstate_dir: %s\ndiscord:\n  enabled: true\n  webhooks_file: %s\n  kinds: {deploy: HOOK, pr: HOOK}\n%s", filepath.Join(dir, "state"), env, extra)
	if e := os.WriteFile(cfg, []byte(body), 0o600); e != nil {
		t.Fatal(e)
	}
	return cfg
}

func TestNotifyDeployCommand(t *testing.T) {
	log := &hookLog{}
	srv := httptest.NewServer(log)
	defer srv.Close()
	t.Setenv("GITHUB_TOKEN", "") // needs no GitHub credential
	cfg := discordHost(t, srv.URL+"/deploy", "  opt_out: [example/skipped]\n")
	deploy := func(args ...string) (string, error) {
		var out, stderr bytes.Buffer
		e := run(context.Background(), append([]string{"notify", "deploy", "--config", cfg}, args...), &out, &stderr)
		return out.String(), e
	}
	got, e := deploy("--repo", "example/project", "--sha", testSHA, "--status", "ok", "--text", "pull deploy finished")
	if e != nil || !strings.Contains(got, `"posted":true`) {
		t.Fatalf("%s %v", got, e)
	}
	_, bodies := log.snapshot()
	if len(bodies) != 1 || !strings.Contains(bodies[0], "pull deploy finished") || !strings.Contains(bodies[0], "example/project@aaaaaaa") {
		t.Fatalf("%v", bodies)
	}
	// A deployer's retry posts once.
	if got, e = deploy("--repo", "example/project", "--sha", testSHA, "--status", "ok"); e != nil || !strings.Contains(got, `"posted":false`) {
		t.Fatalf("%s %v", got, e)
	}
	// A failure on the same commit is a different event.
	if got, e = deploy("--repo", "example/project", "--sha", testSHA, "--status", "failed"); e != nil || !strings.Contains(got, `"posted":true`) {
		t.Fatalf("%s %v", got, e)
	}
	// An opted-out repo succeeds quietly, so a deployer needs no special case.
	if got, e = deploy("--repo", "example/skipped", "--sha", testSHA, "--status", "ok"); e != nil || !strings.Contains(got, `"posted":false`) {
		t.Fatalf("%s %v", got, e)
	}
	if paths, _ := log.snapshot(); len(paths) != 2 {
		t.Fatalf("%v", paths)
	}
}

func TestNotifyDeployValidation(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	cfg := discordHost(t, srv.URL+"/s3cr3t", "")
	off := filepath.Join(t.TempDir(), "off.yaml")
	if e := os.WriteFile(off, []byte("version: 1\nhost: host-a\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	good := []string{"--repo", "example/project", "--sha", testSHA, "--status", "ok"}
	for name, tt := range map[string]struct {
		args []string
		want string
	}{
		"no subcommand": {[]string{"notify"}, "usage: mergewake notify deploy"},
		"other subject": {[]string{"notify", "pr"}, "usage: mergewake notify deploy"},
		"bad repo":      {[]string{"notify", "deploy", "--config", cfg, "--repo", "../x", "--sha", testSHA, "--status", "ok"}, "--repo"},
		"bad sha":       {[]string{"notify", "deploy", "--config", cfg, "--repo", "example/project", "--sha", "main", "--status", "ok"}, "--sha"},
		"bad status":    {[]string{"notify", "deploy", "--config", cfg, "--repo", "example/project", "--sha", testSHA, "--status", "maybe"}, "--status"},
		"feed off":      {append([]string{"notify", "deploy", "--config", off}, good...), "discord.enabled"},
		"refused":       {append([]string{"notify", "deploy", "--config", cfg}, good...), "refused"},
	} {
		t.Run(name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			e := run(context.Background(), tt.args, &out, &stderr)
			if e == nil || !strings.Contains(e.Error(), tt.want) || strings.Contains(e.Error(), "s3cr3t") {
				t.Fatalf("%v", e)
			}
		})
	}
}

// The daemon loop, end to end against a fake GitHub and a fake webhook: the pr
// role's pass reaches the webhook through the sender it starts, and the role's
// status records no failure when the webhook is the only thing wrong.
func TestDaemonPostsToDiscord(t *testing.T) {
	home := t.TempDir() // the daemon's role locks live under the cache dir
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", home)
	log := &hookLog{}
	hook := httptest.NewServer(log)
	defer hook.Close()
	merged := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	pull := fmt.Sprintf(`{"number":7,"state":"closed","title":"Add feature","html_url":"https://github.com/example/project/pull/7","merged_at":%q,"closed_at":%q,"user":{"login":"pr-author","type":"User"},"head":{"sha":%q,"ref":"feature"},"base":{"ref":"main"},"changed_files":3,"additions":40,"deletions":2}`, merged, merged, testSHA)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/example/project/pulls" && r.URL.Query().Get("state") == "closed":
			if r.URL.Query().Get("base") != "main" {
				t.Errorf("closed list ignores the branch: %s", r.URL.RawQuery)
			}
			fmt.Fprintf(w, "[%s]", pull)
		case r.URL.Path == "/repos/example/project/pulls":
			fmt.Fprint(w, "[]")
		case r.URL.Path == "/repos/example/project/pulls/7":
			fmt.Fprint(w, pull)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	cfg := discordHost(t, hook.URL+"/pr-log", "repos:\n  - name: example/project\ndaemon:\n  pr: {enabled: true, interval: 1h}\n")
	c, e := config.Load(cfg)
	if e != nil {
		t.Fatal(e)
	}
	gh := github.New("")
	gh.BaseURL = api.URL
	gh.Polling = true
	var out, stderr bytes.Buffer
	s := &pipeline.Service{Config: c, GH: gh, Runner: integrations.Exec{}, Sink: integrations.Noop{}, Notifier: integrations.Noop{}, Out: &lockedWriter{out: &out}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runDaemonLoops(ctx, s, gh, "", pipeline.Options{Shadow: true}, &lockedWriter{out: &stderr})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		paths, bodies := log.snapshot()
		if len(paths) > 0 {
			if paths[0] != "/pr-log" || !strings.Contains(bodies[0], "example/project#7 Add feature") || !strings.Contains(bodies[0], "+40 / -2") {
				t.Fatalf("%v %v", paths, bodies)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing posted; stderr: %s", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the daemon did not stop; the sender must end with it")
	}
	if !strings.Contains(stderr.String(), `"msg":"github requests"`) {
		t.Fatalf("missing request summary: %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), `"by_credential":{"user":`) {
		t.Fatalf("request summary lacks the per-credential split: %s", stderr.String())
	}
}
