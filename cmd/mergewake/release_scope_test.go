package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// requestLog records every request that reaches the fake GitHub.
type requestLog struct {
	next  http.RoundTripper
	mu    sync.Mutex
	calls []string
}

func (l *requestLog) RoundTrip(r *http.Request) (*http.Response, error) {
	l.mu.Lock()
	l.calls = append(l.calls, r.Method+" "+r.URL.Path)
	l.mu.Unlock()
	return l.next.RoundTrip(r)
}

// to returns the requests addressed to one repository.
func (l *requestLog) to(repo string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, c := range l.calls {
		if strings.Contains(c, " /repos/"+repo+"/") {
			out = append(out, c)
		}
	}
	return out
}

func (l *requestLog) has(repo, fragment string) bool {
	for _, c := range l.to(repo) {
		if strings.Contains(c, fragment) {
			return true
		}
	}
	return false
}

// roleScope runs one pass of a single daemon role over two repositories with a
// green main: o/with has an install inventory, o/without has none.
func roleScope(t *testing.T, role string) *requestLog {
	t.Helper()
	m := newMultiRepo(t, []string{"o/with", "o/without"}, 0, 1)
	m.s.Config.Daemon.PR.Loop.Enabled = false
	loop := config.Loop{Enabled: true, Interval: "1h"}
	switch role {
	case "release":
		m.s.Config.Daemon.Release = loop
	case "rollout":
		m.s.Config.Daemon.Rollout = loop
	}
	m.s.Config.Repos[0].Install = []config.Install{{Kind: "command", Target: "bin/tool"}}
	m.s.Config.Repos[0].Smoke.Command = "true"
	m.s.Config.Repos[1].Smoke.Command = "true"
	log := &requestLog{next: m.gh.HTTP.Transport}
	m.gh.HTTP = &http.Client{Transport: log}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exit := make(chan error, 1)
	go func() { exit <- runDaemonLoops(ctx, m.s, m.gh, "", pipeline.Options{}, io.Discard) }()
	waitFor(t, role+" pass", func() bool {
		var st daemon.Status
		if b, err := os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", role+".json")); err == nil {
			_ = json.Unmarshal(b, &st)
		}
		return !st.Running && !st.LastFinish.IsZero()
	})
	cancel()
	select {
	case err := <-exit:
		if err != nil {
			t.Fatalf("daemon: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("daemon did not stop")
	}
	return log
}

func TestDaemonReleaseSkipsRepoWithoutInstall(t *testing.T) {
	log := roleScope(t, "release")
	if !log.has("o/with", "/git/ref/tags/") {
		t.Fatalf("repo with install did not reach Publish: %v", log.to("o/with"))
	}
	if got := log.to("o/without"); len(got) != 0 {
		t.Fatalf("repo without install was read: %v", got)
	}
}

func TestDaemonRolloutSkipsRepoWithoutInstall(t *testing.T) {
	log := roleScope(t, "rollout")
	if len(log.to("o/with")) == 0 {
		t.Fatal("repo with install was not rolled out")
	}
	if got := log.to("o/without"); len(got) != 0 {
		t.Fatalf("repo without install was read: %v", got)
	}
}
