package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/prstatus"
)

func prStatusHost(t *testing.T) (cfg, stateDir string) {
	t.Helper()
	dir := t.TempDir()
	stateDir = filepath.Join(dir, "state")
	cfg = filepath.Join(dir, "config.yaml")
	if e := os.WriteFile(cfg, []byte("version: 1\nhost: host-a\nstate_dir: "+stateDir+"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	return cfg, stateDir
}

// (e) The CLI prints the JSON the HTTP endpoint returns for the same snapshot,
// from local state alone.
func TestPRStatusCLIPrintsTheEndpointJSON(t *testing.T) {
	cfg, stateDir := prStatusHost(t)
	snap := prstatus.Snapshot{Repo: "o/r", PR: 7, Head: "aaaaaaa", State: "blocked", Reasons: []string{"review_missing"},
		Checks: []prstatus.Check{{Name: "test", State: "green"}}, ObservedAt: time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC),
		Findings: []prstatus.Finding{{ID: "T1", Location: "a.go:3", Severity: "high", Blocking: true, Summary: "bug"}}}
	if e := prstatus.Write(stateDir, snap); e != nil {
		t.Fatal(e)
	}
	srv := httptest.NewServer(prstatus.Handler{StateDir: stateDir})
	defer srv.Close()
	resp, e := http.Get(srv.URL + "?repo=o/r&pr=7")
	if e != nil {
		t.Fatal(e)
	}
	want, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var out, stderr bytes.Buffer
	if e := run(context.Background(), []string{"pr-status", "--config", cfg, "--repo", "o/r", "--pr", "7", "--json"}, &out, &stderr); e != nil {
		t.Fatal(e)
	}
	if out.String() != string(want) || !strings.Contains(out.String(), `"fresh_at":null`) {
		t.Fatalf("CLI:\n%s\nHTTP:\n%s", out.String(), want)
	}

	out.Reset()
	if e := run(context.Background(), []string{"pr-status", "--config", cfg, "--repo", "o/r", "--pr", "7"}, &out, &stderr); e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"o/r#7 blocked head=aaaaaaa", "reasons: review_missing", "test=green", "findings: 1 open"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("text output lacks %q:\n%s", want, out.String())
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr: %s", stderr.String())
	}

	for _, args := range [][]string{
		{"--repo", "o/r", "--pr", "8"},
		{"--repo", "nope", "--pr", "7"},
		{"--repo", "o/r"},
	} {
		if e := run(context.Background(), append([]string{"pr-status", "--config", cfg}, args...), &out, &stderr); e == nil {
			t.Errorf("%v succeeded", args)
		}
	}
}

// The CLI's fresh read writes the marker and nothing else: no daemon to wake.
func TestPRStatusCLIFreshMarksChangedPR(t *testing.T) {
	_, stateDir := prStatusHost(t)
	if e := prstatus.Write(stateDir, prstatus.Snapshot{Repo: "o/r", PR: 7, Head: "aaaaaaa", State: "ready", ObservedAt: time.Now()}); e != nil {
		t.Fatal(e)
	}
	fresh := func(context.Context, string, int) (github.PRFresh, error) {
		return github.PRFresh{Head: "bbbbbbb", ChecksRollup: ""}, nil
	}
	var out bytes.Buffer
	if e := printPRStatus(context.Background(), stateDir, "o/r", 7, fresh, true, &out); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), `"head":"bbbbbbb"`) || !strings.Contains(out.String(), `"gate_head":"aaaaaaa"`) || !prstatus.DirtyPRs(stateDir, "o/r")[7] {
		t.Fatalf("%s dirty=%v", out.String(), prstatus.DirtyPRs(stateDir, "o/r"))
	}
}

// Marked PRs go first; each group keeps its order.
func TestPRStatusDirtyPRsOrderedFirst(t *testing.T) {
	pull := func(n int) github.Pull { return github.Pull{Number: n} }
	got := dirtyFirst([]github.Pull{pull(1), pull(2), pull(3), pull(4)}, map[int]bool{3: true, 2: true})
	var order []int
	for _, p := range got {
		order = append(order, p.Number)
	}
	if len(order) != 4 || order[0] != 2 || order[1] != 3 || order[2] != 1 || order[3] != 4 {
		t.Fatalf("order %v", order)
	}
}
