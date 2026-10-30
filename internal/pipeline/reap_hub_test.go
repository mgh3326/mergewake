package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/reap"
)

// fakeReapHub answers the panewire hub's node-command endpoints for one
// machine, node-a, whose only agent sits in the merged PR's worktree.
type fakeReapHub struct {
	mu       sync.Mutex
	agents   map[string]any // the node-a row of GET /v1/nodes/agents
	capture  map[string]any
	close    map[string]any
	forbid   map[string]bool // paths or ops answered 403 scope_forbidden
	commands []map[string]any
}

func (h *fakeReapHub) ops(op string) []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]any
	for _, c := range h.commands {
		if c["op"] == op {
			out = append(out, c["args"].(map[string]any))
		}
	}
	return out
}

func (h *fakeReapHub) serve(t *testing.T) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer svc-token" || r.Header.Get("X-Panewire-Machine-ID") != "svc-mergewake" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		var body map[string]any
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		if op, _ := body["op"].(string); h.forbid[r.URL.Path] || h.forbid[op] {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "scope_forbidden"})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/nodes/agents" {
			_ = json.NewEncoder(w).Encode(map[string]any{"nodes": []any{h.agents}})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/nodes/node-a/command" {
			t.Errorf("unexpected hub request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		h.commands = append(h.commands, body)
		var result any
		switch body["op"] {
		case "worktree.inspect":
			result = map[string]any{"worktree": nil}
			if body["args"].(map[string]any)["cwd"] == "/repo.wt" {
				result = map[string]any{"worktree": map[string]any{"path": "/repo.wt", "branch": "wk/x", "head": headSHA, "origin_url": "git@github.com:example/project.git", "common": "/repo/.git"}}
			}
		case "pane.capture":
			result = h.capture
		case "pane.close":
			result = h.close
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"request_id": "cmd-1", "machine": "node-a", "op": body["op"], "result": result})
	}))
	t.Cleanup(server.Close)
	return server
}

func newFakeReapHub() *fakeReapHub {
	return &fakeReapHub{
		agents: map[string]any{"machine_id": "node-a", "state": "connected",
			"agents":   []any{map[string]any{"pane_id": "w1:p1", "name": "wk-7", "agent_status": "idle", "agent": "claude", "cwd": "/repo.wt"}},
			"panes":    []any{map[string]any{"pane_id": "w1:p1", "agent_status": "idle", "cwd": "/repo.wt"}},
			"panes_ok": true},
		capture: map[string]any{"outcome": "report", "saved": "/node/jobs/wk-7/report.md"},
		close:   map[string]any{"outcome": "closed"},
	}
}

// hubSvc runs reap with the panewire transport. Its script runner has no
// answers for the hub host, so any ssh or local command for it fails the pass.
func hubSvc(t *testing.T, hub *fakeReapHub, run *scriptRunner, hosts ...reap.Host) (*Service, *strings.Builder) {
	t.Helper()
	server := hub.serve(t)
	dir := t.TempDir()
	token := filepath.Join(dir, "svc-mergewake.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := repoConfig(dir)
	c.Reap.Transport = config.ReapTransportPanewire
	c.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	c.Hosts = append([]reap.Host{{Name: "node", SSH: "ops@node.example", Machine: "node-a"}}, hosts...)
	var out strings.Builder
	return &Service{Config: c, GH: &reapGH{}, Runner: run, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: &out}, &out
}

func TestReapPanewireClosesThroughHub(t *testing.T) {
	hub := newFakeReapHub()
	r := &scriptRunner{}
	s, _ := hubSvc(t, hub, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 0 {
		t.Fatalf("hub host ran local or ssh commands: %q", r.calls)
	}
	captures := hub.ops("pane.capture")
	merged := parsedTime(*mergedPull.MergedAt).Unix()
	if len(captures) != 1 || captures[0]["pane_id"] != "w1:p1" || captures[0]["name"] != "wk-7" || captures[0]["cwd"] != "/repo.wt" || captures[0]["merged_after"] != float64(merged) || captures[0]["write"] != true {
		t.Fatalf("capture args %v", captures)
	}
	closes := hub.ops("pane.close")
	if len(closes) != 1 || closes[0]["pane_id"] != "w1:p1" || closes[0]["name"] != "wk-7" || closes[0]["agent_status"] != "idle" || closes[0]["cwd"] != "/repo.wt" {
		t.Fatalf("close args %v", closes)
	}
	var closed, cleanup bool
	for _, row := range reapLogRows(t, s, "example/project") {
		closed = closed || row["action"] == "close-pane" && row["result"] == "closed" && row["host"] == "node" && row["pane"] == "w1:p1" && row["saved"] == "/node/jobs/wk-7/report.md" && row["worktree"] == "/repo.wt" && row["branch"] == "wk/x"
		cleanup = cleanup || row["action"] == "remove-worktree" && row["result"] == "skipped" && row["reason"] == "cleanup-unsupported:panewire"
	}
	if !closed || !cleanup {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
}

func TestReapPanewireShadowNeverActs(t *testing.T) {
	hub := newFakeReapHub()
	hub.capture = map[string]any{"outcome": "would-screen", "saved": "/node/jobs/wk-7/pane-screen.txt"}
	s, out := hubSvc(t, hub, &scriptRunner{})
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if captures := hub.ops("pane.capture"); len(captures) != 1 || captures[0]["write"] != false {
		t.Fatalf("shadow capture args %v", captures)
	}
	if closes := hub.ops("pane.close"); len(closes) != 0 {
		t.Fatalf("shadow closed %v", closes)
	}
	if !strings.Contains(out.String(), `"result":"would-close"`) || !strings.Contains(out.String(), `"saved":"/node/jobs/wk-7/pane-screen.txt"`) {
		t.Fatal(out.String())
	}
}

func TestReapPanewireNodeVerdictsSkip(t *testing.T) {
	for _, tt := range []struct {
		name           string
		capture, close map[string]any
		reason         string
		closeCalls     int
	}{
		{"not a job", map[string]any{"outcome": "not-a-job"}, nil, "not-a-job", 0},
		{"gone before capture", map[string]any{"outcome": "pane-gone"}, nil, "pane-gone", 0},
		{"changed at close", nil, map[string]any{"outcome": "pane-changed"}, "pane-changed", 1},
		{"gone at close", nil, map[string]any{"outcome": "pane-gone"}, "pane-gone", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hub := newFakeReapHub()
			if tt.capture != nil {
				hub.capture = tt.capture
			}
			if tt.close != nil {
				hub.close = tt.close
			}
			s, _ := hubSvc(t, hub, &scriptRunner{})
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if got := len(hub.ops("pane.close")); got != tt.closeCalls {
				t.Fatalf("close calls %d", got)
			}
			var skipped bool
			for _, row := range reapLogRows(t, s, "example/project") {
				skipped = skipped || row["action"] == "close-pane" && row["result"] == "skipped" && row["reason"] == tt.reason
			}
			if !skipped {
				t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
			}
		})
	}
}

func TestReapPanewireUnavailableNodeIsAnError(t *testing.T) {
	hub := newFakeReapHub()
	hub.agents = map[string]any{"machine_id": "node-a", "state": "disconnected", "agents": nil, "error": "node_unavailable"}
	s, _ := hubSvc(t, hub, &scriptRunner{})
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "node_unavailable") {
		t.Fatalf("err=%v", err)
	}
	for _, op := range []string{"worktree.inspect", "pane.capture", "pane.close"} {
		if sent := hub.ops(op); len(sent) != 0 {
			t.Fatalf("%s sent to an unavailable node: %v", op, sent)
		}
	}
}

func TestReapPanewireKeepsLocalHostLocal(t *testing.T) {
	hub := newFakeReapHub()
	r := &scriptRunner{outs: map[string]string{
		key("herdr", "agent", "list"): `{"result":{"agents":[]}}`,
		key("herdr", "pane", "list"):  `{"result":{"panes":[]}}`,
	}}
	s, _ := hubSvc(t, hub, r, reap.Host{Name: "local", Jobs: "/jobs"})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, c := range r.calls {
		if strings.HasPrefix(c, "ssh\x00") {
			t.Fatalf("ssh used: %q", c)
		}
	}
	if !r.called("herdr\x00agent\x00list") || len(hub.ops("pane.close")) != 1 {
		t.Fatalf("local calls %q hub closes %v", r.calls, hub.ops("pane.close"))
	}
}

func TestReapPanewireRejectsLooseTokenFile(t *testing.T) {
	hub := newFakeReapHub()
	s, _ := hubSvc(t, hub, &scriptRunner{})
	if err := os.Chmod(s.Config.Reap.Hub.TokenFile, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err == nil || !strings.Contains(err.Error(), "token_file") {
		t.Fatalf("err=%v", err)
	}
}

// The operator credential is refused when a pass reads the token file, so a
// config edit cannot put it back on the daemon host; the hub is never called.
func TestReapPanewireRefusesOperatorCredential(t *testing.T) {
	hub := newFakeReapHub()
	s, _ := hubSvc(t, hub, &scriptRunner{})
	if err := os.WriteFile(s.Config.Reap.Hub.TokenFile, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=svc-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "operator") || !strings.Contains(err.Error(), "token_file") {
		t.Fatalf("err=%v", err)
	}
	if len(hub.commands) != 0 {
		t.Fatalf("hub called: %v", hub.commands)
	}
}

// A 403 scope_forbidden is a scope error for that host, never a host with no
// panes: discovery fails the host, and a refused node command is an error row.
func TestReapPanewireScopeForbiddenIsAnError(t *testing.T) {
	t.Run("agents", func(t *testing.T) {
		hub := newFakeReapHub()
		hub.forbid = map[string]bool{"/v1/nodes/agents": true}
		s, out := hubSvc(t, hub, &scriptRunner{})
		err := s.Reap(context.Background(), testRepo, 0, Options{})
		if !errors.Is(err, reap.ErrHubAuth) || !strings.Contains(err.Error(), "node: hub scope/auth error") || !strings.Contains(err.Error(), "scope_forbidden") {
			t.Fatalf("err=%v", err)
		}
		if len(hub.commands) != 0 {
			t.Fatalf("commands sent after a refused discovery: %v", hub.commands)
		}
		if strings.Contains(out.String(), `"action":"close-pane"`) || strings.Contains(out.String(), `"action":"remove-worktree"`) {
			t.Fatalf("refused host reaped as empty:\n%s", out.String())
		}
	})
	t.Run("worktree.inspect", func(t *testing.T) {
		hub := newFakeReapHub()
		hub.forbid = map[string]bool{"worktree.inspect": true}
		s, out := hubSvc(t, hub, &scriptRunner{})
		err := s.Reap(context.Background(), testRepo, 0, Options{})
		if !errors.Is(err, reap.ErrHubAuth) || !strings.Contains(err.Error(), "node: hub scope/auth error") {
			t.Fatalf("err=%v", err)
		}
		if len(hub.ops("pane.capture")) != 0 || strings.Contains(out.String(), `"action":"close-pane"`) {
			t.Fatalf("refused inspect read as panes outside a worktree:\n%s", out.String())
		}
	})
	t.Run("pane.capture", func(t *testing.T) {
		hub := newFakeReapHub()
		hub.forbid = map[string]bool{"pane.capture": true}
		s, _ := hubSvc(t, hub, &scriptRunner{})
		// A row error reaches the pass error as its receipt reason text.
		err := s.Reap(context.Background(), testRepo, 0, Options{})
		if err == nil || !strings.Contains(err.Error(), "scope/auth error") || !strings.Contains(err.Error(), "scope_forbidden") {
			t.Fatalf("err=%v", err)
		}
		if len(hub.ops("pane.close")) != 0 {
			t.Fatal("closed without a capture")
		}
		var errored bool
		for _, row := range reapLogRows(t, s, "example/project") {
			errored = errored || row["action"] == "close-pane" && row["result"] == "error" && strings.Contains(row["reason"].(string), "scope/auth error")
		}
		if !errored {
			t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
		}
	})
}

// Under panewire a remote host is archived over no transport at all, even
// when it also has an ssh destination: reap writes a skipped archive row and
// runs nothing locally or over ssh.
func TestReapPanewireArchiveSkipsHubHostWithSSH(t *testing.T) {
	hub := newFakeReapHub()
	hub.agents = map[string]any{"machine_id": "node-a", "state": "connected", "agents": []any{}, "panes": []any{}, "panes_ok": true}
	r := &scriptRunner{}
	s, _ := hubSvc(t, hub, r)
	s.Config.Hosts[0].Jobs = "/home/ops/jobs"
	s.Config.Reap.Archive = config.Archive{Enabled: true, DriveDir: t.TempDir(), TextMaxBytes: 1 << 20, MinAge: "24h"}
	b, _ := json.Marshal(reapReceipt{Time: time.Now().Add(-48 * time.Hour), Action: "close-pane", Repo: "example/project", PR: 12, Host: "node",
		Pane: "w1:p1", Cwd: "/repo.wt", Saved: "/node/jobs/wk-7/report.md", Job: "wk-7", Result: "closed"})
	writeFile(t, s.reapLog("example/project"), string(b)+"\n")
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 0 {
		t.Fatalf("hub host reached over ssh or locally: %q", r.calls)
	}
	var skipped bool
	for _, row := range reapLogRows(t, s, "example/project") {
		skipped = skipped || row["action"] == "archive-job" && row["host"] == "node" && row["job"] == "wk-7" && row["result"] == "skipped" && row["reason"] == "archive-over-hub"
	}
	if !skipped {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
}
