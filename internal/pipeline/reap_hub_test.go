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
	composer map[string]any
	capture  map[string]any
	close    map[string]any
	forbid   map[string]bool // paths or ops answered 403 scope_forbidden
	wtHead   string          // HEAD worktree.inspect reports for /repo.wt; headSHA when empty
	wtPaths  map[string]bool // extra cwds worktree.inspect reports as linked worktrees at wtHead
	// removeOutcome is worktree.remove's answer; "removed" when empty.
	removeOutcome string
	commands      []map[string]any
	// detached cwds are linked worktrees at wtHead with no branch (reviewer
	// checkouts); dropClosed and dropRemoved make a closed pane leave the
	// lists and a removed worktree stop inspecting, as on a real node.
	detached    map[string]bool
	foreign     map[string]bool // cwds whose worktree belongs to another repository
	dropClosed  bool
	dropRemoved bool
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
			if cwd := body["args"].(map[string]any)["cwd"].(string); cwd == "/repo.wt" || h.wtPaths[cwd] || h.detached[cwd] {
				head := h.wtHead
				if head == "" {
					head = headSHA
				}
				branch := "wk/x"
				if h.detached[cwd] {
					branch = ""
				}
				origin := "git@github.com:example/project.git"
				if h.foreign[cwd] {
					origin = "git@github.com:example/other.git"
				}
				result = map[string]any{"worktree": map[string]any{"path": cwd, "branch": branch, "head": head, "origin_url": origin, "common": "/repo/.git"}}
			}
		case "pane.read":
			result = h.composer
		case "job.done":
			result = map[string]any{"outcome": "done"}
		case "pane.capture":
			result = h.capture
		case "pane.close":
			result = h.close
			if h.dropClosed && h.close["outcome"] == "closed" {
				id := body["args"].(map[string]any)["pane_id"]
				for _, list := range []string{"agents", "panes"} {
					var keep []any
					for _, p := range h.agents[list].([]any) {
						if p.(map[string]any)["pane_id"] != id {
							keep = append(keep, p)
						}
					}
					h.agents[list] = append([]any{}, keep...)
				}
			}
		case "worktree.remove":
			// The hub validates strictly: a null regenerable list is
			// invalid_request (panewire node_command.go worktree.remove).
			if _, ok := body["args"].(map[string]any)["regenerable"].([]any); !ok {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request"})
				return
			}
			outcome := h.removeOutcome
			if outcome == "" {
				outcome = "removed"
			}
			if h.dropRemoved && outcome == "removed" {
				delete(h.detached, body["args"].(map[string]any)["cwd"].(string))
			}
			result = map[string]any{"outcome": outcome}
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

// A missing composer answer keeps the pane while worktree cleanup still uses the node.
func TestReapPanewireKeepsPaneWithUnreadComposer(t *testing.T) {
	hub := newFakeReapHub()
	r := &scriptRunner{}
	s, _ := hubSvc(t, hub, r)
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 0 {
		t.Fatalf("hub host ran local or ssh commands: %q", r.calls)
	}
	if captures, closes := hub.ops("pane.capture"), hub.ops("pane.close"); len(captures)+len(closes) != 0 {
		t.Fatalf("captured %v closed %v", captures, closes)
	}
	var kept, removed bool
	for _, row := range reapLogRows(t, s, "example/project") {
		kept = kept || row["action"] == "close-pane" && row["result"] == "skipped" && row["reason"] == "composer-unknown" && row["host"] == "node" && row["pane"] == "w1:p1"
		removed = removed || row["action"] == "remove-worktree" && row["result"] == "removed"
	}
	if !kept || !removed {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || len(removes[0]) != 4 || removes[0]["cwd"] != "/repo.wt" || removes[0]["expect_head"] != headSHA || removes[0]["base"] != testRepo.Branch || removes[0]["regenerable"] == nil {
		t.Fatalf("remove args: %#v", removes)
	}
}

func TestReapPanewireCodexComposerGuard(t *testing.T) {
	for _, test := range []struct {
		name, composer string
		fail, close    bool
	}{
		{"empty", "empty", false, true},
		{"draft", "has_text", false, false},
		{"unknown", "unknown", false, false},
		{"missing", "", false, false},
		{"hub error", "empty", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			hub := newFakeReapHub()
			hub.agents["agents"].([]any)[0].(map[string]any)["agent"] = "codex"
			hub.composer = map[string]any{"outcome": "read", "composer": test.composer}
			if test.fail {
				hub.forbid = map[string]bool{"pane.read": true}
			}
			r := &scriptRunner{}
			s, _ := hubSvc(t, hub, r)
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			closes, captures := hub.ops("pane.close"), hub.ops("pane.capture")
			want := 0
			if test.close {
				want = 1
			}
			if len(closes) != want || len(captures) != want || len(r.calls) != 0 {
				t.Fatalf("closes=%v captures=%v local=%v", closes, captures, r.calls)
			}
			reads := hub.ops("pane.read")
			if !test.fail && (len(reads) != 1 || reads[0]["tail_lines"] != float64(0) || reads[0]["name"] != "wk-7") {
				t.Fatalf("read=%v", reads)
			}
		})
	}
}

func TestReapPanewireComposerErrorKeepsPaneAndContinues(t *testing.T) {
	hub := newFakeReapHub()
	hub.forbid = map[string]bool{"pane.read": true}
	r := &scriptRunner{outs: worktreeOuts("/repo"), fn: statAnswer("yes\n")}
	s, _ := hubSvc(t, hub, r, reap.Host{Name: "local", Jobs: "/jobs"})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(hub.ops("pane.capture"))+len(hub.ops("pane.close")) != 0 {
		t.Fatal("acted on the pane with an unreadable composer")
	}
	if !r.called(key("herdr", "pane", "close", "w1:p1")) ||
		!r.called(key("git", "-C", "/repo", "worktree", "remove", "/repo.wt")) ||
		len(hub.ops("worktree.remove")) != 1 {
		t.Fatalf("other pane or worktree cleanup skipped: local=%q hub=%v", r.calls, hub.ops("worktree.remove"))
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["action"] == "close-pane" && row["host"] == "node" && row["pane"] == "w1:p1" &&
			row["result"] == "skipped" && row["reason"] == reap.ComposerUnknown {
			return
		}
	}
	t.Fatal("missing keep receipt for the unreadable composer")
}

func TestReapPanewireShadowNeverActs(t *testing.T) {
	hub := newFakeReapHub()
	s, out := hubSvc(t, hub, &scriptRunner{})
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"pane.capture", "pane.close", "worktree.remove"} {
		if sent := hub.ops(op); len(sent) != 0 {
			t.Fatalf("shadow sent %s %v", op, sent)
		}
	}
	var wouldRemove bool
	for _, row := range reapLogRows(t, s, "example/project") {
		wouldRemove = wouldRemove || row["action"] == "remove-worktree" && row["result"] == "would-remove"
	}
	if !wouldRemove {
		t.Fatalf("missing would-remove row: %v", reapLogRows(t, s, "example/project"))
	}
	if !strings.Contains(out.String(), `"reason":"composer-unknown"`) {
		t.Fatal(out.String())
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
	if !r.called("herdr\x00agent\x00list") || len(hub.ops("pane.close")) != 0 || len(hub.ops("worktree.remove")) != 1 {
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
