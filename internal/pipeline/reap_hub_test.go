package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	mu             sync.Mutex
	agents         map[string]any // the node-a row of GET /v1/nodes/agents
	jobResult      json.RawMessage
	jobResultError string
	composer       map[string]any
	capture        map[string]any
	close          map[string]any
	forbid         map[string]bool // paths or ops answered 403 scope_forbidden
	agentReads     int             // GET /v1/nodes/agents answers served
	wtHead         string          // HEAD worktree.inspect reports for /repo.wt; headSHA when empty
	wtPaths        map[string]bool // extra cwds worktree.inspect reports as linked worktrees at wtHead
	// removeOutcome is worktree.remove's answer; "removed" when empty.
	removeOutcome string
	commands      []map[string]any
	// detached cwds are linked worktrees at wtHead with no branch (reviewer
	// checkouts); dropClosed and dropRemoved make a closed pane leave the
	// lists and a removed worktree stop inspecting, as on a real node.
	detached    map[string]bool
	foreign     map[string]bool   // cwds whose worktree belongs to another repository
	tops        map[string]string // cwd → worktree toplevel, when the cwd is a subdirectory
	dropClosed  bool
	dropRemoved bool
	// closeOutcomes answers the next pane.close calls in turn, before close;
	// closeFails makes the next pane.close calls answer a node error.
	closeOutcomes []string
	closeFails    int
	// commons maps a cwd to the common git dir worktree.inspect reports for it
	// (/repo/.git when absent); lists holds each common's worktree.list rows,
	// served listPage rows at a time (all at once when zero) behind an offset
	// cursor; listErr makes worktree.list of a common answer that node error.
	commons  map[string]string
	branches map[string]string // cwd → branch worktree.inspect reports (feat/x when absent)
	lists    map[string][]map[string]any
	listPage int
	listErr  map[string]string
	// hangInspect is a cwd whose worktree.inspect never answers, so the
	// client's budget context ends the read; inspectErr answers that cwd's
	// worktree.inspect with a node error code.
	hangInspect string
	inspectErr  map[string]string
	// errStatus answers an op's node error (listErr, inspectErr) with that HTTP
	// status and the error body, so a test can answer a proxy's 502 or 503;
	// the default is a 200 body error, as a node answers.
	errStatus map[string]int
	// moreNodes are extra node rows GET /v1/nodes/agents reports beside the
	// node-a row, so a test can place labels on more than one host.
	moreNodes []any
	onInspect func()
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

// inspectsCwd reports whether a hub request is a worktree.inspect of cwd. The
// body is read and put back, so the handler still decodes it.
func inspectsCwd(r *http.Request, cwd string) bool {
	if r.Method != http.MethodPost {
		return false
	}
	raw, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var probe struct {
		Op   string `json:"op"`
		Args struct {
			Cwd string `json:"cwd"`
		} `json:"args"`
	}
	return json.Unmarshal(raw, &probe) == nil && probe.Op == "worktree.inspect" && probe.Args.Cwd == cwd
}

// answerError writes op's node error code: a 200 body error, or the HTTP status
// errStatus gives op with the same body.
func (h *fakeReapHub) answerError(w http.ResponseWriter, op, code string) {
	if status := h.errStatus[op]; status != 0 {
		w.WriteHeader(status)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": code})
}

func (h *fakeReapHub) serve(t *testing.T) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer svc-token" || r.Header.Get("X-Panewire-Machine-ID") != "svc-mergewake" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		h.mu.Lock()
		hang := h.hangInspect
		h.mu.Unlock()
		if hang != "" && inspectsCwd(r, hang) {
			<-r.Context().Done()
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
			h.agentReads++
			_ = json.NewEncoder(w).Encode(map[string]any{"nodes": append([]any{h.agents}, h.moreNodes...)})
			return
		}
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/v1/nodes/") || !strings.HasSuffix(r.URL.Path, "/command") {
			t.Errorf("unexpected hub request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		machine := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/nodes/"), "/command")
		h.commands = append(h.commands, body)
		var result any
		switch body["op"] {
		case "worktree.inspect":
			if h.onInspect != nil {
				h.onInspect()
			}
			if code := h.inspectErr[body["args"].(map[string]any)["cwd"].(string)]; code != "" {
				h.answerError(w, "worktree.inspect", code)
				return
			}
			result = map[string]any{"worktree": nil}
			if cwd := body["args"].(map[string]any)["cwd"].(string); cwd == "/repo.wt" || h.wtPaths[cwd] || h.detached[cwd] {
				head := h.wtHead
				if head == "" {
					head = headSHA
				}
				branch := "feat/x"
				if b := h.branches[cwd]; b != "" {
					branch = b
				}
				if h.detached[cwd] {
					branch = ""
				}
				origin := "git@github.com:example/project.git"
				if h.foreign[cwd] {
					origin = "git@github.com:example/other.git"
				}
				top := cwd
				if t := h.tops[cwd]; t != "" {
					top = t
				}
				common := "/repo/.git"
				if c := h.commons[cwd]; c != "" {
					common = c
				}
				result = map[string]any{"worktree": map[string]any{"path": top, "branch": branch, "head": head, "origin_url": origin, "common": common}}
			}
		case "worktree.list":
			args := body["args"].(map[string]any)
			common, _ := args["common"].(string)
			if code := h.listErr[common]; code != "" {
				h.answerError(w, "worktree.list", code)
				return
			}
			rows := h.lists[common]
			start := 0
			if c, ok := args["cursor"].(string); ok {
				start, _ = strconv.Atoi(c)
			}
			end, next := len(rows), ""
			if h.listPage > 0 && start+h.listPage < len(rows) {
				end, next = start+h.listPage, strconv.Itoa(start+h.listPage)
			}
			page := []map[string]any{}
			if start < end {
				page = rows[start:end]
			}
			result = map[string]any{"worktrees": page, "total": len(rows), "next_cursor": next}
		case "pane.read":
			result = h.composer
		case "job.result":
			if h.jobResultError != "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"error": h.jobResultError})
				return
			}
			result = h.jobResult
			if len(h.jobResult) == 0 {
				result = json.RawMessage(`{"found":false}`)
			}
		case "job.done":
			result = map[string]any{"outcome": "done"}
		case "pane.capture":
			result = h.capture
		case "pane.close":
			if h.closeFails > 0 {
				h.closeFails--
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "node_busy"})
				return
			}
			out := h.close
			if len(h.closeOutcomes) > 0 {
				out = map[string]any{"outcome": h.closeOutcomes[0]}
				h.closeOutcomes = h.closeOutcomes[1:]
			}
			result = out
			if h.dropClosed && out["outcome"] == "closed" {
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
			// The node refuses a detached worktree unless the request says
			// detached, and a branch worktree when it does (panewire
			// node_worktree.go worktree.remove).
			args := body["args"].(map[string]any)
			wantDetached, _ := args["detached"].(bool)
			outcome := h.removeOutcome
			switch isDetached := h.detached[args["cwd"].(string)]; {
			case isDetached && !wantDetached:
				outcome = "detached"
			case !isDetached && wantDetached:
				outcome = "not_detached"
			case outcome == "":
				outcome = "removed"
			}
			// Like the real node, the fake refuses a removal while a pane
			// uses the worktree: mergewake relies on that guard through the
			// hub transport instead of closing the pane itself.
			if outcome == "removed" {
				cwd, _ := args["cwd"].(string)
			loop:
				for _, list := range []string{"agents", "panes"} {
					for _, p := range h.agents[list].([]any) {
						if c, _ := p.(map[string]any)["cwd"].(string); c == cwd || strings.HasPrefix(c, cwd+"/") {
							outcome = "pane-in-use"
							break loop
						}
					}
				}
			}
			if h.dropRemoved && outcome == "removed" {
				delete(h.detached, body["args"].(map[string]any)["cwd"].(string))
			}
			result = map[string]any{"outcome": outcome}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"request_id": "cmd-1", "machine": machine, "op": body["op"], "result": result})
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
	s := &Service{Config: c, GH: &reapGH{}, Runner: run, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: &out}
	greenHead(t, s, testRepo, mergedPull.MergeCommitSHA)
	return s, &out
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
	var kept bool
	for _, row := range reapLogRows(t, s, "example/project") {
		kept = kept || row["action"] == "remove-worktree" && row["result"] == "skipped" && row["reason"] == "pane-in-use" && row["host"] == "node"
		if row["action"] == "close-pane" {
			t.Fatalf("close-pane row written: %v", reapLogRows(t, s, "example/project"))
		}
	}
	if !kept {
		t.Fatalf("receipts: %v", reapLogRows(t, s, "example/project"))
	}
	if removes := hub.ops("worktree.remove"); len(removes) != 0 {
		t.Fatalf("removed a worktree still in use: %#v", removes)
	}
}

// The hub's composer is never read and no pane is ever captured or closed:
// whatever it answers, the pane waits out panewire's own close.
func TestReapPanewireNeverReadsComposerOrClosesPane(t *testing.T) {
	for _, composer := range []string{"empty", "has_text", "unknown", ""} {
		t.Run("composer="+composer, func(t *testing.T) {
			hub := newFakeReapHub()
			hub.agents["agents"].([]any)[0].(map[string]any)["agent"] = "codex"
			hub.composer = map[string]any{"outcome": "read", "composer": composer}
			r := &scriptRunner{}
			s, _ := hubSvc(t, hub, r)
			if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if closes, captures, reads := hub.ops("pane.close"), hub.ops("pane.capture"), hub.ops("pane.read"); len(closes)+len(captures)+len(reads) != 0 || len(r.calls) != 0 {
				t.Fatalf("closes=%v captures=%v reads=%v local=%v", closes, captures, reads, r.calls)
			}
		})
	}
}

// A forbidden composer read changes nothing: no pane is read or closed on any
// host, and every worktree in use waits as pane-in-use.
func TestReapPanewireUnreadableComposerKeepsPane(t *testing.T) {
	hub := newFakeReapHub()
	hub.forbid = map[string]bool{"pane.read": true}
	r := &scriptRunner{outs: worktreeOuts("/repo"), fn: statAnswer("yes\n")}
	s, _ := hubSvc(t, hub, r, reap.Host{Name: "local", Jobs: "/jobs"})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(hub.ops("pane.capture"))+len(hub.ops("pane.close"))+len(hub.ops("pane.read")) != 0 {
		t.Fatal("acted on the pane with an unreadable composer")
	}
	if r.called(key("herdr", "pane", "close", "w1:p1")) || r.called("worktree\x00remove") {
		t.Fatalf("acted while panes were in use: local=%q", r.calls)
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["result"] == "removed" || row["result"] == "would-remove" {
			t.Fatalf("removed while panes were in use: %v", row)
		}
	}
	kept := map[string]bool{}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["action"] == "close-pane" {
			t.Fatalf("close-pane row written: %v", reapLogRows(t, s, "example/project"))
		}
		if row["action"] == "remove-worktree" && row["result"] == "skipped" && row["reason"] == "pane-in-use" {
			kept[row["host"].(string)] = true
		}
	}
	if !kept["node"] || !kept["local"] {
		t.Fatalf("missing keep receipts: %v", reapLogRows(t, s, "example/project"))
	}
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
	var kept bool
	for _, row := range reapLogRows(t, s, "example/project") {
		kept = kept || row["action"] == "remove-worktree" && row["result"] == "skipped" && row["reason"] == "pane-in-use"
		if row["result"] == "would-remove" {
			t.Fatalf("shadow planned a removal for a worktree in use: %v", reapLogRows(t, s, "example/project"))
		}
	}
	if !kept {
		t.Fatalf("missing kept row: %v", reapLogRows(t, s, "example/project"))
	}
	if !strings.Contains(out.String(), `"reason":"pane-in-use"`) {
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
	if !r.called("herdr\x00agent\x00list") || len(hub.ops("pane.close")) != 0 {
		t.Fatalf("local calls %q hub closes %v", r.calls, hub.ops("pane.close"))
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["action"] == "remove-worktree" && row["host"] == "node" && (row["result"] != "skipped" || row["reason"] != "pane-in-use") {
			t.Fatalf("node row: %v", row)
		}
		if row["action"] == "close-pane" {
			t.Fatalf("close-pane row written: %v", row)
		}
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
// when it also has an ssh destination: reap enumerates no job dirs there and
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
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["action"] == "archive-job" {
			t.Fatalf("hub host archived: %v", reapLogRows(t, s, "example/project"))
		}
	}
}
