package reap

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHubOrigin(t *testing.T) {
	for raw, want := range map[string]string{
		"https://hub.example.invalid":      "https://hub.example.invalid",
		"https://hub.example.invalid/":     "https://hub.example.invalid",
		"http://127.0.0.1:8080":            "http://127.0.0.1:8080",
		"http://localhost:9":               "http://localhost:9",
		"http://hub.example.invalid":       "",
		"https://hub.example.invalid/v1":   "",
		"https://u:p@hub.example.invalid":  "",
		"https://hub.example.invalid/?x=1": "",
		"ftp://hub.example.invalid":        "",
		"hub.example.invalid":              "",
	} {
		got, err := HubOrigin(raw)
		if got != want || (err == nil) != (want != "") {
			t.Errorf("%s: got %q %v, want %q", raw, got, err, want)
		}
	}
}

func TestReadEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "op.env")
	if err := os.WriteFile(path, []byte("# operator\nHUB_MACHINE_ID=operator\nHUB_TOKEN=\"tok\"\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	values, err := ReadEnvFile(path)
	if err != nil || values["HUB_TOKEN"] != "tok" || values["HUB_MACHINE_ID"] != "operator" {
		t.Fatalf("%v %v", values, err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEnvFile(path); err == nil {
		t.Fatal("group/other-readable credential file accepted")
	}
	link := filepath.Join(dir, "link.env")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEnvFile(link); err == nil {
		t.Fatal("symlinked credential file accepted")
	}
}

type hubRequest struct {
	method, path, auth, cfID, cfSecret string
	body                               map[string]any
}

func fakeHub(t *testing.T, answer func(hubRequest) (int, any)) (*Hub, *[]hubRequest) {
	t.Helper()
	var seen []hubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := hubRequest{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization"), cfID: r.Header.Get("CF-Access-Client-Id"), cfSecret: r.Header.Get("CF-Access-Client-Secret")}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&req.body)
		}
		seen = append(seen, req)
		status, body := answer(req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(server.Close)
	return &Hub{URL: server.URL, Token: "op-token", CFID: "cf-id", CFSecret: "cf-secret"}, &seen
}

func TestHubAgentsFetchedOncePerPass(t *testing.T) {
	hub, seen := fakeHub(t, func(hubRequest) (int, any) {
		return http.StatusOK, map[string]any{"nodes": []any{
			map[string]any{"machine_id": "node-a", "state": "connected", "agents": []any{map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/w"}}, "panes": []any{map[string]any{"pane_id": "w1:p1", "cwd": "/w"}}, "panes_ok": true},
			map[string]any{"machine_id": "node-b", "state": "connected", "agents": []any{}, "panes": nil, "panes_ok": false},
			map[string]any{"machine_id": "node-c", "state": "disconnected", "agents": nil, "error": "node_unavailable"},
			map[string]any{"machine_id": "node-d", "state": "disconnected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		}}
	})
	a := HubHost{Hub: hub, Machine: "node-a"}
	panes, err := a.Panes(context.Background())
	if err != nil || len(panes) != 1 || panes[0] != (Pane{ID: "w1:p1", Name: "job-a", Status: "idle", Cwd: "/w"}) {
		t.Fatalf("%+v %v", panes, err)
	}
	if all, err := a.AllPanes(context.Background()); err != nil || len(all) != 1 {
		t.Fatalf("%+v %v", all, err)
	}
	if _, err := (HubHost{Hub: hub, Machine: "node-b"}).AllPanes(context.Background()); err == nil {
		t.Fatal("unknown pane usage read as empty")
	}
	for _, machine := range []string{"node-c", "node-d", "node-z"} {
		if _, err := (HubHost{Hub: hub, Machine: machine}).Panes(context.Background()); err == nil {
			t.Fatalf("%s read as a host with no panes", machine)
		}
	}
	if len(*seen) != 1 {
		t.Fatalf("agent list fetched %d times", len(*seen))
	}
	got := (*seen)[0]
	if got.method != http.MethodGet || got.path != "/v1/nodes/agents" || got.auth != "Bearer op-token" || got.cfID != "cf-id" || got.cfSecret != "cf-secret" {
		t.Fatalf("%+v", got)
	}
}

func TestHubHostCommands(t *testing.T) {
	var reply func(op string, args map[string]any) (int, any)
	hub, seen := fakeHub(t, func(r hubRequest) (int, any) {
		args, _ := r.body["args"].(map[string]any)
		return reply(r.body["op"].(string), args)
	})
	host := HubHost{Hub: hub, Machine: "node-a"}
	ctx := context.Background()
	pane := Pane{ID: "w1:p1", Name: "job-a", Status: "idle", Cwd: "/w/repo.wt"}
	result := func(v any) (int, any) { return http.StatusOK, map[string]any{"result": v} }

	reply = func(op string, args map[string]any) (int, any) {
		if op != "worktree.inspect" || args["cwd"] != "/w/repo.wt" {
			t.Fatalf("%s %v", op, args)
		}
		return result(map[string]any{"worktree": map[string]any{"path": "/w/repo.wt", "branch": "wk/x", "head": "aaaa", "origin_url": "https://github.com/example/project.git", "common": "/w/repo/.git"}})
	}
	w, err := host.Inspect(ctx, "/w/repo.wt")
	if err != nil || *w != (Worktree{Path: "/w/repo.wt", Branch: "wk/x", Head: "aaaa", Repo: "example/project", Common: "/w/repo/.git"}) {
		t.Fatalf("%+v %v", w, err)
	}
	if (*seen)[0].path != "/v1/nodes/node-a/command" || (*seen)[0].method != http.MethodPost {
		t.Fatalf("%+v", (*seen)[0])
	}
	reply = func(string, map[string]any) (int, any) { return result(map[string]any{"worktree": nil}) }
	if w, err := host.Inspect(ctx, "/w/plain"); w != nil || err != nil {
		t.Fatalf("%+v %v", w, err)
	}

	merged := time.Unix(1_800_000_000, 0)
	for outcome, want := range map[string]Capture{
		"report":       {Saved: "/jobs/job-a/report.md"},
		"screen":       {Saved: "/jobs/job-a/pane-screen.txt"},
		"not-a-job":    {Skip: CaptureNotAJob},
		"pane-gone":    {Skip: CapturePaneGone},
		"pane-changed": {Skip: CapturePaneChanged},
	} {
		reply = func(op string, args map[string]any) (int, any) {
			if op != "pane.capture" || args["pane_id"] != "w1:p1" || args["name"] != "job-a" || args["cwd"] != "/w/repo.wt" || args["merged_after"] != float64(merged.Unix()) || args["write"] != true {
				t.Fatalf("%s %v", op, args)
			}
			v := map[string]any{"outcome": outcome}
			if want.Saved != "" {
				v["saved"] = want.Saved
			}
			return result(v)
		}
		if got, err := host.Capture(ctx, pane, &merged, true); err != nil || got != want {
			t.Fatalf("%s: %+v %v", outcome, got, err)
		}
	}
	reply = func(_ string, args map[string]any) (int, any) {
		if args["merged_after"] != float64(math.MaxInt64) || args["write"] != false {
			t.Fatalf("%v", args)
		}
		return result(map[string]any{"outcome": "would-screen", "saved": "/jobs/job-a/pane-screen.txt"})
	}
	if got, err := host.Capture(ctx, pane, nil, false); err != nil || got.Saved != "/jobs/job-a/pane-screen.txt" {
		t.Fatalf("%+v %v", got, err)
	}

	for _, outcome := range []string{CloseClosed, CapturePaneGone, CapturePaneChanged} {
		reply = func(op string, args map[string]any) (int, any) {
			if op != "pane.close" || args["pane_id"] != "w1:p1" || args["name"] != "job-a" || args["agent_status"] != "idle" || args["cwd"] != "/w/repo.wt" || len(args) != 4 {
				t.Fatalf("%s %v", op, args)
			}
			return result(map[string]any{"outcome": outcome})
		}
		if got, err := host.CloseExact(ctx, pane); err != nil || got != outcome {
			t.Fatalf("%s: %q %v", outcome, got, err)
		}
	}
	reply = func(string, map[string]any) (int, any) { return result(map[string]any{"outcome": "deleted"}) }
	if _, err := host.CloseExact(ctx, pane); err == nil {
		t.Fatal("unknown close outcome accepted")
	}
	reply = func(string, map[string]any) (int, any) {
		return http.StatusBadGateway, map[string]any{"error": "close_failed"}
	}
	var he *hubError
	if _, err := host.CloseExact(ctx, pane); !errors.As(err, &he) || he.Code != "close_failed" || !strings.Contains(err.Error(), "502") {
		t.Fatalf("%v", err)
	}
}

func TestHubRefusesRedirect(t *testing.T) {
	var leaked bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = leaked || r.Header.Get("Authorization") != ""
	}))
	t.Cleanup(target.Close)
	hub, _ := fakeHub(t, func(hubRequest) (int, any) { return http.StatusOK, nil })
	redirect := httptest.NewServer(http.RedirectHandler(target.URL+"/v1/nodes/agents", http.StatusTemporaryRedirect))
	t.Cleanup(redirect.Close)
	hub.URL = redirect.URL
	if _, err := (HubHost{Hub: hub, Machine: "node-a"}).Panes(context.Background()); err == nil || leaked {
		t.Fatalf("redirect followed: err=%v leaked=%v", err, leaked)
	}
}
