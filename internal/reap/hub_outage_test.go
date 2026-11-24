package reap

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

// hubOutageCase is one hub answer for node-a: row is the node's error code in
// the inventory and in a command's 200 body; status, when set, answers every
// call with that HTTP status and body instead.
type hubOutageCase struct {
	name   string
	row    string
	status int
	body   map[string]any
	want   string // the HostUnreachable reason, or "" when the call must not be one
}

var hubOutageCases = []hubOutageCase{
	{name: "node_lost", row: "node_lost", want: "node_lost"},
	{name: "node_unavailable", row: "node_unavailable", want: "node_unavailable"},
	{name: "node_timeout", row: "node_timeout", want: "node_timeout"},
	{name: "hub_draining", status: http.StatusServiceUnavailable, body: map[string]any{"error": "hub_draining"}, want: "hub_draining"},
	{name: "502 unexpected-response", status: http.StatusBadGateway, body: map[string]any{}, want: "unexpected-response"},
	{name: "503 unexpected-response", status: http.StatusServiceUnavailable, body: map[string]any{}, want: "unexpected-response"},
	{name: "504 unexpected-response", status: http.StatusGatewayTimeout, body: map[string]any{}, want: "unexpected-response"},
	// Answers that are not host outages stay errors, whatever the call.
	{name: "500 unexpected-response", status: http.StatusInternalServerError, body: map[string]any{}, want: ""},
	{name: "400 invalid_args", status: http.StatusBadRequest, body: map[string]any{"error": "invalid_args"}, want: ""},
	{name: "401 unauthorized", status: http.StatusUnauthorized, body: map[string]any{"error": "unauthorized"}, want: ""},
	{name: "403 scope_forbidden", status: http.StatusForbidden, body: map[string]any{"error": "scope_forbidden"}, want: ""},
}

// outageHub answers node-a as c says: the inventory row carries c.row, and a
// command answers c.row as a 200 body error unless c.status overrides it.
func outageHub(t *testing.T, c hubOutageCase) *Hub {
	t.Helper()
	hub, _ := fakeHub(t, func(req hubRequest) (int, any) {
		if c.status != 0 {
			return c.status, c.body
		}
		if req.method == http.MethodGet {
			return http.StatusOK, map[string]any{"nodes": []any{
				map[string]any{"machine_id": "node-a", "state": "disconnected", "agents": nil, "error": c.row},
			}}
		}
		return http.StatusOK, map[string]any{"error": c.row}
	})
	return hub
}

// Every hub call the reap makes classifies a host outage the same way: the
// inventory, the worktree inspect and listing, and the guarded removal.
func TestHubOutageIsHostUnreachableOnEveryCall(t *testing.T) {
	ctx := context.Background()
	for _, c := range hubOutageCases {
		hub := outageHub(t, c)
		h := HubHost{Hub: hub, Machine: "node-a"}
		calls := map[string]func() error{
			"Panes":        func() error { _, err := h.Panes(ctx); return err },
			"AllPanes":     func() error { _, err := h.AllPanes(ctx); return err },
			"Inspect":      func() error { _, err := h.Inspect(ctx, "/w"); return err },
			"WorktreeList": func() error { _, err := h.WorktreeList(ctx, "/repo/.git", ""); return err },
			"WorktreeRemove": func() error {
				_, err := h.WorktreeRemove(ctx, "/w", "abc", "main", nil, false)
				return err
			},
		}
		for name, call := range calls {
			err := call()
			var unreachable *HostUnreachable
			isUnreachable := errors.As(err, &unreachable)
			if c.want == "" {
				if isUnreachable {
					t.Fatalf("%s %s: a host outage for an answer that is not one: %v", c.name, name, err)
				}
				continue
			}
			if !isUnreachable || unreachable.Reason != c.want || unreachable.Machine != "node-a" {
				t.Fatalf("%s %s: %v, want HostUnreachable %s", c.name, name, err, c.want)
			}
		}
	}
}

// An auth refusal is still ErrHubAuth on every call, never a host skip, and a
// cwd's invalid_args is still InvalidArgs, so the caller can tell them apart.
func TestHubOutageLeavesAuthAndArgsAnswersAlone(t *testing.T) {
	ctx := context.Background()
	for _, c := range hubOutageCases {
		if c.name != "401 unauthorized" && c.name != "400 invalid_args" {
			continue
		}
		h := HubHost{Hub: outageHub(t, c), Machine: "node-a"}
		_, err := h.WorktreeList(ctx, "/repo/.git", "")
		var unreachable *HostUnreachable
		if errors.As(err, &unreachable) {
			t.Fatalf("%s: a host skip for an answer that is not an outage: %v", c.name, err)
		}
		if c.name == "401 unauthorized" && !errors.Is(err, ErrHubAuth) {
			t.Fatalf("%s: %v, want ErrHubAuth", c.name, err)
		}
		if c.name == "400 invalid_args" && !InvalidArgs(err) {
			t.Fatalf("%s: %v, want InvalidArgs", c.name, err)
		}
	}
}
