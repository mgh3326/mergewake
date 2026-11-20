package reap

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestHubUnreachableNodeIsATypedHostUnreachable(t *testing.T) {
	hub, _ := fakeHub(t, func(hubRequest) (int, any) {
		return http.StatusOK, map[string]any{"nodes": []any{
			map[string]any{"machine_id": "node-c", "state": "disconnected", "agents": nil, "error": "node_unavailable"},
			map[string]any{"machine_id": "node-t", "state": "disconnected", "agents": nil, "error": "node_timeout"},
			map[string]any{"machine_id": "node-d", "state": "disconnected", "agents": []any{}, "panes": []any{}, "panes_ok": true},
		}}
	})
	cases := []struct {
		machine, reason string
	}{
		{"node-c", "node_unavailable"},
		{"node-t", "node_timeout"},
		{"node-z", "unknown"},
	}
	for _, c := range cases {
		_, err := (HubHost{Hub: hub, Machine: c.machine}).Panes(context.Background())
		var unreachable *HostUnreachable
		if !errors.As(err, &unreachable) || unreachable.Reason != c.reason || unreachable.Machine != c.machine {
			t.Fatalf("%s: %v", c.machine, err)
		}
		if want := "hub: machine " + c.machine + " " + c.reason; err.Error() != want {
			t.Fatalf("message %q, want %q", err.Error(), want)
		}
	}
	// A node that is not connected without a hub error code is not classified.
	_, err := (HubHost{Hub: hub, Machine: "node-d"}).Panes(context.Background())
	var unreachable *HostUnreachable
	if err == nil || errors.As(err, &unreachable) {
		t.Fatalf("disconnected node without a code: %v", err)
	}
}
