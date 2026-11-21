package reap

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

// An agents answer without a nodes array is a failed pass, never an inventory
// with every machine unknown: the daemon must keep the reap failure status.
func TestHubAgentsEnvelopeMustCarryNodesArray(t *testing.T) {
	for name, body := range map[string]any{
		"empty object": map[string]any{},
		"null nodes":   map[string]any{"nodes": nil},
		"string nodes": map[string]any{"nodes": "node-a"},
	} {
		hub, _ := fakeHub(t, func(hubRequest) (int, any) { return http.StatusOK, body })
		_, err := (HubHost{Hub: hub, Machine: "node-a"}).Panes(context.Background())
		var unreachable *HostUnreachable
		if err == nil || errors.As(err, &unreachable) {
			t.Fatalf("%s: got %v, want a pass error that is not a skipped host", name, err)
		}
	}
}

// An empty but present array is a valid inventory: a machine absent from it is
// unknown to the hub, which is the skipped-host answer.
func TestHubAgentsEmptyInventoryIsValid(t *testing.T) {
	hub, _ := fakeHub(t, func(hubRequest) (int, any) {
		return http.StatusOK, map[string]any{"nodes": []any{}}
	})
	_, err := (HubHost{Hub: hub, Machine: "node-a"}).Panes(context.Background())
	var unreachable *HostUnreachable
	if !errors.As(err, &unreachable) || unreachable.Reason != "unknown" {
		t.Fatalf("empty inventory: %v", err)
	}
}
