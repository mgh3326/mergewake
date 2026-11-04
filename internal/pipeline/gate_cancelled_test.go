package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestGateCompletedWithoutVerdict(t *testing.T) {
	for _, tt := range []struct {
		name          string
		vet, test     github.CI
		done          bool
		state, reason string
		cancelled     []string
		summary       string
	}{
		{"mixed ready", github.Green, github.Cancelled, true, "ready", "", []string{"test"}, "1 required check(s) cancelled: test"},
		{"queued", github.Green, github.Pending, true, "pending", "", nil, ""},
		{"red sibling", github.Red, github.Cancelled, true, "blocked", "check_failed:vet", []string{"test"}, ""},
		{"all cancelled", github.Cancelled, github.Cancelled, true, "ready", "", []string{"vet", "test"}, "2 required check(s) cancelled: vet, test"},
		{"review missing", github.Cancelled, github.Cancelled, false, "blocked", "review_missing", []string{"vet", "test"}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, r := notifySetup(t)
			g.required = []string{"vet", "test"}
			g.checkNames = map[string]map[string]github.CI{shaA: {"vet": tt.vet, "test": tt.test}}
			if !tt.done {
				g.refs = map[string]bool{}
			}
			v := gateAt(t, s, g, shaB)
			if v.State != tt.state || reasons(v) != tt.reason || !slices.Equal(v.Review.ChecksCancelled, tt.cancelled) {
				t.Fatalf("verdict=%+v", v)
			}
			if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			if tt.summary != "" && !strings.Contains(lastEvent(t, n), tt.summary) {
				t.Fatalf("notice=%s", lastEvent(t, n))
			}
			if tt.state == "pending" && n.count != 0 {
				t.Fatalf("pending notified: %+v", n.events)
			}
			failed, err := s.failedChecks(context.Background(), r.Name, shaA, g.required)
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(failed, "test") {
				t.Fatalf("cancelled or pending reported as failed: %v", failed)
			}
		})
	}
}
