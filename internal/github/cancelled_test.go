package github

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCheckStatesCompletedWithoutVerdict(t *testing.T) {
	for _, tt := range []struct {
		name, runs, statuses string
		state, aggregate     CI
	}{
		{"cancelled", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled"}]`, `[]`, CI("cancelled"), Pending},
		{"skipped", `[{"id":1,"name":"CI","status":"completed","conclusion":"skipped"}]`, `[]`, Skipped, Pending},
		{"neutral", `[{"id":1,"name":"CI","status":"completed","conclusion":"neutral"}]`, `[]`, Skipped, Pending},
		{"skipped sibling", `[{"id":1,"name":"CI","status":"completed","conclusion":"success"},{"id":2,"name":"CI","status":"completed","conclusion":"skipped"}]`, `[]`, Green, Green},
		{"queued", `[{"id":1,"name":"CI","status":"queued"}]`, `[]`, Pending, Pending},
		{"failure", `[{"id":1,"name":"CI","status":"completed","conclusion":"failure"}]`, `[]`, Red, Red},
		{"failure then cancelled", `[{"id":1,"name":"CI","status":"completed","conclusion":"failure"},{"id":2,"name":"CI","status":"completed","conclusion":"cancelled"}]`, `[]`, Red, Pending},
		{"cancelled listed before failure", `[{"id":2,"name":"CI","status":"completed","conclusion":"cancelled"},{"id":1,"name":"CI","status":"completed","conclusion":"failure"}]`, `[]`, Red, Pending},
		{"success then cancelled", `[{"id":1,"name":"CI","status":"completed","conclusion":"success"},{"id":2,"name":"CI","status":"completed","conclusion":"cancelled"}]`, `[]`, Green, Pending},
		{"newer failure", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled"},{"id":2,"name":"CI","status":"completed","conclusion":"failure"}]`, `[]`, Red, Red},
		{"status error", `[]`, `[{"id":1,"context":"CI","state":"error"}]`, Red, Red},
		{"pending status and cancelled", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled"}]`, `[{"id":1,"context":"CI","state":"pending"}]`, Pending, Pending},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/check-runs") {
					fmt.Fprint(w, `{"check_runs":`+tt.runs+`}`)
				} else {
					fmt.Fprint(w, tt.statuses)
				}
			})
			states, err := c.CheckStates(context.Background(), "o/r", "sha", []string{"CI"})
			if err != nil || states["CI"] != tt.state {
				t.Errorf("states=%v err=%v, want %s", states, err, tt.state)
			}
			got, err := c.Check(context.Background(), "o/r", "sha", []string{"CI"})
			if err != nil || got != tt.aggregate {
				t.Errorf("aggregate=%s err=%v, want %s", got, err, tt.aggregate)
			}
		})
	}
}

// A cancelled run reports when it completed; a newer run on the head replaces it.
func TestCancelledAtLatestRun(t *testing.T) {
	for _, tt := range []struct {
		name, runs string
		state      CI
		at         time.Time
	}{
		{"cancelled", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled","completed_at":"2026-11-05T10:00:00Z"}]`, Cancelled, time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)},
		{"cancelled without a time", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled"}]`, Cancelled, time.Time{}},
		{"newer green", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled","completed_at":"2026-11-05T10:00:00Z"},{"id":2,"name":"CI","status":"completed","conclusion":"success"}]`, Green, time.Time{}},
		{"newer queued", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled","completed_at":"2026-11-05T10:00:00Z"},{"id":2,"name":"CI","status":"queued"}]`, Pending, time.Time{}},
		{"skipped", `[{"id":1,"name":"CI","status":"completed","conclusion":"skipped","completed_at":"2026-11-05T10:00:00Z"}]`, Skipped, time.Time{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/check-runs") {
					fmt.Fprint(w, `{"check_runs":`+tt.runs+`}`)
				} else {
					fmt.Fprint(w, `[]`)
				}
			})
			states, err := c.CheckStates(context.Background(), "o/r", "sha", []string{"CI"})
			if err != nil || states["CI"] != tt.state {
				t.Fatalf("states=%v err=%v, want %s", states, err, tt.state)
			}
			at, err := c.CancelledAt(context.Background(), "o/r", "sha", []string{"CI"})
			got, ok := at["CI"]
			if err != nil || ok != (tt.state == Cancelled) || !got.Equal(tt.at) {
				t.Fatalf("cancelledAt=%v err=%v, want %v", at, err, tt.at)
			}
		})
	}
}
