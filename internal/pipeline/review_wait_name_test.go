package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// (a) The wait text follows the flag that won: a round hold joined with a
// quota stop is named held, and only a quota stop on its own is named after its
// pool.
func TestReviewWaitTextFollowsTheFlagThatWon(t *testing.T) {
	quota := &reviewQuotaStopped{label: "reviewer", pool: "seat"}
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{"round hold with a quota stop", errors.Join(ErrReviewHeld, quota), "held (round-hold)"},
		{"round hold alone", ErrReviewHeld, "held (round-hold)"},
		{"quota stop alone", quota, "quota-stopped (pool seat)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var class reviewFailureClass
			class.add(tt.err)
			if got := class.waitText(); got != tt.want {
				t.Fatalf("wait text=%q, want %q", got, tt.want)
			}
		})
	}
}

// (b) The held wait reaches the stall alert: a round hold joined with a quota
// stop names the hold in the alert and in the stall row's wait key, never the
// pool.
func TestRoundHoldWithQuotaStopStallNamesTheHold(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	if _, err := s.reviewWaitSince(r, g.pull); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(reviewStallAfter + time.Minute)

	var class reviewFailureClass
	class.add(errors.Join(ErrReviewHeld, &reviewQuotaStopped{label: "reviewer", pool: "seat"}))
	wait := reviewWaitErr(ErrReviewHeld, class.waitText())
	if err := s.NotifyReviewStall(context.Background(), r, g.pull, Options{}, wait); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("events=%+v, want one stall alert", n.events)
	}
	if text := n.events[0].Text; !strings.Contains(text, "held (round-hold)") || strings.Contains(text, "quota-stopped") {
		t.Fatalf("alert=%q, want the held wait named and no quota stop", text)
	}
	rows := auditRows(t, s, r, "pr-notify")
	if len(rows) == 0 {
		t.Fatal("no pr-notify row for the stall")
	}
	if got := rows[len(rows)-1]["wait"]; got != "held (round-hold)" {
		t.Fatalf("stall row wait=%v, want held (round-hold)", got)
	}
}
