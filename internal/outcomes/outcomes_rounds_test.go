package outcomes

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// publishedNotes publishes v through a fake reps CLI and returns the --rounds
// value and the decoded notes of its reps add.
func publishedNotes(t *testing.T, v Outcome) (rounds string, notes map[string]json.RawMessage) {
	t.Helper()
	r := &cliRunner{}
	if _, err := (CLI{Runner: r, Command: "panewire"}).Publish(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(r.args, " "), "--rounds ") {
		t.Fatal("reps add has no --rounds")
	}
	for i, a := range r.args {
		if a == "--rounds" {
			rounds = r.args[i+1]
		}
	}
	if err := json.Unmarshal([]byte(r.args[len(r.args)-1]), &notes); err != nil {
		t.Fatal(err)
	}
	return rounds, notes
}

// Heads with a review but no recorded place (an outcome kept before the order
// was) still count toward rounds, but their order is unknown, so no
// high_by_head is published rather than a guessed one.
func TestRoundsWithoutRecordedOrderOmitHighByHead(t *testing.T) {
	var v Outcome
	complete(&v)
	v.Reviews = map[string]map[string]int{"head": {"high": 1}, "older": {}}
	rounds, notes := publishedNotes(t, v)
	if rounds != "2" {
		t.Fatalf("rounds=%s, want 2", rounds)
	}
	if _, ok := notes["high_by_head"]; ok {
		t.Fatalf("high_by_head published without a recorded order: %s", notes["high_by_head"])
	}
}

// An outcome with no reviewed head publishes 0 rounds and an empty
// high_by_head, never 1.
func TestNoReviewedHeadPublishesZeroRounds(t *testing.T) {
	var v Outcome
	complete(&v)
	v.Reviews = map[string]map[string]int{}
	rounds, notes := publishedNotes(t, v)
	if rounds != "0" {
		t.Fatalf("rounds=%s, want 0", rounds)
	}
	if string(notes["high_by_head"]) != "[]" {
		t.Fatalf("high_by_head=%s, want []", notes["high_by_head"])
	}
}

// A head in the recorded order that has no review is not a round.
func TestRecordedHeadWithoutReviewIsNotARound(t *testing.T) {
	var v Outcome
	complete(&v)
	v.Reviews = map[string]map[string]int{}
	v.AddReviewedHead("unreviewed")
	if rounds, _ := publishedNotes(t, v); rounds != "0" {
		t.Fatalf("rounds=%s, want 0", rounds)
	}
}

// The recorded order wins over the map, and each head is kept once at the
// place it was first reviewed.
func TestRecordedOrderIsHeadOrder(t *testing.T) {
	var v Outcome
	complete(&v)
	v.Reviews = map[string]map[string]int{"head-a": {}, "head-z": {"critical": 1, "high": 1}}
	v.AddReviewedHead("head-z")
	v.AddReviewedHead("head-a")
	v.AddReviewedHead("head-z")
	if !reflect.DeepEqual(v.ReviewedHeads, []string{"head-z", "head-a"}) {
		t.Fatal(v.ReviewedHeads)
	}
	rounds, notes := publishedNotes(t, v)
	if rounds != "2" || string(notes["high_by_head"]) != "[2,0]" {
		t.Fatalf("rounds=%s high_by_head=%s", rounds, notes["high_by_head"])
	}
}

// SetReviewedHeadOrder takes the history's order and keeps a head the history
// does not list after it.
func TestSetReviewedHeadOrderKeepsUnlistedHeads(t *testing.T) {
	var v Outcome
	v.AddReviewedHead("local-only")
	v.SetReviewedHeadOrder([]string{"other-host", "local-only", "newest"})
	if !reflect.DeepEqual(v.ReviewedHeads, []string{"other-host", "local-only", "newest"}) {
		t.Fatal(v.ReviewedHeads)
	}
	v.SetReviewedHeadOrder([]string{"newest"})
	if !reflect.DeepEqual(v.ReviewedHeads, []string{"newest", "other-host", "local-only"}) {
		t.Fatal(v.ReviewedHeads)
	}
}
