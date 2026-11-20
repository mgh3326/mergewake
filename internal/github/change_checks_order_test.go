package github

import (
	"context"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/changestate"
)

// queuedWriter keeps the sections a Client queues, in order.
type queuedWriter struct{ sections []changestate.Section }

func (q *queuedWriter) Queue(_ string, _ int, _ string, s changestate.Section) {
	q.sections = append(q.sections, s)
}
func (q *queuedWriter) Async(fn func())            { fn() }
func (q *queuedWriter) Counts() changestate.Counts { return changestate.Counts{} }
func (q *queuedWriter) Wait(context.Context)       {}

// The checks section lists its checks by name every time, so the same set
// hashes alike and is not written again. The checks come from a map, whose
// iteration order is random: unlisted, the order changed from one observation
// to the next and every observation of two or more checks was a new write.
func TestWriteChecksListsChecksByName(t *testing.T) {
	w := &queuedWriter{}
	c := &Client{Changes: w}
	c.changeHeads.Store("o/r\x007", "abc")
	statuses := []status{
		{ID: 4, Context: "vet", State: "success"},
		{ID: 2, Context: "build", State: "success"},
		{ID: 3, Context: "test", State: "pending"},
		{ID: 1, Context: "lint", State: "success"},
	}
	want := []string{"build", "lint", "test", "vet"}
	for range 12 {
		c.writeChecks(context.Background(), "o/r", "abc", nil, statuses)
	}
	if len(w.sections) != 12 {
		t.Fatalf("queued sections = %d, want 12", len(w.sections))
	}
	for i, s := range w.sections {
		checks := s.Value.(map[string]any)["checks"].([]map[string]any)
		var names []string
		for _, item := range checks {
			names = append(names, item["name"].(string))
		}
		if !slices.Equal(names, want) {
			t.Fatalf("observation %d lists checks %v, want %v", i, names, want)
		}
	}
}
