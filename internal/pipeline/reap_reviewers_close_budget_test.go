package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// A reviewer job close the repo budget cuts is a budget stop, not an error
// row: the job stays unclosed and the label is revisited on a later sweep.
func TestReapReviewerJobCloseBudgetCutIsAStopNotAnError(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	f.s.Config.Review.Panewire.Owner = "lane-x"
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	f.s.Runner = refusingClose{scriptRunner: f.run, refusal: fmt.Errorf("%w: panewire job close", ErrReapRepoBudget)}

	err := f.reap(t, 0, Options{})
	if other := ReapNonBudgetErrors(err); other != nil {
		t.Fatalf("a budget-cut job close added a non-budget error: %v", other)
	}
	var held int
	for _, r := range f.rows(t) {
		switch {
		case r.Result == "error" || r.Result == "abandoned" || r.Result == "closed":
			t.Fatalf("row %s %s: %s", r.Label, r.Result, r.Reason)
		case r.Result == "pending-cleanup" && r.Reason == reviewerBudgetHold:
			held++
		}
	}
	if held != 1 || strings.Contains(fmt.Sprint(f.run.calls), "job\x00close") == false {
		t.Fatalf("want one budget hold row after a close attempt: rows %v calls %q", rowSummary(f.rows(t)), f.run.calls)
	}
}
