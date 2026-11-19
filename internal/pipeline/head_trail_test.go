package pipeline

import (
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// A head that follows a clean head writes one head-after-clean row, and its
// cause is the first that holds in the order merge-only, failed-check,
// medium-low, other. shaA is the clean head; shaD is the head pushed after it.
func TestHeadAfterCleanRowNamesItsCause(t *testing.T) {
	cases := []struct {
		name     string
		medium   bool // an open medium finding on the clean head
		merge    bool // the new head merges the clean head
		redCheck bool // a required check failed on the clean head
		cause    string
	}{
		{"merge-only", false, true, false, "merge-only"},
		{"merge-only beats medium-low", true, true, false, "merge-only"},
		{"failed-check", false, false, true, "failed-check"},
		{"medium-low", true, false, false, "medium-low"},
		{"other", false, false, false, "other"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, g, _, _, r, _ := deliverSetup(t)
			if c.medium {
				g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "medium", 3)}
			}
			if c.redCheck {
				g.checkNames = map[string]map[string]github.CI{shaA: {"CI": github.Red}}
			}
			deliver(t, s, r)
			g.pull.Head.SHA = shaD
			if c.merge {
				g.parents2 = map[string][]string{shaD: {shaA, shaC}}
			}
			markReviewed(g)
			deliver(t, s, r)
			rows := auditRows(t, s, r, "head-after-clean")
			if len(rows) != 1 || rows[0]["cause"] != c.cause || rows[0]["head"] != shaD || rows[0]["previous_head"] != shaA {
				t.Fatalf("head-after-clean rows = %v, want one row with cause %q", rows, c.cause)
			}
			deliver(t, s, r)
			if rows := auditRows(t, s, r, "head-after-clean"); len(rows) != 1 {
				t.Fatalf("a later pass wrote %d rows for the head, want one", len(rows))
			}
		})
	}
}

// A head whose review ended with an open high is not clean, so the head pushed
// after it writes no row.
func TestHeadAfterDirtyHeadWritesNoRow(t *testing.T) {
	s, g, _, _, r, _ := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	g.pull.Head.SHA = shaD
	markReviewed(g)
	deliver(t, s, r)
	if rows := auditRows(t, s, r, "head-after-clean"); len(rows) != 0 {
		t.Fatalf("head after a head with an open high wrote rows: %v", rows)
	}
}

// A head's outcome is recorded only once its review ends: until the -done
// marker exists the head is not judged, and the row waits for it.
func TestHeadAfterCleanWaitsForTheReviewToEnd(t *testing.T) {
	s, g, _, _, r, _ := deliverSetup(t)
	deliver(t, s, r)
	g.pull.Head.SHA = shaD
	deliver(t, s, r)
	if rows := auditRows(t, s, r, "head-after-clean"); len(rows) != 0 {
		t.Fatalf("a head whose review had not ended wrote rows: %v", rows)
	}
	markReviewed(g)
	deliver(t, s, r)
	if rows := auditRows(t, s, r, "head-after-clean"); len(rows) != 1 || rows[0]["cause"] != "other" {
		t.Fatalf("rows after the review ended = %v, want one other row", rows)
	}
}
