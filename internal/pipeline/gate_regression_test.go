package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// The review marker is head-bound: a marker for another head, or none at all,
// blocks with review_missing; only the current head's -done ref merges.
func TestLandMarkerBoundToHead(t *testing.T) {
	for _, which := range []string{"missing", "stale", "current"} {
		t.Run(which, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			head := shaB
			if which == "current" {
				head = g.pull.Head.SHA
			}
			if which != "missing" {
				g.refs = map[string]bool{doneRef(7, head): true}
			}
			if which == "current" {
				g.reviewed = true
			}
			if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			want := 0
			if which == "current" {
				want = 1
			}
			if g.mergeCount != want {
				t.Fatalf("merges=%d action=%s", g.mergeCount, landAction(t, s, r.Name))
			}
		})
	}
}

// The marker alone is not trusted: a hand-created -done ref without the
// review identity's head-bound passes still reports review_missing.
func TestLandForgedMarkerNeedsPasses(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.refs = map[string]bool{doneRef(7, g.pull.Head.SHA): true}
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "blocked" {
		t.Fatal("forged marker merged")
	}
}

// A stale base is its own verdict even on an otherwise ready head: the worker
// updates the branch, which produces a new head to gate.
func TestLandStaleBaseBlocks(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.stale = true
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "blocked" {
		t.Fatal("stale-base head merged")
	}
	g.stale = false
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 {
		t.Fatal("fresh base did not land")
	}
}

// Any unresolved review thread holds the head regardless of who opened it;
// resolving it releases the merge.
func TestLandOpenThreadsBlock(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.findings = 3
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "blocked" {
		t.Fatal("unresolved threads landed")
	}
	g.findings = 0
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 {
		t.Fatal("resolved threads did not land")
	}
}

// A resolved review.login finding is resolved: resolving is its disposition,
// at any severity, and needs no new head.
func TestLandResolvedOwnFindingLands(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedback.Threads = []github.ReviewThread{{IsResolved: true, Comments: []github.ReviewComment{{Author: "review-bot", CommitSHA: shaA, Body: "<!-- mergewake-severity:high --> finding"}, {Author: "dev", AuthorType: "User", Body: "ok"}}}}
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 {
		t.Fatalf("a resolved review finding held the merge: %s", landAction(t, s, r.Name))
	}
}

// A conflicting PR is blocked; mergeable evidence GitHub has not computed yet
// waits silently.
func TestLandConflictAndUnknownMergeable(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	no := false
	g.pull.Mergeable = &no
	g.pull.MergeableState = "dirty"
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "blocked" {
		t.Fatal("conflicting PR merged")
	}

	s, g, _, _, r = setup(t)
	g.enabled = true
	markReviewed(g)
	g.pull.Mergeable = nil
	g.pull.MergeableState = "unknown"
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "pending" {
		t.Fatal("uncomputed mergeable merged")
	}
}

// The -done marker a review leaves is the gate's review evidence: a completed
// review makes the head landable without further review bookkeeping.
func TestReviewDoneMarkerLands(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 {
		t.Fatal("reviewed head did not land")
	}
}

func TestReviewReclaimsExpiredClaim(t *testing.T) {
	for _, age := range []time.Duration{time.Second, 2 * time.Minute} {
		t.Run(age.String(), func(t *testing.T) {
			s, g, _, _, r := setup(t)
			ref := claimRef(7, g.pull.Head.SHA, 1)
			g.refs = map[string]bool{ref: true}
			g.leaseTimes = map[string]time.Time{ref: time.Now().Add(-age)}
			var out strings.Builder
			s.Out = &out
			if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil {
				t.Fatal(err)
			}
			want := 0
			if age == 2*time.Minute {
				want = 1
			}
			if g.postCount != want || s.Runner.(*fakeRunner).calls != want {
				t.Fatalf("posts=%d output=%s", g.postCount, out.String())
			}
		})
	}
}

// GitHub moves a comment's commit to each new head while its line is
// unchanged. A finding posted on an older head and resolved after the fix must
// not stay held: only a finding posted on this head blocks a resolve.
func TestLandResolvedFindingFromOlderHeadDoesNotHold(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedback.Threads = []github.ReviewThread{{IsResolved: true, Comments: []github.ReviewComment{{Author: "review-bot", CommitSHA: shaA, OriginalCommitSHA: shaB, Body: "<!-- mergewake-severity:high --> finding"}, {Author: "dev", AuthorType: "User", Body: "fixed"}}}}
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if landAction(t, s, r.Name) == "blocked" {
		t.Fatalf("finding resolved on an older head was held: %s", landAction(t, s, r.Name))
	}
}

// The same finding posted on the current head is released by a resolve too.
func TestLandResolvedFindingPostedOnHeadLands(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedback.Threads = []github.ReviewThread{{IsResolved: true, Comments: []github.ReviewComment{{Author: "review-bot", CommitSHA: shaA, OriginalCommitSHA: shaA, Body: "<!-- mergewake-severity:high --> finding"}, {Author: "dev", AuthorType: "User", Body: "ok"}}}}
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 {
		t.Fatalf("a resolved finding posted on the head held the merge: %s", landAction(t, s, r.Name))
	}
}
