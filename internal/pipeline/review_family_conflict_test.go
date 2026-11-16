package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// conflictSetup is a fleet PR whose trailer family disagrees with the profile
// its own trailer names. devin-swe2 is family cognition, as panewire reports.
func conflictSetup(t *testing.T, family string) blockFixture {
	t.Helper()
	f := familyBlockSetup(t)
	if f.s.Config.Review.Profiles == nil {
		f.s.Config.Review.Profiles = map[string]config.Reviewer{}
	}
	f.s.Config.Review.Profiles["devin-swe2"] = config.Reviewer{Family: "cognition", Command: "pi", Args: []string{"--model", "devin-model"}}
	f.g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Profile: devin-swe2@high\nMergewake-Author-Family: " + family + "\n"
	return f
}

// A trailer family that conflicts with the profile blocks the head with the
// author-family-conflict kind and a comment naming both values and the fix.
// It is a block, not a plain error, so RetryReview records no review failure.
func TestAuthorFamilyConflictBlocksWithRemedy(t *testing.T) {
	f := conflictSetup(t, "other")
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("want ErrReviewBlocked, got %v", e)
	}
	if got := f.blocked(t, 7); got != blockAuthorFamilyConflict {
		t.Fatalf("block=%q", got)
	}
	if f.gh.posts != 1 || len(f.gh.comments) != 1 {
		t.Fatalf("posts=%d comments=%+v", f.gh.posts, f.gh.comments)
	}
	body := f.gh.comments[0].Body
	for _, want := range []string{"`other`", "`cognition`", "devin-swe2", "PR body", "Mergewake-Author-Family: cognition"} {
		if !strings.Contains(body, want) {
			t.Fatalf("comment missing %q: %s", want, body)
		}
	}
	if f.g.postCount != 0 || f.g.claimCount != 0 {
		t.Fatal("conflicting head wrote review state")
	}
}

// After the trailer is corrected in the PR body on the same head, the next
// evaluation clears the block and the review proceeds.
func TestAuthorFamilyConflictClearsAfterBodyFix(t *testing.T) {
	f := conflictSetup(t, "other")
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("want ErrReviewBlocked, got %v", e)
	}
	f.g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Profile: devin-swe2@high\nMergewake-Author-Family: cognition\n"
	if e := f.review(t); e != nil {
		t.Fatalf("after fix: %v", e)
	}
	if f.blocked(t, 7) != "" || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("block=%q refs=%v", f.blocked(t, 7), f.g.refs)
	}
}
