package pipeline

import (
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// A clean base-merge chain inherits a review of another patch. The inherited
// head's blocking outside-diff finding must stay open: its passes are not
// dropped for the gated head's patch, and they cannot settle it either.
func TestOutsideFindingOfInheritedHeadPersistsAcrossCleanChain(t *testing.T) {
	f := holdFinding("internal/pipeline/base_merge.go", 40, "high", "Chained base merge clears an open finding")
	history := []github.ReviewedPass{outsidePass(shaA, "p-a", 1, 1, f)}
	records := foldOutside(history, shaD, "p-d", shaA, true)
	if len(records) != 1 || records[0].Ref != f.Ref || records[0].State != outsideOpen || records[0].Head != shaA {
		t.Fatalf("inherited finding dropped by a clean chain: %+v", records)
	}
}

// The gated head's own settled review still clears a finding it was shown and
// does not report again.
func TestOutsideFindingReviewedByGatedHeadStillClears(t *testing.T) {
	f := holdFinding("internal/pipeline/base_merge.go", 40, "high", "Chained base merge clears an open finding")
	history := []github.ReviewedPass{outsidePass(shaA, "p-a", 1, 1, f)}
	own := outsidePass(shaD, "p-d", 1, 2)
	own.OutsideShown = f.Ref
	history = append(history, own)
	records := foldOutside(history, shaD, "p-d", "", true)
	if len(records) != 1 || records[0].State != outsideReviewed {
		t.Fatalf("a settled re-review of the gated head no longer clears: %+v", records)
	}
}
