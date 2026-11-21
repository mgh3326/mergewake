package pipeline

import (
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// Decision 462: a two-pass set counts whatever the reviewer families, so a
// same-family pass is a full pass. Only two different receipts for one pass
// number make the set incomplete.
func TestDistinctPassesCountsSameFamilyPasses(t *testing.T) {
	cases := []struct {
		name     string
		receipts []github.PassReceipt
		want     bool
	}{
		{
			name: "same family, default lenses are complete",
			receipts: []github.PassReceipt{
				{Pass: 1, Family: "anthropic", Lens: "default", SameFamily: true},
				{Pass: 2, Family: "anthropic", Lens: "default", SameFamily: true},
			},
			want: true,
		},
		{
			name: "empty family on one pass is still a counted pass",
			receipts: []github.PassReceipt{
				{Pass: 1, Lens: "correctness"},
				{Pass: 2, Family: "openai", Lens: "state-risk"},
			},
			want: true,
		},
		{
			name: "two different receipts for pass 1 stay incomplete",
			receipts: []github.PassReceipt{
				{Pass: 1, Family: "anthropic", Lens: "correctness", Round: 1},
				{Pass: 1, Family: "anthropic", Lens: "correctness", Round: 2},
				{Pass: 2, Family: "openai", Lens: "state-risk"},
			},
			want: false,
		},
		{
			name: "identical duplicate receipt does not add coverage",
			receipts: []github.PassReceipt{
				{Pass: 1, Family: "anthropic", Lens: "correctness"},
				{Pass: 1, Family: "anthropic", Lens: "correctness"},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, got := distinctPasses(tc.receipts, 2); got != tc.want {
				t.Fatalf("distinctPasses complete = %v, want %v", got, tc.want)
			}
		})
	}
}
