package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

// repsAdds is a reps CLI that records every `reps add` it is given. Its first
// failAdds adds fail, as a shared store that is down would.
type repsAdds struct {
	failAdds int
	adds     [][]string
}

func (r *repsAdds) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	if len(args) < 2 || args[1] != "add" {
		return nil, nil
	}
	r.adds = append(r.adds, slices.Clone(args))
	if r.failAdds > 0 {
		r.failAdds--
		return nil, errors.New("offline")
	}
	return []byte("recorded rep id=srv:" + strconv.Itoa(len(r.adds)) + "\n"), nil
}

// repFlag returns the value after name in a reps add argument list.
func repFlag(t *testing.T, args []string, name string) string {
	t.Helper()
	i := slices.Index(args, name)
	if i < 0 || i+1 >= len(args) {
		t.Fatalf("reps add has no %s: %v", name, args)
	}
	return args[i+1]
}

// publishedHighByHead decodes the notes of a reps add. ok is false when the
// notes carry no high_by_head.
func publishedHighByHead(t *testing.T, args []string) (high []int, ok bool) {
	t.Helper()
	var notes map[string]json.RawMessage
	if err := json.Unmarshal([]byte(repFlag(t, args, "--notes")), &notes); err != nil {
		t.Fatal(err)
	}
	raw, ok := notes["high_by_head"]
	if !ok {
		return nil, false
	}
	if err := json.Unmarshal(raw, &high); err != nil {
		t.Fatal(err)
	}
	return high, true
}

// reviewedHead records a review of head with high critical-or-high findings,
// the way the live review path records it.
func reviewedHead(t *testing.T, s *Service, repo config.Repo, head string, high int) {
	t.Helper()
	findings := make([]github.Finding, 0, high)
	for range high {
		findings = append(findings, github.Finding{Severity: "high"})
	}
	if err := s.recordReview(repo.Name, 7, head, findings, github.PassReceipt{}, false); err != nil {
		t.Fatal(err)
	}
}

// Reviews are recorded per head in the order they were posted. Each head's
// high count is published oldest first, and rounds is the number of heads. The
// names sort the other way, so a sorted order would fail.
type headReview struct {
	sha  string
	high int
}

func TestRepRoundsCountReviewedHeadsInReviewOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		heads  []headReview
		rounds string
		high   []int
	}{
		{"two heads", []headReview{{"head-z", 2}, {"head-a", 0}}, "2", []int{2, 0}},
		{"one head", []headReview{{"head-z", 1}}, "1", []int{1}},
		{"no reviewed head", nil, "0", []int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, _, repo := setup(t)
			finalRow(t, s, repo, 7)
			r := &repsAdds{}
			s.Runner = r
			for _, h := range tc.heads {
				reviewedHead(t, s, repo, h.sha, h.high)
			}
			if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
				t.Fatal(err)
			}
			if len(r.adds) != 1 {
				t.Fatalf("reps add ran %d times, want 1", len(r.adds))
			}
			args := r.adds[0]
			if got := repFlag(t, args, "--rounds"); got != tc.rounds {
				t.Fatalf("--rounds %s, want %s", got, tc.rounds)
			}
			high, ok := publishedHighByHead(t, args)
			if !ok || !(len(high) == 0 && len(tc.high) == 0 || reflect.DeepEqual(high, tc.high)) {
				t.Fatalf("high_by_head=%v ok=%v, want %v", high, ok, tc.high)
			}
		})
	}
}

// A later revision of the same outcome publishes the counts of that revision.
func TestRepRoundsFollowupCountsThirdHead(t *testing.T) {
	s, _, _, _, repo := setup(t)
	finalRow(t, s, repo, 7)
	r := &repsAdds{}
	s.Runner = r
	reviewedHead(t, s, repo, "head-z", 2)
	reviewedHead(t, s, repo, "head-a", 0)
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	reviewedHead(t, s, repo, "head-m", 1)
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	if len(r.adds) != 2 {
		t.Fatalf("reps add ran %d times, want one per revision", len(r.adds))
	}
	args := r.adds[1]
	if got := repFlag(t, args, "--rounds"); got != "3" {
		t.Fatalf("followup --rounds %s, want 3", got)
	}
	if high, ok := publishedHighByHead(t, args); !ok || !reflect.DeepEqual(high, []int{2, 0, 1}) {
		t.Fatalf("followup high_by_head=%v ok=%v", high, ok)
	}
}

// A failed publish is retried with the exact same payload.
func TestRepRoundsRetryReplaysSamePayload(t *testing.T) {
	s, _, _, _, repo := setup(t)
	finalRow(t, s, repo, 7)
	r := &repsAdds{failAdds: 1}
	s.Runner = r
	reviewedHead(t, s, repo, "head-z", 2)
	reviewedHead(t, s, repo, "head-a", 0)
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	if len(r.adds) != 2 {
		t.Fatalf("reps add ran %d times, want 2", len(r.adds))
	}
	if !reflect.DeepEqual(r.adds[0], r.adds[1]) {
		t.Fatalf("retry payload changed:\nfirst  %v\nretry  %v", r.adds[0], r.adds[1])
	}
}

// fillGH serves the reviews by head as a map, and the review history
// separately, in the order the reviews were posted.
type fillGH struct {
	*fakeGH
	heads []github.ReviewedHead
}

func (g *fillGH) ReviewOutcomes(context.Context, string, int, string) (map[string]map[string]int, error) {
	return map[string]map[string]int{"head-a": {}, "head-z": {"high": 2}}, nil
}
func (g *fillGH) ReviewedHeads(context.Context, string, int, string) ([]github.ReviewedHead, error) {
	return g.heads, nil
}

// Observing a PR records the review order from the history, not from the map,
// which would sort head-a before head-z.
func TestObserveRecordsReviewOrderFromHistory(t *testing.T) {
	s, g, _, _, repo := setup(t)
	s.GH = &fillGH{fakeGH: g, heads: []github.ReviewedHead{{SHA: "head-z"}, {SHA: "head-a"}}}
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
	if !reflect.DeepEqual(v.ReviewedHeads, []string{"head-z", "head-a"}) {
		t.Fatalf("review order %v", v.ReviewedHeads)
	}
}
