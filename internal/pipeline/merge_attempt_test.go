package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

const shaSquashed = "2222222222222222222222222222222222222222"

// unknownGH answers the first merge PUT with status. With landed it writes the
// squash onto the base first, the way the incident did: GitHub wrote the commit
// and left the PR open. With merged the PR reads back merged. With neither,
// nothing changed on GitHub.
type unknownGH struct {
	*fakeGH
	status         int
	landed, merged bool
	calls          int
}

func (g *unknownGH) Merge(ctx context.Context, repo string, n int, sha string) error {
	g.calls++
	if g.calls > 1 {
		return g.fakeGH.Merge(ctx, repo, n, sha)
	}
	if g.landed {
		g.fakeGH.head = shaSquashed
		g.fakeGH.parents[shaSquashed] = shaB
		g.fakeGH.messages[shaSquashed] = "Add the thing (#7)"
	}
	if g.merged {
		at := "2026-11-24T23:54:48Z"
		g.fakeGH.pull.State, g.fakeGH.pull.MergedAt = "closed", &at
	}
	if g.status >= 500 {
		return &github.UnknownOutcomeError{Err: &github.APIError{Code: g.status, Method: "PUT", Path: "/pulls/7/merge"}}
	}
	return &github.APIError{Code: g.status, Method: "PUT", Path: "/pulls/7/merge"}
}

func unknownMergeSetup(t *testing.T, status int) (*Service, *unknownGH, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, _, n, r := setup(t)
	g.enabled = true
	g.messages = map[string]string{}
	g.parents = map[string]string{}
	markReviewed(g)
	u := &unknownGH{fakeGH: g, status: status}
	s.GH = u
	return s, u, n, r
}

// A PUT that ends in 5xx and left its squash on the base is never merged
// again: the next pass holds the PR once and does not send a second PUT.
func TestUnknownMergeLandedHoldsWithoutSecondMerge(t *testing.T) {
	s, u, n, r := unknownMergeSetup(t, 500)
	u.landed = true
	ctx := context.Background()
	if e := s.Land(ctx, r, 7, Options{}); !github.IsUnknownOutcome(e) {
		t.Fatalf("first land: %v", e)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); e != nil {
		t.Fatalf("unknown attempt not kept: %v", e)
	}
	for pass := 0; pass < 2; pass++ {
		if e := s.Land(ctx, r, 7, Options{}); e == nil || !strings.Contains(e.Error(), "landed on main") {
			t.Fatalf("pass %d: want hold, got %v", pass, e)
		}
	}
	if u.calls != 1 || u.fakeGH.mergeCount != 0 {
		t.Fatalf("merge PUTs %d, merges %d; want 1 and 0", u.calls, u.fakeGH.mergeCount)
	}
	notices := 0
	for _, e := range n.events {
		if strings.HasPrefix(e.ID, "mergewake-merge-held-7-") {
			notices++
			if e.Type != "blocked" {
				t.Fatalf("hold notice type %q", e.Type)
			}
		}
	}
	if notices != 1 {
		t.Fatalf("hold notices %d, want 1", notices)
	}
}

// A PUT that ends in 5xx with nothing written leaves the merge undone: the
// next pass settles that and merges once.
func TestUnknownMergeNothingWrittenMergesOnLaterPass(t *testing.T) {
	s, u, _, r := unknownMergeSetup(t, 500)
	if e := s.Land(context.Background(), r, 7, Options{}); !github.IsUnknownOutcome(e) {
		t.Fatalf("first land: %v", e)
	}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatalf("later land: %v", e)
	}
	if u.calls != 2 || u.fakeGH.mergeCount != 1 {
		t.Fatalf("merge PUTs %d, merges %d; want 2 and 1", u.calls, u.fakeGH.mergeCount)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); !os.IsNotExist(e) {
		t.Fatalf("attempt still pending after the merge: %v", e)
	}
}

// A PUT that ends in 5xx while the PR reads back merged is recorded as merged
// and never sent again.
func TestUnknownMergeReadBackMergedRecordedWithoutSecondMerge(t *testing.T) {
	s, u, _, r := unknownMergeSetup(t, 500)
	u.merged = true
	ctx := context.Background()
	if e := s.Land(ctx, r, 7, Options{}); !github.IsUnknownOutcome(e) {
		t.Fatalf("first land: %v", e)
	}
	if e := s.Land(ctx, r, 7, Options{}); e != nil {
		t.Fatalf("land after merged read-back: %v", e)
	}
	if u.calls != 1 || u.fakeGH.mergeCount != 0 {
		t.Fatalf("merge PUTs %d, merges %d; want 1 and 0", u.calls, u.fakeGH.mergeCount)
	}
	audit, e := os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl"))
	if e != nil || !strings.Contains(string(audit), `"state":"merged"`) {
		t.Fatalf("merge not recorded as merged: %v %s", e, audit)
	}
	if _, e := os.Stat(s.mergeAttemptFile(r.Name, 7)); !os.IsNotExist(e) {
		t.Fatalf("attempt still pending: %v", e)
	}
}

// A refusal (405) is an ordinary refusal: nothing is kept pending and a later
// pass merges as it did before.
func TestRefusalIsNotUnknownAndMergesLater(t *testing.T) {
	s, u, _, r := unknownMergeSetup(t, 405)
	e := s.Land(context.Background(), r, 7, Options{})
	if e == nil || github.IsUnknownOutcome(e) {
		t.Fatalf("refusal: %v", e)
	}
	if _, se := os.Stat(s.mergeAttemptFile(r.Name, 7)); !os.IsNotExist(se) {
		t.Fatalf("refusal left an attempt pending: %v", se)
	}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatalf("later land: %v", e)
	}
	if u.calls != 2 || u.fakeGH.mergeCount != 1 {
		t.Fatalf("merge PUTs %d, merges %d; want 2 and 1", u.calls, u.fakeGH.mergeCount)
	}
}

// A revert merge that waits on the merge lock reads main after the lock is
// taken: when another caller moved main meanwhile, it holds with the "main
// changed" outcome and sends no PUT.
func TestRevertWaitingOnTheLockSeesMainMove(t *testing.T) {
	s, rg, _, n, r := recoverySetup(t, nil)
	ws := &watchStream{Service: s}
	unlock, e := s.mergeLock(r.Name)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		done <- ws.mergeRevert(context.Background(), r, shaC, nil, rg.revert, recoveryIncident{}, watchResult{}, Options{})
	}()
	time.Sleep(300 * time.Millisecond)
	rg.head = advancedHead
	unlock()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mergeRevert did not return after the merge lock was released")
	}
	if rg.merges != 0 {
		t.Fatalf("revert PUT %d times after main moved", rg.merges)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "main changed during recovery") {
		t.Fatalf("%d design-lane lines: %v", len(lines), lines)
	}
}

// The watch's revert merge settles a pending unknown outcome before it reads
// main again: the squash of the revert is on main, so the revert is held once
// and is not sent a second time.
func TestRecoveryUnknownOutcomeLandedHolds(t *testing.T) {
	s, rg, _, n, r := recoverySetup(t, nil)
	rg.messages = map[string]string{shaSquashed: "Revert the culprit (#9)"}
	rg.parents[shaSquashed] = shaC
	rg.head = shaSquashed
	if e := fsutil.WriteJSON(s.mergeAttemptFile(r.Name, 9), mergeAttempt{PR: 9, Head: revertHead, Base: shaC, At: time.Now()}); e != nil {
		t.Fatal(e)
	}
	ws := &watchStream{Service: s}
	ctx := context.Background()
	for pass := 0; pass < 2; pass++ {
		if e := ws.mergeRevert(ctx, r, shaC, nil, rg.revert, recoveryIncident{}, watchResult{}, Options{}); e != nil {
			t.Fatalf("pass %d: %v", pass, e)
		}
	}
	if rg.merges != 0 {
		t.Fatalf("revert merged %d times after its squash landed", rg.merges)
	}
	if lines := ownerLaneEvents(n); len(lines) != 1 || !strings.Contains(lines[0], "already on main") {
		t.Fatalf("%d design-lane lines: %v", len(lines), lines)
	}
}
