package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/reap"
)

type livenessFixture struct {
	s     *Service
	n     *fakeNotifier
	repos []config.Repo
	now   time.Time
}

func newLivenessFixture(t *testing.T, now time.Time) *livenessFixture {
	t.Helper()
	s, _, _, n, _ := setup(t)
	_ = os.MkdirAll(s.Config.StateDir, 0755)
	repoA := config.Repo{Name: "example/a", Branch: "main", Lane: "design-lane"}
	repoB := config.Repo{Name: "example/b", Branch: "main", Lane: "design-lane"}
	s.Config.Repos = []config.Repo{repoA, repoB}
	s.Config.Hosts = []reap.Host{{Name: "node-a"}, {Name: "node-b"}}
	s.Config.Liveness.Lane = "liveness-desk"
	s.Now = func() time.Time { return now }
	return &livenessFixture{s: s, n: n, repos: []config.Repo{repoA, repoB}, now: now}
}

func (f *livenessFixture) audit(t *testing.T, repo, cmd string, val any, at time.Time) {
	t.Helper()
	f.s.Now = func() time.Time { return at }
	if err := f.s.audit(repo, cmd, val, false); err != nil {
		t.Fatal(err)
	}
	f.s.Now = func() time.Time { return f.now }
}

func livenessRows(t *testing.T, f *livenessFixture, repo, cmd string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.s.state(repo), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Command string          `json:"command"`
			Value   json.RawMessage `json:"value"`
		}
		if json.Unmarshal([]byte(line), &row) == nil && row.Command == cmd {
			var m map[string]any
			if json.Unmarshal(row.Value, &m) == nil {
				out = append(out, m)
			}
		}
	}
	return out
}

// TestLiveness_MechanismsHealthyAndStalled verifies that each mechanism stays quiet
// on a synthetic healthy day and raises on a synthetic stalled day.
func TestLiveness_MechanismsHealthyAndStalled(t *testing.T) {
	day := "2026-11-17"
	dTime := balanceDayTime(day, 12)
	ctx := context.Background()

	// 1. review-pass
	t.Run(MechReviewPass, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		// Healthy: 10 made, 10 done (100% >= 50%)
		for i := 1; i <= 10; i++ {
			head := "sha-pass-" + strconv.Itoa(i)
			f.audit(t, f.repos[0].Name, "review-start", map[string]any{"head": head, "pr": i}, dTime)
			f.audit(t, f.repos[0].Name, "review", map[string]any{"head": head, "pr": i}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechReviewPass {
				m = item
			}
		}
		if m.Stalled || m.Made != 10 || m.Done != 10 {
			t.Fatalf("healthy review-pass: %+v", m)
		}

		// Stalled: 10 made, 2 done (20% < 50%)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 10; i++ {
			head := "sha-pass-" + strconv.Itoa(i)
			f2.audit(t, f2.repos[0].Name, "review-start", map[string]any{"head": head, "pr": i}, dTime)
			if i <= 2 {
				f2.audit(t, f2.repos[0].Name, "review", map[string]any{"head": head, "pr": i}, dTime)
			}
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechReviewPass {
				m = item
			}
		}
		if !m.Stalled || m.Made != 10 || m.Done != 2 {
			t.Fatalf("stalled review-pass: %+v", m)
		}

		// Superseded head without verdict does not count as done:
		// 10 PRs have head A and head B with review-start, but neither gets a verdict
		f3 := newLivenessFixture(t, dTime)
		for i := 1; i <= 10; i++ {
			headA := "sha-pass-a-" + strconv.Itoa(i)
			headB := "sha-pass-b-" + strconv.Itoa(i)
			f3.audit(t, f3.repos[0].Name, "review-start", map[string]any{"head": headA, "pr": i}, dTime)
			f3.audit(t, f3.repos[0].Name, "review-start", map[string]any{"head": headB, "pr": i}, dTime)
		}
		res3, err := f3.s.livenessForDay(f3.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res3.Mechanisms {
			if item.Mechanism == MechReviewPass {
				m = item
			}
		}
		if !m.Stalled || m.Made != 20 || m.Done != 0 {
			t.Fatalf("superseded heads without verdict must not count as done: %+v", m)
		}
	})

	// 2. reviewer-landing
	t.Run(MechReviewerLanding, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		for i := 1; i <= 10; i++ {
			lbl := "lbl-" + strconv.Itoa(i)
			f.audit(t, f.repos[0].Name, "review-brief-hk", map[string]any{"label": lbl}, dTime)
			f.audit(t, f.repos[0].Name, "reviewer-spawn", map[string]any{"label": lbl}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechReviewerLanding {
				m = item
			}
		}
		if m.Stalled || m.Made != 10 || m.Done != 10 {
			t.Fatalf("healthy reviewer-landing: %+v", m)
		}

		// Stalled: 10 made, 2 landed (20% < 50%)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 10; i++ {
			lbl := "lbl-" + strconv.Itoa(i)
			f2.audit(t, f2.repos[0].Name, "review-brief-hk", map[string]any{"label": lbl}, dTime)
			if i <= 2 {
				f2.audit(t, f2.repos[0].Name, "reviewer-spawn", map[string]any{"label": lbl}, dTime)
			}
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechReviewerLanding {
				m = item
			}
		}
		if !m.Stalled || m.Made != 10 || m.Done != 2 {
			t.Fatalf("stalled reviewer-landing: %+v", m)
		}
	})

	// 3. findings-delivery
	t.Run(MechFindingsDelivery, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		for i := 1; i <= 3; i++ {
			head := "crit-head-" + strconv.Itoa(i)
			f.audit(t, f.repos[0].Name, "review-round-finding", map[string]any{"head": head, "severity": "critical", "pr": i}, dTime)
			f.audit(t, f.repos[0].Name, "findings-delivered", map[string]any{"head": head, "pr": i}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechFindingsDelivery {
				m = item
			}
		}
		if m.Stalled || m.Made != 3 || m.Done != 3 {
			t.Fatalf("healthy findings-delivery: %+v", m)
		}

		// Stalled: 3 made, 0 delivered
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 3; i++ {
			head := "crit-head-" + strconv.Itoa(i)
			f2.audit(t, f2.repos[0].Name, "review-round-finding", map[string]any{"head": head, "severity": "high", "pr": i}, dTime)
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechFindingsDelivery {
				m = item
			}
		}
		if !m.Stalled || m.Made != 3 || m.Done != 0 {
			t.Fatalf("stalled findings-delivery: %+v", m)
		}
	})

	// 4. merge
	t.Run(MechMerge, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		for i := 1; i <= 5; i++ {
			f.audit(t, f.repos[0].Name, "land", map[string]any{"action": "merge", "pr": i}, dTime)
			f.audit(t, f.repos[0].Name, "pr-notify", map[string]any{"state": "merged", "pr": i}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechMerge {
				m = item
			}
		}
		if m.Stalled || m.Made != 5 || m.Done != 5 {
			t.Fatalf("healthy merge: %+v", m)
		}

		// Stalled: 5 made, 1 merged (4 unmerged >= 3)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 5; i++ {
			f2.audit(t, f2.repos[0].Name, "land", map[string]any{"action": "merge", "pr": i}, dTime)
			if i == 1 {
				f2.audit(t, f2.repos[0].Name, "pr-notify", map[string]any{"state": "merged", "pr": i}, dTime)
			}
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechMerge {
				m = item
			}
		}
		if !m.Stalled || m.Made != 5 || m.Done != 1 {
			t.Fatalf("stalled merge: %+v", m)
		}
	})

	// 5. reviewer-checkout-removal
	t.Run(MechReviewerCheckoutRemoval, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		// Healthy: 20 created, 20 removed
		for i := 1; i <= 20; i++ {
			lbl := balanceLabel(i, dTime)
			f.audit(t, f.repos[0].Name, reviewerSpawnCommand, map[string]any{"label": lbl, "host": "node-a"}, dTime)
			f.audit(t, f.repos[0].Name, "reviewer-reap", map[string]any{"label": lbl, "host": "node-a", "result": "removed"}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechReviewerCheckoutRemoval {
				m = item
			}
		}
		if m.Stalled || m.Made != 20 || m.Done != 20 {
			t.Fatalf("healthy reviewer-checkout-removal: %+v", m)
		}

		// Stalled: 20 created, 2 removed (< 70%)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 20; i++ {
			lbl := balanceLabel(i, dTime)
			f2.audit(t, f2.repos[0].Name, reviewerSpawnCommand, map[string]any{"label": lbl, "host": "node-a"}, dTime)
			if i <= 2 {
				f2.audit(t, f2.repos[0].Name, "reviewer-reap", map[string]any{"label": lbl, "host": "node-a", "result": "removed"}, dTime)
			}
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechReviewerCheckoutRemoval {
				m = item
			}
		}
		if !m.Stalled || m.Made != 20 || m.Done != 2 {
			t.Fatalf("stalled reviewer-checkout-removal: %+v", m)
		}
	})

	// 6. author-worktree-sweep
	t.Run(MechAuthorWorktreeSweep, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		for i := 1; i <= 5; i++ {
			rec := reapReceipt{
				Time:     dTime,
				Action:   "remove-worktree",
				Repo:     f.repos[0].Name,
				PR:       i,
				Worktree: "/wt/" + strconv.Itoa(i),
				Result:   "removed",
			}
			if err := f.s.reapNote(rec, false); err != nil {
				t.Fatal(err)
			}
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechAuthorWorktreeSweep {
				m = item
			}
		}
		if m.Stalled || m.Made != 5 || m.Done != 5 {
			t.Fatalf("healthy author-sweep: %+v", m)
		}

		// Stalled: 5 made, 1 done (20% < 30%)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 5; i++ {
			resStr := "skipped"
			reason := "dirty"
			if i == 1 {
				resStr = "removed"
				reason = ""
			}
			rec := reapReceipt{
				Time:     dTime,
				Action:   "remove-worktree",
				Repo:     f2.repos[0].Name,
				PR:       i,
				Worktree: "/wt/" + strconv.Itoa(i),
				Result:   resStr,
				Reason:   reason,
			}
			if err := f2.s.reapNote(rec, false); err != nil {
				t.Fatal(err)
			}
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechAuthorWorktreeSweep {
				m = item
			}
		}
		if !m.Stalled || m.Made != 5 || m.Done != 1 {
			t.Fatalf("stalled author-sweep: %+v", m)
		}
	})

	// 7. watch-revert
	t.Run(MechWatchRevert, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		f.audit(t, f.repos[0].Name, "land", map[string]any{"action": "merge", "head": "sha-m1"}, dTime)
		f.audit(t, f.repos[0].Name, "watch", map[string]any{"head": "sha-m1", "action": "healthy"}, dTime)

		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechWatchRevert {
				m = item
			}
		}
		if m.Stalled || m.Made != 1 || m.Done != 1 {
			t.Fatalf("healthy watch-revert: %+v", m)
		}

		// Stalled: repo has merge but 0 watch verdicts
		f2 := newLivenessFixture(t, dTime)
		f2.audit(t, f2.repos[0].Name, "land", map[string]any{"action": "merge", "head": "sha-m1"}, dTime)
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechWatchRevert {
				m = item
			}
		}
		if !m.Stalled || m.Made != 1 || m.Done != 0 {
			t.Fatalf("stalled watch-revert: %+v", m)
		}
	})

	// 8. lane-notices
	t.Run(MechLaneNotices, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		for i := 1; i <= 10; i++ {
			f.audit(t, f.repos[0].Name, "pr-notify", map[string]any{"pr": i, "sent": true}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechLaneNotices {
				m = item
			}
		}
		if m.Stalled || m.Made != 10 || m.Done != 10 {
			t.Fatalf("healthy lane-notices: %+v", m)
		}

		// Stalled: 10 attempts, 6 errors (60% >= 50% and >= 5)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 4; i++ {
			f2.audit(t, f2.repos[0].Name, "pr-notify", map[string]any{"pr": i, "sent": true}, dTime)
		}
		for i := 5; i <= 10; i++ {
			f2.audit(t, f2.repos[0].Name, "pr-notify-error", map[string]any{"pr": i, "error": "timeout"}, dTime)
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechLaneNotices {
				m = item
			}
		}
		if !m.Stalled || m.Made != 10 || m.Done != 4 {
			t.Fatalf("stalled lane-notices: %+v", m)
		}
	})

	// 9. task-events
	t.Run(MechTaskEvents, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		for i := 1; i <= 20; i++ {
			f.audit(t, f.repos[0].Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i), "result": map[string]any{"id": i}}, dTime)
		}
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechTaskEvents {
				m = item
			}
		}
		if m.Stalled || m.Made != 20 || m.Done != 20 {
			t.Fatalf("healthy task-events: %+v", m)
		}

		// Stalled: 20 attempts, 10 errors (50% >= 20% and >= 10)
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 10; i++ {
			f2.audit(t, f2.repos[0].Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i), "result": map[string]any{"id": i}}, dTime)
			f2.audit(t, f2.repos[0].Name, "hk-task-event", map[string]any{"task": strconv.Itoa(100 + i), "error": "transport error"}, dTime)
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechTaskEvents {
				m = item
			}
		}
		if !m.Stalled || m.Made != 20 || m.Done != 10 {
			t.Fatalf("stalled task-events: %+v", m)
		}
	})

	// 10. outcome-publish
	t.Run(MechOutcomePublish, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		outcomesFile := filepath.Join(f.s.Config.StateDir, "outcomes.jsonl")
		_ = os.WriteFile(outcomesFile, []byte(fmt.Sprintf(`{"kind":"published","at":"%sT12:00:00Z"}`+"\n", day)), 0600)
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechOutcomePublish {
				m = item
			}
		}
		if m.Stalled || m.Made != 1 || m.Done != 1 {
			t.Fatalf("healthy outcome-publish: %+v", m)
		}

		// Stalled: 5 publish errors, 0 published
		f2 := newLivenessFixture(t, dTime)
		for i := 1; i <= 5; i++ {
			f2.audit(t, f2.repos[0].Name, "outcome-publish-error", map[string]any{"error": "network down"}, dTime)
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechOutcomePublish {
				m = item
			}
		}
		if !m.Stalled || m.Made != 5 || m.Done != 0 {
			t.Fatalf("stalled outcome-publish: %+v", m)
		}
	})

	// 11. review-retry
	t.Run(MechReviewRetry, func(t *testing.T) {
		f := newLivenessFixture(t, dTime)
		f.audit(t, f.repos[0].Name, "review-retry-deferred", map[string]any{"head": "head-ret"}, dTime)
		f.audit(t, f.repos[0].Name, "review", map[string]any{"head": "head-ret"}, dTime.Add(2*time.Hour))
		res, err := f.s.livenessForDay(f.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		var m LivenessMechanismResult
		for _, item := range res.Mechanisms {
			if item.Mechanism == MechReviewRetry {
				m = item
			}
		}
		if m.Stalled || m.Made != 1 || m.Done != 1 {
			t.Fatalf("healthy review-retry: %+v", m)
		}

		// Stalled: 3 deferred at 12:00 UTC, unresolved by midnight with threshold 6h
		f2 := newLivenessFixture(t, dTime)
		f2.s.Config.Liveness.Thresholds.ReviewRetryMaxDeferredHours = 6
		for i := 1; i <= 3; i++ {
			f2.audit(t, f2.repos[0].Name, "review-retry-deferred", map[string]any{"head": "head-ret-stuck-" + strconv.Itoa(i)}, dTime)
		}
		res2, err := f2.s.livenessForDay(f2.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res2.Mechanisms {
			if item.Mechanism == MechReviewRetry {
				m = item
			}
		}
		if !m.Stalled || m.Made != 3 || m.Done != 0 {
			t.Fatalf("stalled review-retry: %+v", m)
		}

		// Resolved via reviewer-head-moved with OldHead
		f3 := newLivenessFixture(t, dTime)
		f3.s.Config.Liveness.Thresholds.ReviewRetryMaxDeferredHours = 6
		f3.audit(t, f3.repos[0].Name, "review-retry-deferred", map[string]any{"head": "head-moved-old"}, dTime)
		f3.audit(t, f3.repos[0].Name, "reviewer-head-moved", map[string]any{"old_head": "head-moved-old", "new_head": "head-moved-new"}, dTime.Add(1*time.Hour))
		res3, err := f3.s.livenessForDay(f3.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res3.Mechanisms {
			if item.Mechanism == MechReviewRetry {
				m = item
			}
		}
		if m.Stalled || m.Made != 1 || m.Done != 1 {
			t.Fatalf("review-retry resolved via reviewer-head-moved: %+v", m)
		}

		// Resolved via PR closure
		f4 := newLivenessFixture(t, dTime)
		f4.s.Config.Liveness.Thresholds.ReviewRetryMaxDeferredHours = 6
		f4.audit(t, f4.repos[0].Name, "review-retry-deferred", map[string]any{"pr": 99, "head": "head-pr-99"}, dTime)
		f4.audit(t, f4.repos[0].Name, "pr-notify", map[string]any{"pr": 99, "state": "closed"}, dTime.Add(1*time.Hour))
		res4, err := f4.s.livenessForDay(f4.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range res4.Mechanisms {
			if item.Mechanism == MechReviewRetry {
				m = item
			}
		}
		if m.Stalled || m.Made != 1 || m.Done != 1 {
			t.Fatalf("review-retry resolved via PR closed: %+v", m)
		}

		// Stalled under default 24h threshold: deferrals on previous day (day D-1) unresolved at day D
		prevDayTime := dTime.Add(-24 * time.Hour)
		fDef := newLivenessFixture(t, dTime)
		// Default threshold 24h is used (no override on fDef.s.Config.Liveness.Thresholds)
		for i := 1; i <= 2; i++ {
			fDef.audit(t, fDef.repos[0].Name, "review-retry-deferred", map[string]any{"head": "head-default-stuck-" + strconv.Itoa(i)}, prevDayTime)
		}
		resDef, err := fDef.s.livenessForDay(fDef.repos, day)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range resDef.Mechanisms {
			if item.Mechanism == MechReviewRetry {
				m = item
			}
		}
		if !m.Stalled || m.Made != 2 || m.Done != 0 {
			t.Fatalf("stalled review-retry at default 24h threshold with earlier day deferrals: %+v", m)
		}
	})

	_ = ctx
}

// TestLiveness_NoticeRepeatsUntilAck verifies that:
// - Day 1 sends notice
// - Day 2 repeats notice while unanswered
// - Ack stops notice
func TestLiveness_NoticeRepeatsUntilAck(t *testing.T) {
	day1 := "2026-11-15"
	day2 := "2026-11-16"
	t1 := balanceDayTime(day1, 12)
	t2 := balanceDayTime(day2, 12)
	now := balanceDayTime("2026-11-17", 8)

	f := newLivenessFixture(t, now)
	target := f.repos[0]
	ctx := context.Background()

	// Seed stall for task-events on day 1
	for i := 1; i <= 10; i++ {
		f.audit(t, target.Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i)}, t1)
		f.audit(t, target.Name, "task-event-fields-refused", map[string]any{"task": strconv.Itoa(i)}, t1)
	}

	// Run day 1
	if err := f.s.livenessDay(ctx, target, day1, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 1 {
		t.Fatalf("expected 1 notice on day 1, got %d", f.n.count)
	}
	if !strings.Contains(f.n.events[0].Text, MechTaskEvents) {
		t.Fatalf("expected task-events notice, got %s", f.n.events[0].Text)
	}

	// Seed stall for task-events on day 2
	for i := 1; i <= 10; i++ {
		f.audit(t, target.Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i + 20)}, t2)
		f.audit(t, target.Name, "task-event-fields-refused", map[string]any{"task": strconv.Itoa(i + 20)}, t2)
	}

	// Run day 2 (unanswered): repeats notice!
	if err := f.s.livenessDay(ctx, target, day2, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 2 {
		t.Fatalf("expected notice to repeat on day 2, got count %d", f.n.count)
	}

	// Now ack the mechanism
	if err := f.s.AckLiveness(MechTaskEvents, "investigating refusal burst"); err != nil {
		t.Fatal(err)
	}

	// Seed stall on day 3
	day3 := "2026-11-17"
	t3 := balanceDayTime(day3, 12)
	for i := 1; i <= 10; i++ {
		f.audit(t, target.Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i + 40)}, t3)
		f.audit(t, target.Name, "task-event-fields-refused", map[string]any{"task": strconv.Itoa(i + 40)}, t3)
	}

	// Run day 3: notice suppressed by ack!
	if err := f.s.livenessDay(ctx, target, day3, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 2 {
		t.Fatalf("expected notice to be suppressed by ack on day 3, count=%d", f.n.count)
	}
}

// TestLiveness_AckClearedByRecovery verifies that recovery clears the ack, so a later
// stall raises a new notice.
func TestLiveness_AckClearedByRecovery(t *testing.T) {
	day1 := "2026-11-14"
	day2 := "2026-11-15"
	day3 := "2026-11-16"
	t1 := balanceDayTime(day1, 12)
	t2 := balanceDayTime(day2, 12)
	t3 := balanceDayTime(day3, 12)

	f := newLivenessFixture(t, balanceDayTime(day1, 23))
	target := f.repos[0]
	ctx := context.Background()

	// Day 1: Stalled
	for i := 1; i <= 10; i++ {
		f.audit(t, target.Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i)}, t1)
		f.audit(t, target.Name, "task-event-fields-refused", map[string]any{"task": strconv.Itoa(i)}, t1)
	}
	if err := f.s.livenessDay(ctx, target, day1, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 1 {
		t.Fatalf("expected notice on day 1, got %d", f.n.count)
	}

	// Ack day 1
	f.now = balanceDayTime(day1, 23).Add(10 * time.Minute)
	if err := f.s.AckLiveness(MechTaskEvents, "investigating"); err != nil {
		t.Fatal(err)
	}

	// Day 2: Recovers (healthy), runs on day 2 evening (after ack)
	f.now = balanceDayTime(day2, 23)
	for i := 1; i <= 20; i++ {
		f.audit(t, target.Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i + 20), "result": map[string]any{"id": i}}, t2)
	}
	if err := f.s.livenessDay(ctx, target, day2, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 1 {
		t.Fatalf("expected no notice on recovery day 2, got %d", f.n.count)
	}

	// Day 3: Stalls again! Recovery cleared the ack, so notice must raise again!
	f.now = balanceDayTime(day3, 23)
	for i := 1; i <= 10; i++ {
		f.audit(t, target.Name, "hk-task-event", map[string]any{"task": strconv.Itoa(i + 50)}, t3)
		f.audit(t, target.Name, "task-event-fields-refused", map[string]any{"task": strconv.Itoa(i + 50)}, t3)
	}
	if err := f.s.livenessDay(ctx, target, day3, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 2 {
		t.Fatalf("expected new notice on day 3 after recovery cleared ack, got %d", f.n.count)
	}
}

// TestLiveness_BackfilledHealthyRowDoesNotClearAck verifies that backfilled healthy daily
// rows written for past days (e.g. during daemon catch-up, stamped with current clock) do not clear an ack that was recorded.
func TestLiveness_BackfilledHealthyRowDoesNotClearAck(t *testing.T) {
	ackTime := balanceDayTime("2026-11-17", 12)
	f := newLivenessFixture(t, ackTime)
	target := f.repos[0]

	// Prior stall recorded on 2026-11-17
	f.audit(t, target.Name, livenessDailyCommand, map[string]any{
		"day":       "2026-11-17",
		"mechanism": MechTaskEvents,
		"stalled":   true,
	}, ackTime.Add(-1*time.Hour))

	// Operator acknowledges the stall at ackTime
	f.s.Now = func() time.Time { return ackTime }
	if err := f.s.AckLiveness(MechTaskEvents, "operator investigating"); err != nil {
		t.Fatal(err)
	}

	// Daemon restarts at 2026-11-18 08:00 UTC (production clock) and catch-up backfills
	// a healthy daily row for 2026-11-15 (stamped with the CURRENT catch-up clock, AFTER the ack!)
	catchupClock := balanceDayTime("2026-11-18", 8)
	f.s.Now = func() time.Time { return catchupClock }
	f.audit(t, target.Name, livenessDailyCommand, map[string]any{
		"day":       "2026-11-15",
		"mechanism": MechTaskEvents,
		"stalled":   false,
	}, catchupClock)

	// Verify ack is still active because 2026-11-15 <= 2026-11-17 (the stalled day answered by the ack)
	acks, err := f.s.activeLivenessAcks(target.Name, "2026-11-17")
	if err != nil {
		t.Fatal(err)
	}
	if acks[MechTaskEvents] != "operator investigating" {
		t.Fatalf("expected ack to remain active despite backfilled healthy row with current clock, got %+v", acks)
	}

	// Recovery row for 2026-11-18 (day > 2026-11-17) clears the ack
	recoveryTime := balanceDayTime("2026-11-18", 12)
	f.s.Now = func() time.Time { return recoveryTime }
	f.audit(t, target.Name, livenessDailyCommand, map[string]any{
		"day":       "2026-11-18",
		"mechanism": MechTaskEvents,
		"stalled":   false,
	}, recoveryTime)

	acksAfter, err := f.s.activeLivenessAcks(target.Name, "2026-11-18")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := acksAfter[MechTaskEvents]; exists {
		t.Fatalf("expected ack to be cleared after recovery day 2026-11-18, got %+v", acksAfter)
	}
}

// TestLiveness_IdempotentRetry verifies that retrying a day writes no duplicate rows or notices.
func TestLiveness_IdempotentRetry(t *testing.T) {
	day := "2026-11-15"
	t1 := balanceDayTime(day, 12)
	now := balanceDayTime("2026-11-16", 8)

	f := newLivenessFixture(t, now)
	target := f.repos[0]
	ctx := context.Background()

	// Seed stall for merge
	for i := 1; i <= 5; i++ {
		f.audit(t, target.Name, "land", map[string]any{"action": "merge", "pr": i}, t1)
	}
	f.audit(t, target.Name, "watch", map[string]any{"action": "healthy"}, t1)

	// First pass
	if err := f.s.livenessDay(ctx, target, day, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 1 {
		t.Fatalf("expected 1 notice, got %d", f.n.count)
	}
	daily1 := livenessRows(t, f, target.Name, livenessDailyCommand)
	notices1 := livenessRows(t, f, target.Name, livenessNoticeCommand)

	// Second pass (retry)
	if err := f.s.livenessDay(ctx, target, day, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 1 {
		t.Fatalf("expected still 1 notice after retry, got %d", f.n.count)
	}
	daily2 := livenessRows(t, f, target.Name, livenessDailyCommand)
	notices2 := livenessRows(t, f, target.Name, livenessNoticeCommand)

	if len(daily2) != len(daily1) {
		t.Fatalf("daily rows duplicated: before=%d after=%d", len(daily1), len(daily2))
	}
	if len(notices2) != len(notices1) {
		t.Fatalf("notice rows duplicated: before=%d after=%d", len(notices1), len(notices2))
	}
}

// TestLiveness_FullLoopAdvancesDay verifies the service loop advances st.Last.
func TestLiveness_FullLoopAdvancesDay(t *testing.T) {
	day := "2026-11-15"
	t1 := balanceDayTime(day, 12)
	now := balanceDayTime("2026-11-16", 8) // yesterday is 2026-11-15

	f := newLivenessFixture(t, now)
	target := f.repos[0]
	ctx := context.Background()

	f.audit(t, target.Name, "land", map[string]any{"action": "merge", "pr": 1}, t1)
	f.audit(t, target.Name, "pr-notify", map[string]any{"state": "merged", "pr": 1}, t1)

	if err := f.s.liveness(ctx, Options{}); err != nil {
		t.Fatal(err)
	}

	var st livenessState
	if _, err := fsutil.ReadJSON(f.s.livenessStateFile(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Last != day {
		t.Fatalf("expected last=%s, got %s", day, st.Last)
	}
}
