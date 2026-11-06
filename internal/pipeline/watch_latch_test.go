package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// latchGH is a repo whose recovery latch the test sets up: which refs exist and
// which revert PR, if any, the latch belongs to.
type latchGH struct {
	*fakeGH
	tag, branch bool
	pr          *github.Pull
	clears      int
}

func (g *latchGH) RecoveryActive(context.Context, string) (bool, error) {
	return g.tag || g.branch, nil
}
func (g *latchGH) RecoveryRevert(context.Context, string) (*github.Pull, error) { return g.pr, nil }
func (g *latchGH) ClearRecovery(context.Context, string) error {
	g.clears++
	g.tag, g.branch = false, false
	return nil
}

func mergedRevert(merge string) *github.Pull {
	at := "2026-01-02T00:00:00Z"
	return &github.Pull{Number: 9, State: "closed", Merged: true, MergedAt: &at, MergeCommitSHA: merge, Body: "Reverts " + shaA + " from PR #7. Review and merge this PR."}
}

func latchWatch(t *testing.T, ci github.CI, mutate func(*latchGH)) (*latchGH, *fakeNotifier, string) {
	t.Helper()
	s, g, _, n, r := setup(t)
	g.enabled = true
	g.head = shaC
	g.ci = ci
	l := &latchGH{fakeGH: g, tag: true, branch: true, pr: mergedRevert(shaB)}
	if mutate != nil {
		mutate(l)
	}
	s.GH = l
	var out strings.Builder
	s.Out = &out
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	// A second pass shows what a later poll does with whatever is left.
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	return l, n, out.String()
}

func TestWatchClearsRecoveryLatch(t *testing.T) {
	t.Run("a: merged revert held by a green main clears tag and branch once", func(t *testing.T) {
		l, n, out := latchWatch(t, github.Green, nil)
		if l.tag || l.branch || l.clears != 1 {
			t.Fatalf("tag=%t branch=%t clears=%d", l.tag, l.branch, l.clears)
		}
		if got := strings.Count(out, `"command":"recovery-latch-cleared"`); got != 1 {
			t.Fatalf("%d audit rows\n%s", got, out)
		}
		if !strings.Contains(out, `"revert_pr":9`) || !strings.Contains(out, `"culprit":"`+shaA+`"`) || !strings.Contains(out, `"head":"`+shaC+`"`) {
			t.Fatal(out)
		}
		if n.count != 1 || n.events[0].Type != "info" || !strings.Contains(n.events[0].Text, "#9") {
			t.Fatalf("%d notices: %+v", n.count, n.events)
		}
	})
	t.Run("a: main head that is the merge commit itself counts", func(t *testing.T) {
		l, _, _ := latchWatch(t, github.Green, func(l *latchGH) { l.pr = mergedRevert(shaC) })
		if l.tag || l.branch {
			t.Fatal("latch kept")
		}
	})
	for _, tt := range []struct {
		name   string
		ci     github.CI
		mutate func(*latchGH)
	}{
		{"b: open revert", github.Green, func(l *latchGH) {
			l.pr = mergedRevert(shaB)
			l.pr.State, l.pr.Merged, l.pr.MergedAt, l.pr.MergeCommitSHA = "open", false, nil, ""
		}},
		{"b: closed unmerged revert", github.Green, func(l *latchGH) {
			l.pr = mergedRevert(shaB)
			l.pr.Merged, l.pr.MergedAt, l.pr.MergeCommitSHA = false, nil, ""
		}},
		{"c: merged revert but main red", github.Red, nil},
		{"c: merged revert but main pending", github.Pending, nil},
		{"c: green main does not yet contain the merge commit", github.Green, func(l *latchGH) {
			l.notAncestor = map[[2]string]bool{{shaB, shaC}: true}
		}},
		{"d: latch with no identifiable revert PR", github.Green, func(l *latchGH) { l.pr = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, n, out := latchWatch(t, tt.ci, tt.mutate)
			if !l.tag || !l.branch || l.clears != 0 || n.count != 0 || strings.Contains(out, "recovery-latch-cleared") {
				t.Fatalf("tag=%t branch=%t clears=%d notices=%d\n%s", l.tag, l.branch, l.clears, n.count, out)
			}
		})
	}
	t.Run("e: tag already gone, branch left over", func(t *testing.T) {
		l, n, out := latchWatch(t, github.Green, func(l *latchGH) { l.tag = false })
		if l.branch || l.clears != 1 || n.count != 1 || strings.Count(out, "recovery-latch-cleared") != 1 {
			t.Fatalf("branch=%t clears=%d notices=%d\n%s", l.branch, l.clears, n.count, out)
		}
	})
	t.Run("shadow never deletes", func(t *testing.T) {
		s, g, _, n, r := setup(t)
		g.head, g.ci = shaC, github.Green
		l := &latchGH{fakeGH: g, tag: true, pr: mergedRevert(shaB)}
		s.GH = l
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !l.tag || l.clears != 0 || n.count != 0 {
			t.Fatal("shadow pass touched the latch")
		}
	})
}

// f: after the latch clears, later passes neither clear nor notify again, and
// a new red head opens a new recovery.
func TestWatchAfterLatchClearedStartsNewRecovery(t *testing.T) {
	s, g, _, n, r := setup(t)
	g.enabled = true
	g.head, g.ci = shaC, github.Green
	l := &latchGH{fakeGH: g, tag: true, pr: mergedRevert(shaB)}
	s.GH = l
	for i := 0; i < 3; i++ {
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if l.clears != 1 || n.count != 1 {
		t.Fatalf("clears=%d notices=%d", l.clears, n.count)
	}
	// The green passes above checkpointed shaC; the next head breaks on top of it.
	g.head = shaD
	g.parents = map[string]string{shaD: shaC}
	g.checks = map[string]github.CI{shaD: github.Red, shaC: github.Green}
	merged := "2026-01-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.MergeCommitSHA = shaD
	g.pull.Body = "Mergewake-Task: 42"
	s.Config.Sink.Type = "handoffkeep"
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.revertCount != 1 || !g.latch {
		t.Fatalf("new red head did not start a recovery: reverts=%d", g.revertCount)
	}
}
