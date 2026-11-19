package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// The culprit's file count comes from its full pull. GitHub's commits/{sha}/pulls
// list body has changed_files 0 (the fake answers in that shape), so a culprit
// found through the list must still reach report-culprit or the revert step when
// its file list agrees with the full pull.

// culpritChain sets a linear history where shaB is the culprit: shaC and shaB are
// red, shaA is green, and shaB's merged PR (number 7) landed at shaB.
func culpritChain(t *testing.T) (*Service, *fakeGH, *fakeSink, *fakeNotifier, *strings.Builder, config.Repo) {
	t.Helper()
	s, g, sink, n, r := setup(t)
	g.enabled = true
	g.head = shaC
	g.parents = map[string]string{shaC: shaB, shaB: shaA}
	g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green}
	merged := "2026-01-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.MergeCommitSHA = shaB
	g.pull.Body = "Mergewake-Task: 42"
	s.Config.Sink.Type = "handoffkeep"
	var out strings.Builder
	s.Out = &out
	return s, g, sink, n, &out, r
}

func TestWatchCulpritFileCountFromFullPull(t *testing.T) {
	t.Run("blocking culprit reaches report-culprit", func(t *testing.T) {
		s, g, sink, n, out, r := culpritChain(t)
		g.blocking = true
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), `"action":"report-culprit"`) {
			t.Fatal(out.String())
		}
		if g.revertCount != 0 || sink.records != 1 || n.count != 1 {
			t.Fatalf("reverts=%d records=%d notify=%d", g.revertCount, sink.records, n.count)
		}
	})
	t.Run("non-blocking culprit is recorded, then reverted", func(t *testing.T) {
		s, g, sink, n, out, r := culpritChain(t)
		// Pass 1 bisects to the culprit and records the incident; the list
		// check must pass for that to happen.
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), `"action":"recovery-confirming"`) {
			t.Fatal(out.String())
		}
		inc, found, e := s.loadRecovery(r.Name)
		if e != nil || !found || inc.Culprit != shaB || inc.CulpritPR != 7 {
			t.Fatalf("recorded incident = %+v found=%v err=%v", inc, found, e)
		}
		// The confirmation re-runs land; the next pass confirms and reverts.
		inc.Runs = map[string][]bool{shaA: {true, true}, shaB: {false, false}}
		if e := s.saveRecovery(r.Name, inc); e != nil {
			t.Fatal(e)
		}
		out.Reset()
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), `"action":"opened-revert-pr"`) {
			t.Fatal(out.String())
		}
		if g.revertCount != 1 || sink.reopens != 1 || n.count != 2 {
			t.Fatalf("reverts=%d reopens=%d notify=%d", g.revertCount, sink.reopens, n.count)
		}
	})
	t.Run("full pull with more files than the list still holds", func(t *testing.T) {
		s, g, sink, n, out, r := culpritChain(t)
		g.pull.ChangedFiles = 2
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), "culprit file list is incomplete") {
			t.Fatal(out.String())
		}
		if g.revertCount != 0 || sink.records != 1 || n.count != 1 {
			t.Fatalf("reverts=%d records=%d notify=%d", g.revertCount, sink.records, n.count)
		}
	})
	t.Run("full pull read failure returns the error", func(t *testing.T) {
		s, g, sink, n, out, r := culpritChain(t)
		errPull := errors.New("pull read failed")
		g.pullErr = errPull
		if e := s.Watch(context.Background(), r, Options{}); !errors.Is(e, errPull) {
			t.Fatalf("Watch error = %v, want %v", e, errPull)
		}
		if strings.Contains(out.String(), "culprit file list is incomplete") || strings.Contains(out.String(), `"action":"`) {
			t.Fatal(out.String())
		}
		if g.revertCount != 0 || sink.records != 0 || n.count != 0 {
			t.Fatalf("reverts=%d records=%d notify=%d", g.revertCount, sink.records, n.count)
		}
	})
}
