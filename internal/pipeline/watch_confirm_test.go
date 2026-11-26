package pipeline

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// headCulpritChain sets a history where the head shaB is the only red commit
// after the green shaA, and shaB's merged PR (number 7) landed at shaB. The
// clock is fixed so tests move it across the confirmation window.
func headCulpritChain(t *testing.T) (*Service, *fakeGH, *fakeNotifier, *strings.Builder, *time.Time, config.Repo) {
	t.Helper()
	s, g, _, n, out, r := culpritChain(t)
	g.head = shaB
	g.parents = map[string]string{shaB: shaA}
	g.checks = map[string]github.CI{shaB: github.Red, shaA: github.Green}
	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	s.Config.WatchConfirmWindow = "30m"
	s.Now = func() time.Time { return now }
	return s, g, n, out, &now, r
}

func TestWatchHeadCulpritConfirmationWindow(t *testing.T) {
	t.Run("first sighting holds once and names nothing", func(t *testing.T) {
		s, g, n, out, now, r := headCulpritChain(t)
		g.blocking = true
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), "unconfirmed") || strings.Contains(out.String(), `"action":"report-culprit"`) {
			t.Fatal(out.String())
		}
		if g.revertCount != 0 || n.count != 1 {
			t.Fatalf("reverts=%d notify=%d", g.revertCount, n.count)
		}
		if _, e := os.Stat((&watchStream{Service: s}).redSeenFile(r.Name)); e != nil {
			t.Fatalf("marker not durable: %v", e)
		}
		// A later pass inside the window dedupes the notice.
		*now = now.Add(5 * time.Minute)
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if n.count != 1 || g.revertCount != 0 {
			t.Fatalf("second pass in window: reverts=%d notify=%d", g.revertCount, n.count)
		}
	})

	t.Run("green rerun inside the window is flaky", func(t *testing.T) {
		s, g, n, out, now, r := headCulpritChain(t)
		g.blocking = true
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		g.checks[shaB] = github.Green
		*now = now.Add(10 * time.Minute)
		out.Reset()
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), "flaky: red did not hold on rerun") {
			t.Fatal(out.String())
		}
		if strings.Contains(out.String(), `"culprit":"`+shaB+`"`) || strings.Contains(out.String(), `"pr":7`) {
			t.Fatalf("flaky hold named a culprit: %s", out.String())
		}
		if g.revertCount != 0 || g.mergeCount != 0 || n.count != 2 {
			t.Fatalf("reverts=%d merges=%d notify=%d", g.revertCount, g.mergeCount, n.count)
		}
		if _, e := os.Stat((&watchStream{Service: s}).redSeenFile(r.Name)); !os.IsNotExist(e) {
			t.Fatalf("marker survives a settled flake: %v", e)
		}
	})

	t.Run("red after the window is the culprit as today", func(t *testing.T) {
		s, g, _, out, now, r := headCulpritChain(t)
		g.blocking = true
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		*now = now.Add(31 * time.Minute)
		out.Reset()
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), `"action":"report-culprit"`) {
			t.Fatal(out.String())
		}
		if _, e := os.Stat((&watchStream{Service: s}).redSeenFile(r.Name)); !os.IsNotExist(e) {
			t.Fatalf("marker survives confirmation: %v", e)
		}
	})

	t.Run("later red commit confirms at once", func(t *testing.T) {
		s, g, _, out, _, r := headCulpritChain(t)
		g.blocking = true
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		// A commit lands on top of the culprit and is red on the same check.
		g.head = shaC
		g.parents = map[string]string{shaC: shaB, shaB: shaA}
		g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green}
		out.Reset()
		if e := s.Watch(context.Background(), r, Options{}); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), `"action":"report-culprit"`) {
			t.Fatal(out.String())
		}
		if _, e := os.Stat((&watchStream{Service: s}).redSeenFile(r.Name)); !os.IsNotExist(e) {
			t.Fatalf("marker survives corroboration: %v", e)
		}
	})
}
