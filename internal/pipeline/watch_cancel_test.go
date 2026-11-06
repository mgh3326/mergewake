package pipeline

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// Main CI cancels in progress: a newer push cancels the run on an older head,
// the newest green main is success, and a red newest main is bisected. These
// tests pin what watch does with cancelled evidence. Chain: G green, U1-U3
// cancelled, R the head.

func cancelledChain(g *anyRefGH, head github.CI) {
	g.checks = map[string]github.CI{anyG: github.Green, anyU1: github.Cancelled, anyU2: github.Cancelled, anyU3: github.Cancelled, anyR: head}
}

func readCheckpoint(t *testing.T, s *Service, repo string) string {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(s.state(repo), "last-green"))
	if e != nil && !os.IsNotExist(e) {
		t.Fatal(e)
	}
	return string(b)
}

// (a) The newest main is green and the older heads were cancelled: healthy,
// checkpoint at the newest, no recovery, whether or not any-ref is configured.
func TestWatchCancelNewestGreenIsHealthy(t *testing.T) {
	for _, key := range []bool{false, true} {
		s, g, sink, n, r := anyRefSetup(t, key)
		cancelledChain(g, github.Green)
		// An open incident from an earlier red head ends with this green one.
		if e := s.saveAnyRef(r.Name, anyRefIncident{Head: anyU3, Limit: 4}); e != nil {
			t.Fatal(e)
		}
		got := lastWatch(t, s, r, Options{})
		if got.Action != "healthy" || got.Head != anyR || got.Culprit != "" || got.PR != 0 || got.Reason != "" || len(got.Candidates) != 0 {
			t.Fatalf("any-ref=%v: %+v", key, got)
		}
		if cp := readCheckpoint(t, s, r.Name); cp != anyR {
			t.Fatalf("any-ref=%v: checkpoint %q, want the newest head", key, cp)
		}
		if g.revertCount != 0 || len(g.dispatched) != 0 || sink.records != 0 || n.count != 0 {
			t.Fatalf("any-ref=%v: reverts=%d dispatches=%v records=%d notifications=%d", key, g.revertCount, g.dispatched, sink.records, n.count)
		}
		if _, e := os.Stat(s.anyRefFile(r.Name)); !os.IsNotExist(e) {
			t.Fatalf("any-ref=%v: incident not ended: %v", key, e)
		}
	}
}

// (b) The newest main is cancelled or pending, so a newer push will run: wait,
// blame nobody, and leave the checkpoint where it was.
func TestWatchCancelNewestNotFinishedWaits(t *testing.T) {
	for _, head := range []github.CI{github.Cancelled, github.Pending} {
		for _, key := range []bool{false, true} {
			s, g, sink, n, r := anyRefSetup(t, key)
			cancelledChain(g, head)
			g.checks[anyU3] = github.Red
			if e := os.MkdirAll(s.state(r.Name), 0o700); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(s.state(r.Name), "last-green"), []byte(anyG), 0o600); e != nil {
				t.Fatal(e)
			}
			got := lastWatch(t, s, r, Options{})
			if got.Action != "waiting-for-ci" || got.Head != anyR || got.Culprit != "" || got.PR != 0 || got.Reason != "" || len(got.Candidates) != 0 || got.AnyRefDispatch != "" {
				t.Fatalf("head %s any-ref=%v: %+v", head, key, got)
			}
			if cp := readCheckpoint(t, s, r.Name); cp != anyG {
				t.Fatalf("head %s any-ref=%v: checkpoint moved to %q", head, key, cp)
			}
			if g.revertCount != 0 || len(g.dispatched) != 0 || sink.records != 0 || n.count != 0 {
				t.Fatalf("head %s any-ref=%v: reverts=%d dispatches=%v records=%d notifications=%d", head, key, g.revertCount, g.dispatched, sink.records, n.count)
			}
		}
	}
}

// (c) The newest main is red and the heads between it and the last green were
// cancelled. With any-ref configured, watch dispatches the any-ref CI for
// exactly the commits bisect needs and blames the first red.
func TestWatchCancelRedHeadAnyRefBisects(t *testing.T) {
	s, g, _, n, r := anyRefSetup(t, true)
	cancelledChain(g, github.Red)
	truth := map[string]github.CI{anyU1: github.Green, anyU2: github.Red, anyU3: github.Red}
	var got watchResult
	for pass := 1; pass <= 6; pass++ {
		got = lastWatch(t, s, r, Options{})
		if got.Action != "hold" {
			break
		}
		if got.AnyRefDispatch == "" || got.Reason != reasonFor(got.AnyRefDispatch) {
			t.Fatalf("pass %d: %+v", pass, got)
		}
		// The any-ref run finishes and posts its status on that commit.
		g.checks[got.AnyRefDispatch] = truth[got.AnyRefDispatch]
	}
	// Bisect over [G U1 U2 U3 R] needs U2, then U1; U3 is never needed.
	if want := []string{anyU2, anyU1}; !reflect.DeepEqual(g.dispatched, want) {
		t.Fatalf("dispatched %v, want %v", g.dispatched, want)
	}
	if got.Action != "report-culprit" || got.Culprit != anyU2 || got.PR != 12 {
		t.Fatalf("%+v", got)
	}
	if n.count != 1 {
		t.Fatalf("notifications=%d, want only the culprit report", n.count)
	}
}

// (d) The same history without any-ref holds on incomplete evidence, names the
// candidates, dispatches nothing and blames nobody.
func TestWatchCancelRedHeadWithoutAnyRefHolds(t *testing.T) {
	s, g, sink, n, r := anyRefSetup(t, false)
	cancelledChain(g, github.Red)
	got := lastWatch(t, s, r, Options{})
	if got.Action != "hold" || got.Reason != "incomplete CI evidence in recovery interval" || got.Culprit != "" || got.PR != 0 || got.AnyRefDispatch != "" {
		t.Fatalf("%+v", got)
	}
	if want := []int{11, 12, 13, 14}; !reflect.DeepEqual(got.Candidates, want) {
		t.Fatalf("candidates %v, want %v", got.Candidates, want)
	}
	if g.revertCount != 0 || len(g.dispatched) != 0 || sink.records != 1 || n.count != 1 {
		t.Fatalf("reverts=%d dispatches=%v records=%d notifications=%d", g.revertCount, g.dispatched, sink.records, n.count)
	}
}

// (e) A cancelled head is never counted as red: not as the newest main, and not
// inside the bisect bracket.
func TestWatchCancelledIsNeverRed(t *testing.T) {
	t.Run("newest head", func(t *testing.T) {
		s, g, sink, n, r := anyRefSetup(t, true)
		cancelledChain(g, github.Cancelled)
		got := lastWatch(t, s, r, Options{})
		if got.Action != "waiting-for-ci" || got.Reason != "" {
			t.Fatalf("%+v", got)
		}
		if g.revertCount != 0 || sink.records != 0 || n.count != 0 || len(g.dispatched) != 0 {
			t.Fatalf("reverts=%d records=%d notifications=%d dispatches=%v", g.revertCount, sink.records, n.count, g.dispatched)
		}
	})
	t.Run("bisect bracket", func(t *testing.T) {
		shas := []string{anyG, anyU1, anyR}
		state := map[string]github.CI{anyG: github.Green, anyU1: github.Cancelled, anyR: github.Red}
		idx, e := Bisect(shas, func(sha string) (github.CI, error) { return state[sha], nil })
		var unknown *UnknownEvidenceError
		if !errors.As(e, &unknown) || unknown.SHA != anyU1 {
			t.Fatalf("index %d, err %v: want unknown evidence on the cancelled commit", idx, e)
		}
	})
	t.Run("recovery interval", func(t *testing.T) {
		// Only the cancelled commit lies between green and red: it is not blamed.
		s, g, _, _, r := anyRefSetup(t, false)
		g.parents = map[string]string{anyR: anyU1, anyU1: anyG}
		g.checks = map[string]github.CI{anyG: github.Green, anyU1: github.Cancelled, anyR: github.Red}
		got := lastWatch(t, s, r, Options{})
		if got.Action != "hold" || got.Culprit != "" || got.PR != 0 || g.revertCount != 0 {
			t.Fatalf("%+v reverts=%d", got, g.revertCount)
		}
	})
}
