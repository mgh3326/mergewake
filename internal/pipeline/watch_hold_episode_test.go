package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func episodeHoldSetup(t *testing.T) (*Service, *fakeGH, *fakeSink, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, sink, n, r := setup(t)
	g.enabled = true
	g.ci = github.Red
	g.head = shaB
	s.Config.WatchLimit = 2
	return s, g, sink, n, r
}

func watchEpisode(t *testing.T, s *Service, r config.Repo) string {
	t.Helper()
	var out strings.Builder
	s.Out = &out
	if err := s.Watch(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func suppressedRows(t *testing.T, out string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var row struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.Command == "watch-hold-suppressed" {
			rows = append(rows, row.Value)
		}
	}
	return rows
}

func TestWatchHoldEpisodeSameReasonAcrossHeads(t *testing.T) {
	s, g, sink, n, r := episodeHoldSetup(t)
	heads := []string{shaB, shaC, shaD}
	var all string
	for _, head := range heads {
		g.head = head
		all += watchEpisode(t, s, r)
		all += watchEpisode(t, s, r)
	}
	if n.count != 1 || len(n.events) != 1 {
		t.Fatalf("events=%d, want one", n.count)
	}
	class := "reached history root without a green ancestor"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(class)))[:12]
	episode, err := s.readWatchHoldEpisode(r.Name)
	if err != nil {
		t.Fatal(err)
	}
	wantID := "mergewake-watch-hold-" + episode.ID + "-" + hash
	if n.events[0].ID != wantID {
		t.Fatalf("event id=%q, want %q", n.events[0].ID, wantID)
	}
	rows := suppressedRows(t, all)
	if len(rows) != 2 || sink.records != 3 {
		t.Fatalf("suppressed rows=%d sink records=%d; want 2 and 3", len(rows), sink.records)
	}
	t.Logf("event id=%s suppressed rows=%+v", n.events[0].ID, rows)
	for i, row := range rows {
		if row["head"] != heads[i+1] || row["reason"] != class || row["episode"] != episode.ID {
			t.Fatalf("suppressed row %d = %#v", i, row)
		}
	}
}

func TestWatchHoldEpisodeNotifiesSecondReasonClass(t *testing.T) {
	s, g, _, n, r := episodeHoldSetup(t)
	g.head = shaB // root: reached history root
	watchEpisode(t, s, r)
	g.head = shaC
	g.parents = map[string]string{shaC: shaD} // two red commits exhaust the limit
	watchEpisode(t, s, r)
	if n.count != 2 || len(n.events) != 2 || n.events[0].ID == n.events[1].ID {
		t.Fatalf("events=%d ids=%v", n.count, eventIDs(n.events))
	}
	t.Logf("reason-class event ids=%s,%s", n.events[0].ID, n.events[1].ID)
}

func TestWatchHoldEpisodeReasonTailSharesClass(t *testing.T) {
	s, g, _, n, r := episodeHoldSetup(t)
	g.policyExtra = "watch_checks: [CI, image, lint]\nany_ref_checks: [CI]\n"
	g.checkNames = map[string]map[string]github.CI{
		shaB: {"CI": github.Green, "image": github.Red, "lint": github.Green},
		shaC: {"CI": github.Green, "image": github.Green, "lint": github.Red},
	}
	g.head = shaB
	first := watchEpisode(t, s, r)
	g.head = shaC
	second := watchEpisode(t, s, r)
	if n.count != 1 || !strings.Contains(first, "image") || !strings.Contains(second, "lint") {
		t.Fatalf("notifications=%d first=%s second=%s", n.count, first, second)
	}
	if len(suppressedRows(t, second)) != 1 {
		t.Fatal("second reason tail was not audited as suppressed")
	}
}

func TestWatchHoldEpisodeEndsOnGreen(t *testing.T) {
	s, g, _, n, r := episodeHoldSetup(t)
	g.head = shaB
	watchEpisode(t, s, r)
	g.head = shaC
	g.checks = map[string]github.CI{shaC: github.Green}
	watchEpisode(t, s, r)
	g.head = shaD
	g.checks[shaD] = github.Red
	watchEpisode(t, s, r)
	if n.count != 2 || len(n.events) != 2 || n.events[0].ID == n.events[1].ID {
		t.Fatalf("events=%d ids=%v", n.count, eventIDs(n.events))
	}
}

func eventIDs(events []integrations.Event) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}
	return ids
}

func TestWatchHoldEpisodeRetriesFailedNotifyWithSameID(t *testing.T) {
	s, g, sink, n, r := episodeHoldSetup(t)
	n.err = errors.New("temporary notify failure")
	g.head = shaB
	if err := s.Watch(context.Background(), r, Options{}); err == nil {
		t.Fatal("failed notifier returned nil")
	}
	episode, err := s.readWatchHoldEpisode(r.Name)
	if err != nil {
		t.Fatal(err)
	}
	if episode.ID == "" || len(episode.Announced) != 0 {
		t.Fatalf("failed notification state = %+v", episode)
	}
	n.err = nil
	watchEpisode(t, s, r)
	if len(n.events) != 1 || sink.records != 2 {
		t.Fatalf("successful events=%d sink records=%d", len(n.events), sink.records)
	}
	class := "reached history root without a green ancestor"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(class)))[:12]
	wantID := "mergewake-watch-hold-" + episode.ID + "-" + hash
	if n.events[0].ID != wantID {
		t.Fatalf("retry event id=%q, want %q", n.events[0].ID, wantID)
	}
	watchEpisode(t, s, r)
	if n.count != 1 {
		t.Fatalf("announced class repeated: events=%d", n.count)
	}
}

func TestWatchHoldEpisodeSurvivesNewService(t *testing.T) {
	s, g, _, n, r := episodeHoldSetup(t)
	g.head = shaB
	watchEpisode(t, s, r)
	stateDir := s.Config.StateDir
	fresh, nextGH, _, freshNotifier, _ := setup(t)
	fresh.Config.StateDir = stateDir
	fresh.Config.WatchLimit = 2
	nextGH.enabled, nextGH.ci, nextGH.head = true, github.Red, shaC
	watchEpisode(t, fresh, r)
	if n.count != 1 || freshNotifier.count != 0 {
		t.Fatalf("restart did not retain episode; events before=%d after=%d", n.count, freshNotifier.count)
	}
}

func TestWatchHoldEpisodeDeployStillNotifiesPerHead(t *testing.T) {
	s, g, sink, n, r := episodeHoldSetup(t)
	g.policyExtra = "deploy_checks: [deploy-ok]\n"
	g.ci = github.Green
	g.checkNames = map[string]map[string]github.CI{
		shaB: {"deploy-ok": github.Red},
		shaC: {"deploy-ok": github.Red},
	}
	g.head = shaB
	watchEpisode(t, s, r)
	watchEpisode(t, s, r)
	g.head = shaC
	watchEpisode(t, s, r)
	if n.count != 2 || sink.records != 2 || n.events[0].ID == n.events[1].ID {
		t.Fatalf("events=%d records=%d ids=%v", n.count, sink.records, []string{n.events[0].ID, n.events[1].ID})
	}
}

func TestWatchHoldEpisodeRejectsUnreadableState(t *testing.T) {
	s, _, _, _, r := episodeHoldSetup(t)
	file := filepath.Join(s.state(r.Name), "watch-hold-active.json")
	if err := os.MkdirAll(file, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := s.readWatchHoldEpisode(r.Name); err == nil {
		t.Fatal("unreadable episode state accepted")
	}
}
