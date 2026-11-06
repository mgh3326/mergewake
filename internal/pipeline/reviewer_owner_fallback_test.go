package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// lostReviewerSetup leaves a reviewer whose pane finished without a result, and
// whose hk job record is rec, so the next review replaces it.
func lostReviewerSetup(t *testing.T, rec integrations.JobRecord, owner string, closeErrs ...error) (*Service, config.Repo, *panewireRunner, reviewerJob) {
	t.Helper()
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := newFakeReapHub()
	hub.agents["panes"] = []any{map[string]any{"pane_id": "p1", "cwd": "/repo/" + old.Label}}
	hub.agents["agents"] = []any{map[string]any{"pane_id": "p1", "name": old.Label, "agent_status": "done", "cwd": "/repo/" + old.Label}}
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	s.Config.Review.Panewire.Owner = owner
	runner.sink.jobs[old.Label] = rec
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	runner.closeErrs = closeErrs
	c.t = c.t.Add(time.Second)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	return s, r, runner, old
}

func lostRow(t *testing.T, s *Service, r config.Repo) map[string]any {
	t.Helper()
	rows := auditRows(t, s, r, "review-result-lost")
	if len(rows) != 1 {
		t.Fatalf("review-result-lost rows: %v", rows)
	}
	return rows[0]
}

func replaced(t *testing.T, s *Service, r config.Repo, runner *panewireRunner, old reviewerJob) {
	t.Helper()
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label {
		t.Fatalf("spawns=%v, want a second spawn under a new label", runner.spawns)
	}
}

// (a) A record with a state and no owner lane is closed as the configured owner.
func TestLostReviewerNoJobOwnerClosesAsConfigOwner(t *testing.T) {
	s, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1"}, "cfg-owner")
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 1 || !slices.Equal(runner.closes[0][:6], []string{"job", "close", old.Label, "--lane", "cfg-owner", "--outcome"}) {
		t.Fatalf("closes=%v", runner.closes)
	}
	replaced(t, s, r, runner, old)
	if row := lostRow(t, s, r); row["close_owner_lane"] != "cfg-owner" || row["close_owner_source"] != "config" {
		t.Fatalf("audit row: %v", row)
	}
}

// (b) With no owner anywhere the close is skipped and the audit row says why.
func TestLostReviewerNoOwnerAnywhereSkipsCloseAndReplaces(t *testing.T) {
	s, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1"}, "")
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 {
		t.Fatalf("closes=%v", runner.closes)
	}
	replaced(t, s, r, runner, old)
	row := lostRow(t, s, r)
	if why, _ := row["close_skipped"].(string); !strings.Contains(why, "no owner lane") || row["close_owner_source"] != "" {
		t.Fatalf("audit row: %v", row)
	}
}

// (c) A close refused because the job already has a terminal record counts as
// closed. The text is panewire's own.
func TestLostReviewerCloseAlreadyRevokedContinues(t *testing.T) {
	already := &integrations.CommandError{Command: "panewire", Err: panewireExit(1), Stderr: "close refused: job mwr-12-aaaa111-p1-1793790000 already has a terminal record (00003-job.revoked.json) after its latest claim\n"}
	s, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}, "cfg-owner", already)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 1 {
		t.Fatalf("closes=%v", runner.closes)
	}
	replaced(t, s, r, runner, old)
}

// (d) Any other close error keeps the record and replaces nothing.
func TestLostReviewerOtherCloseErrorKeepsRecord(t *testing.T) {
	other := &integrations.CommandError{Command: "panewire", Err: panewireExit(1), Stderr: "hub unreachable\n"}
	s, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1"}, "cfg-owner", other)
	err := s.Review(context.Background(), r, 7, "", Options{})
	if err == nil {
		t.Fatal("want the close error")
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%v", runner.spawns)
	}
	if rj := readReviewerJob(t, s, r, 1); rj.Label != old.Label {
		t.Fatalf("record %+v", rj)
	}
}

// A close that cannot run from this host (the job's events live on another
// host) or cannot name an owner skips the close, says so in the audit, and
// replaces the reviewer without --operator-override.
func TestLostReviewerCloseUnavailableSkipsAndReplaces(t *testing.T) {
	for _, tt := range []struct{ name, stderr, note string }{
		{"other host", "close: no events directory for job mwr-12-aaaa111-p1-1793790000\n", "close: not on this host"},
		{"no claim owner", "close refused: job mwr-12-aaaa111-p1-1793790000 has no claim naming an owner lane; only --operator-override can close it\n", "operator override only"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fail := &integrations.CommandError{Command: "panewire", Err: panewireExit(1), Stderr: tt.stderr}
			s, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1"}, "cfg-owner", fail)
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(runner.closes) != 1 || slices.Contains(runner.closes[0], "--operator-override") {
				t.Fatalf("closes=%v", runner.closes)
			}
			replaced(t, s, r, runner, old)
			rows := auditRows(t, s, r, "review-result-lost-close-skipped")
			if len(rows) != 1 || rows[0]["close_owner_source"] != "config" || !strings.Contains(rows[0]["close_skipped"].(string), tt.note) {
				t.Fatalf("skip rows: %v", rows)
			}
		})
	}
}

// (e) A record that names an owner lane is closed as that lane, not the config's.
func TestLostReviewerJobOwnerWinsOverConfig(t *testing.T) {
	s, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "job-owner"}, "cfg-owner")
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 1 || runner.closes[0][4] != "job-owner" {
		t.Fatalf("closes=%v", runner.closes)
	}
	replaced(t, s, r, runner, old)
	if row := lostRow(t, s, r); row["close_owner_lane"] != "job-owner" || row["close_owner_source"] != "job" {
		t.Fatalf("audit row: %v", row)
	}
}

// (f) The reaper's abandon path closes as the configured owner when the job
// record names none, and still reports the error when neither does.
func TestReapReviewerAbandonUsesConfigOwner(t *testing.T) {
	merged := "2026-11-04T10:00:00Z"
	for _, tt := range []struct {
		name, owner, lane string
	}{
		{"config owner", "cfg-owner", "cfg-owner"},
		{"no owner anywhere", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newReviewerFix(t)
			f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
			f.s.Config.Review.Panewire.Owner = tt.owner
			f.allowJobClose()
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			rec := f.sink.recs[label]
			rec.State, rec.OwnerLane = "spawned", ""
			f.sink.recs[label] = rec
			f.setPane("w1:p5", "agent_status", "done")
			err := f.reap(t, 0, Options{})
			closes, rows := f.jobCloses(), f.rows(t)
			if tt.lane == "" {
				if err == nil || !strings.Contains(err.Error(), "owner lane unavailable") || len(closes) != 0 || len(rows) == 0 || rows[0].Result != "error" {
					t.Fatalf("err=%v closes=%v rows=%v", err, closes, rowSummary(rows))
				}
				return
			}
			if err != nil || len(closes) != 1 || closes[0][4] != tt.lane || rows[0].Result != "abandoned" {
				t.Fatalf("closes=%v rows=%v", closes, rowSummary(rows))
			}
		})
	}
}
