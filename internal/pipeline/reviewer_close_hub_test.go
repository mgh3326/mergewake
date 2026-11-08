package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// writeHubFile writes a 0600 env file for the fake hub: the service token,
// and the cf access pair when cf is set.
func writeHubFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// withReapHub points the service at a fake hub with a service token file and,
// when cf is set, a cf file. The files are fixtures the test writes itself.
func withReapHub(t *testing.T, s *Service, cf bool) config.ReapHub {
	t.Helper()
	server := newFakeReapHub().serve(t)
	dir := t.TempDir()
	hub := config.ReapHub{URL: server.URL, TokenFile: writeHubFile(t, dir, "svc-mergewake.env", "HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n")}
	if cf {
		hub.CFFile = writeHubFile(t, dir, "cf.env", "CF_ACCESS_CLIENT_ID=id\nCF_ACCESS_CLIENT_SECRET=secret\n")
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = hub
	return hub
}

// assertHubFlags checks that a job close carries the configured reap.hub
// credential paths, and that the paths are the only credential it names.
func assertHubFlags(t *testing.T, args []string, hub config.ReapHub) {
	t.Helper()
	if arg(args, "--hub-url") != hub.URL || arg(args, "--hub-token-env") != hub.TokenFile || arg(args, "--hub-cf-env") != hub.CFFile {
		t.Fatalf("close %v carries no reap.hub credential %+v", args, hub)
	}
}

// A lost reviewer's close carries the reap.hub service credential, so the hub
// accepts it for a job whose pane ran on another host.
func TestLostReviewerCloseCarriesTheReapHubCredential(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	hub := withReapHub(t, s, true)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	closedOnce(t, runner, old.Label)
	assertHubFlags(t, runner.closes[0], hub)
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 0 {
		t.Fatalf("skipped audit=%v", rows)
	}
}

// A close the hub refuses, with the credential passed, still ends as one
// reviewer-close-skipped row and the replacement runs.
func TestLostReviewerRefusedHubCloseIsAuditedOnce(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	hub := withReapHub(t, s, true)
	runner.closeErrs = []error{panewireExit(1)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	closedOnce(t, runner, old.Label)
	assertHubFlags(t, runner.closes[0], hub)
	rows := auditRows(t, s, r, "reviewer-close-skipped")
	if len(rows) != 1 || rows[0]["label"] != old.Label || rows[0]["head"] != shaA || rows[0]["reason"] == "" || rows[0]["reason"] == "no owner lane" {
		t.Fatalf("skipped audit=%v", rows)
	}
	reviewerJobGone(t, s, r, 1)
}

// With reap.hub unset the lost reviewer's close is today's argv: no hub flag.
func TestLostReviewerCloseWithoutReapHubIsTheBareArgv(t *testing.T) {
	s, g, r, runner, old := lostReviewerSetup(t, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	s.Config.Reap.Hub = config.ReapHub{}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacedOnce(t, s, g, r, runner, old)
	closedOnce(t, runner, old.Label)
	if len(runner.closes[0]) != 9 || !slices.Equal(runner.closes[0][:8], []string{"job", "close", old.Label, "--lane", "owner", "--outcome", "abandoned", "--reason"}) {
		t.Fatalf("close %v is not the bare argv", runner.closes[0])
	}
}

// The reap reviewer sweep's abandon close carries the reap.hub credential too.
func TestReapReviewerAbandonCarriesTheReapHubCredential(t *testing.T) {
	f := newReviewerFix(t)
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Head: github.Ref{SHA: headSHA}}}
	f.allowJobClose()
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	f.s.Config.Reap.Hub.CFFile = writeHubFile(t, t.TempDir(), "cf.env", "CF_ACCESS_CLIENT_ID=id\nCF_ACCESS_CLIENT_SECRET=secret\n")
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	closes := f.jobCloses()
	if len(closes) != 1 || closes[0][2] != label || arg(closes[0], "--lane") != "lane-x" || arg(closes[0], "--outcome") != "abandoned" {
		t.Fatalf("job closes=%v", closes)
	}
	assertHubFlags(t, closes[0], f.s.Config.Reap.Hub)
}

// hubFlagArgs spells out the reap.hub credential flags a close carries when
// the hub is configured.
func hubFlagArgs(hub config.ReapHub) []string {
	out := []string{"--hub-url", hub.URL, "--hub-token-env", hub.TokenFile}
	if hub.CFFile != "" {
		out = append(out, "--hub-cf-env", hub.CFFile)
	}
	return out
}
