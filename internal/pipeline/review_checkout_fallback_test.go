package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reviewReceipts is every reap receipt the pass wrote, reviewer rows included.
func reviewReceipts(t *testing.T, s *Service, r config.Repo) []reapReceipt {
	t.Helper()
	rows, err := s.reapReceipts(r.Name)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// A spec-placed spawn that ends without a pane result writes a pending-cleanup
// receipt naming the node's <label>-checkout worktree, and a later sweep with
// the job ended removes that worktree through the hub's guarded removal
// (hk 4815).
func TestSpecPlacedSpawnWithoutPaneResultIsReaped(t *testing.T) {
	freshReviewerSpecs(t)
	runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
	s, g, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	// A wait that ended on its own deadline is pending, not failed: the job may
	// still report, so the pass only records what the node may hold.
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); err != nil && !errors.Is(err, errPassPending) {
		t.Fatalf("err=%v", err)
	}
	if len(runner.spawns) != 1 || !slices.Contains(runner.spawns[0], "--checkout-origin") {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	label := arg(runner.spawns[0], "-l")
	// The node prepares <label>-checkout; no local checkout is named, so the
	// receipt names the node worktree by label and the sweep resolves its host
	// and path from the job record.
	var cleanups []reapReceipt
	for _, row := range reviewReceipts(t, s, r) {
		if row.Action == "reviewer-cleanup" {
			cleanups = append(cleanups, row)
		}
	}
	if len(cleanups) != 1 || cleanups[0].Result != "pending-cleanup" || cleanups[0].Label != label || cleanups[0].Worktree != label+reviewerCheckoutSuffix {
		t.Fatalf("cleanup receipts: %+v", cleanups)
	}

	// The job ends on its own clock; the sweep then removes the node worktree.
	cwd := "/node/" + label + reviewerCheckoutSuffix
	runner.sink.jobs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", Cwd: cwd}
	hub := newFakeReapHub()
	hub.agents["agents"], hub.agents["panes"] = []any{}, []any{}
	hub.detached, hub.wtHead = map[string]bool{cwd: true}, shaA
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	s.Config.Reap.Enabled, s.Config.Reap.Act = true, true
	s.Config.Reap.WorktreeAfter, s.Config.Reap.ScanAfter = "168h", "720h"
	s.Config.Review.Timeout = "10m"
	s.Config.Hosts = []reap.Host{{Name: "node", SSH: "ops@node.example", Machine: "node-a"}}
	// A pass that spawned the label may still be waiting on it for one review
	// timeout; the sweep visits it once that has gone.
	reviewed, ok := parseReviewerLabel(label)
	if !ok {
		t.Fatalf("label %q does not parse", label)
	}
	s.Now = func() time.Time { return reviewed.started.Add(time.Hour) }
	// The reviewer sweep runs whatever the PR's state, but only a merged PR
	// keeps the author's path from failing the pass.
	g.pull.Merged = true
	if err := s.Reap(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != cwd || removes[0]["expect_head"] != shaA {
		t.Fatalf("removes: %v (rows %v)", removes, g.postCount)
	}
}

// A hub that refuses the checkout spec leaves the pass on the local checkout;
// when that checkout cannot be made, nothing was placed: the lease goes at once
// and the reviewer record with it, and no receipt names a path that was never
// created (hk 4815).
func TestRefusedSpecWithAFailedLocalCheckoutReleasesTheNeverPlacedSpawn(t *testing.T) {
	freshReviewerSpecs(t)
	runner := &panewireRunner{family: "openai", specRefused: true, addFails: true}
	s, g, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if err := s.Review(context.Background(), r, 7, "openai", Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want a failed pass", err)
	}
	if len(runner.spawns) != 1 || !slices.Contains(runner.spawns[0], "--checkout-origin") {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	for _, row := range reviewReceipts(t, s, r) {
		if row.Action == "reviewer-cleanup" {
			t.Fatalf("receipt for a checkout that was never made: %+v", row)
		}
	}
	if _, err := os.Stat(s.reviewerJobPath(r.Name, 7, 1)); !os.IsNotExist(err) {
		t.Fatalf("reviewer record kept for a spawn that placed nothing: %v", err)
	}
	// The next evaluation finds no held lease and starts the pass at once.
	runner.addFails = false
	assertLeaseReleased(t, s, g, r, runner, "spawn-local-checkout-failed")
}
