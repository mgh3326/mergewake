package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/mgh3326/mergewake/internal/integrations"
	"os"
	"strings"
	"testing"
	"time"
)

// (a),(b) Without the hub environment panewire cannot place a reviewer: the
// pass fails before the lease claim, the reviewer record and the spawn, and
// the skip names the missing variable.
func TestPanewireHubEnvMissingFailsBeforeSpawn(t *testing.T) {
	for name, missing := range map[string]string{
		"url unset":   hubURLEnv,
		"token empty": hubTokenEnv,
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := panewireSetup(t, runner)
			t.Setenv(missing, "")
			e := s.Review(context.Background(), r, 7, "openai", Options{})
			if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), missing) {
				t.Fatalf("err=%v, want ErrReviewUnavailable naming %s", e, missing)
			}
			if len(runner.spawns) != 0 || len(runner.git) != 0 || runner.headless != 0 {
				t.Fatalf("runner called: spawns=%d git=%d headless=%d", len(runner.spawns), len(runner.git), runner.headless)
			}
			if g.claimCount != 0 || g.refs[claimRef(7, shaA, 1)] || g.refs[doneRef(7, shaA)] {
				t.Fatalf("lease claimed: claims=%d refs=%v", g.claimCount, g.refs)
			}
			if _, err := os.Stat(s.reviewerJobPath(r.Name, 7, 1)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("reviewer record written: %v", err)
			}
			if g.postCount != 0 || len(g.passes[shaA]) != 0 {
				t.Fatalf("PR marked reviewed: posts=%d passes=%+v", g.postCount, g.passes[shaA])
			}
		})
	}
}

// (c) With both variables set the spawn runs exactly as before.
func TestPanewireHubEnvSetSpawnsAsBefore(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	t.Setenv(hubURLEnv, testHubURL)
	t.Setenv(hubTokenEnv, testHubTokenEnv)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 1 || len(g.passes[shaA]) != 1 || g.passes[shaA][0].Executor != "panewire" {
		t.Fatalf("spawns=%d headless=%d posts=%d passes=%+v", len(runner.spawns), runner.headless, g.postCount, g.passes[shaA])
	}
}

// (d) The failure names variables only: neither the error nor the
// review-skipped audit carries the value of a set variable.
func TestPanewireHubEnvErrorNamesNoValues(t *testing.T) {
	for name, tc := range map[string]struct {
		missing string
		value   string
	}{
		"url missing":   {hubURLEnv, testHubTokenEnv},
		"token missing": {hubTokenEnv, testHubURL},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
			s, _, r := panewireSetup(t, runner)
			t.Setenv(tc.missing, "")
			e := s.Review(context.Background(), r, 7, "openai", Options{})
			if e == nil {
				t.Fatal("review did not fail")
			}
			if strings.Contains(e.Error(), tc.value) {
				t.Fatalf("error carries the value of a set variable: %v", e)
			}
			skipped := auditRows(t, s, r, "review-skipped")
			if len(skipped) != 1 {
				t.Fatalf("review-skipped audit=%v", skipped)
			}
			if body := mustJSON(t, skipped[0]); strings.Contains(body, tc.value) {
				t.Fatalf("audit carries the value of a set variable: %s", body)
			}
		})
	}
}

// A resumed job that already completed is answered from its hk record with no
// panewire call, so a run without the hub environment still posts it.
func TestPanewireHubEnvMissingResumedCompletedPosts(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	c.t = c.t.Add(2 * time.Second)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	t.Setenv(hubURLEnv, "")
	t.Setenv(hubTokenEnv, "")
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 || g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
}

// A resumed job that still needs a re-attach calls panewire, so it fails with
// the missing names and keeps its record for the next run.
func TestPanewireHubEnvMissingResumedReattachSkips(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	c.t = c.t.Add(2 * time.Second)
	t.Setenv(hubTokenEnv, "")
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), hubTokenEnv) {
		t.Fatalf("err=%v, want ErrReviewUnavailable naming %s", err, hubTokenEnv)
	}
	if len(runner.spawns) != 1 || g.postCount != 0 || len(g.passes[shaA]) != 0 {
		t.Fatalf("spawns=%d posts=%d passes=%+v", len(runner.spawns), g.postCount, g.passes[shaA])
	}
	if _, err := os.Stat(s.reviewerJobPath(r.Name, 7, 1)); err != nil {
		t.Fatalf("reviewer record lost: %v", err)
	}
}
