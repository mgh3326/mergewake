package pipeline

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// passGH has no pull requests in any repository: the pass's host scans are what
// these tests count.
type passGH struct{ reapGH }

func (passGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return nil, nil
}

// passRunCalls is how many runner calls ran the command k.
func passRunCalls(run *scriptRunner, k string) int {
	n := 0
	for _, c := range run.calls {
		if c == k {
			n++
		}
	}
	return n
}

// A reap pass visits its repositories one after another. Each host is read
// once for the whole pass: the second repository reuses the first one's scan.
func TestReapPassScansEachHostOnce(t *testing.T) {
	run := &scriptRunner{outs: worktreeOuts("/repo.wt")}
	s := svc(t, &passGH{}, run)
	s.Out = io.Discard
	s.Config.Repos = append(s.Config.Repos, config.Repo{Name: "example/other", Branch: "dev"})
	ctx := WithReapPassHosts(context.Background())
	for _, r := range s.Config.Repos {
		if err := s.Reap(ctx, r, 0, Options{Shadow: true}); err != nil {
			t.Fatalf("reap %s: %v", r.Name, err)
		}
	}
	if n := passRunCalls(run, key("herdr", "pane", "list")); n != 1 {
		t.Fatalf("pane list scans for two repositories of one pass = %d, want 1", n)
	}
}

// A host that failed its scan stays failed for the pass: every repository
// reports it and none reads it again, so its removals stay refused.
func TestReapPassFailedHostIsReportedOnceScanned(t *testing.T) {
	run := &scriptRunner{outs: worktreeOuts("/repo.wt"), errs: map[string]error{key("herdr", "pane", "list"): errors.New("host down")}}
	s := svc(t, &passGH{}, run)
	s.Out = io.Discard
	s.Config.Repos = append(s.Config.Repos, config.Repo{Name: "example/other", Branch: "dev"})
	ctx := WithReapPassHosts(context.Background())
	for _, r := range s.Config.Repos {
		if err := s.Reap(ctx, r, 0, Options{Shadow: true}); err == nil {
			t.Fatalf("reap %s returned no error for an unreadable host", r.Name)
		}
	}
	if n := passRunCalls(run, key("herdr", "pane", "list")); n != 1 {
		t.Fatalf("pane list reads of a failed host in one pass = %d, want 1", n)
	}
}
