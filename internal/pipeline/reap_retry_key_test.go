package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// tempNameRefusal is a runner whose job closes fail on every call with a
// refusal that names a fresh temp file, as a close that writes one does. The
// exit status and refusal code never change; only the stderr text does.
type tempNameRefusal struct {
	*scriptRunner
	n int
}

func (r *tempNameRefusal) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if k := name + "\x00" + strings.Join(args, "\x00"); strings.Contains(k, "\x00job\x00close\x00") {
		r.calls = append(r.calls, k)
		r.n++
		return nil, closeRefusal(fmt.Sprintf("close refused: cannot replace .close-%d.tmp: permission denied", r.n))
	}
	return r.scriptRunner.Run(ctx, name, args, input, dir)
}

// Two consecutive closes that differ only by a temp file name in stderr share
// one retry key, so the backoff escalates instead of resetting to one attempt
// on every pass (PR #503 review r4651665588, hk 6005).
func TestReapReviewerCloseBackoffEscalatesAcrossTempNames(t *testing.T) {
	f := newStuckJobFix(t, 7*time.Hour)
	f.s.Runner = &tempNameRefusal{scriptRunner: f.run}
	f.passAfter(t, 0)
	f.clearPanes()
	if err := f.passAt(t, 30*time.Minute); err == nil {
		t.Fatalf("refused close did not fail its pass: %v", rowSummary(f.rows(t)))
	}
	// The next pass waits out the backoff; a later pass retries the close, which fails again.
	_ = f.passAt(t, 30*time.Minute)
	_ = f.passAt(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 2 {
		t.Fatalf("job closes=%d, want 2: %v", n, rowSummary(f.rows(t)))
	}
	state := f.s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	var got reapRetry
	for k, v := range state.retries {
		if strings.HasSuffix(k, "\x00"+f.label) {
			got = v
		}
	}
	if got.attempts != 2 || got.failure != "exit 5 other" {
		t.Fatalf("retry state attempts=%d key %q, want attempts 2 under key %q", got.attempts, got.failure, "exit 5 other")
	}
}

// The retry key is the exit status plus a normalised refusal code, whatever
// stderr says around it.
func TestAbandonRetryKeyIsStableFailureID(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"temp name a", closeRefusal("close refused: .close-a.tmp: permission denied"), "exit 5 other"},
		{"temp name b", closeRefusal("close refused: .close-b.tmp: permission denied"), "exit 5 other"},
		{"already closed", closeRefusal("job x already has a terminal record"), "exit 5 already_closed"},
		{"job unknown", closeRefusal("job_unknown"), "exit 5 job_unknown"},
		{"scope forbidden", closeRefusal("scope_forbidden"), "exit 5 scope_forbidden"},
		{"other exit", &integrations.CommandError{Command: "panewire", Err: panewireExit(1), Stderr: "boom"}, "exit 1 other"},
	}
	for _, c := range cases {
		if got := abandonRetryKey(c.err); got != c.want {
			t.Errorf("%s: abandonRetryKey=%q, want %q", c.name, got, c.want)
		}
	}
}

// A restart restores the backoff from the row's own retry key and deadline, not
// from the reason text, which carries stderr that may differ from the key.
func TestReapReviewerCloseRetryRestoresFromRetryKey(t *testing.T) {
	s := &Service{Now: func() time.Time { return reviewerNow }}
	w := &reviewerSweep{s: s, r: config.Repo{Name: testRepo.Name}, streams: map[string]map[string]reapReceipt{
		"label": {"abandon": {Result: "error", Reason: "abandon job: close refused: .close-9.tmp", RetryKey: "exit 5 other", RetryAfter: reviewerNow.Add(time.Hour)}},
	}}
	if !w.closeRetryBlocked("label", 1) {
		t.Fatal("a durable failure row did not hold its retry")
	}
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if got := state.retries[testRepo.Name+"\x00label"].failure; got != "exit 5 other" {
		t.Fatalf("restored key %q, want the row's retry key", got)
	}
}

// A row an older version wrote has no deadline. It is expired (hk 6983): a
// restart onto the new binary tries the close again rather than restoring a
// backoff from the row, so a one-off reap is not blocked by it.
func TestReapReviewerCloseRetryLegacyRowWithoutDeadlineIsExpired(t *testing.T) {
	s := &Service{Now: func() time.Time { return reviewerNow }}
	w := &reviewerSweep{s: s, r: config.Repo{Name: testRepo.Name}, streams: map[string]map[string]reapReceipt{
		"label": {"abandon": {Result: "error", Reason: "abandon job: panewire exit status 5 owner_mismatch"}},
	}}
	if w.closeRetryBlocked("label", 1) {
		t.Fatal("a legacy failure row without a deadline held its retry")
	}
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, ok := state.retries[testRepo.Name+"\x00label"]; ok {
		t.Fatal("a legacy failure row restored a backoff from its reason")
	}
}
