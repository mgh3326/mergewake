package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// holdNoticeCounter counts the merge-held lane notices a pass sends.
type holdNoticeCounter struct {
	mu   sync.Mutex
	held int
}

func (n *holdNoticeCounter) Notify(_ context.Context, e integrations.Event) error {
	if strings.HasPrefix(e.ID, "mergewake-merge-held-") {
		n.mu.Lock()
		n.held++
		n.mu.Unlock()
	}
	return nil
}

func (n *holdNoticeCounter) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.held
}

// A landed-but-open hold is a verdict, not a clean settle: every pass re-runs
// Land and shows land-held, the passes count no role failure (so the role's
// backoff never moves), and the merge-held notice goes out once.
func TestHeldPRIsEvaluatedEveryPass(t *testing.T) {
	f := newSkipFixture(t, 1)
	oldBase := strings.Repeat("a", 40)
	landed := strings.Repeat("2", 40)
	f.landedSHA = landed
	f.landedMsg = "Squash the thing (#1)"
	// The squash of headOf(1) landed on main after oldBase; GitHub left the
	// PR open, so every pass must hold it.
	attemptPath := filepath.Join(f.s.Config.StateDir, f.repo.Name, "pr-1-merge-attempt.json")
	if err := os.MkdirAll(filepath.Dir(attemptPath), 0o755); err != nil {
		t.Fatal(err)
	}
	attempt := fmt.Sprintf(`{"pr":1,"head":%q,"base":%q,"at":%q}`, headOf(1), oldBase, f.clock.UTC().Format(time.RFC3339))
	if err := os.WriteFile(attemptPath, []byte(attempt), 0o644); err != nil {
		t.Fatal(err)
	}
	notices := &holdNoticeCounter{}
	f.s.Notifier = notices

	const interval = time.Minute
	failures := 0
	for pass := 1; pass <= 3; pass++ {
		res, err := f.passErr()
		if err != nil {
			failures++
			t.Fatalf("pass %d: %v", pass, err)
		}
		if res.skipped != 0 || res.evaluated != 1 {
			t.Fatalf("pass %d: skipped=%d evaluated=%d, want the held PR evaluated: %s", pass, res.skipped, res.evaluated, res)
		}
		if n := strings.Count(res.audit, `"action":"land-held"`); n != 1 {
			t.Fatalf("pass %d: %d land-held rows, want 1: %s", pass, n, res.audit)
		}
		if _, ok := cachedEvaluation(f.skips, f.repo.Name, 1); ok {
			t.Fatalf("pass %d: the held PR was cached as a clean settle", pass)
		}
	}
	if got := notices.count(); got != 1 {
		t.Fatalf("merge-held notices %d, want 1", got)
	}
	// The hold writes no land-error row: it is a verdict, not a failure.
	audit, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, f.repo.Name, "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(audit), `"command":"land-error"`) {
		t.Fatalf("land-error audited for the hold: %s", audit)
	}
	// Clean passes leave the role's failure count at zero, so its backoff
	// never moves off the base interval.
	if failures != 0 {
		t.Fatalf("role failures %d, want 0", failures)
	}
	if got := daemon.Delay(interval, failures, 0, 0); got != interval {
		t.Fatalf("backoff %v after %d failures, want the base interval %v", got, failures, interval)
	}
}
