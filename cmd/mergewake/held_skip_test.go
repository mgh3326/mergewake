package main

import (
	"context"
	"encoding/json"
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

	// A one-second role interval keeps the two inter-pass waits short;
	// the count and the delay still go through the real daemon loop.
	const interval = time.Second

	// The held passes run through the real daemon loop, so the failure
	// count and the next delay below are the role's own: runLoop counts
	// a pass error as one role failure (resetting on a clean pass),
	// records it in daemon/pr.json, and schedules NextRun with Delay.
	// Jitter is zero here so the expected delay is exactly the base
	// interval; production passes real jitter.
	type heldPass struct {
		res passResult
		err error
	}
	var mu sync.Mutex
	var passes []heldPass
	thirdReturned := make(chan time.Time, 1)
	run := func(ctx context.Context) error {
		res, err := f.passErr()
		mu.Lock()
		passes = append(passes, heldPass{res: res, err: err})
		n := len(passes)
		mu.Unlock()
		if n == 3 {
			thirdReturned <- time.Now()
		}
		return err
	}
	d := daemon.Daemon{
		StateDir: f.s.Config.StateDir,
		LockDir:  t.TempDir(),
		Jitter:   func() float64 { return 0 },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan error, 1)
	go func() { exited <- d.Run(ctx, []daemon.Loop{{Role: "pr", Interval: interval, Run: run}}) }()
	var third time.Time
	select {
	case third = <-thirdReturned:
	case err := <-exited:
		t.Fatalf("daemon exited before 3 held passes: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("daemon did not run 3 held passes")
	}
	var st daemon.Status
	statusPath := filepath.Join(f.s.Config.StateDir, "daemon", "pr.json")
	waitFor(t, "the daemon to record the third held pass", func() bool {
		b, err := os.ReadFile(statusPath)
		if err != nil {
			return false
		}
		var cur daemon.Status
		if json.Unmarshal(b, &cur) != nil {
			return false
		}
		if cur.Running || !cur.LastFinish.After(third) {
			return false
		}
		st = cur
		return true
	})
	mu.Lock()
	done := append([]heldPass(nil), passes...)
	mu.Unlock()
	if len(done) != 3 {
		t.Fatalf("daemon ran %d held passes, want 3", len(done))
	}
	// The hold writes no role failure and no error: it is a verdict, not
	// a failure. A regression that counts a held pass as a failure moves
	// Failures off zero here, through the same status the daemon serves.
	if st.Failures != 0 || st.LastError != "" {
		t.Fatalf("role failures %d (last error %q), want 0: %+v", st.Failures, st.LastError, st)
	}
	// With no failures the daemon's next delay stays the base interval.
	if gap := st.NextRun.Sub(st.LastFinish); gap < interval || gap > interval+time.Second {
		t.Fatalf("backoff %v after %d failures, want the base interval %v: %+v", gap, st.Failures, interval, st)
	}
	for i, p := range done {
		if p.err != nil {
			t.Fatalf("pass %d: %v", i+1, p.err)
		}
		if p.res.skipped != 0 || p.res.evaluated != 1 {
			t.Fatalf("pass %d: skipped=%d evaluated=%d, want the held PR evaluated: %s", i+1, p.res.skipped, p.res.evaluated, p.res.audit)
		}
		if n := strings.Count(p.res.audit, `"action":"land-held"`); n != 1 {
			t.Fatalf("pass %d: %d land-held rows, want 1: %s", i+1, n, p.res.audit)
		}
	}
	if _, ok := cachedEvaluation(f.skips, f.repo.Name, 1); ok {
		t.Fatalf("the held PR was cached as a clean settle")
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
	cancel()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("daemon: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("daemon did not stop")
	}
}
