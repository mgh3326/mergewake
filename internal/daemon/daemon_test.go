package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fleetevent"
)

func TestRolesLockShutdownAndStatus(t *testing.T) {
	state := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	var wg sync.WaitGroup
	d := Daemon{StateDir: state, LockDir: filepath.Join(state, "locks"), Host: "host-a"}
	loops := []Loop{}
	for _, role := range []string{"pr", "watch"} {
		loops = append(loops, Loop{Role: role, Interval: time.Hour, Run: func(ctx context.Context) error { started <- struct{}{}; <-ctx.Done(); return ctx.Err() }})
	}
	done := make(chan error, 1)
	wg.Add(1)
	go func() { defer wg.Done(); done <- d.Run(ctx, loops) }()
	for range loops {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("roles did not start concurrently")
		}
	}
	other := Daemon{StateDir: state, LockDir: filepath.Join(state, "locks"), Host: "different-config-label"}
	if err := other.Run(context.Background(), loops[:1]); err == nil {
		t.Fatal("same-host role lock bypassed")
	}
	cancel()
	wg.Wait()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"pr", "watch"} {
		b, err := os.ReadFile(filepath.Join(state, "daemon", role+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var s Status
		if err = json.Unmarshal(b, &s); err != nil {
			t.Fatal(err)
		}
		if s.Running || !s.NextRun.IsZero() || s.LastRun.IsZero() || s.LastFinish.IsZero() {
			t.Fatalf("status=%+v", s)
		}
	}
	var out bytes.Buffer
	if err := ReadStatus(state, &out); err != nil || bytes.Count(out.Bytes(), []byte("\n")) != 2 {
		t.Fatalf("status output=%s err=%v", &out, err)
	}
	// Advisory locks release when the process/file exits, without stale stealing.
	again, stop := context.WithCancel(context.Background())
	if err := other.Run(again, []Loop{{Role: "pr", Interval: time.Hour, Run: func(context.Context) error { stop(); return nil }}}); err != nil {
		t.Fatal(err)
	}
}
func TestBackoffAndJitter(t *testing.T) {
	for failures := 0; failures < 12; failures++ {
		base := time.Second * time.Duration(1<<min(failures, 8))
		if delay := Delay(time.Second, failures, 0, 0); delay != base {
			t.Fatalf("no jitter: delay=%s base=%s", delay, base)
		}
		if delay := Delay(time.Second, failures, 0, 0.5); delay != base+base/10 {
			t.Fatalf("half jitter: delay=%s base=%s", delay, base)
		}
	}
	if Delay(time.Second, 0, time.Hour, 0.99) < time.Hour {
		t.Fatal("jitter bypassed rate limit")
	}
}
func TestTriggerSeamAndNextRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a"}
	called := make(chan struct{}, 3)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "reap", Interval: time.Hour, Run: func(context.Context) error { called <- struct{}{}; return nil }}})
	}()
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("no initial poll")
	}
	if !d.Trigger("reap") || d.Trigger("unknown") {
		t.Fatal("trigger routing")
	}
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("trigger did not wake role")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHealthcheckRunsAfterCleanLoopOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	pings, runs := 0, 0
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a",
		Healthy: func(context.Context) error { mu.Lock(); pings++; mu.Unlock(); return errors.New("pinger down") }}
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "watch", Interval: time.Hour, Run: func(context.Context) error {
			mu.Lock()
			runs++
			n := runs
			mu.Unlock()
			if n == 1 {
				return nil
			}
			cancel()
			return errors.New("poll failed")
		}}})
	}()
	for deadline := time.Now().Add(3 * time.Second); ; {
		mu.Lock()
		n := pings
		mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no ping after clean loop")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !d.Trigger("watch") {
		t.Fatal("trigger lost")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if pings != 1 || runs != 2 {
		t.Fatalf("pings=%d runs=%d after failing loop", pings, runs)
	}
}

func TestHealthcheckMaskedByFailingLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proceed := make(chan struct{})
	var mu sync.Mutex
	pings, cleanRuns, prRuns := 0, 0, 0
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a",
		Healthy: func(context.Context) error { mu.Lock(); pings++; mu.Unlock(); return nil }}
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{
			{Role: "watch", Interval: time.Hour, Run: func(context.Context) error {
				select {
				case <-proceed:
					mu.Lock()
					cleanRuns++
					mu.Unlock()
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}},
			{Role: "pr", Interval: time.Hour, Run: func(context.Context) error {
				mu.Lock()
				prRuns++
				n := prRuns
				mu.Unlock()
				if n == 1 {
					return errors.New("poll failed")
				}
				<-ctx.Done()
				return ctx.Err()
			}},
		})
	}()
	// A healthy sibling must not keep the shared ping alive while another
	// loop's last iteration carries a recorded failure.
	for deadline := time.Now().Add(3 * time.Second); ; {
		d.mu.Lock()
		f := d.failed["pr"]
		d.mu.Unlock()
		if f > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pr failure never recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(proceed)
	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if cleanRuns != 1 || pings != 0 {
		t.Fatalf("cleanRuns=%d pings=%d; failing loop was masked by a healthy sibling", cleanRuns, pings)
	}
}
func TestFixedClockAndJitterPinStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	state := t.TempDir()
	d := Daemon{StateDir: state, LockDir: t.TempDir(), Host: "host-a", Now: func() time.Time { return at }, Jitter: func() float64 { return 0 }}
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "reap", Interval: time.Hour, Run: func(context.Context) error { return nil }}})
	}()
	var s Status
	for deadline := time.Now().Add(3 * time.Second); s.NextRun.IsZero(); {
		if time.Now().After(deadline) {
			t.Fatal("next run never recorded")
		}
		if b, err := os.ReadFile(filepath.Join(state, "daemon", "reap.json")); err == nil {
			_ = json.Unmarshal(b, &s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !s.LastRun.Equal(at) || !s.NextRun.Equal(at.Add(time.Hour)) {
		t.Fatalf("status=%+v", s)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTriggerWaitsForWakeGap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a", WakeGap: 400 * time.Millisecond}
	runs := make(chan time.Time, 4)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "pr", Interval: time.Hour, Run: func(context.Context) error { runs <- time.Now(); return nil }}})
	}()
	first := <-runs
	d.Trigger("pr")
	select {
	case second := <-runs:
		if gap := second.Sub(first); gap < 350*time.Millisecond {
			t.Fatalf("woken pass ran %v after the previous one, want at least the wake gap", gap)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("trigger did not wake role after the gap")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTriggerDoesNotCutRateWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a", RateWait: func() time.Duration { return 400 * time.Millisecond }}
	runs := make(chan time.Time, 4)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "pr", Interval: time.Millisecond, Run: func(context.Context) error { runs <- time.Now(); return nil }}})
	}()
	first := <-runs
	d.Trigger("pr")
	second := <-runs
	if gap := second.Sub(first); gap < 350*time.Millisecond {
		t.Fatalf("woken pass ran %v after the previous one inside a rate-limit wait", gap)
	}
	cancel()
	<-done
}

func TestWakeNeverPostponesScheduledPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a", WakeGap: time.Hour, Jitter: func() float64 { return 0 }}
	runs := make(chan time.Time, 4)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "pr", Interval: 300 * time.Millisecond, Run: func(context.Context) error { runs <- time.Now(); return nil }}})
	}()
	first := <-runs
	d.Trigger("pr")
	select {
	case second := <-runs:
		if gap := second.Sub(first); gap > 2*time.Second {
			t.Fatalf("scheduled poll ran %v later, a wake postponed it", gap)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a wake inside the gap postponed the scheduled poll")
	}
	cancel()
	<-done
}

// runUntil runs one loop whose passes return errs in order (nil is success)
// and cancels once every pass has run. It returns the loop's Run error.
func runUntil(t *testing.T, d *Daemon, role string, errs []error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := 0
	d.LockDir = t.TempDir()
	d.Host = "host-a"
	d.Jitter = func() float64 { return 0 }
	loop := Loop{Role: role, Interval: time.Millisecond, Run: func(context.Context) error {
		e := errs[n]
		n++
		if n == len(errs) {
			cancel()
		}
		return e
	}}
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, []Loop{loop}) }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not finish")
		return nil
	}
}

func readRoleEvents(t *testing.T, state string) []fleetevent.Event {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "events", fleetevent.FileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []fleetevent.Event
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		var ev fleetevent.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Fatalf("bad event line %q: %v", line, err)
		}
		out = append(out, ev)
	}
	return out
}

func TestRoleErrorEventPerFailedPass(t *testing.T) {
	state := t.TempDir()
	d := &Daemon{StateDir: state}
	// Two failures, then a success whose cancel ends the loop.
	if err := runUntil(t, d, "pr", []error{errors.New("boom one"), errors.New("boom two"), nil}); err != nil {
		t.Fatal(err)
	}
	evs := readRoleEvents(t, state)
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(evs), evs)
	}
	for i, ev := range evs {
		if ev.Kind != "role_error" || ev.Outcome != "failed" || ev.Reason != "role_failed" || ev.Job != "pr" || ev.Tool != "mergewake" {
			t.Fatalf("event %d = %+v", i, ev)
		}
		want := []string{`role=pr failures=1 error="boom one"`, `role=pr failures=2 error="boom two"`}[i]
		if ev.Detail != want {
			t.Fatalf("event %d detail = %q, want %q", i, ev.Detail, want)
		}
	}
	// The status file keeps its shape: the success cleared the error.
	var s Status
	b, err := os.ReadFile(filepath.Join(state, "daemon", "pr.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &s); err != nil || s.Failures != 0 || s.LastError != "" {
		t.Fatalf("status = %+v, err = %v", s, err)
	}
}

func TestRoleErrorEventRedactsSecrets(t *testing.T) {
	state := t.TempDir()
	d := &Daemon{StateDir: state}
	secret := "ghp_" + "abcdefghijklmnopqrstuvwxyz0123456789"
	err := errors.New("fetch failed: Authorization: Bearer " + secret + " and GITHUB_TOKEN=hunter2hunter2")
	if e := runUntil(t, d, "watch", []error{err}); e != nil {
		t.Fatal(e)
	}
	evs := readRoleEvents(t, state)
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	for _, leak := range []string{secret, "hunter2hunter2"} {
		if strings.Contains(evs[0].Detail, leak) {
			t.Fatalf("detail leaks %q: %q", leak, evs[0].Detail)
		}
	}
	if !strings.Contains(evs[0].Detail, "role=watch failures=1") {
		t.Fatalf("detail = %q", evs[0].Detail)
	}
}

func TestRoleErrorEventWriteFailureKeepsLoopAndStatus(t *testing.T) {
	state := t.TempDir()
	// A regular file where the events directory belongs makes every write fail.
	if err := os.WriteFile(filepath.Join(state, "events"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{StateDir: state}
	if err := runUntil(t, d, "pr", []error{errors.New("boom"), nil}); err != nil {
		t.Fatalf("loop must survive an unwritable events dir: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(state, "daemon", "pr.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s Status
	if err := json.Unmarshal(b, &s); err != nil || s.Role != "pr" || s.Running || s.LastFinish.IsZero() {
		t.Fatalf("status = %+v, err = %v", s, err)
	}
}

// A reap pass cut by cancellation (a daemon stop mid-pass) leaves nothing that
// delays the next start: the first pass of a new run starts at once, not one
// interval after the cut one, even though the interval is an hour.
func TestCancelledPassRunsAgainAtNextStart(t *testing.T) {
	state, locks := t.TempDir(), t.TempDir()
	started := make(chan struct{}, 4)
	cut := func(ctx context.Context) error { started <- struct{}{}; <-ctx.Done(); return ctx.Err() }
	run := func(pass func(context.Context) error) (stop func() error) {
		ctx, cancel := context.WithCancel(context.Background())
		d := Daemon{StateDir: state, LockDir: locks, Host: "host-a"}
		done := make(chan error, 1)
		go func() { done <- d.Run(ctx, []Loop{{Role: "reap", Interval: time.Hour, Run: pass}}) }()
		return func() error { cancel(); return <-done }
	}

	stop := run(cut)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first run did not start a pass")
	}
	// The pass is cancelled mid-way; its error is the cancellation.
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(state, "daemon", "reap.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st Status
	if err := json.Unmarshal(b, &st); err != nil || !strings.Contains(st.LastError, "context canceled") || !st.NextRun.IsZero() {
		t.Fatalf("status after the cut pass: %+v, %v", st, err)
	}

	again := time.Now()
	stop = run(cut)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatalf("no pass within 3s of the next start (interval 1h); the cut pass was not retried")
	}
	if waited := time.Since(again); waited > 3*time.Second {
		t.Fatalf("pass started after %s", waited)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
