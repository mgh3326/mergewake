package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
		done <- d.Run(ctx, []Loop{{Role: "release", Interval: time.Hour, Run: func(context.Context) error { called <- struct{}{}; return nil }}})
	}()
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("no initial poll")
	}
	if !d.Trigger("release") || d.Trigger("unknown") {
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
		done <- d.Run(ctx, []Loop{{Role: "release", Interval: time.Hour, Run: func(context.Context) error { return nil }}})
	}()
	var s Status
	for deadline := time.Now().Add(3 * time.Second); s.NextRun.IsZero(); {
		if time.Now().After(deadline) {
			t.Fatal("next run never recorded")
		}
		if b, err := os.ReadFile(filepath.Join(state, "daemon", "release.json")); err == nil {
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
