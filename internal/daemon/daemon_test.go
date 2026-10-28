package daemon

import (
	"bytes"
	"context"
	"encoding/json"
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
		delay := Delay(time.Second, failures, 0)
		base := time.Second * time.Duration(1<<min(failures, 8))
		if delay < base || delay > base+base/5 {
			t.Fatalf("delay=%s base=%s", delay, base)
		}
	}
	if Delay(time.Second, 0, time.Hour) < time.Hour {
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
