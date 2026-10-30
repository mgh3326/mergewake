// Package daemon schedules independent pipeline roles. Triggers are an optional
// webhook seam; the timer alone always supplies complete polling.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

type Loop struct {
	Role     string
	Interval time.Duration
	Run      func(context.Context) error
}
type Status struct {
	Host       string    `json:"host"`
	Role       string    `json:"role"`
	Running    bool      `json:"running"`
	LastRun    time.Time `json:"last_run"`
	LastFinish time.Time `json:"last_finish"`
	LastError  string    `json:"last_error"`
	NextRun    time.Time `json:"next_run"`
	Failures   int       `json:"failures"`
}
type Daemon struct {
	StateDir, Host string
	LockDir        string
	Log            *slog.Logger
	RateWait       func() time.Duration
	// Now stamps status times and Jitter returns a fraction in [0, 1) for the
	// retry delay; nil means time.Now and math/rand.
	Now    func() time.Time
	Jitter func() float64
	mu     sync.Mutex
	wake   map[string]chan struct{}
}

// Trigger wakes a role after a trusted adapter receives an event. It coalesces
// bursts; the role's local lock and timer still govern all work.
func (d *Daemon) Trigger(role string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ch, ok := d.wake[role]
	if !ok {
		return false
	}
	select {
	case ch <- struct{}{}:
	default:
	}
	return true
}
func lock(name string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("daemon role already running: %w", err)
	}
	return f, nil
}
func (d *Daemon) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Delay backs off exponentially from interval, waits at least rate, and adds
// jitter (a fraction in [0, 1)) of up to 20% of the backed-off interval.
func Delay(interval time.Duration, failures int, rate time.Duration, jitter float64) time.Duration {
	n := min(failures, 8)
	base := interval
	for i := 0; i < n && base < 30*time.Minute; i++ {
		base = min(base*2, 30*time.Minute)
	}
	// Positive jitter never wakes before a rate-limit reset.
	return max(base, rate) + time.Duration(jitter*0.2*float64(base))
}
func (d *Daemon) Run(ctx context.Context, loops []Loop) error {
	if len(loops) == 0 {
		return errors.New("daemon requires at least one enabled role")
	}
	if d.Log == nil {
		d.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	d.mu.Lock()
	d.wake = map[string]chan struct{}{}
	d.mu.Unlock()
	lockDir := d.LockDir
	if lockDir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return err
		}
		lockDir = filepath.Join(cache, "mergewake", "daemon-locks")
	}
	var locks []*os.File
	defer func() {
		for _, f := range locks {
			f.Close()
		}
	}()
	for _, l := range loops {
		if l.Interval <= 0 || l.Run == nil {
			return errors.New("invalid daemon loop")
		}
		d.mu.Lock()
		_, exists := d.wake[l.Role]
		d.mu.Unlock()
		if exists || (l.Role != "pr" && l.Role != "watch" && l.Role != "release" && l.Role != "rollout" && l.Role != "reap") {
			return errors.New("invalid or duplicate daemon role")
		}
		f, err := lock(filepath.Join(lockDir, l.Role+".lock"))
		if err != nil {
			return fmt.Errorf("%s: %w", l.Role, err)
		}
		locks = append(locks, f)
		d.mu.Lock()
		d.wake[l.Role] = make(chan struct{}, 1)
		d.mu.Unlock()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	// One slot per loop keeps the joined error in configuration order.
	errs := make([]error, len(loops))
	for i, loop := range loops {
		wg.Add(1)
		go func(i int, l Loop) {
			defer wg.Done()
			if err := d.runLoop(ctx, l); err != nil {
				errs[i] = err
				cancel()
			}
		}(i, loop)
	}
	wg.Wait()
	return errors.Join(errs...)
}
func (d *Daemon) runLoop(ctx context.Context, l Loop) error {
	st := Status{Host: d.Host, Role: l.Role}
	save := func() error {
		return fsutil.WriteJSON(filepath.Join(d.StateDir, "daemon", l.Role+".json"), st)
	}
	d.mu.Lock()
	ch := d.wake[l.Role]
	d.mu.Unlock()
	for ctx.Err() == nil {
		st.Running = true
		st.LastRun = d.now().UTC()
		st.NextRun = time.Time{}
		if err := save(); err != nil {
			return err
		}
		err := l.Run(ctx)
		st.Running = false
		st.LastFinish = d.now().UTC()
		st.LastError = ""
		if err != nil {
			st.LastError = err.Error()
			st.Failures++
		} else {
			st.Failures = 0
		}
		rate := time.Duration(0)
		if d.RateWait != nil {
			rate = d.RateWait()
		}
		jitter := rand.Float64
		if d.Jitter != nil {
			jitter = d.Jitter
		}
		delay := Delay(l.Interval, st.Failures, rate, jitter())
		if ctx.Err() == nil {
			st.NextRun = d.now().UTC().Add(delay)
		}
		if e := save(); e != nil {
			return e
		}
		d.Log.Info("loop completed", "role", l.Role, "error", st.LastError, "failures", st.Failures, "next_run", st.NextRun)
		if ctx.Err() != nil {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		case <-ch:
			timer.Stop()
		}
	}
	st.Running = false
	st.NextRun = time.Time{}
	return save()
}

func ReadStatus(state string, out io.Writer) error {
	files, err := filepath.Glob(filepath.Join(state, "daemon", "*.json"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("no daemon status recorded")
	}
	for _, name := range files {
		b, e := os.ReadFile(name)
		if e != nil {
			return e
		}
		var s Status
		if e = json.Unmarshal(b, &s); e != nil {
			return e
		}
		if e = json.NewEncoder(out).Encode(s); e != nil {
			return e
		}
	}
	return nil
}
