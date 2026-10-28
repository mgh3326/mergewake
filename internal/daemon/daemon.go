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

	"github.com/mgh3326/mergewake/internal/rollout"
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
	Log            *slog.Logger
	RateWait       func() time.Duration
	mu             sync.Mutex
	wake           map[string]chan struct{}
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
func Delay(interval time.Duration, failures int, rate time.Duration) time.Duration {
	n := min(failures, 8)
	base := min(interval*time.Duration(1<<n), max(interval, 30*time.Minute))
	// Positive jitter never wakes before a rate-limit reset.
	return max(base, rate) + time.Duration(rand.Float64()*0.2*float64(base))
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
		if exists || (l.Role != "pr" && l.Role != "watch" && l.Role != "release" && l.Role != "rollout") {
			return errors.New("invalid or duplicate daemon role")
		}
		f, err := lock(filepath.Join(d.StateDir, "daemon", l.Role+".lock"))
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
	errs := make(chan error, len(loops))
	for _, loop := range loops {
		wg.Add(1)
		go func(l Loop) {
			defer wg.Done()
			if err := d.runLoop(ctx, l); err != nil {
				errs <- err
				cancel()
			}
		}(loop)
	}
	wg.Wait()
	close(errs)
	var all []error
	for err := range errs {
		all = append(all, err)
	}
	return errors.Join(all...)
}
func (d *Daemon) runLoop(ctx context.Context, l Loop) error {
	st := Status{Host: d.Host, Role: l.Role}
	save := func() error {
		b, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			return err
		}
		return rollout.Atomic(filepath.Join(d.StateDir, "daemon", l.Role+".json"), b, 0600)
	}
	d.mu.Lock()
	ch := d.wake[l.Role]
	d.mu.Unlock()
	for ctx.Err() == nil {
		st.Running = true
		st.LastRun = time.Now().UTC()
		st.NextRun = time.Time{}
		if err := save(); err != nil {
			return err
		}
		err := l.Run(ctx)
		st.Running = false
		st.LastFinish = time.Now().UTC()
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
		delay := Delay(l.Interval, st.Failures, rate)
		if ctx.Err() == nil {
			st.NextRun = time.Now().UTC().Add(delay)
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
