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
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/fsutil"
)

type Loop struct {
	Role     string
	Interval time.Duration
	Run      func(context.Context) error
	Requests func() any
}
type Status struct {
	Host           string    `json:"host"`
	Role           string    `json:"role"`
	Running        bool      `json:"running"`
	LastRun        time.Time `json:"last_run"`
	LastFinish     time.Time `json:"last_finish"`
	LastError      string    `json:"last_error"`
	NextRun        time.Time `json:"next_run"`
	Failures       int       `json:"failures"`
	GitHubRequests any       `json:"github_requests,omitempty"`
}
type Daemon struct {
	StateDir, Host string
	LockDir        string
	Log            *slog.Logger
	RateWait       func() time.Duration
	// Healthy runs after a loop iteration when no loop has a recorded failure,
	// e.g. an external healthcheck ping: one shared URL must not be kept alive
	// by a healthy role while another fails every iteration. It is fail-open:
	// its error is logged and never counted as a loop failure; nil means off.
	Healthy func(ctx context.Context) error
	// StartupCheck runs once when Run starts, before any loop. Its error names
	// configuration the operator must fix (a missing hk credential key, see
	// integrations.HKEndpointCheck); it is logged as one error line and the
	// daemon keeps running. nil means off.
	StartupCheck func() error
	// Now stamps status times and Jitter returns a fraction in [0, 1) for the
	// retry delay; nil means time.Now and math/rand.
	Now    func() time.Time
	Jitter func() float64
	// Webhook, when set, is the GitHub delivery receiver served next to the
	// loops; it shuts down with the daemon context. nil means off.
	Webhook *Webhook
	// WakeGap is the shortest time from the end of a pass to a pass a Trigger
	// wakes. A wake also never cuts short a rate-limit wait.
	WakeGap time.Duration
	// ReviewerWakeGap is the same for a wake from the reviewer watch; zero means
	// ReviewerGap. A finished reviewer job costs no GitHub request to notice, so
	// it does not wait for WakeGap.
	ReviewerWakeGap time.Duration
	// Reviewers, when set, runs next to the loops while the pr role runs and
	// wakes it when a reviewer job finishes. nil means off.
	Reviewers *ReviewerWatch
	mu        sync.Mutex
	wake      map[string]chan struct{}
	pending   map[string][]WakeSource
	failed    map[string]int
}

// ReviewerGap is the shortest time from the end of a pass to a pass a finished
// reviewer job wakes. It is only long enough to keep a burst of finishing jobs
// from running passes back to back.
const ReviewerGap = 30 * time.Second

// WakeSource names what started a pass.
type WakeSource string

const (
	WakeInterval WakeSource = "interval"
	WakeWebhook  WakeSource = "webhook"
	WakeReviewer WakeSource = "reviewer"
)

// Trigger wakes a role after a trusted adapter receives an event. It coalesces
// bursts; the role's local lock and timer still govern all work.
func (d *Daemon) Trigger(role string) bool { return d.trigger(role, WakeWebhook) }

// TriggerReviewer wakes a role because a reviewer job finished.
func (d *Daemon) TriggerReviewer(role string) bool { return d.trigger(role, WakeReviewer) }

func (d *Daemon) trigger(role string, src WakeSource) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ch, ok := d.wake[role]
	if !ok {
		return false
	}
	// The source is kept beside the coalesced signal: a reviewer wake behind a
	// pending webhook wake must still be seen, it has the shorter gap.
	if !slices.Contains(d.pending[role], src) {
		d.pending[role] = append(d.pending[role], src)
	}
	select {
	case ch <- struct{}{}:
	default:
	}
	return true
}

func (d *Daemon) takeWakes(role string) []WakeSource {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.pending[role]
	delete(d.pending, role)
	return out
}

func (d *Daemon) gap(src WakeSource) time.Duration {
	if src != WakeReviewer {
		return d.WakeGap
	}
	if d.ReviewerWakeGap > 0 {
		return d.ReviewerWakeGap
	}
	return ReviewerGap
}
func lock(name string) (func(), error) {
	unlock, err := fsutil.Lock(name, false)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return nil, fmt.Errorf("daemon role already running: %w", err)
	}
	return unlock, err
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
	d.failed = map[string]int{}
	d.pending = map[string][]WakeSource{}
	d.mu.Unlock()
	lockDir := d.LockDir
	if lockDir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return err
		}
		lockDir = filepath.Join(cache, "mergewake", "daemon-locks")
	}
	var locks []func()
	defer func() {
		for _, unlock := range locks {
			unlock()
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
		unlock, err := lock(filepath.Join(lockDir, l.Role+".lock"))
		if err != nil {
			return fmt.Errorf("%s: %w", l.Role, err)
		}
		locks = append(locks, unlock)
		d.mu.Lock()
		d.wake[l.Role] = make(chan struct{}, 1)
		d.mu.Unlock()
	}
	if d.StartupCheck != nil {
		if err := d.StartupCheck(); err != nil {
			d.Log.Error("startup check failed", "err", err.Error())
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	if d.Webhook != nil {
		if err := d.serveWebhook(ctx, &wg); err != nil {
			return err
		}
	}
	if d.Reviewers != nil && d.Reviewers.Jobs != nil {
		d.mu.Lock()
		_, hasPR := d.wake["pr"]
		d.mu.Unlock()
		if hasPR {
			rw := *d.Reviewers
			rw.Log = d.Log
			if rw.Wake == nil {
				rw.Wake = func() { d.TriggerReviewer("pr") }
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				rw.Run(ctx)
			}()
		}
	}
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
	events := fleetevent.New(d.StateDir, "mergewake", d.now)
	// The first pass is the start; every later one is named by what ended the wait.
	src := WakeInterval
	for ctx.Err() == nil {
		st.Running = true
		st.LastRun = d.now().UTC()
		st.NextRun = time.Time{}
		if err := save(); err != nil {
			return err
		}
		err := l.Run(ctx)
		if l.Requests != nil {
			st.GitHubRequests = l.Requests()
		}
		st.Running = false
		st.LastFinish = d.now().UTC()
		st.LastError = ""
		if err != nil {
			st.LastError = err.Error()
			st.Failures++
			// Best effort: the status file keeps only the latest error, so each
			// failed pass also leaves one line outside it. A write failure
			// changes nothing about the loop.
			events.Emit("role_error", l.Role, "failed", "role_failed",
				fmt.Sprintf("role=%s failures=%d error=%q", l.Role, st.Failures, st.LastError))
		} else {
			st.Failures = 0
		}
		d.mu.Lock()
		d.failed[l.Role] = st.Failures
		healthy := st.Failures == 0
		for _, n := range d.failed {
			healthy = healthy && n == 0
		}
		d.mu.Unlock()
		if healthy && d.Healthy != nil && ctx.Err() == nil {
			if e := d.Healthy(ctx); e != nil {
				d.Log.Warn("healthcheck ping failed", "role", l.Role, "error", e)
			}
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
		d.Log.Info("loop completed", "role", l.Role, "wake", string(src), "error", st.LastError, "failures", st.Failures, "next_run", st.NextRun)
		if ctx.Err() != nil {
			break
		}
		src = d.wait(ctx, l.Role, ch, delay, st.LastFinish)
	}
	st.Running = false
	st.NextRun = time.Time{}
	return save()
}

// wait sleeps until the scheduled poll, or earlier when a wake arrives, and
// returns what ended it: the source of the wake that set the start time, or the
// interval. A woken pass starts no sooner than the wake source's gap (WakeGap,
// or ReviewerGap for the reviewer watch) after lastFinish and never inside a
// rate-limit wait, read again at each wake because another role may have moved
// it. A wake never pushes the scheduled poll later. Cancellation returns at
// once; the caller's loop condition then stops.
func (d *Daemon) wait(ctx context.Context, role string, ch <-chan struct{}, delay time.Duration, lastFinish time.Time) WakeSource {
	due := d.now().Add(delay)
	src := WakeInterval
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return src
		case <-timer.C:
			return src
		case <-ch:
			for _, w := range d.takeWakes(role) {
				now := d.now()
				at := lastFinish.Add(d.gap(w))
				if d.RateWait != nil {
					if r := now.Add(d.RateWait()); r.After(at) {
						at = r
					}
				}
				if at.Before(now) {
					at = now
				}
				if !at.Before(due) {
					continue
				}
				due, src = at, w
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(at.Sub(now))
			}
		}
	}
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
