// Package crashreport sends fatal crashes of the mergewake daemon to Sentry.
//
// A fatal panic in any goroutine is written by the Go runtime to this
// process's crash file (runtime/debug.SetCrashOutput). The next start uploads
// crash files left by processes that are gone, then deletes them. Only
// Crash events contain no breadcrumbs, request data or user data. The daemon
// also sends sampled performance traces when configured. Without a DSN nothing
// is written or sent.
package crashreport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/tracing"
)

const (
	DSNEnv = "SENTRY_DSN_MERGEWAKE"
	// crashReportMaxPerStart and crashReportDailyBudget sample a crash loop:
	// a start considers at most the newest three crash files, and the host
	// sends at most three reports in any 24 hours (counted in crash/sent
	// across restarts). A launchd/systemd restart loop therefore costs at
	// most ~90 events a month, well inside Sentry's free tier.
	crashReportMaxPerStart = 3
	crashReportDailyBudget = 3
	crashReportMaxBytes    = 16 << 10
	crashReportFlush       = 10 * time.Second
)

// Run runs main with this process's crash output armed under stateDir. It
// disarms only after a normal return: a deferred disarm would run while a
// panic on main's goroutine unwinds, and the runtime would then write the
// traceback to stderr only.
func Run(stateDir, tool, version string, main func() error) error {
	stop := Start(stateDir, tool, version)
	err := main()
	stop()
	return err
}

// Start arms this process's crash file under <stateDir>/crash and uploads
// earlier crashes in the background. The returned stop disarms it and removes
// the file when nothing crashed.
func Start(stateDir, tool, version string) (stop func()) {
	dsn := strings.TrimSpace(os.Getenv(DSNEnv))
	state := stateDir
	if dsn == "" || state == "" {
		return func() {}
	}
	dir := filepath.Join(state, "crash")
	if os.MkdirAll(dir, 0o700) != nil {
		return func() {}
	}
	go reportPendingCrashes(dsn, tool, version, dir, time.Now())
	name := filepath.Join(dir, fmt.Sprintf("crash-%d-%d.log", time.Now().UnixNano(), os.Getpid()))
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return func() {}
	}
	// SetCrashOutput duplicates the descriptor; ours can close at once.
	armed := debug.SetCrashOutput(file, debug.CrashOptions{}) == nil
	_ = file.Close()
	if !armed {
		_ = os.Remove(name)
		return func() {}
	}
	return func() {
		_ = debug.SetCrashOutput(nil, debug.CrashOptions{})
		if info, err := os.Stat(name); err == nil && info.Size() == 0 {
			_ = os.Remove(name)
		}
	}
}

// StartContext shares one isolated Sentry client between tracing and crash uploads.
func StartContext(ctx context.Context, stateDir, tool, version string, log io.Writer) (context.Context, func()) {
	delivery := &crashDelivery{base: http.DefaultTransport}
	ctx, client := tracing.StartWithOptions(ctx, log, sentry.ClientOptions{
		Release: tool + "@" + version, Environment: "fleet", SampleRate: 1,
		SendDefaultPII: false, ServerName: fleetevent.ShortHost(), HTTPTransport: delivery,
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			event.User = sentry.User{}
			event.Request = nil
			event.Breadcrumbs = nil
			return event
		},
	})
	if client == nil {
		return ctx, Start(stateDir, tool, version)
	}
	stopClient := func() { client.Flush(crashReportFlush) }
	if stateDir == "" {
		return ctx, stopClient
	}
	dir := filepath.Join(stateDir, "crash")
	if os.MkdirAll(dir, 0o700) != nil {
		return ctx, stopClient
	}
	go reportPendingCrashesWithClient(client, delivery, tool, dir, time.Now())
	name := filepath.Join(dir, fmt.Sprintf("crash-%d-%d.log", time.Now().UnixNano(), os.Getpid()))
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return ctx, stopClient
	}
	armed := debug.SetCrashOutput(file, debug.CrashOptions{}) == nil
	_ = file.Close()
	if !armed {
		_ = os.Remove(name)
		return ctx, stopClient
	}
	return ctx, func() {
		_ = debug.SetCrashOutput(nil, debug.CrashOptions{})
		if info, err := os.Stat(name); err == nil && info.Size() == 0 {
			_ = os.Remove(name)
		}
		stopClient()
	}
}

// collectCrashFiles returns the non-empty crash files of processes that are no
// longer running, newest first and at most crashReportMaxPerStart; it deletes
// the older surplus and the empty files of gone processes.
func collectCrashFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var pending []string
	for _, entry := range entries {
		name := entry.Name()
		pid, ok := crashFilePID(name)
		if !ok || !entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil || crashProcessAlive(pid) {
			continue
		}
		if info.Size() == 0 {
			_ = os.Remove(path)
			continue
		}
		pending = append(pending, path)
	}
	// Names start with the start time in nanoseconds: sort newest first.
	slices.SortFunc(pending, func(a, b string) int { return strings.Compare(b, a) })
	if len(pending) > crashReportMaxPerStart {
		for _, path := range pending[crashReportMaxPerStart:] {
			_ = os.Remove(path)
		}
		pending = pending[:crashReportMaxPerStart]
	}
	return pending
}

// crashFilePID parses crash-<nanos>-<pid>.log.
func crashFilePID(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, "crash-")
	if !ok {
		return 0, false
	}
	rest, ok = strings.CutSuffix(rest, ".log")
	if !ok {
		return 0, false
	}
	_, pidText, ok := strings.Cut(rest, "-")
	pid, err := strconv.Atoi(pidText)
	return pid, ok && err == nil && pid > 0
}

func crashProcessAlive(pid int) bool {
	if pid == os.Getpid() {
		return true
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// reportPendingCrashes sends what the daily budget allows and drops the rest.
// Only reports Sentry accepted consume the budget, so an outage leaves the
// unsent files for the next start instead of spending the day's budget. The
// daemon and the hub share crash/, so collection, the budget check, the
// upload and the send log run under one exclusive lock.
func reportPendingCrashes(dsn, tool, version, dir string, now time.Time) {
	unlock, ok := lockCrashDir(dir)
	if !ok {
		return
	}
	defer unlock()
	pending := collectCrashFiles(dir)
	allowed := min(len(pending), crashBudgetLeft(dir, now))
	for _, path := range pending[allowed:] {
		_ = os.Remove(path)
	}
	// Fail closed: a send log that cannot be written could not enforce the
	// daily cap, so nothing is sent until it can.
	if allowed == 0 || writeCrashSendLog(dir, crashSendLog(dir, now)) != nil {
		return
	}
	sent, _ := uploadCrashReports(dsn, tool, version, pending[:allowed])
	_ = recordCrashSends(dir, now, sent)
}

func reportPendingCrashesWithClient(client *sentry.Client, delivery *crashDelivery, tool, dir string, now time.Time) {
	unlock, ok := lockCrashDir(dir)
	if !ok {
		return
	}
	defer unlock()
	pending := collectCrashFiles(dir)
	allowed := min(len(pending), crashBudgetLeft(dir, now))
	for _, path := range pending[allowed:] {
		_ = os.Remove(path)
	}
	// Fail closed: a send log that cannot be written could not enforce the
	// daily cap, so nothing is sent until it can.
	if allowed == 0 || writeCrashSendLog(dir, crashSendLog(dir, now)) != nil {
		return
	}
	sent, _ := uploadWithClient(client, delivery, tool, pending[:allowed])
	_ = recordCrashSends(dir, now, sent)
}

// lockCrashDir takes an exclusive lock on crash/lock, waiting for the other
// process (another mergewake) when it holds it.
func lockCrashDir(dir string) (unlock func(), ok bool) {
	unlock, err := fsutil.Lock(filepath.Join(dir, "lock"), true)
	return unlock, err == nil
}

// crashSendLog reads <dir>/sent: one unix second per accepted report, kept
// for the last 24 hours.
func crashSendLog(dir string, now time.Time) []string {
	var kept []string
	if raw, err := os.ReadFile(filepath.Join(dir, "sent")); err == nil {
		for _, field := range strings.Fields(string(raw)) {
			sec, err := strconv.ParseInt(field, 10, 64)
			if err == nil && now.Sub(time.Unix(sec, 0)) < 24*time.Hour {
				kept = append(kept, field)
			}
		}
	}
	return kept
}

// crashBudgetLeft is how many more reports this host may send at now.
func crashBudgetLeft(dir string, now time.Time) int {
	return max(0, crashReportDailyBudget-len(crashSendLog(dir, now)))
}

// recordCrashSends appends n accepted reports at now to the send log.
func recordCrashSends(dir string, now time.Time, n int) error {
	kept := crashSendLog(dir, now)
	for range n {
		kept = append(kept, strconv.FormatInt(now.Unix(), 10))
	}
	return writeCrashSendLog(dir, kept)
}

func writeCrashSendLog(dir string, kept []string) error {
	return fsutil.Atomic(filepath.Join(dir, "sent"), []byte(strings.Join(kept, "\n")+"\n"), 0600)
}

// uploadCrashReports sends one Sentry event per crash file and deletes a file
// only after Sentry answered its event with a 2xx; a rejected or failed
// delivery keeps the file for the next start.
func uploadCrashReports(dsn, tool, version string, paths []string) (sent int, err error) {
	delivery := &crashDelivery{base: http.DefaultTransport}
	client, err := crashClient(dsn, tool, version, delivery)
	if err != nil {
		return 0, err
	}
	return uploadWithClient(client, delivery, tool, paths)
}

func crashClient(dsn, tool, version string, delivery *crashDelivery) (*sentry.Client, error) {
	return sentry.NewClient(sentry.ClientOptions{
		Dsn:              dsn,
		Release:          tool + "@" + version,
		Environment:      "fleet",
		SampleRate:       1.0,
		EnableTracing:    false,
		TracesSampleRate: 0,
		SendDefaultPII:   false,
		ServerName:       fleetevent.ShortHost(),
		HTTPTransport:    delivery,
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			event.User = sentry.User{}
			event.Request = nil
			event.Breadcrumbs = nil
			return event
		},
	})
}

func uploadWithClient(client *sentry.Client, delivery *crashDelivery, tool string, paths []string) (sent int, err error) {
	var failed error
	for _, path := range paths {
		event := crashReportEvent(tool, path)
		if event == nil {
			continue
		}
		client.CaptureEvent(event, nil, sentry.NewScope())
		if !client.Flush(crashReportFlush) {
			// A late answer could otherwise be credited to the next file:
			// stop, and leave this and the rest for the next start.
			failed = errors.New("crash report delivery timed out")
			break
		}
		if accepted, rejected := delivery.take(); accepted == 0 || rejected > 0 {
			failed = errors.New("sentry did not accept a crash report")
			continue
		}
		_ = os.Remove(path)
		sent++
	}
	return sent, failed
}

// crashDelivery records Sentry's HTTP answers: sentry-go's Flush reports a
// drained queue, not an accepted event.
type crashDelivery struct {
	base               http.RoundTripper
	mu                 sync.Mutex
	accepted, rejected int
}

func (d *crashDelivery) RoundTrip(request *http.Request) (*http.Response, error) {
	// Transactions share the transport but do not consume the crash budget.
	crash := true
	if request.GetBody != nil {
		if body, e := request.GetBody(); e == nil {
			raw, _ := io.ReadAll(body)
			_ = body.Close()
			lines := bytes.SplitN(raw, []byte("\n"), 3)
			crash = len(lines) > 1 && bytes.Contains(lines[1], []byte(`"type":"event"`))
		}
	}
	response, err := d.base.RoundTrip(request)
	if !crash {
		return response, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil || response.StatusCode < 200 || response.StatusCode > 299 {
		d.rejected++
	} else {
		d.accepted++
	}
	return response, err
}

// take returns and resets the counts since the last take.
func (d *crashDelivery) take() (accepted, rejected int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	accepted, rejected = d.accepted, d.rejected
	d.accepted, d.rejected = 0, 0
	return accepted, rejected
}

// crashReportEvent builds the event for one crash file: the panic line as the
// exception value and the redacted, bounded traceback as extra data.
func crashReportEvent(tool, path string) *sentry.Event {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		return nil
	}
	text := crashReportScrub(string(raw))
	first, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	event := sentry.NewEvent()
	event.Level = sentry.LevelFatal
	event.Message = fleetevent.Bound(first, 200)
	event.Exception = []sentry.Exception{{Type: "fatal", Value: event.Message}}
	event.Tags = map[string]string{"tool": tool}
	event.Extra = map[string]any{"traceback": fleetevent.Bound(text, crashReportMaxBytes)}
	if info, err := os.Stat(path); err == nil {
		event.Timestamp = info.ModTime().UTC()
	}
	return event
}

// crashReportScrub redacts credentials and replaces the home directory, which
// names the operator's account, in a traceback.
func crashReportScrub(text string) string {
	if home, err := os.UserHomeDir(); err == nil && len(home) > 1 {
		text = strings.ReplaceAll(text, home, "~")
	}
	return fleetevent.Redact(text)
}
