// Package crashreport sends fatal crashes of the mergewake daemon to Sentry.
//
// A fatal panic in any goroutine is written by the Go runtime to this
// process's crash file (runtime/debug.SetCrashOutput). The next start uploads
// crash files left by processes that are gone, then deletes them. Only
// crashes are sent: no errors, traces, breadcrumbs, request data or user
// data. Without SENTRY_DSN_MERGEWAKE nothing is written or sent.
package crashreport

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/mergewake/internal/fleetevent"
)

const (
	DSNEnv = "SENTRY_DSN_MERGEWAKE"
	// crashReportMaxPerStart samples a crash loop: each start sends at most
	// this many reports and drops the older rest, which keeps a restart loop
	// inside Sentry's free tier.
	crashReportMaxPerStart = 3
	crashReportMaxBytes    = 16 << 10
	crashReportFlush       = 10 * time.Second
)

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
	if pending := collectCrashFiles(dir); len(pending) > 0 {
		go func() {
			_ = uploadCrashReports(dsn, tool, version, pending)
		}()
	}
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

// uploadCrashReports sends one Sentry event per crash file and deletes the
// files once Sentry's transport has flushed them.
func uploadCrashReports(dsn, tool, version string, paths []string) error {
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              dsn,
		Release:          tool + "@" + version,
		Environment:      "fleet",
		SampleRate:       1.0,
		EnableTracing:    false,
		TracesSampleRate: 0,
		SendDefaultPII:   false,
		ServerName:       fleetevent.ShortHost(),
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			event.User = sentry.User{}
			event.Request = nil
			event.Breadcrumbs = nil
			return event
		},
	})
	if err != nil {
		return err
	}
	for _, path := range paths {
		event := crashReportEvent(tool, path)
		if event == nil {
			continue
		}
		client.CaptureEvent(event, nil, sentry.NewScope())
	}
	if !client.Flush(crashReportFlush) {
		return errors.New("crash reports not flushed")
	}
	for _, path := range paths {
		_ = os.Remove(path)
	}
	return nil
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
