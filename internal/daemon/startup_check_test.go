package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// runWithStartupCheck runs one loop until its first pass and returns the
// daemon's log lines.
func runWithStartupCheck(t *testing.T, sink string) []map[string]any {
	t.Helper()
	logs := &syncBuffer{}
	cfg := config.Config{Sink: config.Sink{Type: sink}}
	d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a", Log: slog.New(slog.NewJSONHandler(logs, nil)),
		StartupCheck: func() error { return integrations.HKEndpointCheck(cfg) }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "watch", Interval: time.Hour, Run: func(context.Context) error {
			select {
			case ran <- struct{}{}:
			default:
			}
			return nil
		}}})
	}()
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the daemon did not keep running after its startup check")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// A daemon with the handoffkeep sink and no svc-mergewake credential logs one
// error line naming the unset keys, without their values, and keeps running.
func TestStartupCheckNamesMissingHKKeys(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", "https://hk-url-value.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN", "cli-token-value")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	var errs []map[string]any
	for _, l := range runWithStartupCheck(t, "handoffkeep") {
		if l["level"] == "ERROR" {
			errs = append(errs, l)
		}
	}
	if len(errs) != 1 {
		t.Fatalf("want one error line, got %v", errs)
	}
	msg, _ := errs[0]["err"].(string)
	if !strings.Contains(msg, "HANDOFFKEEP_TOKEN_SVC_MERGEWAKE") || strings.Contains(msg, "HANDOFFKEEP_URL") {
		t.Fatalf("line does not name exactly the unset key: %v", errs[0])
	}
	raw, _ := json.Marshal(errs[0])
	if strings.Contains(string(raw), "cli-token-value") || strings.Contains(string(raw), "hk-url-value") {
		t.Fatalf("line carries a value: %s", raw)
	}
}

// A configured credential, or a sink that does not use hk, logs nothing.
func TestStartupCheckQuietWhenConfiguredOrUnused(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, tc := range []struct{ sink, url, token string }{
		{"handoffkeep", "https://hk.example.invalid", "svc"},
		{"noop", "", ""},
		{"", "", ""},
	} {
		t.Setenv("HANDOFFKEEP_URL", tc.url)
		t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", tc.token)
		for _, l := range runWithStartupCheck(t, tc.sink) {
			if l["level"] == "ERROR" {
				t.Fatalf("sink %q: unexpected error line %v", tc.sink, l)
			}
		}
	}
}
