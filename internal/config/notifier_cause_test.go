package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNotifierCauseThresholdAndWindow(t *testing.T) {
	for _, tt := range []struct {
		name, extra, wantError string
		threshold              int
		window                 time.Duration
	}{
		{"unset", "", "", DefaultCauseThreshold, time.Hour},
		{"valid", "  cause_threshold: 4\n  cause_window: 30m\n", "", 4, 30 * time.Minute},
		{"zero threshold", "  cause_threshold: 0\n", "notifier.cause_threshold", 0, 0},
		{"negative threshold", "  cause_threshold: -3\n", "notifier.cause_threshold", 0, 0},
		{"zero window", "  cause_window: 0s\n", "notifier.cause_window", 0, 0},
		{"bad window", "  cause_window: soon\n", "notifier.cause_window", 0, 0},
		{"negative window", "  cause_window: -1h\n", "notifier.cause_window", 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			contents := "version: 1\nhost: host-a\nnotifier:\n  type: stdout\n  lane: operator-desk\n" + tt.extra
			if err := os.WriteFile(p, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("Load error = %v, want error naming %s", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Notifier.CauseThresholdOrDefault(); got != tt.threshold {
				t.Fatalf("cause threshold = %d, want %d", got, tt.threshold)
			}
			if got := c.Notifier.CauseWindowOrDefault(); got != tt.window {
				t.Fatalf("cause window = %v, want %v", got, tt.window)
			}
		})
	}
}
