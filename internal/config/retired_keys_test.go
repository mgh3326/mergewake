package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The six keys whose readers were removed are refused at load, and the error
// names the key.
func TestRetiredConfigKeysAreRefused(t *testing.T) {
	for _, tc := range []struct{ key, extra string }{
		{"scopefuel", "review:\n  scopefuel: /y\n"},
		{"quota", "review:\n  quota: /x/quota\n"},
		{"select", "review:\n  select: pace\n"},
		{"renotify_after", "daemon:\n  pr: {enabled: true, notify: {renotify_after: 1h}}\n"},
		{"renotify_max", "daemon:\n  pr: {enabled: true, notify: {renotify_max: 5}}\n"},
		{"stall_after", "daemon:\n  pr: {enabled: true, stall_after: 30m}\n"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if e := os.WriteFile(path, []byte("version: 1\nhost: host-a\n"+tc.extra), 0600); e != nil {
				t.Fatal(e)
			}
			_, e := Load(path)
			if e == nil {
				t.Fatalf("config setting %s was accepted", tc.key)
			}
			if !strings.Contains(e.Error(), tc.key) {
				t.Fatalf("error %q does not name %s", e, tc.key)
			}
		})
	}
}
