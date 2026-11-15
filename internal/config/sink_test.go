package config

import (
	"os"
	"path/filepath"
	"testing"
)

// sink.reconcile_every takes a positive duration or off, and refuses anything
// else so a typo cannot silently turn the check off.
func TestSinkReconcileEveryValidation(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		fail        bool
	}{
		{"unset", "", false},
		{"off", "off", false},
		{"duration", "15m", false},
		{"zero", "0s", true},
		{"garbage", "soon", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			contents := "version: 1\nhost: host-a\nsink:\n  type: handoffkeep\n"
			if tc.value != "" {
				contents += "  reconcile_every: " + tc.value + "\n"
			}
			if e := os.WriteFile(p, []byte(contents), 0600); e != nil {
				t.Fatal(e)
			}
			_, e := Load(p)
			if (e != nil) != tc.fail {
				t.Fatalf("Load error = %v, want fail = %t", e, tc.fail)
			}
		})
	}
}
