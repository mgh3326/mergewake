package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Config validation refuses !x, /x, a/../b and more than 64 patterns.
func TestSparseCheckoutConfigValidation(t *testing.T) {
	for _, p := range []string{"!x", "/x", "a/../b", "a\x00b", "a\nb", ""} {
		if e := ValidateCheckoutExcludePattern(p); e == nil {
			t.Fatalf("pattern %q accepted", p)
		}
	}
	if e := ValidateCheckoutExcludePattern("Assets"); e != nil {
		t.Fatalf("valid pattern refused: %v", e)
	}
	p := filepath.Join(t.TempDir(), "config.yaml")
	many := "version: 1\nhost: host-a\nrepos:\n  - name: example/project\n    review:\n      checkout_exclude:\n"
	for i := 0; i < MaxCheckoutExcludes+1; i++ {
		many += "        - dir\n"
	}
	if e := os.WriteFile(p, []byte(many), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Load(p); e == nil || !strings.Contains(e.Error(), "checkout_exclude") {
		t.Fatalf("Load = %v, want checkout_exclude bound", e)
	}
	bad := "version: 1\nhost: host-a\nrepos:\n  - name: example/project\n    review:\n      checkout_exclude:\n        - '!x'\n"
	if e := os.WriteFile(p, []byte(bad), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Load(p); e == nil || !strings.Contains(e.Error(), "checkout_exclude") {
		t.Fatalf("Load = %v, want checkout_exclude refusal", e)
	}
	ok := "version: 1\nhost: host-a\nrepos:\n  - name: example/project\n    review:\n      checkout_exclude:\n        - Assets\n"
	if e := os.WriteFile(p, []byte(ok), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := Load(p)
	if e != nil {
		t.Fatal(e)
	}
	if len(c.Repos[0].Review.CheckoutExclude) != 1 || c.Repos[0].Review.CheckoutExclude[0] != "Assets" {
		t.Fatalf("exclude = %v", c.Repos[0].Review.CheckoutExclude)
	}
}
