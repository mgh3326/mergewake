package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// fakeBin places a single executable script as the only thing on PATH.
func fakeBin(t *testing.T, name, script string) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if e := os.WriteFile(p, []byte(script), 0o755); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir)
}

func TestResolveTokenPrefersEnv(t *testing.T) {
	fakeBin(t, "gh", "#!/bin/sh\nexit 1\n")
	t.Setenv("MERGEWAKE_TEST_TOKEN", "env-token")
	tok, e := resolveToken(context.Background(), integrations.Exec{}, "MERGEWAKE_TEST_TOKEN")
	if e != nil || tok != "env-token" {
		t.Fatalf("tok=%q e=%v", tok, e)
	}
}

func TestResolveTokenFallsBackToGH(t *testing.T) {
	fakeBin(t, "gh", "#!/bin/sh\necho gh-token\n")
	t.Setenv("MERGEWAKE_TEST_TOKEN", "")
	tok, e := resolveToken(context.Background(), integrations.Exec{}, "MERGEWAKE_TEST_TOKEN")
	if e != nil || tok != "gh-token" {
		t.Fatalf("tok=%q e=%v", tok, e)
	}
}

func TestResolveTokenFailsClosed(t *testing.T) {
	fakeBin(t, "not-gh", "#!/bin/sh\nexit 0\n")
	t.Setenv("MERGEWAKE_TEST_TOKEN", "")
	if _, e := resolveToken(context.Background(), integrations.Exec{}, "MERGEWAKE_TEST_TOKEN"); e == nil ||
		!strings.Contains(e.Error(), "MERGEWAKE_TEST_TOKEN") {
		t.Fatalf("e=%v", e)
	}

	fakeBin(t, "gh", "#!/bin/sh\necho no token >&2\nexit 1\n")
	e := error(nil)
	_, e = resolveToken(context.Background(), integrations.Exec{}, "MERGEWAKE_TEST_TOKEN")
	if e == nil || !strings.Contains(e.Error(), "MERGEWAKE_TEST_TOKEN") || strings.Contains(e.Error(), "no token") {
		t.Fatalf("e=%v", e)
	}
}
