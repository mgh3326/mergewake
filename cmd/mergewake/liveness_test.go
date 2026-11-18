package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLivenessCLI_FlagsAndAck(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := "version: 1\nhost: test-host\nstate_dir: " + dir + "\nrepos:\n  - name: test/repo\n    branch: main\n"
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0600); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Test help
	var out bytes.Buffer
	if err := run(ctx, []string{"liveness", "--help"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "-mechanism") {
		t.Fatal("expected -mechanism in liveness help")
	}

	// Test ack without reason fails
	out.Reset()
	if err := run(ctx, []string{"liveness", "ack", "merge", "--config", cfgPath}, &out, &out); err == nil {
		t.Fatal("expected error without --reason")
	}

	// Test ack with reason succeeds
	out.Reset()
	if err := run(ctx, []string{"liveness", "ack", "merge", "--reason", "fixing gate", "--config", cfgPath}, &out, &out); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	if !strings.Contains(out.String(), "liveness-ack") || !strings.Contains(out.String(), "fixing gate") {
		t.Fatalf("unexpected ack output: %s", out.String())
	}

	// Test print liveness
	out.Reset()
	if err := run(ctx, []string{"liveness", "--day", "2026-11-15", "--config", cfgPath}, &out, &out); err != nil {
		t.Fatalf("liveness print failed: %v", err)
	}
	if !strings.Contains(out.String(), "2026-11-15") || !strings.Contains(out.String(), "review-pass") {
		t.Fatalf("unexpected liveness output: %s", out.String())
	}
}
