package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelpAndFlagValidation(t *testing.T) {
	for _, cmd := range []string{"land", "review", "watch", "daemon", "status", "reap", "archive-jobs"} {
		t.Run(cmd, func(t *testing.T) {
			var out bytes.Buffer
			if e := run(context.Background(), []string{cmd, "--help"}, &out, &out); e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(out.String(), "dry-run") {
				t.Fatal("missing dry-run flag")
			}
		})
	}
	// The deploy commands are gone: each now fails as an unknown command.
	for _, args := range [][]string{{"unknown"}, {"land", "--unknown"}, {"superfan", "--launch"}, {"release"}, {"rollout"}, {"agent"}, {"promote"}, {"admit"}, {"notify", "deploy"}} {
		if e := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); e == nil {
			t.Fatal(args)
		}
	}
}
func TestArchiveJobsGates(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "config.yaml")
		if e := os.WriteFile(p, []byte("version: 1\nhost: host-a\nstate_dir: "+dir+"\n"+body), 0o600); e != nil {
			t.Fatal(e)
		}
		return p
	}
	for name, tc := range map[string]struct{ cfg, flag, want string }{
		"disabled":  {"", "14d", "requires reap.archive.enabled"},
		"bad age":   {"reap:\n  archive: {enabled: true, drive_dir: /d}\n", "14x", "invalid positive duration"},
		"no host":   {"reap:\n  archive: {enabled: true, drive_dir: /d}\n", "14d", "no configured host"},
		"extra arg": {"", "14d", "unexpected positional"},
	} {
		args := []string{"archive-jobs", "--config", write(tc.cfg), "--older-than", tc.flag, "--host", "h1"}
		if name == "extra arg" {
			args = append(args, "now")
		}
		e := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{})
		if e == nil || !strings.Contains(e.Error(), tc.want) {
			t.Errorf("%s: %v", name, e)
		}
	}
}
