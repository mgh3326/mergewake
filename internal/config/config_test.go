package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStrictConfig(t *testing.T) {
	for _, tt := range []struct {
		name, extra string
		fail        bool
	}{
		{"defaults", "", false}, {"unknown key", "typo: true\n", true}, {"bad repo", "repos:\n  - name: ../escape\n", true}, {"empty interval", "poll_interval: 0s\n", true}, {"second doc", "---\nversion: 1\n", true}, {"bad adapter", "notifier:\n  type: unknown\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if e := os.WriteFile(p, []byte("version: 1\nhost: host-a\n"+tt.extra), 0600); e != nil {
				t.Fatal(e)
			}
			c, e := Load(p)
			if (e != nil) != tt.fail {
				t.Fatalf("%+v %v", c, e)
			}
		})
	}
}
func TestReviewerArgs(t *testing.T) {
	for _, args := range [][]string{{"--continue"}, {"--tools", "read"}, {"--model", "x", "--resume", "old"}, {"--model"}} {
		if ValidateReviewArgs(args) == nil {
			t.Fatal(args)
		}
	}
	if e := ValidateReviewArgs([]string{"--model", "example-model", "--provider", "anthropic"}); e != nil {
		t.Fatal(e)
	}
}
func TestRepoAndHome(t *testing.T) {
	for _, r := range []string{"../x", "a/..", "a/b/c", "a/b?query"} {
		if ValidRepo(r) {
			t.Fatal(r)
		}
	}
	if !ValidRepo("example/project") {
		t.Fatal("valid repo rejected")
	}
	h, _ := os.UserHomeDir()
	if Expand("~") != h || !strings.HasPrefix(Expand("~/state"), h) {
		t.Fatal("home expansion")
	}
}

func TestExampleConfig(t *testing.T) {
	c, e := Load("../../examples/host.yaml")
	if e != nil {
		t.Fatal(e)
	}
	if c.Host != "host-a" || len(c.Repos) != 1 {
		t.Fatal(c)
	}
}

func TestCommandAndDaemonValidation(t *testing.T) {
	for _, tc := range []struct {
		extra string
		fail  bool
	}{
		{"daemon:\n  pr: {enabled: true, interval: 0s}\n", true},
		{"repos:\n  - name: example/project\n    install:\n      - kind: command\n        target: tools/example\n        command: {command: installer, args: ['{sha}']}\n        verify: {command: verify}\n", false},
		{"repos:\n  - name: example/project\n    install:\n      - kind: command\n        target: tools/example\n        command: {command: installer, args: [main]}\n        verify: {command: verify}\n", true},
		{"repos:\n  - name: example/project\n    install:\n      - kind: release\n        target: bin/mergewake\n        verify: {command: verify}\n        canary: host-a\n", true},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		os.WriteFile(path, []byte("version: 1\nhost: host-a\n"+tc.extra), 0600)
		_, err := Load(path)
		if (err != nil) != tc.fail {
			t.Fatalf("config %s err=%v", tc.extra, err)
		}
	}
}
