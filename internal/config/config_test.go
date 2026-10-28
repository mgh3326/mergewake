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
