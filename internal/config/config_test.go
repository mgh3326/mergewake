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
func TestReapConfig(t *testing.T) {
	for _, tt := range []struct {
		name, extra string
		fail        bool
	}{
		{"defaults", "", false},
		{"enabled", "reap:\n  enabled: true\n", false},
		{"act", "reap:\n  enabled: true\n  act: true\n", false},
		{"act no enable", "reap:\n  act: true\n", true},
		{"bad after", "reap:\n  worktree_after: 0s\n", true},
		{"bad glob", "reap:\n  protect: ['..']\n", true},
		{"regenerable", "reap:\n  regenerable: ['.venv', 'vendor/**']\n", false},
		{"bad regenerable", "reap:\n  regenerable: ['..']\n", true},
		{"local host", "hosts:\n  - {name: desk}\n", false},
		{"ssh host", "hosts:\n  - {name: node, ssh: ops@node.example}\n", false},
		{"local ssh", "hosts:\n  - {name: node, ssh: local}\n", false},
		{"dup host", "hosts:\n  - {name: node}\n  - {name: node}\n", true},
		{"bad host name", "hosts:\n  - {name: 'bad name'}\n", true},
		{"ssh option", "hosts:\n  - {name: node, ssh: '-oProxy=x'}\n", true},
		{"ssh space", "hosts:\n  - {name: node, ssh: 'ops@node -p 22'}\n", true},
		{"local jobs", "hosts:\n  - {name: desk, jobs: '~/jobs'}\n", false},
		{"local jobs rel", "hosts:\n  - {name: desk, jobs: 'jobs'}\n", true},
		{"ssh jobs abs", "hosts:\n  - {name: node, ssh: ops@node.example, jobs: /home/ops/jobs}\n", false},
		{"ssh jobs home", "hosts:\n  - {name: node, ssh: ops@node.example, jobs: '~/jobs'}\n", true},
		{"ssh jobs rel", "hosts:\n  - {name: node, ssh: ops@node.example, jobs: 'jobs'}\n", true},
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
			if e == nil {
				if c.Reap.WorktreeAfter != "168h" {
					t.Fatal(c.Reap.WorktreeAfter)
				}
				if len(c.Reap.Protect) == 0 {
					t.Fatal("missing default protect list")
				}
				if len(c.Reap.Regenerable) == 0 {
					t.Fatal("missing default regenerable list")
				}
				if tt.name == "regenerable" && c.Reap.Regenerable[1] != "vendor/**" {
					t.Fatal("configured regenerable list did not replace defaults")
				}
			}
		})
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

func TestExampleContainerConfig(t *testing.T) {
	c, e := Load("../../examples/container.yaml")
	if e != nil {
		t.Fatal(e)
	}
	spec := c.Repos[0].Install[0]
	if spec.Kind != "container" || spec.Image != "registry.example/api" || spec.Name != "fleet-api" {
		t.Fatalf("container example %+v", spec)
	}
	if len(spec.Ports) != 1 || spec.Ports[0] != "127.0.0.1:8080:8080" || spec.Health == nil || spec.Health.Retries != 30 {
		t.Fatalf("container example ports/health %+v", spec)
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

func containerYAML(replacements ...string) string {
	text := `version: 1
host: host-a
repos:
  - name: example/project
    install:
      - kind: container
        target: api
        image: registry.example/api
        name: fleet-api
        ports: ["8080:8080"]
        env_file: /etc/fleet/api.env
        health:
          url: http://127.0.0.1:8080/healthz
`
	for i := 0; i+1 < len(replacements); i += 2 {
		text = strings.Replace(text, replacements[i], replacements[i+1], 1)
	}
	return text
}

func TestContainerConfigValidation(t *testing.T) {
	const health = "        health:\n          url: http://127.0.0.1:8080/healthz\n"
	for _, tt := range []struct {
		name string
		text string
		fail bool
	}{
		{"minimal", containerYAML(), false},
		{"image tag", containerYAML("        image: registry.example/api\n", "        image: registry.example/api:latest\n"), true},
		{"missing health", containerYAML(health, ""), true},
		{"relative health", containerYAML(health, "        health: {url: /healthz}\n"), true},
		{"credential health", containerYAML(health, "        health: {url: 'http://user:pass@127.0.0.1/healthz'}\n"), true},
		{"bad port", containerYAML(`        ports: ["8080:8080"]`, "        ports: ['127.0.0.1:0:8080']"), true},
		{"hostname port", containerYAML(`        ports: ["8080:8080"]`, "        ports: ['localhost:8080:8080']"), true},
		{"too many ports parts", containerYAML(`        ports: ["8080:8080"]`, "        ports: ['1:2:3:4']"), true},
		{"bad keep", containerYAML("        env_file: /etc/fleet/api.env\n", "        env_file: /etc/fleet/api.env\n        keep: 101\n"), true},
		{"bad runtime", containerYAML("        env_file: /etc/fleet/api.env\n", "        env_file: /etc/fleet/api.env\n        runtime: lxc\n"), true},
		{"bad restart", containerYAML("        env_file: /etc/fleet/api.env\n", "        env_file: /etc/fleet/api.env\n        restart: sometimes\n"), true},
		{"relative env", containerYAML("        env_file: /etc/fleet/api.env\n", "        env_file: fleet.env\n"), true},
		{"bad digest", containerYAML("        env_file: /etc/fleet/api.env\n", "        env_file: /etc/fleet/api.env\n        digest: sha256:short\n"), true},
		{"bad volume", containerYAML("        env_file: /etc/fleet/api.env\n", "        env_file: /etc/fleet/api.env\n        volumes: ['data']\n"), true},
		{"missing name", containerYAML("        name: fleet-api\n", ""), true},
		{"mixed inventory", containerYAML() + "      - source: tool\n        target: bin/tool\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if e := os.WriteFile(path, []byte(tt.text), 0600); e != nil {
				t.Fatal(e)
			}
			c, e := Load(path)
			if (e != nil) != tt.fail {
				t.Fatalf("%+v %v", c, e)
			}
		})
	}
}

func TestContainerConfigDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(path, []byte(containerYAML()), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := Load(path)
	if e != nil {
		t.Fatal(e)
	}
	spec := c.Repos[0].Install[0]
	if spec.Restart != "unless-stopped" || spec.Runtime != "docker" || spec.Keep == nil || *spec.Keep != 2 {
		t.Fatalf("defaults %+v", spec)
	}
	if spec.Health.Status != 200 || spec.Health.Timeout != "5s" || spec.Health.Retries != 20 {
		t.Fatalf("health defaults %+v", spec.Health)
	}
	if len(spec.Ports) != 1 || spec.Ports[0] != "127.0.0.1:8080:8080" {
		t.Fatalf("ports %v", spec.Ports)
	}
}
