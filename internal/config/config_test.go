package config

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
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
		{"bad scan", "reap:\n  scan_after: 0s\n", true},
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
		{"hub transport", "reap:\n  hub: {url: 'https://hub.example.invalid', token_file: /etc/op.env}\nhosts:\n  - {name: node, machine: node-a}\n", false},
		{"explicit ssh with hub", "reap:\n  transport: ssh\n  hub: {url: 'https://hub.example.invalid', token_file: /etc/op.env}\n", false},
		{"bad transport", "reap:\n  transport: telnet\n", true},
		{"panewire no url", "reap:\n  transport: panewire\n", true},
		{"panewire no token", "reap:\n  hub: {url: 'https://hub.example.invalid'}\n", true},
		{"hub plain http", "reap:\n  hub: {url: 'http://hub.example.invalid', token_file: /etc/op.env}\n", true},
		{"hub loopback http", "reap:\n  hub: {url: 'http://127.0.0.1:8080', token_file: /etc/op.env}\n", false},
		{"hub url path", "reap:\n  hub: {url: 'https://hub.example.invalid/v1', token_file: /etc/op.env}\n", true},
		{"hub url userinfo", "reap:\n  hub: {url: 'https://u:p@hub.example.invalid', token_file: /etc/op.env}\n", true},
		{"hub token rel", "reap:\n  hub: {url: 'https://hub.example.invalid', token_file: op.env}\n", true},
		{"machine needs panewire", "hosts:\n  - {name: node, machine: node-a}\n", true},
		{"bad machine", "reap:\n  hub: {url: 'https://hub.example.invalid', token_file: /etc/op.env}\nhosts:\n  - {name: node, machine: 'Node A'}\n", true},
		{"archive off", "reap:\n  archive: {drive_dir: rel}\n", false},
		{"archive local", "reap:\n  archive: {enabled: true, drive_dir: '~/Drive/jobs', min_age: 14d}\n", false},
		{"archive ssh", "reap:\n  archive: {enabled: true, archive_host: ops@mac.example, drive_dir: /Users/ops/Drive/jobs, text_max_bytes: 4096}\n", false},
		{"archive no dir", "reap:\n  archive: {enabled: true}\n", true},
		{"archive ssh home", "reap:\n  archive: {enabled: true, archive_host: ops@mac.example, drive_dir: '~/Drive'}\n", true},
		{"archive root", "reap:\n  archive: {enabled: true, drive_dir: /}\n", true},
		{"archive ssh option", "reap:\n  archive: {enabled: true, archive_host: '-oProxy=x', drive_dir: /d}\n", true},
		{"archive bad age", "reap:\n  archive: {min_age: 0d}\n", true},
		{"archive text cap", "reap:\n  archive: {text_max_bytes: 4194304}\n", true},
		{"archive unknown key", "reap:\n  archive: {drive: /d}\n", true},
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
				if c.Reap.ScanAfter != "720h" {
					t.Fatal(c.Reap.ScanAfter)
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
				want := ReapTransportSSH
				if tt.name == "hub transport" || tt.name == "hub loopback http" {
					want = ReapTransportPanewire
				}
				if c.Reap.Transport != want {
					t.Fatalf("transport %q, want %q", c.Reap.Transport, want)
				}
				if a := c.Reap.Archive; a.TextMaxBytes == 0 || a.MinAge == "" {
					t.Fatalf("archive defaults missing: %+v", a)
				}
				if tt.name == "archive local" && !filepath.IsAbs(c.Reap.Archive.DriveDir) {
					t.Fatal("local drive_dir did not expand")
				}
			}
		})
	}
}

// The hub operator credential is refused when the config loads, so it cannot
// come back to the daemon host through a config edit. A token file that does
// not exist yet is left to the per-pass read.
func TestReapHubRefusesOperatorCredential(t *testing.T) {
	dir := t.TempDir()
	for _, tt := range []struct {
		name, token, want string
	}{
		{"service", "HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=tok\n", ""},
		{"operator", "HUB_MACHINE_ID=operator\nHUB_TOKEN=tok\n", "hub operator credential"},
		{"no machine id", "HUB_TOKEN=tok\n", "no HUB_MACHINE_ID"},
		{"missing", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token := filepath.Join(dir, strings.ReplaceAll(tt.name, " ", "-")+".env")
			if tt.token != "" {
				if e := os.WriteFile(token, []byte(tt.token), 0o600); e != nil {
					t.Fatal(e)
				}
			}
			p := filepath.Join(t.TempDir(), "config.yaml")
			body := "version: 1\nhost: host-a\nreap:\n  hub: {url: 'https://hub.example.invalid', token_file: '" + token + "'}\n"
			if e := os.WriteFile(p, []byte(body), 0o600); e != nil {
				t.Fatal(e)
			}
			_, e := Load(p)
			if tt.want == "" && e != nil || tt.want != "" && (e == nil || !strings.Contains(e.Error(), "reap.hub.token_file") || !strings.Contains(e.Error(), tt.want)) {
				t.Fatalf("err=%v, want %q", e, tt.want)
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
		{"daemon:\n  pr: {enabled: true, notify: {enabled: false}}\n", false},
		{"daemon:\n  watch: {enabled: true, notify: {enabled: false}}\n", true},
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

func TestPRNotifyDefaults(t *testing.T) {
	var n PRNotify
	if !n.On() {
		t.Fatal("absent notify defaults to enabled")
	}
	off := false
	n.Enabled = &off
	if n.On() {
		t.Fatal("notify.enabled: false ignored")
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
func TestCanonicalFamily(t *testing.T) {
	for in, want := range map[string]string{
		"claude": "anthropic", "sonnet": "anthropic", "opus": "anthropic", "fable": "anthropic", "haiku": "anthropic", "kiro": "anthropic",
		"codex": "openai", "sol": "openai", "gpt": "openai",
		"devin": "cognition", "swe": "cognition",
		"grok":   "xai",
		"gemini": "google", "agy": "google",
		"kimi":       "moonshot",
		" Claude ":   "anthropic",
		"ANTHROPIC":  "anthropic",
		"openai":     "openai",
		"deepseek":   "deepseek",
		"some-other": "some-other",
	} {
		if got := CanonicalFamily(in); got != want {
			t.Errorf("CanonicalFamily(%q) = %q, want %q", in, got, want)
		}
	}
}
func TestFamilyAliasNormalisation(t *testing.T) {
	yaml := `version: 1
host: host-a
review:
  profiles:
    sonnet: {family: sonnet, command: pi, args: [--model, m1]}
    gpt: {family: codex, command: pi, args: [--model, m2]}
  pairings:
    claude: [gpt]
`
	p := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(p, []byte(yaml), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := Load(p)
	if e != nil {
		t.Fatal(e)
	}
	if c.Review.Profiles["sonnet"].Family != "anthropic" || c.Review.Profiles["gpt"].Family != "openai" {
		t.Fatalf("profiles %+v", c.Review.Profiles)
	}
	if len(c.Review.Pairings["anthropic"]) != 1 || c.Review.Pairings["anthropic"][0] != "gpt" {
		t.Fatalf("pairings %+v", c.Review.Pairings)
	}

	yaml = `version: 1
host: host-a
review:
  profiles:
    gpt: {family: openai, command: pi, args: [--model, m2]}
  pairings:
    claude: [gpt]
    anthropic: [gpt]
`
	p = filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(p, []byte(yaml), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = Load(p); e == nil {
		t.Fatal("colliding canonical pairings accepted")
	}
}

// fallback_lane defaults to the operator lane; default_family must name a
// configured profile family and normalises like every other family key.
func TestFallbackLaneAndDefaultFamily(t *testing.T) {
	base := "version: 1\nhost: host-a\nnotifier:\n  type: panewire\n  lane: operator-desk\n"
	load := func(extra string) (Config, error) {
		p := filepath.Join(t.TempDir(), "config.yaml")
		if e := os.WriteFile(p, []byte(base+extra), 0600); e != nil {
			t.Fatal(e)
		}
		return Load(p)
	}
	c, e := load("")
	if e != nil || c.Notifier.FallbackLane != "operator-desk" {
		t.Fatalf("%+v %v", c.Notifier, e)
	}
	p := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(p, []byte("version: 1\nhost: host-a\nnotifier:\n  type: panewire\n  lane: operator-desk\n  fallback_lane: desk-2\n"), 0600); e != nil {
		t.Fatal(e)
	}
	c, e = Load(p)
	if e != nil || c.Notifier.FallbackLane != "desk-2" {
		t.Fatalf("%+v %v", c.Notifier, e)
	}

	review := "review:\n  default_family: %s\n  profiles:\n    rev:\n      family: anthropic\n      command: pi\n      args: [--model, m]\n"
	c, e = load(fmt.Sprintf(review, "claude"))
	if e != nil || c.Review.DefaultFamily != "anthropic" {
		t.Fatalf("%q %v", c.Review.DefaultFamily, e)
	}
	if _, e = load(fmt.Sprintf(review, "openai")); e == nil {
		t.Fatal("default_family outside the configured profiles accepted")
	}
}

func TestReviewExecutor(t *testing.T) {
	load := func(extra string) (Config, error) {
		p := filepath.Join(t.TempDir(), "config.yaml")
		if e := os.WriteFile(p, []byte("version: 1\nhost: host-a\nstate_dir: /state\n"+extra), 0600); e != nil {
			t.Fatal(e)
		}
		return Load(p)
	}
	c, e := load("")
	rt := c.Review.Runtug
	if e != nil || c.Review.Executor != "headless" || rt.Command != "runtug" || rt.Workspace != "reviews" || rt.Host != "auto" || rt.Checkouts != "/state/review-checkouts" || !filepath.IsAbs(rt.Jobs) {
		t.Fatalf("%+v %v", c.Review, e)
	}
	c, e = load("review:\n  executor: runtug\n  runtug: {owner: review-desk, host: host-b, checkouts: /work/reviews}\n")
	if e != nil || c.Review.Executor != "runtug" || c.Review.Runtug.Owner != "review-desk" || c.Review.Runtug.Host != "host-b" || c.Review.Runtug.Checkouts != "/work/reviews" {
		t.Fatalf("%+v %v", c.Review, e)
	}
	if rt := c.Review.Runtug; rt.Lane != "review-desk" || rt.Panewire != "panewire" || rt.ExcludeHosts != nil {
		t.Fatalf("lane defaults to owner: %+v", rt)
	}
	c, e = load("review:\n  runtug: {owner: review-desk, lane: findings-desk, exclude_hosts: [host-a, host-b]}\n")
	if rt := c.Review.Runtug; e != nil || rt.Lane != "findings-desk" || !slices.Equal(rt.ExcludeHosts, []string{"host-a", "host-b"}) {
		t.Fatalf("%+v %v", rt, e)
	}
	for _, bad := range []string{"review:\n  executor: pane\n", "review:\n  runtug: {bogus: x}\n", "review:\n  runtug: {lane: 'a b'}\n", "review:\n  runtug: {exclude_hosts: ['a,b']}\n", "review:\n  runtug: {host: host-a, exclude_hosts: [host-a]}\n"} {
		if _, e = load(bad); e == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestAge(t *testing.T) {
	for in, want := range map[string]time.Duration{"14d": 14 * 24 * time.Hour, "36h": 36 * time.Hour, "1d": 24 * time.Hour} {
		if got, e := Age(in); e != nil || got != want {
			t.Errorf("Age(%q) = %v, %v", in, got, e)
		}
	}
	for _, in := range []string{"", "0d", "-1d", "d", "1.5d", "0s", "10000000000d", "106752d"} {
		if _, e := Age(in); e == nil {
			t.Errorf("Age(%q) accepted", in)
		}
	}
}

func TestWebhookConfig(t *testing.T) {
	load := func(extra string) (Config, error) {
		p := filepath.Join(t.TempDir(), "config.yaml")
		if e := os.WriteFile(p, []byte("version: 1\nhost: host-a\n"+extra), 0600); e != nil {
			t.Fatal(e)
		}
		return Load(p)
	}
	// Disabled by default: a webhook block only validates when enabled.
	c, e := load("daemon:\n  webhook: {enabled: false, listen: '0.0.0.0:9'}\n")
	if e != nil || c.Daemon.Webhook.Enabled {
		t.Fatalf("disabled webhook: %+v %v", c.Daemon.Webhook, e)
	}
	t.Setenv("MW_TEST_WH_SECRET", "x")
	c, e = load("daemon:\n  webhook: {enabled: true, listen: '127.0.0.1:9000', secret_env: MW_TEST_WH_SECRET}\n")
	if e != nil || c.Daemon.Webhook.Path != "/github" {
		t.Fatalf("loopback webhook: %+v %v", c.Daemon.Webhook, e)
	}
	for _, listen := range []string{"localhost:9000", "127.0.0.2:9000", "[::1]:9000"} {
		if c, e = load("daemon:\n  webhook: {enabled: true, listen: '" + listen + "', secret_env: MW_TEST_WH_SECRET}\n"); e != nil {
			t.Fatalf("listen %s: %v", listen, e)
		}
	}
	if c.Daemon.Webhook.Listen != "[::1]:9000" {
		t.Fatalf("loopback literal rewritten: %s", c.Daemon.Webhook.Listen)
	}
	c, e = load("daemon:\n  webhook: {enabled: true, listen: 'localhost:9000', secret_env: MW_TEST_WH_SECRET}\n")
	// localhost normalises to a literal bind; resolution can never reroute it.
	if e != nil || c.Daemon.Webhook.Listen != "127.0.0.1:9000" {
		t.Fatalf("localhost normalisation: %+v %v", c.Daemon.Webhook, e)
	}
	for _, listen := range []string{"0.0.0.0:9000", ":9000", "10.0.0.1:9000", "example.invalid:9000", "127.0.0.1", "127.0.0.1:notaport", "127.0.0.1:0"} {
		if _, e = load("daemon:\n  webhook: {enabled: true, listen: '" + listen + "', secret_env: MW_TEST_WH_SECRET}\n"); e == nil {
			t.Fatalf("accepted listen %s", listen)
		}
	}
	// The refusal names the key, never the listen value or the env var name.
	_, e = load("daemon:\n  webhook: {enabled: true, listen: '203.0.113.9:9000', secret_env: MW_TEST_WH_SECRET}\n")
	if e == nil || !strings.Contains(e.Error(), "daemon.webhook.listen") || strings.Contains(e.Error(), "203.0.113.9") {
		t.Fatalf("listen error: %v", e)
	}
	_, e = load("daemon:\n  webhook: {enabled: true, listen: '127.0.0.1:9000'}\n")
	if e == nil || !strings.Contains(e.Error(), "daemon.webhook") {
		t.Fatalf("missing secret_env: %v", e)
	}
	_, e = load("daemon:\n  webhook: {enabled: true, listen: '127.0.0.1:9000', secret_env: MW_TEST_UNSET_MARKER}\n")
	if e == nil || !strings.Contains(e.Error(), "daemon.webhook.secret_env") || strings.Contains(e.Error(), "MW_TEST_UNSET_MARKER") {
		t.Fatalf("unset secret error: %v", e)
	}
	if _, e = load("daemon:\n  webhook: {enabled: true, listen: '127.0.0.1:9000', path: '../escape', secret_env: MW_TEST_WH_SECRET}\n"); e == nil {
		t.Fatal("accepted non-absolute path")
	}
}
