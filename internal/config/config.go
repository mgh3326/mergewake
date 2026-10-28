// Package config reads strict, operator-owned YAML. PRs never supply host config.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/policy"
	"gopkg.in/yaml.v3"
)

type Command struct {
	Command string   `yaml:"command" json:"command"`
	Args    []string `yaml:"args" json:"args"`
}
type Reviewer struct {
	Family      string   `yaml:"family"`
	Command     string   `yaml:"command"`
	Args        []string `yaml:"args"`
	GateProfile string   `yaml:"gate_profile"`
}
type Review struct {
	Login     string              `yaml:"login"`
	Pairings  map[string][]string `yaml:"pairings"`
	Profiles  map[string]Reviewer `yaml:"profiles"`
	Scopefuel string              `yaml:"scopefuel"`
	Timeout   string              `yaml:"timeout"`
}
type Notifier struct {
	Type    string `yaml:"type"`
	Command string `yaml:"command"`
	Lane    string `yaml:"lane"`
}
type Sink struct {
	Type        string `yaml:"type"`
	Command     string `yaml:"command"`
	ReopenState string `yaml:"reopen_state"`
}
type Install struct {
	Kind         string   `yaml:"kind" json:"kind"`
	Command      Command  `yaml:"command" json:"command"`
	Verify       Command  `yaml:"verify" json:"verify"`
	VerifyOutput string   `yaml:"verify_output" json:"verify_output"`
	InitialSHA   string   `yaml:"initial_sha" json:"initial_sha"`
	Timeout      string   `yaml:"timeout" json:"timeout"`
	Serial       []string `yaml:"serial" json:"serial"`
	Canary       string   `yaml:"canary" json:"canary"`
	ReceiptsDir  string   `yaml:"receipts_dir" json:"receipts_dir"`
	Source       string   `yaml:"source"`
	Target       string   `yaml:"target"`
	Mode         string   `yaml:"mode"`
}
type Repo struct {
	Name         string    `yaml:"name"`
	Root         string    `yaml:"root"`
	Branch       string    `yaml:"branch"`
	Policy       string    `yaml:"policy"`
	Deny         []string  `yaml:"deny"`
	Install      []Install `yaml:"install"`
	Smoke        Command   `yaml:"smoke"`
	SmokeTimeout string    `yaml:"smoke_timeout"`
	Task         string    `yaml:"task"`
}
type Loop struct {
	Enabled  bool   `yaml:"enabled"`
	Interval string `yaml:"interval"`
}
type Daemon struct {
	PR      Loop `yaml:"pr"`
	Watch   Loop `yaml:"watch"`
	Release Loop `yaml:"release"`
	Rollout Loop `yaml:"rollout"`
	Act     bool `yaml:"act"`
}
type Config struct {
	Version      int      `yaml:"version"`
	Daemon       Daemon   `yaml:"daemon"`
	Host         string   `yaml:"host"`
	StateDir     string   `yaml:"state_dir"`
	TokenEnv     string   `yaml:"token_env"`
	PollInterval string   `yaml:"poll_interval"`
	WatchLimit   int      `yaml:"watch_limit"`
	Repos        []Repo   `yaml:"repos"`
	Review       Review   `yaml:"review"`
	Notifier     Notifier `yaml:"notifier"`
	Sink         Sink     `yaml:"sink"`
}

func Decode(r io.Reader, v any) error {
	d := yaml.NewDecoder(r)
	d.KnownFields(true)
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("expected one YAML document")
	}
	return nil
}
func Load(name string) (Config, error) {
	var c Config
	f, e := os.Open(name)
	if e != nil {
		return c, e
	}
	defer f.Close()
	if e = Decode(f, &c); e != nil {
		return c, e
	}
	if c.Version != 1 || c.Host == "" {
		return c, errors.New("config requires version: 1 and host")
	}
	if c.StateDir == "" {
		c.StateDir = "~/.local/state/mergewake"
	}
	c.StateDir = Expand(c.StateDir)
	if c.TokenEnv == "" {
		c.TokenEnv = "GITHUB_TOKEN"
	}
	if c.PollInterval == "" {
		c.PollInterval = "60s"
	}
	if c.WatchLimit == 0 {
		c.WatchLimit = 100
	}
	if c.WatchLimit < 2 || c.WatchLimit > 1000 {
		return c, errors.New("watch_limit must be 2..1000")
	}
	if _, e = Duration(c.PollInterval); e != nil {
		return c, e
	}
	for _, loop := range []*Loop{&c.Daemon.PR, &c.Daemon.Watch, &c.Daemon.Release, &c.Daemon.Rollout} {
		if loop.Interval == "" {
			loop.Interval = c.PollInterval
		}
		if _, err := Duration(loop.Interval); err != nil {
			return c, err
		}
	}
	if c.Review.Timeout == "" {
		c.Review.Timeout = "10m"
	}
	if _, e = Duration(c.Review.Timeout); e != nil {
		return c, e
	}
	seen := map[string]bool{}
	for i := range c.Repos {
		r := &c.Repos[i]
		if !ValidRepo(r.Name) || seen[r.Name] {
			return c, errors.New("repos require unique owner/name values")
		}
		seen[r.Name] = true
		if r.Branch == "" {
			r.Branch = "main"
		}
		if r.Policy == "" {
			r.Policy = ".mergewake/policy.yaml"
		}
		if r.SmokeTimeout == "" {
			r.SmokeTimeout = "30s"
		}
		if _, e = Duration(r.SmokeTimeout); e != nil {
			return c, e
		}
		for j := range r.Install {
			spec := &r.Install[j]
			if spec.Kind == "" {
				spec.Kind = "file"
			}
			switch spec.Kind {
			case "file":
			case "command", "release":
				if spec.Verify.Command == "" {
					return c, errors.New("command/release installs require verify")
				}
				if spec.Kind == "command" && (spec.Command.Command == "" || !strings.Contains(strings.Join(spec.Command.Args, " "), "{sha}")) {
					return c, errors.New("command install requires a pinned {sha} argument")
				}
				if spec.VerifyOutput == "" {
					spec.VerifyOutput = "{sha}"
				}
				if !strings.Contains(spec.VerifyOutput, "{sha}") {
					return c, errors.New("verify_output must include {sha}")
				}
				if spec.InitialSHA != "" && !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(spec.InitialSHA) {
					return c, errors.New("initial_sha requires a full SHA")
				}
				if spec.Timeout == "" {
					spec.Timeout = "5m"
				}
				if _, err := Duration(spec.Timeout); err != nil {
					return c, err
				}
				if len(spec.Serial) > 0 || spec.Canary != "" {
					if spec.ReceiptsDir == "" {
						return c, errors.New("serial/canary requires receipts_dir")
					}
					names := append(append([]string{}, spec.Serial...), spec.Canary)
					for _, host := range names {
						if host != "" && (!regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`).MatchString(host)) {
							return c, errors.New("invalid serial/canary host label")
						}
					}
					seenHosts := map[string]bool{}
					for _, host := range spec.Serial {
						if seenHosts[host] {
							return c, errors.New("duplicate serial host")
						}
						seenHosts[host] = true
					}
				}
			default:
				return c, errors.New("install kind must be file, command or release")
			}
		}
		for _, g := range r.Deny {
			if _, e = policy.Match(g, "probe"); e != nil {
				return c, e
			}
		}
	}
	switch c.Notifier.Type {
	case "", "noop", "stdout", "panewire":
	default:
		return c, errors.New("unknown notifier type")
	}
	switch c.Sink.Type {
	case "", "noop", "handoffkeep":
	default:
		return c, errors.New("unknown sink type")
	}
	if c.Notifier.Type == "panewire" && c.Notifier.Lane == "" {
		return c, errors.New("panewire notifier requires lane")
	}
	if c.Sink.ReopenState == "" {
		c.Sink.ReopenState = "reopened"
	}
	if c.Sink.ReopenState != "reopened" {
		return c, errors.New("sink.reopen_state must be reopened")
	}
	for name, p := range c.Review.Profiles {
		if name == "" || p.Family == "" || (filepath.Base(p.Command) != "pi" && filepath.Base(p.Command) != "cmd") {
			return c, errors.New("review profiles require a name, family, and pi or cmd command")
		}
		if e := ValidateReviewArgs(p.Args); e != nil {
			return c, e
		}
	}
	for family, names := range c.Review.Pairings {
		if family == "" || len(names) == 0 {
			return c, errors.New("empty review pairing")
		}
		for _, name := range names {
			p, ok := c.Review.Profiles[name]
			if !ok || p.Family == family {
				return c, errors.New("pairings must name configured profiles from a different family")
			}
		}
	}
	return c, nil
}
func Duration(s string) (time.Duration, error) {
	d, e := time.ParseDuration(s)
	if e != nil || d <= 0 {
		return 0, fmt.Errorf("invalid positive duration %q", s)
	}
	return d, nil
}
func Expand(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		h, e := os.UserHomeDir()
		if e == nil {
			return filepath.Join(h, strings.TrimPrefix(strings.TrimPrefix(p, "~"), "/"))
		}
	}
	return p
}
func (c Config) Repo(name string) (Repo, error) {
	for _, r := range c.Repos {
		if r.Name == name {
			return r, nil
		}
	}
	return Repo{}, fmt.Errorf("repo %q is not configured", name)
}

var repoPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$`)

func ValidRepo(s string) bool {
	return repoPattern.MatchString(s) && strings.Split(s, "/")[1] != "." && strings.Split(s, "/")[1] != ".."
}

// Model selection is configurable; context/tool/session options are controlled
// by the diff-only adapter and cannot be overridden in a profile.
func ValidateReviewArgs(args []string) error {
	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) || args[i+1] == "" || strings.HasPrefix(args[i+1], "-") {
			return errors.New("review args must be model option/value pairs")
		}
		switch args[i] {
		case "--model", "--provider", "--thinking", "--effort":
		default:
			return fmt.Errorf("review option %q is not a model-selection option", args[i])
		}
	}
	return nil
}
