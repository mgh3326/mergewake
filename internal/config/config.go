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
	Source string `yaml:"source"`
	Target string `yaml:"target"`
	Mode   string `yaml:"mode"`
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
type Config struct {
	Version      int      `yaml:"version"`
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
		c.Sink.ReopenState = "in_progress"
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
