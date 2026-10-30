// Package config reads strict, operator-owned YAML. PRs never supply host config.
package config

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/reap"
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
	GateEffort  string   `yaml:"gate_effort"`
}
type Review struct {
	Login        string              `yaml:"login"`
	Pairings     map[string][]string `yaml:"pairings"`
	Profiles     map[string]Reviewer `yaml:"profiles"`
	Scopefuel    string              `yaml:"scopefuel"`
	Quota        string              `yaml:"quota"`
	DefaultGrade string              `yaml:"default_grade"`
	// DefaultFamily is the reviewer family used when the PR carries no
	// author-family trailer and the head commit identities yield no known
	// model alias. Reviewed heads record that the family was inferred.
	DefaultFamily string `yaml:"default_family"`
	Timeout      string              `yaml:"timeout"`
	// Select orders admitted reviewers: "pace" (default) by projected use at
	// each pool's reset from `<quota> --json`; "rank" keeps the quota tool's
	// recommendation order without reading usage.
	Select string `yaml:"select"`
	// Executor runs each pass: "headless" (default) invokes a profile's CLI on
	// the diff here; "runtug" spawns a reviewer pane through runtug in a
	// checkout of the PR head and keeps headless as the fallback.
	Executor string       `yaml:"executor"`
	Runtug   RuntugReview `yaml:"runtug"`
}

// RuntugReview configures the runtug review executor. The reviewer pane writes
// its findings into the job directory, so Jobs must be the inbox runtug uses on
// the host that runs the pane.
type RuntugReview struct {
	Command   string `yaml:"command"`
	Workspace string `yaml:"workspace"`
	Owner     string `yaml:"owner"`
	Host      string `yaml:"host"`
	Jobs      string `yaml:"jobs"`
	Checkouts string `yaml:"checkouts"`
}
type Outcomes struct {
	Command string `yaml:"command"`
	Tier    string `yaml:"default_tier"`
}
type Notifier struct {
	Type    string `yaml:"type"`
	Command string `yaml:"command"`
	Lane    string `yaml:"lane"`
	// FallbackLane receives events for PRs with no owning lane; empty means
	// Lane, the operator lane.
	FallbackLane string `yaml:"fallback_lane"`
}
type Sink struct {
	Type        string `yaml:"type"`
	Command     string `yaml:"command"`
	ReopenState string `yaml:"reopen_state"`
}
type Health struct {
	URL     string `yaml:"url" json:"url"`
	Status  int    `yaml:"status" json:"status"`
	Timeout string `yaml:"timeout" json:"timeout"`
	Retries int    `yaml:"retries" json:"retries"`
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
	Image        string   `yaml:"image" json:"image"`
	Name         string   `yaml:"name" json:"name"`
	Ports        []string `yaml:"ports" json:"ports"`
	EnvFile      string   `yaml:"env_file" json:"env_file"`
	Volumes      []string `yaml:"volumes" json:"volumes"`
	Restart      string   `yaml:"restart" json:"restart"`
	Health       *Health  `yaml:"health" json:"health"`
	Keep         *int     `yaml:"keep" json:"keep"`
	Runtime      string   `yaml:"runtime" json:"runtime"`
	User         string   `yaml:"user" json:"user"`
	Digest       string   `yaml:"digest" json:"digest"`
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

// PRNotify gates the pr role's READY/BLOCKED lane events. Absent means enabled;
// `notify: {enabled: false}` silences them.
type PRNotify struct {
	Enabled *bool `yaml:"enabled"`
}

func (n PRNotify) On() bool { return n.Enabled == nil || *n.Enabled }

// PRLoop extends the shared loop keys with the notify gate; other roles reject
// the key as an unknown field.
type PRLoop struct {
	Loop   `yaml:",inline"`
	Notify PRNotify `yaml:"notify"`
}
type Daemon struct {
	PR      PRLoop `yaml:"pr"`
	Watch   Loop   `yaml:"watch"`
	Release Loop   `yaml:"release"`
	Rollout Loop   `yaml:"rollout"`
	Act     bool   `yaml:"act"`
}
type Reap struct {
	Enabled       bool     `yaml:"enabled"`
	Act           bool     `yaml:"act"`
	WorktreeAfter string   `yaml:"worktree_after"`
	ScanAfter     string   `yaml:"scan_after"`
	Protect       []string `yaml:"protect"`
	Regenerable   []string `yaml:"regenerable"`
	// Transport reaches remote hosts: panewire (the hub's node commands) or
	// ssh. Empty resolves to panewire when hub.url is set, else ssh.
	Transport string  `yaml:"transport"`
	Hub       ReapHub `yaml:"hub"`
}

// ReapHub locates the panewire hub and the operator credential files reap
// sends it. The files are the mode-0600 env files the panewire operator CLI
// takes: HUB_TOKEN, and optionally CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET.
type ReapHub struct {
	URL       string `yaml:"url"`
	TokenFile string `yaml:"token_file"`
	CFFile    string `yaml:"cf_file"`
}

const (
	ReapTransportPanewire = "panewire"
	ReapTransportSSH      = "ssh"
)

// DefaultReapProtect names resident and desk panes that reap never closes. A
// configured reap.protect list replaces this list; unnamed panes and panes
// outside a linked worktree are always protected regardless of the list.
var DefaultReapProtect = []string{"*-integrator", "director-*", "fleet-tooling-*", "consult-*", "cmd"}

// DefaultReapRegenerable names gitignored paths whose loss is acceptable
// because they rebuild on demand: language caches, build output, vendored
// deps, and Unity's generated directories. A configured reap.regenerable
// list replaces this list; anything else ignored — .env*, notes, unknown
// files — still refuses worktree removal.
var DefaultReapRegenerable = []string{
	".venv", "node_modules", "__pycache__", ".pytest_cache",
	"dist", "build", "target", "obj", "bin",
	"Library", "Temp", "Logs", "UserSettings",
}

type Config struct {
	Version      int         `yaml:"version"`
	Daemon       Daemon      `yaml:"daemon"`
	Host         string      `yaml:"host"`
	StateDir     string      `yaml:"state_dir"`
	TokenEnv     string      `yaml:"token_env"`
	PollInterval string      `yaml:"poll_interval"`
	WatchLimit   int         `yaml:"watch_limit"`
	Repos        []Repo      `yaml:"repos"`
	Hosts        []reap.Host `yaml:"hosts"`
	Reap         Reap        `yaml:"reap"`
	Review       Review      `yaml:"review"`
	Outcomes     Outcomes    `yaml:"outcomes"`
	Notifier     Notifier    `yaml:"notifier"`
	Sink         Sink        `yaml:"sink"`
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
	if c.Version != 1 || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`).MatchString(c.Host) {
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
	for _, loop := range []*Loop{&c.Daemon.PR.Loop, &c.Daemon.Watch, &c.Daemon.Release, &c.Daemon.Rollout} {
		if loop.Interval == "" {
			loop.Interval = c.PollInterval
		}
		if _, err := Duration(loop.Interval); err != nil {
			return c, err
		}
	}
	if c.Reap.Act && !c.Reap.Enabled {
		return c, errors.New("reap.act requires reap.enabled")
	}
	if c.Reap.WorktreeAfter == "" {
		c.Reap.WorktreeAfter = "168h"
	}
	if _, e = Duration(c.Reap.WorktreeAfter); e != nil {
		return c, e
	}
	if c.Reap.ScanAfter == "" {
		c.Reap.ScanAfter = "720h"
	}
	if _, e = Duration(c.Reap.ScanAfter); e != nil {
		return c, e
	}
	if len(c.Reap.Protect) == 0 {
		c.Reap.Protect = DefaultReapProtect
	}
	for _, g := range c.Reap.Protect {
		if _, e = policy.Match(g, "probe"); e != nil {
			return c, e
		}
	}
	if len(c.Reap.Regenerable) == 0 {
		c.Reap.Regenerable = DefaultReapRegenerable
	}
	for _, g := range c.Reap.Regenerable {
		if _, e = policy.Match(g, "probe"); e != nil {
			return c, e
		}
	}
	if c.Reap.Transport == "" {
		c.Reap.Transport = ReapTransportSSH
		if c.Reap.Hub.URL != "" {
			c.Reap.Transport = ReapTransportPanewire
		}
	}
	switch c.Reap.Transport {
	case ReapTransportSSH:
	case ReapTransportPanewire:
		if _, e = reap.HubOrigin(c.Reap.Hub.URL); e != nil {
			return c, e
		}
		if c.Reap.Hub.TokenFile == "" {
			return c, errors.New("reap.transport panewire requires reap.hub.token_file")
		}
		c.Reap.Hub.TokenFile = Expand(c.Reap.Hub.TokenFile)
		if c.Reap.Hub.CFFile != "" {
			c.Reap.Hub.CFFile = Expand(c.Reap.Hub.CFFile)
		}
		if !filepath.IsAbs(c.Reap.Hub.TokenFile) || (c.Reap.Hub.CFFile != "" && !filepath.IsAbs(c.Reap.Hub.CFFile)) {
			return c, errors.New("reap.hub credential files must be absolute paths")
		}
	default:
		return c, errors.New("reap.transport must be panewire or ssh")
	}
	seenHosts := map[string]bool{}
	for i, h := range c.Hosts {
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`).MatchString(h.Name) || seenHosts[h.Name] {
			return c, errors.New("hosts require unique name labels")
		}
		seenHosts[h.Name] = true
		if strings.HasPrefix(h.SSH, "-") || strings.ContainsAny(h.SSH, " \t\r\n") {
			return c, errors.New("hosts[].ssh must be empty, local, or an ssh destination")
		}
		if h.Machine != "" && !regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`).MatchString(h.Machine) {
			return c, errors.New("hosts[].machine must be a panewire machine id")
		}
		if c.Reap.Transport == ReapTransportSSH && !h.Local() && (h.SSH == "" || h.SSH == "local") {
			return c, errors.New("hosts[].machine without ssh requires reap.transport panewire")
		}
		if h.Jobs == "" {
			continue
		}
		if h.Local() {
			c.Hosts[i].Jobs = Expand(h.Jobs)
			if !filepath.IsAbs(c.Hosts[i].Jobs) {
				return c, errors.New("hosts[].jobs must be an absolute path")
			}
		} else if !filepath.IsAbs(h.Jobs) {
			return c, errors.New("ssh host jobs must be an absolute path; ~ does not expand remotely")
		}
	}
	if c.Review.Timeout == "" {
		c.Review.Timeout = "10m"
	}
	if c.Review.DefaultGrade == "" {
		c.Review.DefaultGrade = "B"
	}
	if !ValidGrade(c.Review.DefaultGrade) {
		return c, errors.New("invalid review.default_grade")
	}
	if c.Review.Quota == "" {
		c.Review.Quota = c.Review.Scopefuel
	}
	if c.Review.Quota == "" {
		c.Review.Quota = "scopefuel"
	}
	c.Review.Quota = Expand(c.Review.Quota)
	if _, e = Duration(c.Review.Timeout); e != nil {
		return c, e
	}
	switch c.Review.Executor {
	case "":
		c.Review.Executor = "headless"
	case "headless", "runtug":
	default:
		return c, errors.New("review.executor must be headless or runtug")
	}
	rt := &c.Review.Runtug
	if rt.Command == "" {
		rt.Command = "runtug"
	}
	if rt.Workspace == "" {
		rt.Workspace = "reviews"
	}
	if rt.Host == "" {
		rt.Host = "auto"
	}
	if rt.Jobs == "" {
		rt.Jobs = "~/work/herdr-inbox/jobs"
	}
	if rt.Checkouts == "" {
		rt.Checkouts = filepath.Join(c.StateDir, "review-checkouts")
	}
	rt.Command, rt.Jobs, rt.Checkouts = Expand(rt.Command), Expand(rt.Jobs), Expand(rt.Checkouts)
	if c.Outcomes.Command == "" {
		c.Outcomes.Command = c.Review.Quota
	}
	c.Outcomes.Command = Expand(c.Outcomes.Command)
	if c.Outcomes.Tier == "" {
		c.Outcomes.Tier = "T1"
	}
	switch c.Outcomes.Tier {
	case "T0", "T1", "T2", "T3":
	default:
		return c, errors.New("invalid outcomes.default_tier")
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
			case "container":
				if e = validateContainer(spec); e != nil {
					return c, e
				}
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
			default:
				return c, errors.New("install kind must be file, command, release or container")
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
		}
		family := ""
		for _, spec := range r.Install {
			f := installFamily(spec.Kind)
			if family == "" {
				family = f
			} else if family != f {
				return c, errors.New("one repo inventory cannot mix file, command/release and container installs")
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
	if c.Notifier.FallbackLane == "" {
		c.Notifier.FallbackLane = c.Notifier.Lane
	}
	if c.Sink.ReopenState == "" {
		c.Sink.ReopenState = "reopened"
	}
	if c.Sink.ReopenState != "reopened" {
		return c, errors.New("sink.reopen_state must be reopened")
	}
	for _, name := range slices.Sorted(maps.Keys(c.Review.Profiles)) {
		p := c.Review.Profiles[name]
		if name == "" || p.Family == "" || !ReviewerCommand(p.Command) {
			return c, errors.New("review profiles require a name, family, and supported reviewer command")
		}
		if e := ValidateReviewer(p); e != nil {
			return c, e
		}
		p.Family = CanonicalFamily(p.Family)
		if p.Family == "" {
			return c, errors.New("review profiles require a name, family, and supported reviewer command")
		}
		c.Review.Profiles[name] = p
	}
	if c.Review.Select != "" && c.Review.Select != "pace" && c.Review.Select != "rank" {
		return c, errors.New("review.select must be pace or rank")
	}
	pairings := map[string][]string{}
	for _, family := range slices.Sorted(maps.Keys(c.Review.Pairings)) {
		names := c.Review.Pairings[family]
		if family == "" || len(names) == 0 {
			return c, errors.New("empty review pairing")
		}
		canonical := CanonicalFamily(family)
		if canonical == "" {
			return c, errors.New("empty review pairing")
		}
		if _, dup := pairings[canonical]; dup {
			return c, errors.New("review pairings collide after family normalisation")
		}
		pairings[canonical] = names
		for _, name := range names {
			p, ok := c.Review.Profiles[name]
			if !ok || p.Family == canonical {
				return c, errors.New("pairings must name configured profiles from a different family")
			}
		}
	}
	if len(pairings) > 0 {
		c.Review.Pairings = pairings
	}
	if c.Review.DefaultFamily != "" {
		c.Review.DefaultFamily = CanonicalFamily(c.Review.DefaultFamily)
		ok := false
		for _, p := range c.Review.Profiles {
			if p.Family == c.Review.DefaultFamily {
				ok = true
				break
			}
		}
		if !ok {
			return c, errors.New("review.default_family must name a configured profile family")
		}
	}
	return c, nil
}

func ValidGrade(s string) bool {
	switch s {
	case "S+", "S", "A+", "A", "B", "C":
		return true
	}
	return false
}

// familyAliases maps model and CLI names to one canonical model family, so an
// author family like "claude" and a reviewer profile family like "anthropic"
// compare equal and a same-family reviewer is never counted as independent.
var familyAliases = map[string]string{
	"anthropic": "anthropic", "claude": "anthropic", "sonnet": "anthropic", "opus": "anthropic", "haiku": "anthropic", "fable": "anthropic", "kiro": "anthropic",
	"openai": "openai", "codex": "openai", "sol": "openai", "gpt": "openai",
	"cognition": "cognition", "devin": "cognition", "swe": "cognition",
	"xai": "xai", "grok": "xai",
	"google": "google", "gemini": "google", "agy": "google",
	"moonshot": "moonshot", "kimi": "moonshot",
	"deepseek": "deepseek",
}

// FamilyToken reports whether s is a listed model or CLI alias and returns
// its canonical family, so commit identities can be classified.
func FamilyToken(s string) (string, bool) {
	f, ok := familyAliases[strings.ToLower(strings.TrimSpace(s))]
	return f, ok
}

// CanonicalFamily normalises an author or reviewer family alias to its
// canonical model family. Unlisted names are their own family.
func CanonicalFamily(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if f, ok := familyAliases[s]; ok {
		return f
	}
	return s
}

func ReviewerCommand(s string) bool {
	switch filepath.Base(s) {
	case "agy", "claude", "codex", "devin", "grok", "kimi", "pi", "cmd":
		return true
	}
	return false
}
func Duration(s string) (time.Duration, error) {
	d, e := time.ParseDuration(s)
	if e != nil || d <= 0 {
		return 0, fmt.Errorf("invalid positive duration %q", s)
	}
	return d, nil
}

func installFamily(kind string) string {
	switch kind {
	case "command", "release":
		return "command"
	case "container":
		return "container"
	}
	return "file"
}

// Container inventories describe a runtime that the host already trusts. Values
// are validated here so a bad host file fails before any journal or mutation.
var (
	containerName  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	imageName      = regexp.MustCompile(`^(?:[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?(?::[0-9]{1,5})/)?[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*$`)
	imageDigest    = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	restartPolicy  = regexp.MustCompile(`^(no|always|unless-stopped|on-failure(:[0-9]+)?)$`)
	containerUser  = regexp.MustCompile(`^[A-Za-z0-9_][-A-Za-z0-9_.:]*$`)
	volumeValue    = regexp.MustCompile(`^[A-Za-z0-9_./@+-]+$`)
	containerLabel = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]*$`)
)

func validContainerTarget(p string) bool {
	return p != "" && p != "." && !path.IsAbs(p) && path.Clean(p) == p && p != ".." &&
		!strings.HasPrefix(p, "../") && !strings.ContainsAny(p, "\\\x00\r\n") && containerLabel.MatchString(p)
}

func validateContainer(spec *Install) error {
	if !validContainerTarget(spec.Target) {
		return errors.New("container install requires a clean relative target label")
	}
	if spec.Image == "" || strings.Contains(spec.Image, "@") || strings.Contains(path.Base(spec.Image), ":") || !imageName.MatchString(spec.Image) {
		return errors.New("container image must be a registry/repository without a tag or digest")
	}
	if !containerName.MatchString(spec.Name) {
		return errors.New("container name is invalid")
	}
	for i, p := range spec.Ports {
		n, e := normalizePort(p)
		if e != nil {
			return e
		}
		spec.Ports[i] = n
	}
	if spec.EnvFile != "" {
		env := Expand(spec.EnvFile)
		if !filepath.IsAbs(env) || strings.ContainsAny(env, "\x00\r\n") {
			return errors.New("container env_file must be an absolute operator-owned path")
		}
	}
	for _, v := range spec.Volumes {
		if e := validateVolume(v); e != nil {
			return e
		}
	}
	if spec.Restart == "" {
		spec.Restart = "unless-stopped"
	} else if !restartPolicy.MatchString(spec.Restart) {
		return errors.New("container restart policy is invalid")
	}
	if spec.Health == nil {
		return errors.New("container install requires health")
	}
	if e := validateHealth(spec.Health); e != nil {
		return e
	}
	if spec.Keep == nil {
		keep := 2
		spec.Keep = &keep
	} else if *spec.Keep < 0 || *spec.Keep > 100 {
		return errors.New("container keep must be 0..100")
	}
	switch spec.Runtime {
	case "":
		spec.Runtime = "docker"
	case "docker", "podman":
	default:
		return errors.New("container runtime must be docker or podman")
	}
	if spec.User != "" && !containerUser.MatchString(spec.User) {
		return errors.New("container user is invalid")
	}
	if spec.Digest != "" && !imageDigest.MatchString(spec.Digest) {
		return errors.New("container digest must be sha256:<64 hex>")
	}
	return nil
}

func validateHealth(h *Health) error {
	u, e := url.Parse(h.URL)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return errors.New("container health URL must be an absolute http(s) URL without credentials")
	}
	if h.Status == 0 {
		h.Status = 200
	}
	if h.Status < 100 || h.Status > 599 {
		return errors.New("container health status must be 100..599")
	}
	if h.Timeout == "" {
		h.Timeout = "5s"
	}
	if _, e = Duration(h.Timeout); e != nil {
		return e
	}
	if h.Retries == 0 {
		h.Retries = 20
	}
	if h.Retries < 1 || h.Retries > 1000 {
		return errors.New("container health retries must be 1..1000")
	}
	return nil
}

func validateVolume(v string) error {
	parts := strings.Split(v, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return errors.New("container volume must be source:destination[:ro|rw]")
	}
	for _, p := range parts {
		if p == "" || !volumeValue.MatchString(p) {
			return errors.New("container volume contains an empty or unsupported field")
		}
	}
	if len(parts) == 3 && parts[2] != "ro" && parts[2] != "rw" {
		return errors.New("container volume mode must be ro or rw")
	}
	return nil
}

// normalizePort accepts host_ip:host_port:container_port and host_port:container_port.
// The omitted host address is loopback, so a default inventory exposes nothing else.
func normalizePort(raw string) (string, error) {
	host, rest := "127.0.0.1", raw
	if strings.HasPrefix(raw, "[") {
		end := strings.Index(raw, "]")
		if end < 0 || end+1 >= len(raw) || raw[end+1] != ':' {
			return "", errors.New("container port must be host_ip:host_port:container_port")
		}
		host, rest = raw[1:end], raw[end+2:]
	} else {
		switch parts := strings.Split(raw, ":"); len(parts) {
		case 3:
			host, rest = parts[0], parts[1]+":"+parts[2]
		case 2:
		default:
			return "", errors.New("container port must be host_ip:host_port:container_port or host_port:container_port")
		}
	}
	ports := strings.Split(rest, ":")
	if len(ports) != 2 || host == "" {
		return "", errors.New("container port must name host and container ports")
	}
	if net.ParseIP(host) == nil {
		return "", errors.New("container port host must be an IP literal")
	}
	for _, p := range ports {
		n, e := strconv.Atoi(p)
		if e != nil || n < 1 || n > 65535 {
			return "", errors.New("container port must be 1..65535")
		}
	}
	return net.JoinHostPort(host, ports[0]) + ":" + ports[1], nil
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

func ValidateReviewer(p Reviewer) error {
	if e := ValidateReviewArgs(p.Args); e != nil {
		return e
	}
	kind := filepath.Base(p.Command)
	for i := 0; i < len(p.Args); i += 2 {
		k := p.Args[i]
		if k == "--model" {
			continue
		}
		if kind == "pi" && (k == "--provider" || k == "--thinking") {
			continue
		}
		if k == "--effort" && (kind == "agy" || kind == "claude" || kind == "codex" || kind == "grok" || kind == "cmd") {
			continue
		}
		return fmt.Errorf("review option %s unsupported by %s", k, kind)
	}
	if p.GateEffort != "" {
		switch p.GateEffort {
		case "low", "medium", "high", "xhigh", "max":
		default:
			return errors.New("invalid reviewer gate_effort")
		}
	}
	return nil
}
