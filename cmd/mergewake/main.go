package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/pipeline"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/superfan"
)

var version = "dev"
var commit = "unknown"

const usage = `mergewake: the after-merge pipeline for your agent fleet

Usage: mergewake <command> [flags]
  land       Classify and squash-merge a PR (shadow until policy opts in)
  review     Run one independent diff-only review per PR head
  watch      Watch main CI and open one recovery PR (shadow until opt in)
  release    Publish a verified manifest as a GitHub Release asset
  rollout    Pull and install the latest verified manifest once
  daemon     Run enabled pipeline roles (shadow by default)
  agent      Poll configured repositories and perform verified rollouts
  outcomes   Report or sync the shadow-safe model outcomes ledger
  status     Read installed receipts and check local drift
  superfan   Print or launch an experimental resident monitor spec

All commands accept --help. Acting commands accept --dry-run.
Config defaults to ~/.config/mergewake/config.yaml.
`

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:], os.Stdout, os.Stderr); e != nil {
		fmt.Fprintln(os.Stderr, "mergewake:", e)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		_, e := fmt.Fprint(out, usage)
		return e
	}
	if args[0] == "--version" || args[0] == "version" {
		_, e := fmt.Fprintln(out, version)
		return e
	}
	if args[0] == "commit" {
		_, err := fmt.Fprintln(out, commit)
		return err
	}
	if args[0] == "outcomes" {
		return runOutcomes(ctx, args[1:], out, stderr)
	}
	command := args[0]
	switch command {
	case "land", "review", "watch", "release", "rollout", "agent", "daemon", "status", "superfan":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	apply := fs.Bool("apply", false, "allow acting review, release, rollout or daemon (daemon also needs daemon.act)")
	daemonStatus := fs.Bool("daemon", false, "read loop status (status only)")
	dry := fs.Bool("dry-run", false, "print planned actions without mutations or model execution")
	repo := fs.String("repo", "", "configured owner/name (required except agent, status, superfan)")
	var pr *int
	var family, commit *string
	var shadow, once *bool
	var name, runner *string
	var launch *bool
	switch command {
	case "land", "review":
		pr = fs.Int("pr", 0, "pull request number")
		if command == "review" {
			family = fs.String("author-family", "", "PR author model family (or PR body trailer)")
		} else {
			shadow = fs.Bool("shadow", false, "force decision-only mode")
		}
	case "daemon":
		shadow = fs.Bool("shadow", false, "force all roles into shadow mode")
	case "watch":
		shadow = fs.Bool("shadow", false, "force decision-only mode")
		once = fs.Bool("once", false, "inspect once instead of polling")
	case "release":
		commit = fs.String("commit", "", "full main commit SHA (default branch head)")
	case "agent":
		once = fs.Bool("once", false, "poll each repo once")
	case "superfan":
		name = fs.String("name", "", "relay, ci, quota, drift; empty lists specs")
		launch = fs.Bool("launch", false, "launch runner, sending spec JSON on stdin")
		runner = fs.String("runner", "", "trusted session-runner executable; accepts spec JSON on stdin")
	}
	if e := fs.Parse(args[1:]); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if command == "superfan" {
		return runSuperfan(ctx, *name, *launch, *runner, *dry, out)
	}
	c, e := config.Load(config.Expand(*cfg))
	if e != nil {
		return e
	}
	r := config.Repo{}
	if command != "agent" && command != "status" && command != "daemon" || *repo != "" {
		if *repo == "" {
			return errors.New("--repo is required")
		}
		r, e = c.Repo(*repo)
		if e != nil {
			return e
		}
	}
	if command == "status" {
		if *daemonStatus {
			return daemon.ReadStatus(c.StateDir, out)
		}
		return status(c, *repo, out)
	}
	if command == "daemon" {
		out = &lockedWriter{out: out}
		stderr = &lockedWriter{out: stderr}
	}
	exec := integrations.Exec{}
	sink, notifier := integrations.New(c, exec, out)
	gh := github.New(os.Getenv(c.TokenEnv))
	s := pipeline.Service{Config: c, GH: gh, Runner: exec, Sink: sink, Notifier: notifier, Out: out}
	o := pipeline.Options{DryRun: *dry, Shadow: !*apply}
	if shadow != nil {
		o.Shadow = o.Shadow || *shadow
	}
	if pr != nil && *pr <= 0 {
		return errors.New("--pr must be positive")
	}
	switch command {
	case "daemon":
		o.Shadow = o.Shadow || !c.Daemon.Act
		return runDaemon(ctx, &s, gh, *repo, o, stderr)
	case "land":
		// Landing's opt-in is repository policy; --shadow still forces observation.
		o.Shadow = *shadow
		return s.Land(ctx, r, *pr, o)
	case "review":
		return s.Review(ctx, r, *pr, *family, o)
	case "release":
		return s.Release(ctx, r, *commit, o)
	case "rollout":
		return s.Rollout(ctx, r, o)
	case "watch":
		o.Shadow = *shadow
		return poll(ctx, c.PollInterval, *once || *dry, func() error { return s.Watch(ctx, r, o) }, stderr)
	case "agent":
		if len(c.Repos) == 0 {
			return errors.New("agent requires at least one configured repository")
		}
		return poll(ctx, c.PollInterval, *once || *dry, func() error {
			var errs []error
			for _, r := range c.Repos {
				if *repo != "" && r.Name != *repo {
					continue
				}
				if e := s.Rollout(ctx, r, o); e != nil {
					errs = append(errs, fmt.Errorf("%s: %w", r.Name, e))
				}
			}
			return errors.Join(errs...)
		}, stderr)
	}
	return nil
}
func poll(ctx context.Context, interval string, once bool, fn func() error, stderr io.Writer) error {
	d, e := config.Duration(interval)
	if e != nil {
		return e
	}
	for {
		e = fn()
		if once {
			return e
		}
		if e != nil {
			fmt.Fprintln(stderr, e)
		}
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
func runSuperfan(ctx context.Context, name string, launch bool, runner string, dry bool, out io.Writer) error {
	if name == "" {
		if launch {
			return errors.New("launch requires --name")
		}
		for _, n := range superfan.Names() {
			s, e := superfan.Get(n)
			if e != nil {
				return e
			}
			if e = json.NewEncoder(out).Encode(s); e != nil {
				return e
			}
		}
		return nil
	}
	spec, e := superfan.Get(name)
	if e != nil {
		return e
	}
	b, e := json.Marshal(spec)
	if e != nil {
		return e
	}
	if !launch || dry {
		_, e = fmt.Fprintln(out, string(b))
		return e
	}
	if runner == "" {
		return errors.New("--launch requires --runner")
	}
	result, e := (integrations.Exec{}).Run(ctx, runner, nil, string(b), "")
	if e != nil {
		return e
	}
	_, e = out.Write(result)
	return e
}
func status(c config.Config, repo string, out io.Writer) error {
	var errs []error
	for _, r := range c.Repos {
		if repo != "" && r.Name != repo {
			continue
		}
		if r.Root == "" {
			errs = append(errs, fmt.Errorf("%s: root required for status", r.Name))
			continue
		}
		commands := false
		for _, spec := range r.Install {
			commands = commands || spec.Kind == "command" || spec.Kind == "release"
		}
		if commands {
			if err := commandStatus(c, r, out); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		b, e := os.ReadFile(filepath.Join(c.StateDir, r.Name, "rollout", "installed.json"))
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, e))
			continue
		}
		var receipt struct {
			Manifest manifest.Manifest `json:"manifest"`
		}
		if e = json.Unmarshal(b, &receipt); e != nil {
			errs = append(errs, e)
			continue
		}
		if receipt.Manifest.Repo != r.Name {
			errs = append(errs, fmt.Errorf("%s: receipt repo mismatch", r.Name))
			continue
		}
		if e = receipt.Manifest.Validate(); e != nil {
			errs = append(errs, e)
			continue
		}
		files := []map[string]any{}
		for _, f := range receipt.Manifest.Files {
			denied := false
			for _, g := range r.Deny {
				match, _ := policy.Match(g, f.Target)
				denied = denied || match
			}
			if denied {
				files = append(files, map[string]any{"target": f.Target, "status": "denied"})
				continue
			}
			data, mode, e := manifest.ReadRegular(config.Expand(r.Root), f.Target)
			state := "healthy"
			if e != nil || manifest.Verify(f, data) != nil || uint32(mode) != f.Mode {
				state = "drift"
				errs = append(errs, fmt.Errorf("%s %s: drift", r.Name, f.Target))
			}
			files = append(files, map[string]any{"target": f.Target, "status": state, "expected_sha256": f.SHA256, "observed_sha256": manifest.Sum(data)})
		}
		if e = json.NewEncoder(out).Encode(map[string]any{"host": c.Host, "repo": r.Name, "commit": receipt.Manifest.Commit, "files": files}); e != nil {
			return e
		}
	}
	return errors.Join(errs...)
}

func runOutcomes(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "report" && args[0] != "sync") {
		return errors.New("usage: mergewake outcomes report|sync [--config PATH] [--since DATE|RFC3339|DURATION] [--source local|hk]")
	}
	fs := flag.NewFlagSet("outcomes "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	sinceArg := fs.String("since", "", "include PRs opened since this date, timestamp or duration ago")
	source := fs.String("source", "local", "report source: local or hk (via reps CLI)")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	c, err := config.Load(config.Expand(*cfg))
	if err != nil {
		return err
	}
	ledger := outcomes.Ledger{Dir: c.StateDir}
	reps := outcomes.CLI{Command: c.Outcomes.Command, Runner: integrations.Exec{}}
	if args[0] == "sync" {
		return ledger.Flush(ctx, reps)
	}
	var since time.Time
	if *sinceArg != "" {
		since, err = time.Parse(time.RFC3339, *sinceArg)
		if err != nil {
			since, err = time.Parse("2006-01-02", *sinceArg)
		}
		if err != nil {
			var d time.Duration
			d, err = time.ParseDuration(*sinceArg)
			if err == nil && d > 0 {
				since = time.Now().Add(-d)
			} else {
				return errors.New("invalid --since; use YYYY-MM-DD, RFC3339 or a positive duration")
			}
		}
	}
	values := []outcomes.Outcome{}
	switch *source {
	case "local":
		state, e := ledger.Read()
		if e != nil {
			return e
		}
		for _, v := range state.Latest {
			values = append(values, v)
		}
	case "hk":
		values, err = reps.Read(ctx)
		if err != nil {
			return err
		}
	default:
		return errors.New("source must be local or hk")
	}
	return json.NewEncoder(out).Encode(outcomes.Report(values, since))
}
