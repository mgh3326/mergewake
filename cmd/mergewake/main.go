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
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/pipeline"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/superfan"
)

var version = "dev"

const usage = `mergewake: the after-merge pipeline for your agent fleet

Usage: mergewake <command> [flags]
  land       Classify and squash-merge a PR (shadow until policy opts in)
  review     Run one independent diff-only review per PR head
  watch      Watch main CI and open one recovery PR (shadow until opt in)
  release    Publish a verified manifest as a GitHub Release asset
  rollout    Pull and install the latest verified manifest once
  agent      Poll configured repositories and perform verified rollouts
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
	command := args[0]
	switch command {
	case "land", "review", "watch", "release", "rollout", "agent", "status", "superfan":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
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
	if command != "agent" && command != "status" || *repo != "" {
		if *repo == "" {
			return errors.New("--repo is required")
		}
		r, e = c.Repo(*repo)
		if e != nil {
			return e
		}
	}
	if command == "status" {
		return status(c, *repo, out)
	}
	exec := integrations.Exec{}
	sink, notifier := integrations.New(c, exec, out)
	s := pipeline.Service{Config: c, GH: github.New(os.Getenv(c.TokenEnv)), Runner: exec, Sink: sink, Notifier: notifier, Out: out}
	o := pipeline.Options{DryRun: *dry}
	if shadow != nil {
		o.Shadow = *shadow
	}
	if pr != nil && *pr <= 0 {
		return errors.New("--pr must be positive")
	}
	switch command {
	case "land":
		return s.Land(ctx, r, *pr, o)
	case "review":
		return s.Review(ctx, r, *pr, *family, o)
	case "release":
		return s.Release(ctx, r, *commit, o)
	case "rollout":
		return s.Rollout(ctx, r, o)
	case "watch":
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
