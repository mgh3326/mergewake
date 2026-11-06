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
	"github.com/mgh3326/mergewake/internal/rollout"
	"github.com/mgh3326/mergewake/internal/server/store"
	"github.com/mgh3326/mergewake/internal/superfan"
)

var version = "dev"
var commit = "unknown"

const usage = `mergewake: the after-merge pipeline for your agent fleet

Usage: mergewake <command> [flags]
  land       Run the merge gate for a PR and squash-merge the gated head (shadow until policy opts in)
  review     Run one independent diff-only review per PR head
  watch      Watch main CI and open one recovery PR (shadow until opt in)
  release    Publish a verified manifest as a GitHub Release asset
  rollout    Pull and install the latest verified manifest once
  daemon     Run enabled pipeline roles (shadow by default)
  server     Migrate or check the durable PR server store
  agent      Poll configured repositories and perform verified rollouts
  reap       Close merged-PR sessions and clean old worktrees (needs reap.enabled)
  archive-jobs  Move old job dirs to the Drive archive and record them in hk (needs reap.archive.enabled)
  report     Report review latency from state and GitHub
  outcomes   Report or sync the shadow-safe model outcomes ledger
  notify     Post a deploy from another deployer to the Discord feed (needs discord.enabled)
  status     Read installed receipts and check local drift
  promote    Certify one main commit and its image digest, record it, and advance the deployable pointer
  admit      Print the digest the deployable pointer and its record agree on, or refuse; --deploy hands it to shiftgate
  superfan   Print or launch an experimental resident monitor spec

All commands accept --help. Acting commands accept --dry-run.
Config defaults to ~/.config/mergewake/config.yaml.
`

func main() {
	// SIGHUP (a closed pane or terminal) cancels too: children run in their own
	// process groups, so the terminal's signal would never reach them.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()
	if e := run(ctx, os.Args[1:], os.Stdout, os.Stderr); e != nil {
		fmt.Fprintln(os.Stderr, "mergewake:", e)
		var x *exitError
		if errors.As(e, &x) && x.code > 1 {
			os.Exit(x.code)
		}
		os.Exit(1)
	}
}

// ownFlagCommands parse their own flags and do not take the shared
// --config/--apply/--repo set.
var ownFlagCommands = map[string]func(context.Context, []string, io.Writer, io.Writer) error{
	"report":       runReport,
	"outcomes":     runOutcomes,
	"archive-jobs": runArchiveJobs,
	"notify":       runNotify,
	"promote":      runPromote,
	"admit":        runAdmit,
	"server":       runServerStore,
}

// shadowMode decides, in one place, whether a shared-flag command only
// observes. Without --apply every command observes, and --shadow always
// forces it. land and watch are the exceptions: their opt-in is repository
// policy, so --apply does not matter and only --shadow forces observation.
// The daemon also observes until daemon.act is set. reap.act, checked inside
// Reap, is reap's own acting gate on top of this.
func shadowMode(command string, apply, forced, daemonAct bool) bool {
	switch command {
	case "land", "watch":
		return forced
	case "daemon":
		return !apply || forced || !daemonAct
	}
	return !apply || forced
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
	if sub, ok := ownFlagCommands[args[0]]; ok {
		return sub(ctx, args[1:], out, stderr)
	}
	command := args[0]
	switch command {
	case "land", "review", "watch", "release", "rollout", "agent", "daemon", "status", "superfan", "reap":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	apply := fs.Bool("apply", false, "allow acting review, release, rollout, reap or daemon (daemon also needs daemon.act; reap also needs reap.act)")
	daemonStatus := fs.Bool("daemon", false, "read loop status (status only)")
	dry := fs.Bool("dry-run", false, "print planned actions without mutations or model execution")
	repo := fs.String("repo", "", "configured owner/name (required except agent, status, superfan)")
	var pr *int
	var family, commit *string
	var shadow, once, legacy *bool
	var legacyHost *string
	var name, runner *string
	var launch *bool
	var digest, adopt *string
	switch command {
	case "land", "review", "reap":
		pr = fs.Int("pr", 0, "pull request number (reap scans merged PRs when omitted)")
		if command == "review" {
			family = fs.String("author-family", "", "PR author model family (or PR body trailer)")
		} else if command == "reap" {
			shadow = fs.Bool("shadow", false, "force observation mode")
			legacy = fs.Bool("legacy", false, "one-time sweep of worktrees whose PR merged at least 14 days ago, past the daemon's scan window (every configured repo unless --repo; plans unless --apply)")
			legacyHost = fs.String("host", "", "with --legacy: sweep one configured host only")
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
	case "rollout":
		digest = fs.String("digest", "", "container digest repo@sha256:... for a single container target")
		adopt = fs.String("adopt", "", "record an already-running container as the current receipt")
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
	if command == "reap" {
		switch {
		case !*legacy && *legacyHost != "":
			return errors.New("--host needs --legacy")
		case *legacy && *pr != 0:
			return errors.New("--legacy looks at every PR; it does not take --pr")
		case *legacy && *apply && *dry:
			return errors.New("--apply and --dry-run are exclusive")
		}
	}
	if command == "superfan" {
		return runSuperfan(ctx, *name, *launch, *runner, *dry, out)
	}
	c, e := config.Load(config.Expand(*cfg))
	if e != nil {
		return e
	}
	r := config.Repo{}
	sweepAll := legacy != nil && *legacy && *repo == ""
	if (command != "agent" && command != "status" && command != "daemon" && !sweepAll) || *repo != "" {
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
	token, e := resolveToken(ctx, exec, c.TokenEnv)
	if e != nil {
		return e
	}
	gh := github.New(token)
	if c.GitHubApp != nil {
		key, e := c.GitHubApp.Key()
		if e != nil {
			return e
		}
		gh.EnableApp(c.GitHubApp.AppID, key)
	}
	if command != "daemon" {
		var counts *github.RequestCounts
		ctx, counts = github.CountRequests(ctx, command)
		defer func() {
			requests := counts.Snapshot()
			total := 0
			for _, request := range requests {
				total += request.Count
			}
			cache := counts.CacheStats()
			_ = json.NewEncoder(stderr).Encode(map[string]any{"msg": "github requests", "role": command, "total": total, "cache_hits": cache.Hits, "cache_misses": cache.Misses, "memo_hits": cache.MemoHits, "not_modified": cache.NotModified, "by_credential": counts.ByCredential(), "rate_remaining": gh.RateRemaining(), "routes": requests})
		}()
	}
	gh.Stderr = stderr
	// The object cache exists only when a database DSN is configured; the
	// database is dialled on first use, never here.
	if objects := store.NewObjects(os.Getenv); objects != nil {
		gh.Objects = objects
		defer objects.Close()
	}
	// Only looping commands honour X-Poll-Interval; one-shot commands must not
	// stall on an interval recorded by an earlier request.
	gh.Polling = command == "daemon" || command == "watch" || command == "agent"
	s := pipeline.Service{Config: c, GH: gh, Runner: exec, Sink: sink, Notifier: notifier, Out: out}
	o := pipeline.Options{DryRun: *dry, Shadow: shadowMode(command, *apply, shadow != nil && *shadow, c.Daemon.Act)}
	if digest != nil {
		o.Digest = *digest
	}
	if adopt != nil {
		o.Adopt = *adopt
	}
	if pr != nil && (command == "reap" && *pr < 0 || command != "reap" && *pr <= 0) {
		return errors.New("--pr must be positive")
	}
	switch command {
	case "daemon":
		return runDaemon(ctx, &s, gh, *repo, o, stderr)
	case "land":
		return s.Land(ctx, r, *pr, o)
	case "review":
		return s.Review(ctx, r, *pr, *family, o)
	case "release":
		return s.Release(ctx, r, *commit, o)
	case "rollout":
		return s.Rollout(ctx, r, o)
	case "watch":
		return poll(ctx, c.PollInterval, *once || *dry, func(ctx context.Context) error { return s.Watch(ctx, r, o) }, stderr)
	case "reap":
		if !c.Reap.Enabled {
			return errors.New("reap requires reap.enabled")
		}
		// reap.act (checked inside Reap) is the acting gate; daemon.act only
		// applies when the daemon drives the role.
		o.Shadow = o.Shadow || *shadow
		if *legacy {
			return runReapLegacy(ctx, &s, c, *repo, *legacyHost, o)
		}
		return s.Reap(ctx, r, *pr, o)
	case "agent":
		if len(c.Repos) == 0 {
			return errors.New("agent requires at least one configured repository")
		}
		return poll(ctx, c.PollInterval, *once || *dry, func(ctx context.Context) error {
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

// runReapLegacy runs the one-time legacy sweep over --repo or, without it,
// every configured repo. It is not part of the daemon loop.
func runReapLegacy(ctx context.Context, s *pipeline.Service, c config.Config, repo, host string, o pipeline.Options) error {
	var errs []error
	for _, r := range c.Repos {
		if repo != "" && r.Name != repo {
			continue
		}
		if e := s.ReapLegacy(ctx, r, host, o); e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, e))
		}
	}
	return errors.Join(errs...)
}

// poll runs fn once per interval, each iteration as its own pass: the read memo
// and the object-cache breaker never carry over, while request counts run on.
func poll(ctx context.Context, interval string, once bool, fn func(context.Context) error, stderr io.Writer) error {
	d, e := config.Duration(interval)
	if e != nil {
		return e
	}
	for {
		e = fn(github.NewPass(ctx))
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
		if r.Root == "" && !hasContainerTarget(r) {
			errs = append(errs, fmt.Errorf("%s: root required for status", r.Name))
			continue
		}
		if hasContainerTarget(r) {
			if err := containerStatus(c, r, out); err != nil {
				errs = append(errs, err)
			}
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
		b, e := os.ReadFile(filepath.Join(rollout.StateDir(c.StateDir, r.Name), rollout.FileReceiptName))
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
			denied, e := policy.MatchAny(r.Deny, f.Target)
			if e != nil {
				return e
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
		return ledger.Flush(ctx, reps, "")
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
