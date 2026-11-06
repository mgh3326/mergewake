package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/prstatus"
)

const prStatusUsage = "usage: mergewake pr-status --repo owner/name --pr N [--fresh] [--json] [--config PATH]"

// runPRStatus prints a PR's last-pass status snapshot from local state, the
// same answer GET /v1/pr-status gives, with no daemon and no GitHub call.
// --fresh adds the one GraphQL read, and only then resolves a token.
func runPRStatus(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("pr-status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	repo := fs.String("repo", "", "owner/name")
	pr := fs.Int("pr", 0, "pull request number")
	fresh := fs.Bool("fresh", false, "make one GitHub GraphQL read of head, checks, threads and mergeable state, store it in the snapshot and mark the PR for re-evaluation if it changed")
	asJSON := fs.Bool("json", false, "print the JSON the HTTP endpoint returns")
	if e := fs.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	switch {
	case fs.NArg() != 0:
		return errors.New("unexpected positional arguments")
	case !config.ValidRepo(*repo):
		return errors.New("--repo must be owner/name; " + prStatusUsage)
	case *pr <= 0:
		return errors.New("--pr must be positive; " + prStatusUsage)
	}
	c, e := config.Load(config.Expand(*cfg))
	if e != nil {
		return e
	}
	var f prstatus.Fresher
	if *fresh {
		token, e := resolveToken(ctx, integrations.Exec{}, c.TokenEnv)
		if e != nil {
			return e
		}
		gh := github.New(token)
		gh.Stderr = stderr
		f = gh.PRFresh
	}
	return printPRStatus(ctx, c.StateDir, *repo, *pr, f, *asJSON, out)
}

func printPRStatus(ctx context.Context, stateDir, repo string, n int, fresh prstatus.Fresher, asJSON bool, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	resp, _, e := prstatus.Respond(ctx, stateDir, repo, n, fresh, time.Now().UTC())
	if errors.Is(e, prstatus.ErrNotFound) {
		return fmt.Errorf("no status snapshot for %s#%d", repo, n)
	}
	if e != nil {
		return e
	}
	if asJSON {
		return json.NewEncoder(out).Encode(resp)
	}
	var checks []string
	for _, ch := range resp.Checks {
		checks = append(checks, ch.Name+"="+ch.State)
	}
	_, e = fmt.Fprintf(out, "%s#%d %s head=%s observed=%s source=%s\nreasons: %s\nchecks: %s\nreview: %d pass(es), done=%t\nthreads: %d blocking, %d other\nfindings: %d open\n",
		resp.Repo, resp.PR, resp.State, resp.Head, resp.ObservedAt.Format(time.RFC3339), resp.Source,
		orNone(resp.Reasons), orNone(checks), len(resp.Review.Passes), resp.Review.Done, resp.Threads.Blocking, resp.Threads.Other, len(resp.Findings))
	if e == nil && resp.Fresh != nil {
		f := resp.Fresh
		_, e = fmt.Fprintf(out, "fresh_at=%s head=%s checks=%s unresolved=%d mergeable=%s/%s\n", resp.FreshAt.Format(time.RFC3339), f.Head, f.ChecksRollup, f.UnresolvedThreads, f.Mergeable, f.MergeState)
	}
	if e == nil && resp.FreshError != "" {
		_, e = fmt.Fprintf(out, "fresh: %s\n", resp.FreshError)
	}
	return e
}

func orNone(v []string) string {
	if len(v) == 0 {
		return "none"
	}
	return strings.Join(v, ", ")
}
