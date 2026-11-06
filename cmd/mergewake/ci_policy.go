package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/mgh3326/mergewake/internal/cipolicy"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

const ciPolicyUsage = "usage: mergewake ci-policy [--repo owner/name] [--json] [--config PATH]"

// ciPolicySource builds the read-only GitHub reader; tests replace it.
var ciPolicySource = func(token string, stderr io.Writer) cipolicy.Source {
	gh := github.New(token)
	gh.Stderr = stderr
	return gh
}

// runCIPolicy reports, for one or all configured repos, whether the repo
// follows the fleet main-CI default. It only reads GitHub.
func runCIPolicy(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("ci-policy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	repo := fs.String("repo", "", "one configured owner/name (default: every configured repo)")
	asJSON := fs.Bool("json", false, "print a JSON array with one report per repo")
	if e := fs.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments; " + ciPolicyUsage)
	}
	c, e := config.Load(config.Expand(*cfg))
	if e != nil {
		return e
	}
	repos := c.Repos
	if *repo != "" {
		r, e := c.Repo(*repo)
		if e != nil {
			return e
		}
		repos = []config.Repo{r}
	}
	token, e := resolveToken(ctx, integrations.Exec{}, c.TokenEnv)
	if e != nil {
		return e
	}
	src := ciPolicySource(token, stderr)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	reports := make([]cipolicy.Report, 0, len(repos))
	failed := 0
	for _, r := range repos {
		rep, e := checkCIPolicy(ctx, src, r)
		if e != nil {
			failed++
			rep = cipolicy.Report{Repo: r.Name, Branch: r.Branch, Verdict: "error", Reasons: []string{e.Error()}, WatchedChecks: []string{}, Workflows: []cipolicy.Workflow{}}
		}
		reports = append(reports, rep)
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if e := enc.Encode(reports); e != nil {
			return e
		}
	} else {
		for _, rep := range reports {
			if rep.Verdict == "error" {
				fmt.Fprintf(out, "%s ERROR: %s\n", rep.Repo, rep.Reasons[0])
				continue
			}
			fmt.Fprint(out, rep.Text())
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d repos could not be read", failed, len(repos))
	}
	return nil
}

func checkCIPolicy(ctx context.Context, src cipolicy.Source, r config.Repo) (cipolicy.Report, error) {
	head, e := src.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return cipolicy.Report{}, e
	}
	in, e := cipolicy.Fetch(ctx, src, r, head)
	if e != nil {
		return cipolicy.Report{}, e
	}
	return cipolicy.Check(in), nil
}
