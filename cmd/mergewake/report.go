package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

func runReport(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "review-latency" {
		return errors.New("usage: mergewake report review-latency [--repo owner/name] [--days 7] [--json]")
	}
	fs := flag.NewFlagSet("report review-latency", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	repo := fs.String("repo", "", "configured owner/name")
	days := fs.Int("days", 7, "merged within this many days")
	asJSON := fs.Bool("json", false, "output JSON")
	if e := fs.Parse(args[1:]); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if fs.NArg() != 0 || *days <= 0 {
		return errors.New("days must be positive and positional arguments are not accepted")
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
	gh := github.New(token)
	gh.Stderr = stderr
	report, e := pipeline.ReviewLatency(ctx, c.StateDir, repos, c.Review.Login, time.Now().Add(-time.Duration(*days)*24*time.Hour), gh)
	if e != nil {
		return e
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(report)
	}
	for _, r := range report.Repos {
		if _, e = fmt.Fprintf(out, "%s: %d merged PRs, %d censored waits\n", r.Repo, r.Count, r.WaitCensored); e != nil {
			return e
		}
		for _, m := range []struct {
			name string
			s    pipeline.LatencyStats
		}{{"wait to first review", r.Wait}, {"review", r.Review}, {"done to merge", r.DoneToMerge}, {"total", r.Total}} {
			if _, e = fmt.Fprintf(out, "  %s: n=%d p50=%s p90=%s max=%s\n", m.name, m.s.Count, seconds(m.s.P50), seconds(m.s.P90), seconds(m.s.Max)); e != nil {
				return e
			}
		}
	}
	if _, e = fmt.Fprintln(out, "Slowest PRs (by total):"); e != nil {
		return e
	}
	for _, r := range report.Slowest {
		if _, e = fmt.Fprintf(out, "  %s#%d head=%s first-seen=%s first-review=%s last-review=%s merged=%s wait=%s censored=%t review=%s done-to-merge=%s total=%s\n", r.Repo, r.PR, r.Head, timestamp(r.FirstSeen), timestamp(r.FirstReview), timestamp(r.LastReview), timestamp(r.Merged), seconds(r.WaitSeconds), r.WaitCensored, seconds(r.ReviewSeconds), seconds(r.DoneToMergeSeconds), seconds(r.TotalSeconds)); e != nil {
			return e
		}
	}
	return nil
}
func seconds(v *float64) string {
	if v == nil {
		return "unknown"
	}
	return fmt.Sprintf("%.0fs", *v)
}

func timestamp(t *time.Time) string {
	if t == nil {
		return "unknown"
	}
	return t.UTC().Format(time.RFC3339)
}
