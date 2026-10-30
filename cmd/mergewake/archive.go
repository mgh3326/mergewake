package main

import (
	"context"
	"errors"
	"flag"
	"io"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// runArchiveJobs archives the job-dir backlog on configured hosts. It needs no
// GitHub token: job dirs, hk and the Drive folder are its only inputs.
func runArchiveJobs(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("archive-jobs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	olderThan := fs.String("older-than", "", "archive job dirs whose newest entry is older than this, e.g. 14d (default reap.archive.min_age)")
	host := fs.String("host", "", "configured hosts[].name to scan (default all)")
	apply := fs.Bool("apply", false, "move, record and delete; without it only the plan is printed and recorded")
	dry := fs.Bool("dry-run", false, "print the plan without recording rows")
	if e := fs.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *dry && *apply {
		return errors.New("--dry-run and --apply are mutually exclusive")
	}
	c, e := config.Load(config.Expand(*cfg))
	if e != nil {
		return e
	}
	age := *olderThan
	if age == "" {
		age = c.Reap.Archive.MinAge
	}
	d, e := config.Age(age)
	if e != nil {
		return e
	}
	s := pipeline.Service{Config: c, Runner: integrations.Exec{}, Out: out}
	return s.ArchiveJobs(ctx, *host, d, pipeline.Options{DryRun: *dry, Shadow: !*apply})
}
