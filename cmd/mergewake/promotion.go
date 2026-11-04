package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/promotion"
)

// exitError carries a process exit code other than 1 (admit --deploy uses 75).
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

func suiteFor(repo string) (promotion.Suite, error) {
	all, e := promotion.Suites()
	if e != nil {
		return promotion.Suite{}, e
	}
	s, ok := all[repo]
	if !ok {
		return promotion.Suite{}, fmt.Errorf("no checked-in promotion suite for %s", repo)
	}
	return s, nil
}

func runPromote(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("promote", flag.ContinueOnError)
	fs.SetOutput(stderr)
	repo := fs.String("repo", "", "owner/name with a checked-in promotion suite")
	sha := fs.String("sha", "", "full main commit SHA to certify")
	seed := fs.Bool("seed", false, "first promotion: the deployable branch must not exist yet")
	dry := fs.Bool("dry-run", false, "certify and print the record; write nothing")
	identity := fs.String("controller", "", "promotion identity recorded in the record (the GitHub App's name)")
	generation := fs.Int64("generation", 1, "controller generation recorded in the record")
	tokenEnv := fs.String("token-env", "GITHUB_TOKEN", "environment variable holding the promotion identity's token")
	lockDir := fs.String("lock-dir", "~/.local/state/mergewake/promotion", "directory for the per-repo promotion lock")
	if e := fs.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if fs.NArg() != 0 || *repo == "" || *sha == "" {
		return errors.New("usage: mergewake promote --repo OWNER/NAME --sha SHA --controller NAME [--seed] [--dry-run]")
	}
	s, e := suiteFor(*repo)
	if e != nil {
		return e
	}
	token := os.Getenv(*tokenEnv)
	if token == "" && !*dry {
		return fmt.Errorf("%s is empty; promotion writes need the promotion identity's token", *tokenEnv)
	}
	p := &promotion.Promoter{
		GH:         promotion.API{C: github.New(token)},
		Registry:   promotion.OCIRegistry{HTTP: &http.Client{Timeout: 45 * time.Second}},
		Lock:       promotion.FileLock{Dir: config.Expand(*lockDir)},
		Now:        time.Now,
		Identity:   *identity,
		Generation: *generation,
	}
	rec, e := p.Promote(ctx, *repo, s, promotion.Options{SHA: *sha, Seed: *seed, DryRun: *dry})
	if e != nil {
		return e
	}
	if !*dry {
		fmt.Fprintf(stderr, "promoted %s seq %d: %s@%s\n", *repo, rec.Seq, rec.Artifact.Image, rec.Artifact.Digest)
	}
	_, e = fmt.Fprintln(out, string(rec.Canonical()))
	return e
}

func runAdmit(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("admit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	repo := fs.String("repo", "", "owner/name with a checked-in promotion suite")
	asJSON := fs.Bool("json", false, "print the admitted record instead of the digest")
	deploy := fs.Bool("deploy", false, "deploy the admitted digest with shiftgate (skip if active, refuse if held, 75 = retry)")
	service := fs.String("service", "", "shiftgate service name (with --deploy)")
	sgPath := fs.String("shiftgate", "/usr/local/bin/shiftgate", "full path of shiftgate (with --deploy)")
	sudo := fs.Bool("sudo", true, "run shiftgate under sudo -n (with --deploy)")
	state := fs.String("state-dir", "~/.local/state/mergewake/admit", "per-service deploy lock and failed-image ledger (with --deploy)")
	cache := fs.String("cache", "~/.cache/mergewake/promotion", "bare git cache for promotion tag objects")
	if e := fs.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if fs.NArg() != 0 || *repo == "" || (*deploy && *service == "") {
		return errors.New("usage: mergewake admit --repo OWNER/NAME [--json] [--deploy --service NAME [--shiftgate PATH] [--sudo=false]]")
	}
	s, e := suiteFor(*repo)
	if e != nil {
		return e
	}
	reader := promotion.Git{Cache: config.Expand(*cache), Exec: promotion.OSExec{}}
	if !*deploy {
		adm, e := promotion.Admit(ctx, reader, *repo, s)
		if e != nil {
			return e
		}
		if *asJSON {
			return json.NewEncoder(out).Encode(adm.Record)
		}
		_, e = fmt.Fprintln(out, adm.Digest)
		return e
	}
	stateDir := config.Expand(*state)
	sg := promotion.Shiftgate{Path: *sgPath, Sudo: *sudo, Exec: promotion.OSExec{}}
	ledger := promotion.FileLedger{Path: filepath.Join(stateDir, "failed-"+*service)}
	adm, outcome, code, e := promotion.Pass(ctx, reader, *repo, s, *service, sg, promotion.FileLock{Dir: stateDir}, ledger)
	fmt.Fprintf(stderr, "%s seq %d %s: %s\n", *repo, adm.Seq, adm.Digest, outcome)
	if e != nil {
		return &exitError{code, e}
	}
	if code != 0 {
		return &exitError{code, fmt.Errorf("%s: retry on the next run", outcome)}
	}
	return nil
}
