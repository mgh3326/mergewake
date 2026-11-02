package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"regexp"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// notifyTimeout bounds one call, retries and rate-limit waits included: a
// deployer calling this must not hang on chat.
const notifyTimeout = 2 * time.Minute

var notifySHA = regexp.MustCompile(`^[a-f0-9]{7,64}$`)

const notifyUsage = "usage: mergewake notify deploy --repo owner/name --sha SHA --status ok|failed [--text TEXT] [--config PATH]"

// runNotify posts a deploy another tool performed. It needs no GitHub token. The
// repo need not be one this host watches: a deployer can report any repo.
func runNotify(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "deploy" {
		return errors.New(notifyUsage)
	}
	fs := flag.NewFlagSet("notify deploy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", "~/.config/mergewake/config.yaml", "host YAML config")
	repo := fs.String("repo", "", "owner/name that was deployed")
	sha := fs.String("sha", "", "deployed commit, 7 to 64 lowercase hex characters")
	status := fs.String("status", "", "ok or failed")
	text := fs.String("text", "", "free text for the message; credential-shaped substrings are redacted")
	if e := fs.Parse(args[1:]); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	switch {
	case fs.NArg() != 0:
		return errors.New("unexpected positional arguments")
	case !config.ValidRepo(*repo):
		return errors.New("--repo must be owner/name")
	case !notifySHA.MatchString(*sha):
		return errors.New("--sha must be 7 to 64 lowercase hex characters")
	case *status != "ok" && *status != "failed":
		return errors.New("--status must be ok or failed")
	}
	c, e := config.Load(config.Expand(*cfg))
	if e != nil {
		return e
	}
	if !c.Discord.Enabled {
		return errors.New("notify requires discord.enabled")
	}
	ctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()
	posted, e := pipeline.NewFeed(c, nil).NotifyDeploy(ctx, c.Host, *repo, *sha, *status == "ok", *text, time.Now())
	if e != nil {
		return e
	}
	return json.NewEncoder(out).Encode(map[string]any{"repo": *repo, "sha": *sha, "status": *status, "posted": posted})
}
