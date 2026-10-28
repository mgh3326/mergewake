package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/rollout"
)

func commandStatus(c config.Config, r config.Repo, out io.Writer) error {
	b, err := os.ReadFile(filepath.Join(c.StateDir, r.Name, "rollout", "command-installed.json"))
	if err != nil {
		return err
	}
	var receipt rollout.CommandReceipt
	if err = json.Unmarshal(b, &receipt); err != nil {
		return err
	}
	if err = receipt.Manifest.Validate(); err != nil {
		return err
	}
	if receipt.Host != c.Host || receipt.Manifest.Repo != r.Name {
		return errors.New("command receipt identity mismatch")
	}
	var errs []error
	for _, s := range r.Install {
		denied := false
		for _, g := range r.Deny {
			match, e := policy.Match(g, s.Target)
			if e != nil {
				return e
			}
			denied = denied || match
		}
		state := "healthy"
		installed := receipt.Targets[s.Target]
		if !denied && !manifest.SHA.MatchString(installed.SHA) {
			return errors.New("missing or invalid installed command SHA")
		}
		if denied {
			state = "denied"
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			e := rollout.VerifyCommand(ctx, integrations.Exec{}, s, r.Name, installed.SHA, config.Expand(r.Root))
			cancel()
			if e != nil {
				state = "drift"
				errs = append(errs, fmt.Errorf("%s %s: %w", r.Name, s.Target, e))
			}
		}
		if err = json.NewEncoder(out).Encode(map[string]any{"host": c.Host, "repo": r.Name, "target": s.Target, "sha": installed.SHA, "previous_sha": installed.PreviousSHA, "status": state}); err != nil {
			return err
		}
	}
	return errors.Join(errs...)
}
