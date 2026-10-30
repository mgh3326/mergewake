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
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/rollout"
)

func hasContainerTarget(r config.Repo) bool {
	for _, spec := range r.Install {
		if spec.Kind == "container" {
			return true
		}
	}
	return false
}

// containerStatus reports the running digest, health and retained images per
// container target, and returns drift or unhealthy targets as errors.
func containerStatus(c config.Config, r config.Repo, out io.Writer) error {
	state := filepath.Join(c.StateDir, r.Name, "rollout")
	receipt, _, err := rollout.LoadContainerReceipt(state, c.Host, r.Name)
	if err != nil {
		return err
	}
	if _, e := os.Stat(filepath.Join(state, rollout.ContainerReceiptName)); e != nil {
		return e
	}
	var errs []error
	for _, s := range r.Install {
		denied, e := policy.MatchAny(r.Deny, s.Target)
		if e != nil {
			return e
		}
		line := containerStatusLine{Host: c.Host, Repo: r.Name, ContainerStatus: rollout.ContainerStatus{Target: s.Target, Image: s.Image, Health: "unknown", Status: "denied"}}
		if denied {
			if err = json.NewEncoder(out).Encode(line); err != nil {
				return err
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		st, e := rollout.InspectContainerStatus(ctx, s, receipt.Targets[s.Target], rollout.RuntimeCLI{Runner: integrations.Exec{}, Runtime: s.Runtime}, rollout.CheckHealth)
		cancel()
		line.ContainerStatus = st
		if e != nil {
			errs = append(errs, fmt.Errorf("%s %s: %w", r.Name, s.Target, e))
		}
		if err = json.NewEncoder(out).Encode(line); err != nil {
			return err
		}
	}
	return errors.Join(errs...)
}

type containerStatusLine struct {
	Host string `json:"host"`
	Repo string `json:"repo"`
	rollout.ContainerStatus
}
