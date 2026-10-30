package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// resolveToken returns the configured token env var's value, falling back to
// `gh auth token` when it is empty. It fails closed — mergewake never sends an
// anonymous GitHub request. Errors name the env var, never its value.
func resolveToken(ctx context.Context, x integrations.Runner, envName string) (string, error) {
	if t := os.Getenv(envName); t != "" {
		return t, nil
	}
	if _, e := exec.LookPath("gh"); e != nil {
		return "", fmt.Errorf("no GitHub token: %s is not set and gh is not installed; set %s or authenticate gh", envName, envName)
	}
	// --hostname pins github.com so a GH_HOST-selected Enterprise token is
	// never sent to the hardcoded api.github.com endpoint.
	out, e := x.Run(ctx, "gh", []string{"auth", "token", "--hostname", "github.com"}, "", "")
	if e != nil {
		return "", fmt.Errorf("no GitHub token: %s is not set and gh auth token failed; set %s or run gh auth login", envName, envName)
	}
	if t := strings.TrimSpace(string(out)); t != "" {
		return t, nil
	}
	return "", fmt.Errorf("no GitHub token: %s is not set and gh auth token returned nothing; set %s or run gh auth login", envName, envName)
}
