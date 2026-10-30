// Package quota keeps recommendation and admission behind a CLI-neutral interface.
package quota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

type Candidate struct {
	PoolClass  string `json:"pool_class"`
	Launch     string `json:"launch"`
	Profile    string `json:"profile"`
	Kind       string `json:"kind"`
	Subscribed *bool  `json:"subscribed"`
	Effort     string `json:"effort"`
	Reason     string `json:"reason"`
	Source     string `json:"source"`
	Pool       string `json:"pool"`
	Billing    string `json:"billing"`
}
type Provider interface {
	Recommend(context.Context, string) ([]Candidate, error)
	Gate(context.Context, string, string) error
}

// CLI supports scopefuel and a compatible pacelever executable. Never execute
// launch/display strings returned by the quota service.
type CLI struct {
	Runner  integrations.Runner
	Command string
	// Now anchors pace projections; nil means time.Now.
	Now func() time.Time
}

func (c CLI) Recommend(ctx context.Context, grade string) ([]Candidate, error) {
	b, err := c.Runner.Run(ctx, c.Command, []string{"--recommend", grade, "--json"}, "", "")
	if err != nil {
		return nil, err
	}
	var v struct {
		Rows     []Candidate `json:"rows"`
		Excluded []Candidate `json:"excluded"`
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	if err = d.Decode(&v); err != nil {
		return nil, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing quota JSON")
	}
	if v.Rows == nil {
		return nil, errors.New("quota response requires rows")
	}
	out := []Candidate{}
	for _, r := range v.Rows {
		if r.Kind != "candidate" || r.Profile == "" || r.PoolClass == "exclude" || denied(r) {
			continue
		}
		if r.Effort == "" {
			parts := strings.Fields(r.Launch)
			if len(parts) == 3 && parts[0] == r.Profile && parts[1] == "--effort" {
				r.Effort = parts[2]
			}
		}
		blocked := false
		for _, x := range v.Excluded {
			if x.Profile == r.Profile && (x.Effort == "" || x.Effort == r.Effort || denied(x)) {
				blocked = true
				break
			}
		}
		if !blocked {
			out = append(out, r)
		}
	}
	return out, nil
}
func denied(c Candidate) bool {
	text := strings.ToLower(c.Reason + " " + c.Source)
	return (c.Subscribed != nil && !*c.Subscribed) || strings.Contains(text, "subscribed=false") || strings.Contains(text, "unsubscribed")
}
func (c CLI) Gate(ctx context.Context, profile, effort string) error {
	args := []string{"gate", "-m", profile, "--role", "reviewer"}
	if effort != "" {
		args = append(args, "--effort", effort)
	}
	if _, err := c.Runner.Run(ctx, c.Command, args, "", ""); err != nil {
		return fmt.Errorf("quota gate profile=%s effort=%s: %s", profile, effort, integrations.Detail(err))
	}
	return nil
}
