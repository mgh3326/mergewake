package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/rollout"
)

type deployReview struct {
	Host     string   `json:"host"`
	Repo     string   `json:"repo"`
	Previous string   `json:"previous"`
	Release  string   `json:"release"`
	Paths    []string `json:"paths"`
	Reasons  []string `json:"reasons"`
}

func (s *Service) approveDeploy(ctx context.Context, r config.Repo, m manifest.Manifest, state string, container bool, o Options) (bool, error) {
	review := deployReview{Host: s.Config.Host, Repo: r.Name, Release: m.Commit, Paths: []string{}, Reasons: []string{}}
	receiptName := "installed.json"
	if container {
		receiptName = rollout.ContainerReceiptName
	} else if len(m.Commands) > 0 {
		receiptName = "command-installed.json"
	}
	var receipt struct {
		Host     string            `json:"host"`
		Repo     string            `json:"repo"`
		Commit   string            `json:"commit"`
		Manifest manifest.Manifest `json:"manifest"`
	}
	found, err := fsutil.ReadJSON(filepath.Join(state, receiptName), &receipt)
	valid := found && err == nil
	if container {
		valid = valid && receipt.Repo == r.Name && receipt.Host == s.Config.Host
		review.Previous = receipt.Commit
	} else {
		valid = valid && receipt.Manifest.Repo == r.Name && (receipt.Host == "" || receipt.Host == s.Config.Host)
		review.Previous = receipt.Manifest.Commit
	}
	if !valid || !manifest.SHA.MatchString(review.Previous) {
		review.Previous = "unknown"
		review.Reasons = append(review.Reasons, "missing or invalid previous deployment receipt")
	} else if review.Previous == m.Commit {
		// An existing receipt is not approval to change host recipes or secrets.
		// The installers independently enforce recipe identity and current bytes.
		return true, nil
	} else {
		source, ok := s.GH.(interface {
			ChangedPaths(context.Context, string, string, string) ([]policy.File, error)
		})
		if !ok {
			review.Reasons = append(review.Reasons, "change range adapter unavailable")
		} else {
			files, e := source.ChangedPaths(ctx, r.Name, review.Previous, m.Commit)
			if e != nil {
				review.Reasons = append(review.Reasons, "change range unavailable")
			} else {
				// Load policy from the deployed commit: a release cannot relax its own gate.
				p, e := s.Policy(ctx, r, review.Previous)
				if e != nil || p.DeployApproval == nil {
					review.Reasons = append(review.Reasons, "deployment approval policy unavailable")
				} else {
					matches := map[string]bool{}
					for _, f := range files {
						for _, name := range []string{f.Path, f.Previous} {
							if name == "" {
								continue
							}
							protected := name == r.Policy || name == ".env" || strings.HasPrefix(filepath.Base(name), ".env.")
							lower := strings.ToLower(name)
							for _, segment := range strings.Split(lower, "/") {
								protected = protected || segment == "secrets" || segment == "secret" || segment == "env" || segment == ".env"
							}
							if protected {
								matches[name] = true
								review.Reasons = append(review.Reasons, "environment, secret or approval policy change")
							}
							for _, rule := range p.DeployApproval {
								match, e := policy.MatchAny(rule.Paths, name)
								if e != nil {
									return false, e
								}
								if match {
									matches[name] = true
									review.Reasons = append(review.Reasons, rule.Reason)
								}
							}
						}
					}
					for name := range matches {
						review.Paths = append(review.Paths, name)
					}
				}
			}
		}
	}
	if len(review.Reasons) == 0 {
		return true, nil
	}
	sort.Strings(review.Paths)
	sort.Strings(review.Reasons)
	data, _ := json.Marshal(review)
	question := "Approve deployment " + string(data)
	if !o.Acting() {
		return false, s.Print(struct {
			Status string       `json:"status"`
			Review deployReview `json:"review"`
		}{"hold", review})
	}
	decisions, ok := s.Sink.(interface {
		DeployDecision(context.Context, string, string) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("deployment held: handoffkeep decision adapter required (%s)", question)
	}
	approved, e := decisions.DeployDecision(ctx, r.DeployTask, question)
	if e != nil {
		return false, fmt.Errorf("deployment held: %w", e)
	}
	if !approved {
		return false, s.Print(struct {
			Status string       `json:"status"`
			Review deployReview `json:"review"`
		}{"hold", review})
	}
	return true, nil
}
