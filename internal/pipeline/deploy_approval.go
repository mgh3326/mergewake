package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/rollout"
)

type deployIdentity struct {
	EnvironmentKnown bool   `json:"environment_known"`
	Config           string `json:"config"`
	Payload          string `json:"payload"`
}

func (s *Service) deploymentIdentity(r config.Repo, m manifest.Manifest, o Options) deployIdentity {
	// Hash locally without exposing values. A changed env file must require a
	// new human answer even when its path and the release commit are unchanged.
	environment := map[string]string{}
	known := true
	for _, spec := range r.Install {
		if spec.EnvFile == "" {
			continue
		}
		name := config.Expand(spec.EnvFile)
		f, e := os.Open(name)
		if e != nil {
			known = false
			environment[name] = "unavailable"
			continue
		}
		h := sha256.New()
		n, e := io.Copy(h, io.LimitReader(f, (1<<20)+1))
		closeErr := f.Close()
		if e != nil || closeErr != nil || n > 1<<20 {
			known = false
			environment[name] = "unavailable"
			continue
		}
		environment[name] = hex.EncodeToString(h.Sum(nil))
	}
	b, _ := json.Marshal(struct {
		Host, Repo, Policy, Root string
		Environment              map[string]string `json:"Environment,omitempty"`
		Deny                     []string
		Install                  []config.Install
		Smoke                    config.Command
		Timeout                  string
	}{s.Config.Host, r.Name, r.Policy, r.Root, environment, r.Deny, r.Install, r.Smoke, r.SmokeTimeout})
	payload, _ := json.Marshal(struct {
		Manifest manifest.Manifest
		Override string
	}{m, o.Digest})
	return deployIdentity{known, manifest.Sum(b), manifest.Sum(payload)}
}

type deployReview struct {
	Identity deployIdentity `json:"identity"`
	Host     string         `json:"host"`
	Repo     string         `json:"repo"`
	Previous string         `json:"previous"`
	Release  string         `json:"release"`
	Paths    []string       `json:"paths"`
	Reasons  []string       `json:"reasons"`
}

func (s *Service) approveDeploy(ctx context.Context, r config.Repo, m manifest.Manifest, state string, container bool, o Options) (bool, error) {
	identity := s.deploymentIdentity(r, m, o)
	review := deployReview{Identity: identity, Host: s.Config.Host, Repo: r.Name, Release: m.Commit, Paths: []string{}, Reasons: []string{}}
	if !identity.EnvironmentKnown {
		review.Reasons = append(review.Reasons, "environment file identity unavailable")
	}
	var priorIdentity deployIdentity
	identityFound, identityErr := fsutil.ReadJSON(filepath.Join(state, approvedInventoryName), &priorIdentity)
	if identityErr != nil || !identityFound || priorIdentity.Config != identity.Config {
		review.Reasons = append(review.Reasons, "host inventory approval missing or changed")
	}
	receiptName := rollout.FileReceiptName
	if container {
		receiptName = rollout.ContainerReceiptName
	} else if len(m.Commands) > 0 {
		receiptName = rollout.CommandReceiptName
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
		if priorIdentity.Payload != identity.Payload {
			review.Reasons = append(review.Reasons, "release payload changed at the deployed commit")
		}
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
							base := strings.ToLower(filepath.Base(name))
							protected := name == r.Policy || strings.HasPrefix(base, ".env") || strings.HasPrefix(base, "env.") || strings.HasSuffix(base, ".env") || strings.HasPrefix(base, "secret") || strings.HasPrefix(base, "credential") || strings.HasSuffix(base, ".pem") || strings.HasSuffix(base, ".key")
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
	// hk's lane mirror caps question plus options at 2048 bytes. Preserve
	// the complete matched-path evidence in a content-addressed document.
	if len(question) > 1400 {
		key := "mergewake/deploy-approval/" + manifest.Sum(data)
		if o.Acting() {
			if err := s.Sink.Record(ctx, key, data); err != nil {
				return false, err
			}
		}
		question = fmt.Sprintf("Approve deployment host=%s repo=%s range=%s..%s release=%s evidence=hk:doc %s", review.Host, review.Repo, review.Previous, review.Release, review.Release, key)
	}
	if !o.Acting() {
		return false, s.Print(struct {
			Status string       `json:"status"`
			Review deployReview `json:"review"`
		}{"hold", review})
	}
	decisions, ok := s.Sink.(interface {
		DeployDecision(context.Context, string, string, time.Time) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("deployment held: handoffkeep decision adapter required (%s)", question)
	}
	approved, e := decisions.DeployDecision(ctx, r.DeployTask, question, s.now())
	if e != nil {
		return false, fmt.Errorf("deployment held: %w", e)
	}
	if !approved || !identity.EnvironmentKnown {
		return false, s.Print(struct {
			Status string       `json:"status"`
			Review deployReview `json:"review"`
		}{"hold", review})
	}
	return true, nil
}
