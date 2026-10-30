package pipeline

import (
	"context"
	"errors"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

func (s *Service) ledger() outcomes.Ledger {
	return outcomes.Ledger{Dir: s.Config.StateDir, Now: s.Now}
}
func trailer(body, key string) string {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `: ([^\r\n]+?)[\t ]*$`)
	matches := re.FindAllStringSubmatch(body, -1)
	if len(matches) != 1 {
		return ""
	}
	return matches[0][1]
}
func parsedTime(value string) *time.Time {
	v, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	return &v
}
func (s *Service) ObservePR(ctx context.Context, r config.Repo, p github.Pull, o Options) error {
	if o.DryRun {
		return nil
	}
	err := s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) {
		v.Head = p.Head.SHA
		v.Shadow = o.Shadow
		if opened := parsedTime(p.CreatedAt); opened != nil {
			v.OpenedAt = *opened
		}
		v.Family = config.CanonicalFamily(trailer(p.Body, "Mergewake-Author-Family"))
		v.Profile = ""
		v.Effort = ""
		v.Experiment = ""
		v.Arm = ""
		author := strings.Split(trailer(p.Body, "Mergewake-Author-Profile"), "@")
		if len(author) == 2 && regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`).MatchString(author[0]) {
			v.Profile = author[0]
			switch author[1] {
			case "low", "medium", "high", "xhigh", "max":
				v.Effort = author[1]
			}
		}
		experiment := strings.SplitN(trailer(p.Body, "Mergewake-Experiment"), "/", 2)
		if len(experiment) == 2 {
			v.Experiment = experiment[0]
			v.Arm = experiment[1]
		}
		v.Model = trailer(p.Body, "Mergewake-Author-Model")
		if profile, ok := s.Config.Review.Profiles[v.Profile]; ok && v.Model == "" {
			for i := 0; i+1 < len(profile.Args); i += 2 {
				if profile.Args[i] == "--model" {
					v.Model = profile.Args[i+1]
				}
			}
		}
		v.Grade = trailer(p.Body, "Mergewake-Grade")
		if v.Grade == "" {
			v.Grade = s.Config.Review.DefaultGrade
		}
		if !config.ValidGrade(v.Grade) {
			v.Grade = ""
		}
		v.Tier = trailer(p.Body, "Mergewake-Tier")
		if v.Tier == "" {
			v.Tier = s.Config.Outcomes.Tier
		}
		if v.Tier == "" {
			v.Tier = "T1"
		}
		switch v.Tier {
		case "T0", "T1", "T2", "T3":
		default:
			v.Tier = ""
		}
		if p.ClosedAt != nil {
			v.ClosedAt = parsedTime(*p.ClosedAt)
		}
		if p.MergedAt != nil {
			v.MergedAt = parsedTime(*p.MergedAt)
			v.Final = true
			v.Decision = "landed"
			if v.MergedAt != nil && !v.OpenedAt.IsZero() {
				seconds := v.MergedAt.Sub(v.OpenedAt).Seconds()
				if seconds >= 0 {
					v.TimeToMerge = &seconds
				}
			}
		} else if p.State == "closed" {
			v.Final = true
			v.Decision = "blocked"
		} else {
			v.Final = false
		}
	})
	if err != nil {
		return err
	}
	state, err := s.ledger().Read()
	if err != nil {
		return err
	}
	v := state.Latest[outcomes.Key(r.Name, p.Number)]
	if _, known := v.Reviews[p.Head.SHA]; !known {
		if source, ok := s.GH.(interface {
			ReviewOutcomes(context.Context, string, int, string) (map[string]map[string]int, error)
		}); ok {
			reviews, e := source.ReviewOutcomes(ctx, r.Name, p.Number, s.Config.Review.Login)
			if e != nil {
				return e
			}
			if len(reviews) > 0 {
				sameFamily := false
				for _, head := range slices.Sorted(maps.Keys(reviews)) {
					passes, e := s.GH.ReviewPasses(ctx, r.Name, p.Number, head, s.Config.Review.Login)
					if e != nil {
						return e
					}
					for _, receipt := range passes {
						sameFamily = sameFamily || receipt.SameFamily
					}
				}
				if e = s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) {
					if v.Reviews == nil {
						v.Reviews = map[string]map[string]int{}
					}
					for head, counts := range reviews {
						v.Reviews[head] = counts
					}
					v.Findings = map[string]int{}
					for _, counts := range v.Reviews {
						for severity, n := range counts {
							v.Findings[severity] += n
						}
					}
					v.SameFamily = v.SameFamily || sameFamily
				}); e != nil {
					return e
				}
			}
		}
	}
	if v.FirstCIGreen == nil {
		if source, ok := s.GH.(interface {
			FirstPRCI(context.Context, string, github.Pull, []string) (*bool, error)
		}); ok {
			head, e := s.GH.Head(ctx, r.Name, r.Branch)
			if e != nil {
				return e
			}
			policy, e := s.Policy(ctx, r, head)
			if e == nil {
				first, e := source.FirstPRCI(ctx, r.Name, p, policy.RequiredChecks)
				if e != nil {
					return e
				}
				if first != nil {
					if e = s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) { v.FirstCIGreen = first }); e != nil {
						return e
					}
				}
			} else if !github.NotFound(e) {
				return e
			}
		}
	}
	if v.MergedAt != nil && v.Reverted == nil {
		if v.RevertPR == 0 && p.MergeCommitSHA != "" {
			if source, ok := s.GH.(interface {
				WatchRevert(context.Context, string, int, string) (*github.Pull, error)
			}); ok {
				recovery, e := source.WatchRevert(ctx, r.Name, p.Number, p.MergeCommitSHA)
				if e != nil {
					return e
				}
				if recovery != nil {
					v.RevertPR = recovery.Number
					v.RevertURL = recovery.HTMLURL
					if e = s.recordRevert(r.Name, p.Number, v.RevertURL); e != nil {
						return e
					}
				}
			}
		}

		var reverted *bool
		if v.RevertPR > 0 {
			recovery, e := s.GH.Pull(ctx, r.Name, v.RevertPR)
			if e != nil {
				return e
			}
			if recovery.MergedAt != nil {
				at := parsedTime(*recovery.MergedAt)
				if at != nil && !at.Before(*v.MergedAt) && !at.After(v.MergedAt.Add(7*24*time.Hour)) {
					yes := true
					reverted = &yes
				}
			}
		}
		if reverted == nil && s.now().After(v.MergedAt.Add(7*24*time.Hour)) {
			no := false
			reverted = &no
		}
		if reverted != nil {
			return s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) { v.Reverted = reverted })
		}
	}
	return nil
}
func (s *Service) recordReview(repo string, pr int, head string, findings []github.Finding, receipt github.PassReceipt, dry bool) error {
	if dry {
		return nil
	}
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		counts := map[string]int{}
		for _, f := range findings {
			severity := f.Severity
			if severity == "" {
				severity = "unspecified"
			}
			counts[severity]++
		}
		if v.Reviews == nil {
			v.Reviews = map[string]map[string]int{}
		}
		v.Reviews[head] = counts
		v.Findings = map[string]int{}
		for _, review := range v.Reviews {
			for k, n := range review {
				v.Findings[k] += n
			}
		}
		v.SameFamily = v.SameFamily || receipt.SameFamily
	})
}
func (s *Service) recordDecision(repo string, pr int, action string, shadow, sameFamily, dry bool) error {
	if dry {
		return nil
	}
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		v.Action = action
		v.Shadow = shadow
		v.SameFamily = v.SameFamily || sameFamily
		if !v.Final {
			if action == "merge" {
				v.Decision = "eligible"
			} else {
				v.Decision = "blocked"
			}
		}
	})
}
func (s *Service) recordRevert(repo string, pr int, url string) error {
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		v.RevertURL = url
		n, _ := strconv.Atoi(url[strings.LastIndex(url, "/")+1:])
		v.RevertPR = n
	})
}

// Revisit PRs no longer in the open list: this observes external desk merges,
// closures and recovery merges, even while the daemon itself is shadow-only.
func (s *Service) ReconcileOutcomes(ctx context.Context, r config.Repo, open map[int]bool, o Options) error {
	if o.DryRun {
		return nil
	}
	state, err := s.ledger().Read()
	if err != nil {
		return err
	}
	var errs []error
	for _, key := range slices.Sorted(maps.Keys(state.Latest)) {
		v := state.Latest[key]
		if v.Repo != r.Name || open[v.PR] {
			continue
		}
		if v.Final && v.Attributed() && v.FirstCIGreen != nil && (v.MergedAt == nil || v.Reverted != nil) {
			continue
		}
		p, e := s.GH.Pull(ctx, r.Name, v.PR)
		if e == nil {
			e = s.ObservePR(ctx, r, p, o)
		}
		if e != nil {
			errs = append(errs, e)
		}
	}
	errs = append(errs, s.FlushOutcomes(ctx))
	return errors.Join(errs...)
}
func (s *Service) FlushOutcomes(ctx context.Context) error {
	command := s.Config.Outcomes.Command
	if command == "" {
		command = s.Config.Review.Quota
	}
	if command == "" {
		command = "scopefuel"
	}
	return s.ledger().Flush(ctx, outcomes.CLI{Runner: s.Runner, Command: command})
}
