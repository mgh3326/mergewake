package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/quota"
	"github.com/mgh3326/mergewake/internal/reviewer"
)

// errPassPending means another host holds the pass claim for this head.
var errPassPending = errors.New("review pass claimed elsewhere")

// passExecutor produces one review pass's findings. claim takes the pass's
// head lease and must succeed before any reviewer is invoked; it is idempotent
// within the pass, so a fallback executor reuses the claim already taken.
// An outcome without a reviewer means no reviewer produced the pass, with the
// reasons in skips; an error ends the review.
type passExecutor interface {
	runPass(ctx context.Context, plan *passPlan, claim func() (bool, error)) (passOutcome, error)
}

type passOutcome struct {
	reviewer string
	family   string
	executor string
	// fallback says why the pass ran on a less preferred path.
	fallback string
	findings []github.Finding
	skips    []reviewSkip
}

// headlessExecutor invokes the plan's candidate profiles one by one on the
// diff in an empty scratch directory, falling through on failure.
type headlessExecutor struct {
	s         *Service
	repo      string
	pr        int
	diff      string
	dir       string
	control   string
	provider  quota.Provider
	cli       quota.CLI
	freePool  map[string]string
	family    func(string) string
	failed    map[string]bool // profiles whose run failed earlier in this review
	exhausted map[string]bool // freePool keys that hit a quota failure
	none      func() error    // explains a plan with no admitted candidate
}

func (h *headlessExecutor) runPass(ctx context.Context, plan *passPlan, claim func() (bool, error)) (passOutcome, error) {
	s := h.s
	var out passOutcome
	if len(plan.candidates) == 0 {
		out.skips = append(out.skips, reviewSkip{Reviewer: "headless", Reason: h.none().Error()})
		return out, nil
	}
	profiles := s.Config.Review.Profiles
	for i, name := range plan.candidates {
		skip := func(reason string, reset time.Time) {
			sk := reviewSkip{Reviewer: name, Reason: reason}
			if !reset.IsZero() {
				sk.Reset = reset.UTC().Format(time.RFC3339)
			}
			out.skips = append(out.skips, sk)
		}
		p := profiles[name]
		if h.failed[name] {
			skip("failed earlier in this review", time.Time{})
			continue
		}
		if key := h.freePool[name]; key != "" && h.exhausted[key] {
			skip("free pool rate-limited earlier in this review", time.Time{})
			continue
		}
		gate := p.GateProfile
		if gate == "" {
			gate = name
		}
		if available, ok := s.Runner.(interface{ Available(string) error }); ok {
			if available.Available(config.Expand(p.Command)) != nil {
				skip("reviewer CLI not installed", time.Time{})
				continue
			}
		}
		if e := h.provider.Gate(ctx, gate, p.GateEffort); e != nil {
			skip("quota gate refused: "+e.Error(), time.Time{})
			continue
		}
		candidateControl := filepath.Join(h.control, fmt.Sprintf("%d-%d", plan.pass, i))
		if e := os.Mkdir(candidateControl, 0700); e != nil {
			return out, e
		}
		invocation, e := reviewer.Prepare(p, candidateControl)
		if e != nil {
			skip("reviewer profile rejected", time.Time{})
			continue
		}
		plan.invocation = invocation
		acquired, e := claim()
		if e != nil {
			return out, e
		}
		if !acquired {
			return out, errPassPending
		}
		if filepath.Base(p.Command) == "cmd" {
			// The hook also denies control-flow tools that bypass permission deny lists.
			if e = os.MkdirAll(filepath.Join(h.dir, ".commandcode"), 0700); e != nil {
				return out, e
			}
			settings := `{"permissions":{"deny":["*"]},"hooks":{"PreToolUse":[{"matcher":".*","hooks":[{"type":"command","command":"printf '%s' '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"Diff-only review: all tools disabled\"}}'"}]}]}}`
			if e = os.WriteFile(filepath.Join(h.dir, ".commandcode", "settings.json"), []byte(settings), 0600); e != nil {
				return out, e
			}
		}
		output, runErr := reviewer.Run(ctx, s.Runner, plan.invocation, promptFor(plan.lens)+h.diff, h.dir, s.Config.TokenEnv)
		if runErr == nil {
			var findings []github.Finding
			if findings, e = parseFindings(output, h.diff); e == nil {
				out.reviewer, out.family, out.findings = name, h.family(name), findings
				return out, nil
			}
			runErr = errors.New("invalid review output")
		}
		// The whole review shares one deadline: nothing is left for another reviewer.
		if ctx.Err() != nil {
			return out, errors.Join(runErr, ctx.Err())
		}
		h.failed[name] = true
		failure := quota.Classify(runErr, s.now())
		if !failure.Quota {
			skip("review failed", time.Time{})
			continue
		}
		skip("quota exhausted", failure.Reset)
		if key := h.freePool[name]; key != "" {
			h.exhausted[key] = true
		}
		if note := s.observeExhausted(ctx, h.cli, p, failure, h.repo, h.pr); note != "" {
			out.skips[len(out.skips)-1].Note = note
		}
	}
	return out, nil
}
