// Package pipeline coordinates policy, GitHub, local reviewers, and adapters.
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/rollout"
)

type GitHub interface {
	Pull(context.Context, string, int) (github.Pull, error)
	Files(context.Context, string, int) ([]policy.File, error)
	Diff(context.Context, string, int) (string, error)
	Merge(context.Context, string, int, string) error
	Head(context.Context, string, string) (string, error)
	CreateRef(context.Context, string, string, string) error
	RecoveryActive(context.Context, string) (bool, error)
	Check(context.Context, string, string, []string) (github.CI, error)
	CheckStates(context.Context, string, string, []string) (map[string]github.CI, error)
	Reviewed(context.Context, string, int, string, string) (bool, error)
	ReviewPasses(context.Context, string, int, string, string) ([]github.PassReceipt, error)
	ReviewClaimed(context.Context, string, int, string) (bool, error)
	ReviewLease(context.Context, string, string, time.Duration) (string, bool, error)
	ClaimReview(context.Context, string, string, string, time.Duration) (bool, error)
	ReviewFeedback(context.Context, string, int) (github.ReviewFeedback, error)
	Unresolved(context.Context, string, int) (int, error)
	PostReview(context.Context, string, int, string, []github.Finding, github.PassReceipt) error
	Commit(context.Context, string, string) (github.Commit, error)
	PRsForCommit(context.Context, string, string) ([]github.Pull, error)
	MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error)
	Ancestor(context.Context, string, string, string) (bool, error)
	ReadFile(context.Context, string, string, string) ([]byte, error)
	Revert(context.Context, string, string, string, string, int) (string, error)
	Publish(context.Context, manifest.Manifest) error
	Latest(context.Context, string) (manifest.Manifest, error)
}
type Service struct {
	Config   config.Config
	GH       GitHub
	Runner   integrations.Runner
	Sink     integrations.Sink
	Notifier integrations.Notifier
	Out      io.Writer
	// Now is the clock for stamps and age decisions; nil means time.Now.
	Now func() time.Time
}
type Options struct {
	DryRun, Shadow, Observed bool
	Digest, Adopt            string
}

func (s *Service) Print(v any) error { return json.NewEncoder(s.Out).Encode(v) }
func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
func (s *Service) state(repo string) string { return filepath.Join(s.Config.StateDir, repo) }
func (s *Service) audit(repo, command string, value any, dry bool) error {
	record := struct {
		Time    time.Time `json:"time"`
		Command string    `json:"command"`
		Value   any       `json:"value"`
	}{s.now().UTC(), command, value}
	b, e := json.Marshal(record)
	if e != nil {
		return e
	}
	if e = s.Print(record); e != nil {
		return e
	}
	if dry {
		return nil
	}
	dir := s.state(repo)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(dir, "decisions.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.Write(append(b, '\n'))
	if e == nil {
		e = f.Sync()
	}
	return e
}
func (s *Service) Policy(ctx context.Context, r config.Repo, sha string) (policy.Policy, error) {
	var p policy.Policy
	b, e := s.GH.ReadFile(ctx, r.Name, sha, r.Policy)
	if e != nil {
		return p, e
	}
	if e = config.Decode(bytes.NewReader(b), &p); e != nil {
		return p, e
	}
	return p, p.Validate()
}

// reviewGate grades only current-head findings. CI bots are optional unless
// the trusted policy explicitly names them in required_bots. The caller has
// already verified the mandatory exact-head mergewake passes.
func (s *Service) reviewGate(ctx context.Context, r config.Repo, n int, class string, p policy.Policy, head string) (string, *policy.GateVerdict, []string, error) {
	fb, e := s.GH.ReviewFeedback(ctx, r.Name, n)
	if e != nil {
		return "", nil, nil, e
	}
	if fb.HeadSHA != head {
		return "", nil, nil, errors.New("review feedback is not bound to the decision head")
	}
	gate := policy.ReviewGate{}.WithDefaults()
	if p.ReviewGate != nil {
		gate = p.ReviewGate.WithDefaults()
	}
	login := policy.NormalizeAuthor(s.Config.Review.Login)
	if login != "" {
		gate.Block = append(gate.Block,
			policy.ReviewGateRule{Author: login, Severity: []string{"critical", "high", "blocking"}},
			policy.ReviewGateRule{Author: login, Severity: []string{"major", "medium", "minor", "low", "unspecified", ""}, Classes: []string{"blocking"}})
	}
	bots := gate.GateAuthors()
	for _, a := range gate.IgnoreAuthors {
		bots[policy.NormalizeAuthor(a)] = true
	}
	var fs []policy.ReviewFinding
	for _, f := range fb.Findings(bots, login) {
		if f.CommitSHA == head {
			fs = append(fs, f)
		}
	}
	v := gate.Evaluate(fs, class)
	missing := gate.MissingRequiredBots(fs, fb.ReviewAuthors, head)
	switch {
	case len(v.Blocking) > 0:
		return "wait-for-blocking-findings", &v, missing, nil
	case len(v.Triage) > 0:
		return "wait-for-triage-reply", &v, missing, nil
	case len(missing) > 0:
		return "wait-for-required-bots", &v, missing, nil
	}

	return "", &v, missing, nil
}

// distinctPasses validates numbered passes and independent lenses when the
// same reviewer family supplies both passes. Duplicates never add coverage.
func distinctPasses(receipts []github.PassReceipt, needed int) (int, bool) {
	passes := map[int]github.PassReceipt{}
	for _, r := range receipts {
		if r.Pass < 1 || r.Pass > needed {
			continue
		}
		if prior, ok := passes[r.Pass]; ok && prior != r {
			return len(passes), false
		}
		passes[r.Pass] = r
	}
	if len(passes) < needed {
		return len(passes), false
	}
	if needed == 2 {
		a, b := passes[1], passes[2]
		if a.Family == "" || b.Family == "" {
			return len(passes), false
		}
		if a.Family == b.Family && (a.Lens == "" || b.Lens == "" || a.Lens == "default" || b.Lens == "default" || a.Lens == b.Lens) {
			return len(passes), false
		}
	}
	return len(passes), true
}

func (s *Service) Land(ctx context.Context, r config.Repo, n int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "land-error", map[string]any{"pr": n, "error": retErr.Error()}, o.DryRun))
		}
	}()
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if !o.Observed {
		if e = s.ObservePR(ctx, r, pr, o); e != nil {
			return e
		}
	}
	if pr.State != "open" || pr.Draft || pr.Base.Ref != r.Branch {
		return errors.New("land requires an open, ready PR against the configured branch")
	}
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return e
	}
	files, e := s.GH.Files(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if len(files) != pr.ChangedFiles {
		return errors.New("incomplete PR file list")
	}
	d := p.ClassifyGuarded(files, r.Policy)
	d.Shadow = d.Shadow || o.Shadow
	d.Action = "wait-for-ci"
	// While main is red or its required evidence is unknown, ordinary landing
	// pauses; only a recovery PR from the watch latch may still land.
	watchChecks := p.RequiredChecks
	if len(p.WatchChecks) > 0 {
		watchChecks = p.WatchChecks
	}
	mainCI, e := s.GH.Check(ctx, r.Name, head, watchChecks)
	if e != nil {
		return e
	}
	recovery := pr.Head.Ref == github.RecoveryBranch && pr.Head.Repo.FullName == r.Name
	paused := mainCI != github.Green && !recovery
	if paused {
		d.Action = "pause-red-main"
		if mainCI != github.Red {
			d.Action = "pause-main-unknown"
		}
		if d.Shadow {
			d.Action = "would-" + d.Action
		}
	}
	ci, e := s.GH.Check(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if e != nil {
		return e
	}
	var verdict *policy.GateVerdict
	var missingBots []string
	var reviewed *int
	sameFamily := false
	if !paused && ci == github.Green {
		d.Action = "merge"
		needed := 1
		if d.Class == "blocking" {
			needed = 2
		}
		receipts, e := s.GH.ReviewPasses(ctx, r.Name, n, pr.Head.SHA, s.Config.Review.Login)
		if e != nil {
			return e
		}
		count, complete := distinctPasses(receipts, needed)
		reviewed = &count
		for _, receipt := range receipts {
			sameFamily = sameFamily || receipt.SameFamily
		}
		if !complete {
			d.Action = "wait-for-independent-review"
		}

		if d.Action == "merge" {
			action, v, g, e := s.reviewGate(ctx, r, n, d.Class, p, pr.Head.SHA)
			if e != nil {
				return e
			}
			verdict, missingBots = v, g
			if action != "" {
				d.Action = action
			}
		}
	}
	if e = s.audit(r.Name, "land", struct {
		PR          int                 `json:"pr"`
		Head        string              `json:"head"`
		Base        string              `json:"base"`
		CI          github.CI           `json:"ci"`
		Main        github.CI           `json:"main"`
		Reviews     *int                `json:"review_passes,omitempty"`
		Findings    *policy.GateVerdict `json:"findings,omitempty"`
		MissingBots []string            `json:"missing_required_bots,omitempty"`
		SameFamily  bool                `json:"same_family,omitempty"`
		policy.Decision
	}{n, pr.Head.SHA, head, ci, mainCI, reviewed, verdict, missingBots, sameFamily, d}, o.DryRun); e != nil {
		return e
	}
	if e = s.recordDecision(r.Name, n, d.Action, d.Shadow, sameFamily, o.DryRun); e != nil {
		return e
	}
	if d.Action != "merge" || d.Shadow || o.DryRun {
		return nil
	}
	// Recheck CI and the trusted policy revision immediately before SHA-bound merge.
	fresh, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	if fresh != head {
		return errors.New("base changed during decision; rerun land with current policy")
	}
	ci, e = s.GH.Check(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if e != nil {
		return e
	}
	if ci != github.Green {
		return errors.New("CI changed during decision")
	}
	freshPR, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if freshPR.Head.SHA != pr.Head.SHA || freshPR.State != "open" || freshPR.Draft {
		return errors.New("PR changed during decision")
	}
	needed := 1
	if d.Class == "blocking" {
		needed = 2
	}
	receipts, e := s.GH.ReviewPasses(ctx, r.Name, n, pr.Head.SHA, s.Config.Review.Login)
	if e != nil {
		return e
	}
	if _, complete := distinctPasses(receipts, needed); !complete {
		return errors.New("review passes changed during decision")
	}

	action, _, _, e := s.reviewGate(ctx, r, n, d.Class, p, pr.Head.SHA)
	if e != nil {
		return e
	}
	if action != "" {
		return errors.New("review findings changed during decision")
	}
	if e = s.GH.Merge(ctx, r.Name, n, pr.Head.SHA); e != nil {
		return e
	}
	merged, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if e = s.ObservePR(ctx, r, merged, o); e != nil {
		return e
	}
	return s.FlushOutcomes(ctx)
}
func (s *Service) Release(ctx context.Context, r config.Repo, sha string, o Options) error {
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	if sha == "" {
		sha = head
	}
	if !manifest.SHA.MatchString(sha) {
		return errors.New("release requires a full commit SHA")
	}
	ok, e := s.GH.Ancestor(ctx, r.Name, sha, head)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("release commit is not on the main branch")
	}
	p, e := s.Policy(ctx, r, sha)
	if e != nil {
		return e
	}
	ci, e := s.GH.Check(ctx, r.Name, sha, p.RequiredChecks)
	if e != nil {
		return e
	}
	if ci != github.Green {
		return errors.New("release requires green CI at the manifest commit")
	}
	m, e := manifest.Build(r.Name, sha, r.Install, func(name string) ([]byte, error) { return s.GH.ReadFile(ctx, r.Name, sha, name) })
	if e != nil {
		return e
	}
	if e = s.Print(m); e != nil {
		return e
	}
	if o.DryRun || o.Shadow {
		return nil
	}
	return s.GH.Publish(ctx, m)
}

// Adopt records a hand-started container as the current receipt so the first
// managed deploy has a known previous digest.
func (s *Service) Adopt(ctx context.Context, r config.Repo, o Options) error {
	var spec config.Install
	found := false
	for _, candidate := range r.Install {
		if candidate.Target == o.Adopt {
			spec, found = candidate, true
			break
		}
	}
	if !found {
		return fmt.Errorf("target %q is not in the host inventory", o.Adopt)
	}
	if spec.Kind != "container" {
		return errors.New("--adopt requires a container install target")
	}
	state := filepath.Join(s.state(r.Name), "rollout")
	result, e := rollout.AdoptContainer(ctx, spec, s.Config.Host, r.Name, rollout.Options{StateDir: state, Deny: r.Deny, DryRun: o.DryRun || o.Shadow, Now: s.Now}, rollout.RuntimeCLI{Runner: s.Runner, Runtime: spec.Runtime})
	if o.DryRun || o.Shadow {
		return errors.Join(e, s.Print(result))
	}
	record := struct {
		Host, Repo string
		Result     rollout.Result
		Error      string
	}{Host: s.Config.Host, Repo: r.Name, Result: result}
	if e != nil {
		record.Error = e.Error()
	}
	return errors.Join(e, s.audit(r.Name, "adopt", record, false), s.Print(result))
}

func (s *Service) Rollout(ctx context.Context, r config.Repo, o Options) error {
	containerOnly := len(r.Install) > 0
	for _, spec := range r.Install {
		containerOnly = containerOnly && spec.Kind == "container"
	}
	if o.Adopt != "" {
		if !containerOnly {
			return errors.New("--adopt requires a container install inventory")
		}
		if o.Digest != "" {
			return errors.New("--adopt and --digest are separate operations")
		}
		return s.Adopt(ctx, r, o)
	}
	if !containerOnly && (r.Root == "" || r.Smoke.Command == "") {
		return errors.New("rollout requires root and a smoke command")
	}
	var m manifest.Manifest
	var e error
	binaryOnly := len(r.Install) > 0
	for _, spec := range r.Install {
		binaryOnly = binaryOnly && spec.Kind == "release"
	}
	if binaryOnly {
		source, ok := s.GH.(interface {
			BinaryHead(context.Context, string) (string, error)
		})
		if !ok {
			return errors.New("GitHub adapter lacks binary releases")
		}
		sha, err := source.BinaryHead(ctx, r.Name)
		if err != nil {
			return err
		}
		m, e = manifest.Build(r.Name, sha, r.Install, nil)
	} else {
		m, e = s.GH.Latest(ctx, r.Name)
	}
	if e != nil {
		return e
	}
	if m.Repo != r.Name {
		return errors.New("manifest repo mismatch")
	}
	if e = m.Validate(); e != nil {
		return e
	}
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	ok, e := s.GH.Ancestor(ctx, r.Name, m.Commit, head)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("manifest commit is no longer on main")
	}
	p, e := s.Policy(ctx, r, m.Commit)
	if e != nil {
		return e
	}
	ci, e := s.GH.Check(ctx, r.Name, m.Commit, p.RequiredChecks)
	if e != nil {
		return e
	}
	if ci != github.Green {
		return errors.New("manifest commit CI is not green")
	}
	state := filepath.Join(s.state(r.Name), "rollout")
	if containerOnly {
		b, re := os.ReadFile(filepath.Join(state, rollout.ContainerReceiptName))
		if re == nil {
			var receipt rollout.ContainerReceipt
			if re = json.Unmarshal(b, &receipt); re != nil {
				return re
			}
			if receipt.Repo != r.Name {
				return errors.New("container receipt repo mismatch")
			}
			if receipt.Commit != "" {
				ok, re = s.GH.Ancestor(ctx, r.Name, receipt.Commit, m.Commit)
				if re != nil {
					return re
				}
				if !ok {
					return errors.New("automatic downgrade or divergent release refused")
				}
			}
		} else if !os.IsNotExist(re) {
			return re
		}
	}
	if !containerOnly {
		receiptName := "installed.json"
		if len(m.Commands) > 0 {
			receiptName = "command-installed.json"
		}
		b, re := os.ReadFile(filepath.Join(state, receiptName))
		if re == nil {
			var old struct {
				Manifest manifest.Manifest `json:"manifest"`
			}
			if re = json.Unmarshal(b, &old); re != nil {
				return re
			}
			if old.Manifest.Repo != r.Name {
				return errors.New("installed receipt repo mismatch")
			}
			ok, re = s.GH.Ancestor(ctx, r.Name, old.Manifest.Commit, m.Commit)
			if re != nil {
				return re
			}
			if !ok {
				return errors.New("automatic downgrade or divergent release refused")
			}
		} else if !os.IsNotExist(re) {
			return re
		}
	}
	var timeout time.Duration
	if r.Smoke.Command != "" {
		timeout, e = config.Duration(r.SmokeTimeout)
		if e != nil {
			return e
		}
	}
	root := config.Expand(r.Root)
	smoke := func(ctx context.Context) error {
		if r.Smoke.Command == "" {
			return nil
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_, err := s.Runner.Run(ctx, r.Smoke.Command, r.Smoke.Args, "", root)
		return err
	}
	installOptions := rollout.Options{Root: root, StateDir: state, Allowed: r.Install, Deny: r.Deny, DryRun: o.DryRun || o.Shadow, Digest: o.Digest, Now: s.Now}
	var result rollout.Result
	var installErr error
	switch {
	case containerOnly:
		runtimeFor := func(spec config.Install) rollout.Runtime {
			return rollout.RuntimeCLI{Runner: s.Runner, Runtime: spec.Runtime}
		}
		result, installErr = rollout.ApplyContainer(ctx, m, installOptions, s.Config.Host, runtimeFor, rollout.CheckHealth)
	case len(m.Commands) > 0:
		result, installErr = rollout.ApplyCommands(ctx, m, installOptions, s.Config.Host, s.Runner, func(ctx context.Context, spec config.Install, sha string) error {
			if spec.Kind == "release" {
				source, ok := s.GH.(interface {
					BinaryAtCommit(context.Context, string, string) ([]byte, error)
				})
				if !ok {
					return errors.New("GitHub adapter lacks pinned binary releases")
				}
				b, err := source.BinaryAtCommit(ctx, r.Name, sha)
				if err != nil {
					return err
				}
				target, err := manifest.Resolve(root, spec.Target)
				if err != nil {
					return err
				}
				return rollout.Atomic(target, b, 0755)
			}
			command := rollout.ExpandCommand(spec.Command, r.Name, sha, root, spec.Target)
			_, err := s.Runner.Run(ctx, command.Command, command.Args, "", root)
			return err
		}, smoke)
	default:
		selected := []config.Install{}
		for _, file := range m.Files {
			denied := false
			for _, g := range r.Deny {
				match, e := policy.Match(g, file.Target)
				if e != nil {
					return e
				}
				denied = denied || match
			}
			if denied {
				continue
			}
			for _, spec := range r.Install {
				if spec.Target == file.Target {
					selected = append(selected, spec)
					break
				}
			}
		}
		if !o.DryRun && !o.Shadow {
			for _, spec := range selected {
				if err := rollout.CheckGate(spec, s.Config.Host, r.Name, m.Commit); err != nil {
					return err
				}
			}
		}
		result, installErr = rollout.Apply(ctx, m, installOptions, func(ctx context.Context, name string) ([]byte, error) {
			return s.GH.ReadFile(ctx, r.Name, m.Commit, name)
		}, smoke)
		if !o.DryRun && !o.Shadow && installErr == nil && (result.Status == "success" || result.Status == "current") {
			for _, spec := range selected {
				installErr = errors.Join(installErr, rollout.PublishGate(spec, s.Config.Host, r.Name, m.Commit))
			}
		}
	}

	if o.DryRun || o.Shadow || result.Status == "current" || result.Status == "rejected" {
		return errors.Join(installErr, s.Print(result))
	}
	record := struct {
		Host, Repo string
		Result     rollout.Result
		Error      string
	}{Host: s.Config.Host, Repo: r.Name, Result: result}
	if installErr != nil {
		record.Error = installErr.Error()
	}
	data, _ := json.Marshal(record)
	id := fmt.Sprintf("rollout-%s-%d", m.Commit[:8], s.now().UnixNano())
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return errors.Join(installErr, s.audit(r.Name, "rollout", record, false), s.Sink.Record(reportCtx, "mergewake/"+s.Config.Host+"/"+id, data), s.Notifier.Notify(reportCtx, integrations.Event{ID: id, Text: fmt.Sprintf("mergewake %s %s %s %s", r.Name, s.Config.Host, m.Commit, result.Status)}))
}
