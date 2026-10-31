// Package pipeline coordinates policy, GitHub, local reviewers, and adapters.
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
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
	ReviewHistory(context.Context, string, int, string) ([]github.ReviewedPass, error)
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
	// inboxFallbacks counts findings returned through the legacy lane/file
	// path while the reviewer job record carried no result (MW-3); the count
	// rides on each review-inbox-fallback audit record for one release.
	inboxFallbacks int
	// patches caches each PR head's patch id (see patchIDOf), guarded by
	// patchMu.
	patches map[string]string
}
type Options struct {
	DryRun, Shadow, Observed bool
	Digest, Adopt            string
}

// Acting reports whether a command may write remotely or mutate the host:
// dry runs decide only, and shadow mode observes without acting.
func (o Options) Acting() bool { return !o.DryRun && !o.Shadow }

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
	return fsutil.AppendLine(filepath.Join(s.state(repo), "decisions.jsonl"), b)
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
		if config.CanonicalFamily(a.Family) == config.CanonicalFamily(b.Family) && (a.Lens == "" || b.Lens == "" || a.Lens == "default" || b.Lens == "default" || a.Lens == b.Lens) {
			return len(passes), false
		}
	}
	return len(passes), true
}

// Land runs the merge gate once for a single PR (the same gate the pr role's
// notice reports) and, when the verdict is ready and the repository policy
// opted in with enabled, squash-merges pinned to the gated head. A dry run
// prints the verdict only.
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
	v, e := s.mergeGate(ctx, r, pr, p, head)
	if e != nil {
		return e
	}
	d := policy.Decision{Reasons: v.Reasons, Shadow: !p.Enabled || o.Shadow}
	d.Action = v.State
	if v.State == "ready" {
		d.Action = "merge"
	}
	if e = s.audit(r.Name, "land", struct {
		PR         int    `json:"pr"`
		Head       string `json:"head"`
		Base       string `json:"base"`
		State      string `json:"state"`
		SameFamily bool   `json:"same_family,omitempty"`
		// Review is the patch id, the review rounds and how the findings
		// stand, so `land --dry-run` shows why a head is or is not ready.
		Review reviewRecord `json:"review"`
		policy.Decision
	}{n, pr.Head.SHA, head, v.State, v.SameFamily, v.Review, d}, o.DryRun); e != nil {
		return e
	}
	if e = s.recordDecision(r.Name, n, d.Action, d.Shadow, v.SameFamily, o.DryRun); e != nil {
		return e
	}
	if v.State != "ready" || d.Shadow || o.DryRun {
		return nil
	}
	// Recheck everything immediately before the SHA-bound merge: the head must
	// be unchanged and must still gate ready under the current base and policy.
	freshPR, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if freshPR.Head.SHA != pr.Head.SHA || freshPR.State != "open" || freshPR.Draft {
		return errors.New("PR changed during decision")
	}
	fresh, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e = s.Policy(ctx, r, fresh)
	if e != nil {
		return e
	}
	if !p.Enabled {
		return errors.New("landing policy changed during decision")
	}
	v, e = s.mergeGate(ctx, r, freshPR, p, fresh)
	if e != nil {
		return e
	}
	if v.State != "ready" {
		return errors.New("gate changed during decision")
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
	// The merge already landed and is observed: the lane event is best-effort,
	// its failure audited rather than returned.
	if s.Config.Daemon.PR.Notify.On() {
		if _, _, _, e = s.laneEvent(ctx, r, pr, "merged", pr.Title); e != nil {
			_ = s.audit(r.Name, "land-notify-error", map[string]any{"pr": n, "error": e.Error()}, false)
		}
	}
	return s.FlushOutcomes(ctx, r.Name)
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
	if !o.Acting() {
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
	result, e := rollout.AdoptContainer(ctx, spec, s.Config.Host, r.Name, rollout.Options{StateDir: state, Deny: r.Deny, DryRun: !o.Acting(), Now: s.Now}, rollout.RuntimeCLI{Runner: s.Runner, Runtime: spec.Runtime})
	if !o.Acting() {
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
		var receipt rollout.ContainerReceipt
		found, re := fsutil.ReadJSON(filepath.Join(state, rollout.ContainerReceiptName), &receipt)
		if re != nil {
			return re
		}
		if found {
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
		}
	}
	if !containerOnly {
		receiptName := "installed.json"
		if len(m.Commands) > 0 {
			receiptName = "command-installed.json"
		}
		var old struct {
			Manifest manifest.Manifest `json:"manifest"`
		}
		found, re := fsutil.ReadJSON(filepath.Join(state, receiptName), &old)
		if re != nil {
			return re
		}
		if found {
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
	installOptions := rollout.Options{Root: root, StateDir: state, Allowed: r.Install, Deny: r.Deny, DryRun: !o.Acting(), Digest: o.Digest, Now: s.Now}
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
				return fsutil.Atomic(target, b, 0755)
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
		if o.Acting() {
			for _, spec := range selected {
				if err := rollout.CheckGate(spec, s.Config.Host, r.Name, m.Commit); err != nil {
					return err
				}
			}
		}
		result, installErr = rollout.Apply(ctx, m, installOptions, func(ctx context.Context, name string) ([]byte, error) {
			return s.GH.ReadFile(ctx, r.Name, m.Commit, name)
		}, smoke)
		if o.Acting() && installErr == nil && (result.Status == "success" || result.Status == "current") {
			for _, spec := range selected {
				installErr = errors.Join(installErr, rollout.PublishGate(spec, s.Config.Host, r.Name, m.Commit))
			}
		}
	}

	if !o.Acting() || result.Status == "current" || result.Status == "rejected" {
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
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	return errors.Join(installErr, s.audit(r.Name, "rollout", record, false), s.Sink.Record(reportCtx, "mergewake/"+s.Config.Host+"/"+id, data), s.Notifier.Notify(reportCtx, integrations.Event{ID: id, Text: fmt.Sprintf("mergewake %s %s %s %s", r.Name, s.Config.Host, m.Commit, result.Status)}))
}
