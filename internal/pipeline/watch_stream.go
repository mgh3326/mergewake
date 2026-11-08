package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
	"gopkg.in/yaml.v3"
)

// watchStream isolates durable evidence while sharing the repository latch.
type watchStream struct {
	*Service
	deploy     bool
	suites     bool
	blocked    bool
	otherLatch bool
	mainHead   string
	mainChecks []string
}

func (s *watchStream) checkpoint(repo string) string {
	name := "last-green"
	if s.deploy {
		name = "last-deploy-ok"
	}
	return filepath.Join(s.state(repo), name)
}
func (s *watchStream) recoveryFile(repo string) string {
	if !s.deploy {
		return s.Service.recoveryFile(repo)
	}
	return filepath.Join(s.state(repo), "recovery-deploy.json")
}
func (s *watchStream) loadRecovery(repo string) (recoveryIncident, bool, error) {
	var st recoveryIncident
	found, err := fsutil.ReadJSON(s.recoveryFile(repo), &st)
	return st, found, err
}
func (s *watchStream) saveRecovery(repo string, st recoveryIncident) error {
	return fsutil.WriteJSON(s.recoveryFile(repo), st)
}
func (s *watchStream) clearRecoveryState(repo string) error {
	if !s.deploy {
		return s.Service.clearRecoveryState(repo)
	}
	if err := os.Remove(s.recoveryFile(repo)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
func (s *watchStream) anyRefFile(repo string) string {
	if !s.deploy {
		return s.Service.anyRefFile(repo)
	}
	return filepath.Join(s.state(repo), "any-ref-deploy-dispatch.json")
}
func (s *watchStream) loadAnyRef(repo string) (anyRefIncident, error) {
	var st anyRefIncident
	_, err := fsutil.ReadJSON(s.anyRefFile(repo), &st)
	return st, err
}
func (s *watchStream) saveAnyRef(repo string, st anyRefIncident) error {
	return fsutil.WriteJSON(s.anyRefFile(repo), st)
}
func (s *watchStream) endAnyRef(repo string) error {
	if err := os.Remove(s.anyRefFile(repo)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// watchDeploy finds the newest terminal deploy result without assigning a
// result to intervening commits that did not run the deploy stage.
func (s *watchStream) watchDeploy(ctx context.Context, r config.Repo, p policy.Policy, mainHead string, o Options) error {
	p.MainChecks = p.DeploySet()
	p.WatchChecks = nil
	p.AnyRefChecks = nil
	st, recorded, err := s.loadRecovery(r.Name)
	if err != nil {
		return err
	}
	// An unseen green after the recorded red closes that incident even if a
	// newer deploy run has since failed. Diagnose that later failure next pass.
	current, terminal := mainHead, ""
	for i := 0; i < s.Config.WatchLimit; i++ {
		ci, err := s.GH.Check(ctx, r.Name, current, p.WatchSet())
		// A same-sha re-run leaves a pass beside the commit's failed run. That
		// read is not green, not red and not a reason to walk past the commit:
		// the newest such commit is the terminal one, and its incident decides
		// the mixed result.
		ambiguous := errors.Is(err, github.ErrAmbiguousCheck)
		if err != nil && !ambiguous {
			return err
		}
		if terminal == "" && (ci == github.Green || ci == github.Red || ambiguous) {
			terminal = current
		}
		if recorded && current != st.Head && ci == github.Green {
			descendant, err := s.GH.Ancestor(ctx, r.Name, st.Head, current)
			if err != nil {
				return err
			}
			if descendant {
				return s.watch(ctx, r, p, current, o)
			}
		}
		if !recorded && terminal != "" {
			break
		}
		if recorded && current == st.Head {
			break
		}
		c, err := s.GH.Commit(ctx, r.Name, current)
		if err != nil {
			return err
		}
		if len(c.Parents) == 0 {
			break
		}
		current = c.Parents[0].SHA
	}
	if terminal != "" {
		if !recorded {
			b, err := os.ReadFile(filepath.Join(s.state(r.Name), "last-deploy-revert"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if len(b) > 0 {
				before, err := s.GH.Ancestor(ctx, r.Name, terminal, string(b))
				if err != nil {
					return err
				}
				if before {
					return nil
				}
			}
		}
		ci, err := s.GH.Check(ctx, r.Name, terminal, p.WatchSet())
		// The terminal commit's mixed read is decided by its incident, not here.
		if err != nil && !errors.Is(err, github.ErrAmbiguousCheck) {
			return err
		}
		// A passed re-run at the failing commit is confirmation evidence,
		// not a descendant fix and never a new checkpoint.
		if recorded && terminal == st.Head && ci == github.Green {
			_, _, _, _, err := s.confirmRecovery(ctx, r, p, st, !o.DryRun && !o.Shadow && p.Enabled, !o.DryRun && !o.Shadow && p.Enabled)
			return err
		}
		if recorded && ci == github.Green {
			return nil
		}
		return s.watch(ctx, r, p, terminal, o)
	}
	return nil
}

func (s *watchStream) targetHead(head string) string {
	if s.deploy && s.mainHead != "" {
		return s.mainHead
	}
	return head
}

// dispatch starts the any-ref workflow for sha and returns the id of the run
// it started, or zero when the client or GitHub does not report one.
func (s *watchStream) dispatch(ctx context.Context, d workflowDispatcher, r config.Repo, p policy.Policy, sha string) (int64, error) {
	inputs := map[string]string{"ref": sha}
	if s.suites {
		raw, err := s.GH.ReadFile(ctx, r.Name, s.targetHead(sha), filepath.Join(".github/workflows", p.AnyRefWorkflow))
		if err != nil {
			return 0, err
		}
		var wf struct {
			On struct {
				Dispatch struct {
					Inputs map[string]any `yaml:"inputs"`
				} `yaml:"workflow_dispatch"`
			} `yaml:"on"`
		}
		if err := yaml.Unmarshal(raw, &wf); err != nil {
			return 0, err
		}
		if _, ok := wf.On.Dispatch.Inputs["suite"]; ok {
			inputs["suite"] = "main"
			if s.deploy {
				inputs["suite"] = "deploy"
			}
		}
	}
	if rd, ok := d.(runDispatcher); ok {
		return rd.DispatchWorkflowRun(ctx, r.Name, p.AnyRefWorkflow, r.Branch, inputs)
	}
	return 0, d.DispatchWorkflow(ctx, r.Name, p.AnyRefWorkflow, r.Branch, inputs)
}

func (s *watchStream) ownsLatch(ctx context.Context, r config.Repo) (bool, error) {
	st, found, err := s.loadRecovery(r.Name)
	if err != nil || !found || st.RevertPR == 0 {
		return false, err
	}
	active, err := s.GH.RecoveryActive(ctx, r.Name)
	if err != nil || !active {
		return false, err
	}
	latch, ok := s.GH.(recoveryLatchGH)
	if !ok {
		return false, nil
	}
	pr, err := latch.RecoveryRevert(ctx, r.Name)
	return pr != nil && pr.Number == st.RevertPR, err
}

// A later main failure takes priority over an open deploy revert. Preserve the
// deploy incident and confirmation, but release its PR and the shared latch.
func (s *watchStream) yieldLatch(ctx context.Context, r config.Repo) (bool, error) {
	st, found, err := s.loadRecovery(r.Name)
	if err != nil || !found {
		return false, err
	}
	pr, err := s.GH.Pull(ctx, r.Name, st.RevertPR)
	if err != nil {
		return false, err
	}
	if pr.Merged || pr.MergedAt != nil {
		return false, nil
	}
	if !st.Yielding {
		st.Yielding = true
		if err := s.saveRecovery(r.Name, st); err != nil {
			return false, err
		}
	}
	if pr.State == "open" {
		closer, ok := s.GH.(revertCloser)
		if !ok {
			return false, errors.New("GitHub client cannot close deploy revert for main recovery")
		}
		if err := closer.ClosePull(ctx, r.Name, pr.Number); err != nil {
			return false, err
		}
	}
	latch, ok := s.GH.(recoveryLatchGH)
	if !ok {
		return false, errors.New("GitHub client cannot release deploy recovery latch")
	}
	if err := latch.ClearRecovery(ctx, r.Name); err != nil {
		return false, err
	}
	st.RevertPR, st.RevertURL, st.Yielding = 0, "", false
	return true, s.saveRecovery(r.Name, st)
}

// Once the revert is on main, remember its merge so stale deploy results cannot
// start the just-resolved incident again while the deploy queue catches up.
func (s *watchStream) finishMerged(ctx context.Context, r config.Repo, head string) error {
	st, found, err := s.loadRecovery(r.Name)
	if err != nil || !found || st.RevertPR == 0 {
		return err
	}
	pr, err := s.GH.Pull(ctx, r.Name, st.RevertPR)
	if err != nil {
		return err
	}
	if (!pr.Merged && pr.MergedAt == nil) || pr.MergeCommitSHA == "" {
		return nil
	}
	contains, err := s.GH.Ancestor(ctx, r.Name, pr.MergeCommitSHA, head)
	if err != nil || !contains {
		return err
	}
	if err := fsutil.Atomic(filepath.Join(s.state(r.Name), "last-deploy-revert"), []byte(pr.MergeCommitSHA), 0600); err != nil {
		return err
	}
	owned, err := s.ownsLatch(ctx, r)
	if err != nil {
		return err
	}
	if owned {
		latch, ok := s.GH.(recoveryLatchGH)
		if !ok {
			return errors.New("GitHub client cannot release deploy recovery latch")
		}
		if err := latch.ClearRecovery(ctx, r.Name); err != nil {
			return err
		}
	}
	if err := s.endAnyRef(r.Name); err != nil {
		return err
	}
	return s.clearRecoveryState(r.Name)
}
