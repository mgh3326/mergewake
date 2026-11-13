package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

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
	p.WatchChecks = p.DeploySet()
	// A dispatched candidate is judged by the checks its any-ref deploy run
	// posts; unset, by the deploy set as before.
	p.AnyRefChecks = p.AnyRefDeployChecks
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

// candidateCI is a commit's verdict for bisect. The deploy stream with
// any_ref_deploy_checks reads the commit's own deploy run first (a decisive
// result there is today's verdict), and only a commit with no decisive deploy
// result is judged by the checks its any-ref deploy run posts. Every other
// stream judges by BisectChecks.
func (s *watchStream) candidateCI(ctx context.Context, repo, sha string, p policy.Policy) (github.CI, error) {
	if s.deploy && len(p.AnyRefChecks) > 0 {
		v, err := s.GH.Check(ctx, repo, sha, p.WatchSet())
		if err != nil || v == github.Green || v == github.Red {
			return v, err
		}
	}
	return s.GH.Check(ctx, repo, sha, p.BisectChecks())
}

// anyRefSuiteError says the any-ref workflow's suite input does not take the
// value a stream needs. The dispatch is not sent: GitHub would refuse it, and
// the stream's evidence stays unknown rather than falling back to the default.
type anyRefSuiteError struct {
	Stream  string
	Options []string
}

func (e *anyRefSuiteError) Error() string {
	return fmt.Sprintf("any-ref CI suite input does not accept %q (options: %s)", e.Stream, strings.Join(e.Options, ", "))
}

// suiteInput is the suite input a workflow file declares.
type suiteInput struct {
	declared bool
	// options is nil for a free string: no type, no options list, or an
	// options list that is not all strings. A declared list is never nil.
	options []string
}

func (in suiteInput) accepts(v string) bool {
	return in.options == nil || slices.Contains(in.options, v)
}

func parseSuiteInput(raw []byte) (suiteInput, error) {
	var wf struct {
		On struct {
			Dispatch struct {
				Inputs map[string]any `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		return suiteInput{}, err
	}
	spec, declared := wf.On.Dispatch.Inputs["suite"]
	if !declared {
		return suiteInput{}, nil
	}
	in := suiteInput{declared: true}
	m, _ := spec.(map[string]any)
	typ, _ := m["type"].(string)
	list, ok := m["options"].([]any)
	if typ == "" || !ok {
		return in, nil
	}
	opts := make([]string, 0, len(list))
	for _, v := range list {
		s, isString := v.(string)
		if !isString {
			return in, nil
		}
		opts = append(opts, s)
	}
	in.options = opts
	return in, nil
}

// dispatch starts the any-ref workflow for sha and returns the id of the run
// it started, or zero when the client or GitHub does not report one. A suite
// input is sent only when the workflow accepts its value: the main stream
// omits it otherwise, and the deploy stream returns an *anyRefSuiteError.
func (s *watchStream) dispatch(ctx context.Context, d workflowDispatcher, r config.Repo, p policy.Policy, sha string) (int64, error) {
	inputs := map[string]string{"ref": sha}
	if s.suites {
		raw, err := s.GH.ReadFile(ctx, r.Name, s.targetHead(sha), filepath.Join(".github/workflows", p.AnyRefWorkflow))
		if err != nil {
			return 0, err
		}
		in, err := parseSuiteInput(raw)
		if err != nil {
			return 0, err
		}
		if in.declared {
			stream, want := "main", "main"
			if s.deploy {
				stream, want = "deploy", "deploy"
			}
			switch {
			case in.accepts(want):
				inputs["suite"] = want
			case s.deploy:
				return 0, &anyRefSuiteError{Stream: stream, Options: in.options}
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
