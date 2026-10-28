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
	Check(context.Context, string, string, []string) (github.CI, error)
	Reviewed(context.Context, string, int, string, string) (bool, error)
	Unresolved(context.Context, string, int) (int, error)
	PostReview(context.Context, string, int, string, []github.Finding) error
	Commit(context.Context, string, string) (github.Commit, error)
	PRsForCommit(context.Context, string, string) ([]github.Pull, error)
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
}
type Options struct{ DryRun, Shadow bool }

func (s *Service) Print(v any) error        { return json.NewEncoder(s.Out).Encode(v) }
func (s *Service) state(repo string) string { return filepath.Join(s.Config.StateDir, repo) }
func (s *Service) audit(repo, command string, value any, dry bool) error {
	record := struct {
		Time    time.Time `json:"time"`
		Command string    `json:"command"`
		Value   any       `json:"value"`
	}{time.Now().UTC(), command, value}
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
func (s *Service) Land(ctx context.Context, r config.Repo, n int, o Options) error {
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
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
	d := p.Classify(files)
	d.Shadow = d.Shadow || o.Shadow
	d.Action = "wait-for-ci"
	ci, e := s.GH.Check(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if e != nil {
		return e
	}
	if ci == github.Green {
		d.Action = "merge"
		if d.Class == "blocking" {
			done, e := s.GH.Reviewed(ctx, r.Name, n, pr.Head.SHA, s.Config.Review.Login)
			if e != nil {
				return e
			}
			if !done {
				d.Action = "wait-for-independent-review"
			} else {
				count, e := s.GH.Unresolved(ctx, r.Name, n)
				if e != nil {
					return e
				}
				if count > 0 {
					d.Action = "wait-for-findings"
				}
			}
		}
	}
	if e = s.audit(r.Name, "land", struct {
		PR   int       `json:"pr"`
		Head string    `json:"head"`
		CI   github.CI `json:"ci"`
		policy.Decision
	}{n, pr.Head.SHA, ci, d}, o.DryRun); e != nil {
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
	if d.Class == "blocking" {
		count, e := s.GH.Unresolved(ctx, r.Name, n)
		if e != nil {
			return e
		}
		if count > 0 {
			return errors.New("review findings changed during decision")
		}
	}
	return s.GH.Merge(ctx, r.Name, n, pr.Head.SHA)
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
	if o.DryRun {
		return nil
	}
	return s.GH.Publish(ctx, m)
}
func (s *Service) Rollout(ctx context.Context, r config.Repo, o Options) error {
	if r.Root == "" || r.Smoke.Command == "" {
		return errors.New("rollout requires root and a smoke command")
	}
	m, e := s.GH.Latest(ctx, r.Name)
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
	b, e := os.ReadFile(filepath.Join(state, "installed.json"))
	if e == nil {
		var old struct {
			Manifest manifest.Manifest `json:"manifest"`
		}
		if e = json.Unmarshal(b, &old); e != nil {
			return e
		}
		if old.Manifest.Repo != r.Name {
			return errors.New("installed receipt repo mismatch")
		}
		ok, e = s.GH.Ancestor(ctx, r.Name, old.Manifest.Commit, m.Commit)
		if e != nil {
			return e
		}
		if !ok {
			return errors.New("automatic downgrade or divergent release refused")
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	timeout, e := config.Duration(r.SmokeTimeout)
	if e != nil {
		return e
	}
	root := config.Expand(r.Root)
	result, installErr := rollout.Apply(ctx, m, rollout.Options{Root: root, StateDir: state, Allowed: r.Install, Deny: r.Deny, DryRun: o.DryRun}, func(ctx context.Context, name string) ([]byte, error) {
		return s.GH.ReadFile(ctx, r.Name, m.Commit, name)
	}, func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_, e := s.Runner.Run(ctx, r.Smoke.Command, r.Smoke.Args, "", root)
		return e
	})
	if o.DryRun || result.Status == "current" || result.Status == "rejected" {
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
	id := fmt.Sprintf("rollout-%s-%d", m.Commit[:8], time.Now().UnixNano())
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return errors.Join(installErr, s.audit(r.Name, "rollout", record, false), s.Sink.Record(reportCtx, "mergewake/"+s.Config.Host+"/"+id, data), s.Notifier.Notify(reportCtx, integrations.Event{ID: id, Text: fmt.Sprintf("mergewake %s %s %s %s", r.Name, s.Config.Host, m.Commit, result.Status)}))
}
