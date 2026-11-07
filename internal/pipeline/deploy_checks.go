package pipeline

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

// rolloutDenyName lists rollout culprits under the repository's state
// directory: one full commit SHA per line, with blank lines and # comments
// ignored.
const rolloutDenyName = "rollout-deny"

// checkStage names the checks that judge a release or rollout commit.
func checkStage(p policy.Policy) string {
	if p.DeployStage() {
		return "deploy"
	}
	return "main"
}

// policyRevision is the git blob id of the repository policy at sha, the same
// revision a gate record cites.
func (s *Service) policyRevision(ctx context.Context, r config.Repo, sha string) (string, error) {
	b, e := s.GH.ReadFile(ctx, r.Name, sha, r.Policy)
	if e != nil {
		return "", e
	}
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(b))
	_, _ = h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// deploymentChecks converts a check read into manifest evidence.
func deploymentChecks(ev []github.CheckEvidence) []manifest.CheckEvidence {
	out := make([]manifest.CheckEvidence, 0, len(ev))
	for _, c := range ev {
		out = append(out, manifest.CheckEvidence{Name: c.Name, State: string(c.State), URL: c.URL})
	}
	return out
}

// rolloutCandidates returns the releases rollout may consider, newest first.
// A single-source inventory (binary releases or followed container releases)
// has one candidate; a manifest release lists every published manifest.
func (s *Service) rolloutCandidates(ctx context.Context, r config.Repo, binaryOnly, containerOnly bool) ([]manifest.Manifest, error) {
	if binaryOnly {
		source, ok := s.GH.(interface {
			BinaryHead(context.Context, string) (string, error)
		})
		if !ok {
			return nil, errors.New("GitHub adapter lacks binary releases")
		}
		sha, e := source.BinaryHead(ctx, r.Name)
		if e != nil {
			return nil, e
		}
		m, e := manifest.Build(r.Name, sha, r.Install, nil)
		if e != nil {
			return nil, e
		}
		return []manifest.Manifest{m}, nil
	}
	if containerOnly && r.FollowRelease {
		m, e := s.containerRelease(ctx, r)
		if e != nil {
			return nil, e
		}
		return []manifest.Manifest{m}, nil
	}
	return s.GH.Manifests(ctx, r.Name)
}

// admitCandidate picks the newest candidate that rollout may install and says
// why every newer one was skipped.
func (s *Service) admitCandidate(ctx context.Context, r config.Repo, head string, candidates []manifest.Manifest) (manifest.Manifest, error) {
	var skipped []string
	for _, m := range candidates {
		if m.Repo != r.Name {
			return manifest.Manifest{}, errors.New("manifest repo mismatch")
		}
		if e := m.Validate(); e != nil {
			return manifest.Manifest{}, e
		}
		// The release's own policy names the checks it promised to pass.
		p, e := s.Policy(ctx, r, m.Commit)
		if e != nil {
			return manifest.Manifest{}, e
		}
		ok, reason, e := s.admitRollout(ctx, r, p, head, m.Commit)
		if e != nil {
			return manifest.Manifest{}, e
		}
		if ok {
			return m, nil
		}
		skipped = append(skipped, short7(m.Commit)+" ("+reason+")")
	}
	if len(skipped) == 0 {
		return manifest.Manifest{}, errors.New("no release manifest to roll out")
	}
	return manifest.Manifest{}, fmt.Errorf("no admissible release: %s", strings.Join(skipped, ", "))
}

// admitRollout reports whether commit may be installed, and why not. A release
// is admitted only on the configured branch's history, green on its checks, and
// outside a recorded red range. A culprit denies its own commit and every
// descendant until the next green: the range ends once the branch head is green
// again, so an intermediate commit between a culprit and a red head is never
// chosen.
func (s *Service) admitRollout(ctx context.Context, r config.Repo, p policy.Policy, head, commit string) (bool, string, error) {
	ok, e := s.GH.Ancestor(ctx, r.Name, commit, head)
	if e != nil {
		return false, "", e
	}
	if !ok {
		return false, "not an ancestor of " + r.Branch, nil
	}
	deny, e := s.rolloutDeny(r.Name)
	if e != nil {
		return false, "", e
	}
	headGreen, resolved := false, false
	for _, culprit := range deny {
		if culprit == commit {
			return false, "listed in " + rolloutDenyName, nil
		}
		after, e := s.GH.Ancestor(ctx, r.Name, culprit, commit)
		if e != nil {
			return false, "", e
		}
		if !after {
			continue
		}
		if !resolved {
			// The head decides whether the red range has ended. It is judged
			// by the checks the current policy applies to the deploy stage.
			hp, e := s.Policy(ctx, r, head)
			if e != nil {
				return false, "", e
			}
			v, e := s.GH.Check(ctx, r.Name, head, hp.DeploySet())
			if e != nil {
				return false, "", e
			}
			headGreen, resolved = v == github.Green, true
		}
		if !headGreen {
			return false, "after recorded culprit " + short7(culprit) + " while " + r.Branch + " is not green", nil
		}
	}
	ci, e := s.GH.Check(ctx, r.Name, commit, p.DeploySet())
	if e != nil {
		return false, "", e
	}
	if ci != github.Green {
		return false, checkStage(p) + " checks are not green", nil
	}
	return true, "", nil
}

// rolloutDeny reads the repository's culprit list. A missing file denies
// nothing; a line that is not a full commit SHA is an error, since an
// unreadable deny entry would silently admit the culprit it names.
func (s *Service) rolloutDeny(repo string) ([]string, error) {
	b, e := os.ReadFile(filepath.Join(s.state(repo), rolloutDenyName))
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var shas []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !manifest.SHA.MatchString(line) {
			return nil, fmt.Errorf("%s: %q is not a full commit SHA", rolloutDenyName, line)
		}
		shas = append(shas, line)
	}
	return shas, nil
}
