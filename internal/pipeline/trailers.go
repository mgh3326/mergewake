package pipeline

import (
	"context"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// prTrailer reads one Mergewake-* trailer of a PR: the PR body first, then the
// head commit message. It is the only place that decides that order. valid
// (nil: any non-empty value) must accept the body's value for it to win; an
// unusable body value falls through to the commit. The commit is read only when
// the body does not answer, and a failed read is returned, never hidden as
// "absent".
func (s *Service) prTrailer(ctx context.Context, r config.Repo, pr github.Pull, key string, valid func(string) bool) (string, error) {
	if valid == nil {
		valid = func(v string) bool { return v != "" }
	}
	if v := trailer(pr.Body, key); valid(v) {
		return v, nil
	}
	c, err := s.headCommit(ctx, r, pr)
	if err != nil {
		return "", err
	}
	if v := trailer(c.Commit.Message, key); valid(v) {
		return v, nil
	}
	return "", nil
}
