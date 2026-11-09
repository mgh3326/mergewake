package pipeline

import (
	"context"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// EscalateConflict sends the one conflict ESC of a PR head (hk 3712). This is
// the compile seam the tests are written against; it does nothing yet.
func (s *Service) EscalateConflict(ctx context.Context, r config.Repo, pr github.Pull, o Options) error {
	return nil
}
