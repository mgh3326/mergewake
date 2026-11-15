package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
)

// legacySchedule is the daemon's memory of one repo's last legacy sweep, kept
// beside that repo's receipts so the interval holds across restarts.
type legacySchedule struct {
	LastRun time.Time `json:"last_run"`
}

func (s *Service) legacyScheduleFile(repo string) string {
	return filepath.Join(s.state(repo), "legacy-sweep.json")
}

// ReapLegacyScheduled runs the legacy sweep for each repo whose
// reap.legacy_every has come round since its last attempt, once over all hosts.
// This is what the daemon's reap role calls: the sweep lists every worktree on
// every host and reads hk task and GitHub ancestry, so it must not ride on
// every reap pass. The attempt is stamped before the sweep starts, so a restart
// inside the interval does not sweep again, and a sweep that fails waits for
// the next interval instead of running on every pass.
//
// A failing repo is audited by ReapLegacy and returned here; the remaining
// repos still sweep. The caller decides what the error costs, so the failure
// never stops the regular reap pass.
func (s *Service) ReapLegacyScheduled(ctx context.Context, repos []config.Repo, o Options) error {
	every, e := s.Config.Reap.LegacyInterval()
	if e != nil {
		return e
	}
	if every <= 0 {
		return nil
	}
	var errs []error
	for _, r := range repos {
		if e := ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		due, e := s.legacyDue(r.Name, every)
		if e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, e))
			continue
		}
		if !due {
			continue
		}
		if e := s.ReapLegacy(ctx, r, "", o); e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, e))
		}
	}
	return errors.Join(errs...)
}

// legacyDue reports whether repo's sweep is due and, when it is, records the
// attempt. A sweep whose attempt cannot be recorded does not run: an
// unrecorded sweep would run again on the next pass, which is the cost this
// interval exists to bound. A stamp in the future means the clock moved back;
// that makes the sweep due once rather than holding it off until the clock
// catches up.
func (s *Service) legacyDue(repo string, every time.Duration) (bool, error) {
	file := s.legacyScheduleFile(repo)
	var last legacySchedule
	if _, e := fsutil.ReadJSON(file, &last); e != nil {
		return false, e
	}
	now := s.now().UTC()
	if since := now.Sub(last.LastRun); !last.LastRun.IsZero() && since >= 0 && since < every {
		return false, nil
	}
	return true, fsutil.WriteJSON(file, legacySchedule{LastRun: now})
}
