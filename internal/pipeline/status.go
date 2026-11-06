package pipeline

import (
	"errors"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/prstatus"
)

// recordStatus stores the gate's verdict for pr as the PR's status snapshot.
// The snapshot is a read model for others: a failed write is audited and never
// holds a verdict, a notification or a merge.
func (s *Service) recordStatus(r config.Repo, pr github.Pull, v gateVerdict, started time.Time) {
	passes := make([]prstatus.Pass, 0, len(v.Status.Passes))
	for _, p := range v.Status.Passes {
		passes = append(passes, prstatus.Pass{Pass: p.Pass, Lens: p.Lens, Family: p.Family})
	}
	snap := prstatus.Snapshot{
		Repo: r.Name, PR: pr.Number, Head: pr.Head.SHA, State: v.State, Reasons: v.Reasons,
		Checks:     v.Status.Checks,
		Review:     prstatus.Review{Passes: passes, Done: v.Status.Done, Inherited: v.Review.Inherited},
		Threads:    v.Status.Threads,
		Findings:   v.Status.Findings,
		BaseFresh:  v.Status.BaseFresh,
		BaseBehind: v.Review.BaseBehind,
		ObservedAt: s.now().UTC(),
	}
	if s.writeStatus(r, snap) {
		// The decision covers any re-evaluation requested before the pass began.
		prstatus.ClearDirty(s.Config.StateDir, r.Name, pr.Number, started)
	}
}

// finalizeStatus gives a PR that left the open list its last snapshot: the
// gate fields of the last pass under the PR's final state. A PR the gate never
// decided gets the final state alone.
func (s *Service) finalizeStatus(r config.Repo, pr github.Pull) {
	final := prstatus.Closed
	if pr.Merged || pr.MergedAt != nil {
		final = prstatus.Merged
	}
	if pr.State == "open" {
		return
	}
	snap, e := prstatus.Read(s.Config.StateDir, r.Name, pr.Number)
	if e != nil && !errors.Is(e, prstatus.ErrNotFound) {
		s.statusError(r, pr.Number, e)
		return
	}
	if snap.State == final && snap.Head == pr.Head.SHA {
		return
	}
	snap.Repo, snap.PR, snap.Head, snap.State, snap.ObservedAt = r.Name, pr.Number, pr.Head.SHA, final, s.now().UTC()
	s.writeStatus(r, snap)
}

func (s *Service) writeStatus(r config.Repo, snap prstatus.Snapshot) bool {
	if e := prstatus.Write(s.Config.StateDir, snap); e != nil {
		s.statusError(r, snap.PR, e)
		return false
	}
	return true
}

func (s *Service) statusError(r config.Repo, n int, e error) {
	_ = s.audit(r.Name, "pr-status-error", map[string]any{"pr": n, "error": integrations.Detail(e)}, false)
}
