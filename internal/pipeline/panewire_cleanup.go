package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reviewerPane is the reviewer pane one pass spawned: where it runs, and what
// the pass must still confirm before its checkout is released.
type reviewerPane struct {
	label     string
	checkout  string                 // local PR-head checkout the spawn named; empty for a spec-placed reviewer
	head      string                 // PR head the pass reviewed
	pass      int                    // review pass number
	placement integrations.JobRecord // host and pane id from this spawn's OK line
	confirmed bool                   // the OK line named a host and a pane
	hold      string                 // why the checkout stays; empty when the pass is terminal
	// force marks a reviewer its pass replaced (lost result, timeout, or moved
	// head). The pane is still never closed here: panewire's node owns that.
	force bool
	// keepLocalCheckout is set while a pane may still run in the checkout.
	keepLocalCheckout *bool
}

// reviewerPanes is the pane surface the cleanup and the review path read;
// reap.Exec serves a local host and reap.HubHost a remote one.
type reviewerPanes interface {
	Panes(context.Context) ([]reap.Pane, error)
	AllPanes(context.Context) ([]reap.Pane, error)
}

// closableStatus is the agent status the hub's pane.close accepts.
var closableStatus = map[string]bool{"idle": true, "done": true}

// reviewerCleanup is one cleanup's state. It writes exactly one result, through
// record: a reap receipt and one reviewer-cleanup audit record.
type reviewerCleanup struct {
	x        *panewireExecutor
	p        reviewerPane
	local    bool
	paneGone bool
	row      reapReceipt
}

// spawnPlacement reads the executing panewire's placement receipt. The
// placement value is base64url JSON containing the chosen host and pane id;
// cwd is available in stdout on versions that report it directly. A spawn the
// hub coordinator placed reports `SPAWNED job=<id> host=<h> profile=<p>
// pane=<id>` instead, with "none" for a value it does not know.
func spawnPlacement(stdout string) (integrations.JobRecord, bool) {
	if v, ok := spawnedPlacement(stdout); ok {
		return v, true
	}
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "OK" || !strings.HasPrefix(fields[1], "pane=") {
			continue
		}
		v := integrations.JobRecord{PaneID: strings.TrimPrefix(fields[1], "pane=")}
		for _, field := range fields[2:] {
			switch {
			case strings.HasPrefix(field, "host="):
				v.Host = strings.TrimPrefix(field, "host=")
			case strings.HasPrefix(field, "cwd="):
				v.Cwd = strings.TrimPrefix(field, "cwd=")
			case strings.HasPrefix(field, "placement="):
				encoded := strings.TrimPrefix(field, "placement=")
				raw, err := base64.URLEncoding.DecodeString(encoded)
				if err != nil {
					raw, err = base64.RawURLEncoding.DecodeString(encoded)
				}
				if err != nil {
					continue
				}
				var p struct {
					Host string `json:"chosen_host"`
					Pane string `json:"pane_id"`
					Cwd  string `json:"cwd"`
				}
				if json.Unmarshal(raw, &p) == nil {
					if p.Host != "" {
						v.Host = p.Host
					}
					if p.Pane != "" {
						v.PaneID = p.Pane
					}
					if p.Cwd != "" {
						v.Cwd = p.Cwd
					}
				}
			}
		}
		return v, v.Host != "" && v.PaneID != ""
	}
	return integrations.JobRecord{}, false
}

// spawnedPlacement reads the hub coordinator's SPAWNED line. The last such line
// wins, and a host or pane of "none" is not a placement.
func spawnedPlacement(stdout string) (integrations.JobRecord, bool) {
	var v integrations.JobRecord
	found := false
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "SPAWNED" || !strings.HasPrefix(fields[1], "job=") {
			continue
		}
		v, found = integrations.JobRecord{}, true
		for _, field := range fields[2:] {
			key, value, _ := strings.Cut(field, "=")
			if value == "none" {
				value = ""
			}
			switch key {
			case "host":
				v.Host = value
			case "pane":
				v.PaneID = value
			case "profile":
				v.Profile = value
			}
		}
	}
	return v, found && v.Host != "" && v.PaneID != ""
}

// closeReviewerPane runs once a pass has reached a terminal outcome. It never
// closes the pane this pass spawned: panewire's node closes an ended job's
// pane, so a pane still present is left for it and for the reviewer sweep. It
// removes the pass's remote detached worktree through the hub's guarded
// removal once the pane is gone. It never changes the review outcome, and it
// runs on a context detached from the pass so an expired deadline cannot skip
// it. It reports whether the pane is gone.
func (x *panewireExecutor) closeReviewerPane(parent context.Context, p reviewerPane) (closed bool) {
	ctx, cancel := reportContext(parent)
	defer cancel()
	worktree, remote := x.reviewerCleanupPaths(p)
	c := &reviewerCleanup{x: x, p: p, row: reapReceipt{Action: "reviewer-cleanup", Repo: x.repo.Name, PR: x.pr.Number, Host: p.placement.Host, Pane: p.placement.PaneID, Cwd: remote, Worktree: worktree, Label: p.label}}
	defer func() {
		closed = c.paneGone
		// A forced close that does not close the pane is a skip, never a pass
		// failure: the replacement has already been decided.
		if p.force && !closed {
			_ = x.s.audit(x.repo.Name, "reviewer-close-skipped", map[string]any{"pr": x.pr.Number, "head": p.head, "pass": p.pass, "label": p.label, "pane": p.placement.PaneID, "reason": c.row.Reason}, false)
		}
	}()
	if !p.confirmed {
		// The spawn's receipt may have come late although the job ran: a
		// completed job record that names a host and a pane is the placement.
		job, err := x.job(ctx, p.label)
		if err != nil || job.State != "completed" || job.Host == "" || job.PaneID == "" {
			// The pane's host is unknown, so it may still run in the local checkout.
			if p.keepLocalCheckout != nil {
				*p.keepLocalCheckout = true
			}
			c.record("pending-cleanup", "spawn named no host and pane; checkout kept")
			return
		}
		p.placement, p.confirmed = job, true
		c.p = p
		c.row.Host, c.row.Pane = job.Host, job.PaneID
	}
	c.local = p.placement.Host == "local" || x.localReviewerHost(p.placement.Host)
	if p.hold != "" {
		c.record("kept", p.hold)
		return
	}
	// The job record must agree with this spawn about where the pane runs.
	if job, err := x.job(ctx, p.label); err == nil {
		if job.Host != "" && job.Host != p.placement.Host || job.PaneID != "" && job.PaneID != p.placement.PaneID {
			c.record("kept", "job record and spawn placement disagree")
			return
		}
		if job.Host == p.placement.Host && job.PaneID == p.placement.PaneID && job.Cwd != "" {
			c.p.placement.Cwd = job.Cwd
		}
	}
	if c.local {
		c.cleanLocal(ctx)
		return
	}
	hub, err := x.s.reapHub()
	if err != nil {
		c.record("error", "hub: "+err.Error())
		return
	}
	if hub == nil {
		c.record("skipped", "reap transport is not the panewire hub")
		return
	}
	host := reap.HubHost{Hub: hub, Machine: p.placement.Host}
	c.cleanRemote(ctx, host)
	return
}

// reviewerCleanupPaths is what a reviewer-cleanup row names: the worktree a
// pass end holds, and the verified remote cwd where the sweep reads a remote
// path first. A spec-placed reviewer holds no local checkout, so the row names
// the label itself and the node's worktree: the placement cwd when it is the
// label's own directory, <label>-checkout otherwise. The bare label is never a
// filesystem path.
func (x *panewireExecutor) reviewerCleanupPaths(p reviewerPane) (worktree, remote string) {
	worktree = p.checkout
	if worktree != "" {
		return worktree, ""
	}
	worktree = p.label + reviewerCheckoutSuffix
	cwd := p.placement.Cwd
	if !reviewerCheckoutDir(filepath.Base(filepath.Clean(cwd)), p.label) {
		return worktree, ""
	}
	worktree = cwd
	if p.placement.Host != "" && p.placement.Host != "local" && !x.localReviewerHost(p.placement.Host) {
		remote = cwd
	}
	return worktree, remote
}

// cleanLocal leaves the checkout while its pane is still present and releases
// it once the pane is gone; the caller removes the checkout then.
func (c *reviewerCleanup) cleanLocal(ctx context.Context) {
	ops := reap.Exec{Runner: c.x.s.Runner, Host: reap.Host{Name: "local", Jobs: c.x.s.Config.Review.Panewire.Jobs}}
	present, ok := c.panePresent(ctx, ops)
	if !ok {
		return
	}
	if present {
		c.record("pending-cleanup", "pane still present; panewire closes it and a later sweep cleans up")
		return
	}
	c.paneGone = true
	c.record("skipped", "pane already gone")
}

// cleanRemote removes the pass's remote detached worktree once its pane is
// gone; a pane still present is left for panewire's node to close.
func (c *reviewerCleanup) cleanRemote(ctx context.Context, host reap.HubHost) {
	present, ok := c.panePresent(ctx, host)
	if !ok {
		return
	}
	if present {
		c.record("pending-cleanup", "pane still present; panewire closes it and a later sweep cleans up")
		return
	}
	c.paneGone = true
	cwd := c.p.placement.Cwd
	if !reviewerCheckoutDir(filepath.Base(filepath.Clean(cwd)), c.p.label) {
		c.record("skipped", "pane gone; remote cwd unavailable or mismatched")
		return
	}
	c.removeWorktree(ctx, host, cwd)
}

// panePresent reports whether the spawn's pane id still lists on the host.
func (c *reviewerCleanup) panePresent(ctx context.Context, ops reviewerPanes) (bool, bool) {
	panes, err := ops.AllPanes(ctx)
	if err != nil {
		c.record("error", "pane list: "+err.Error())
		return false, false
	}
	for _, pane := range panes {
		if pane.ID == c.p.placement.PaneID {
			return true, true
		}
	}
	return false, true
}

// removeWorktree asks the hub to remove the detached worktree with the reviewed
// head. The hub's refusal is recorded as pending-cleanup; nothing removes it
// another way.
func (c *reviewerCleanup) removeWorktree(ctx context.Context, host reap.HubHost, cwd string) {
	c.row.Worktree, c.row.Cwd = cwd, cwd
	tree, err := host.Inspect(ctx, cwd)
	if err != nil {
		c.record("error", "worktree inspect: "+err.Error())
		return
	}
	if tree == nil || tree.Path != cwd || tree.Head != c.p.head || !strings.EqualFold(tree.Repo, c.x.repo.Name) {
		c.record("skipped", "worktree identity changed; not removed")
		return
	}
	removed, err := host.WorktreeRemove(ctx, tree.Path, c.p.head, c.x.pr.Base.Ref, nil, true)
	if err != nil {
		c.record("error", "worktree remove: "+err.Error())
		return
	}
	result, reason := worktreeRemoveVerdict(removed)
	switch {
	case result == "removed":
		c.record("removed", "")
	case reason == "worktree-gone":
		c.record("skipped", reviewerAlreadyGone)
	default:
		c.record("pending-cleanup", "hub guarded removal refused: "+reason)
	}
}

// keepUnfinishedCheckout handles a spawn that ended without a pane result. It
// closes nothing: the checkout stays with a pending-cleanup receipt that names
// it, so the checkout is not removed under a pane that may still run. A
// spec-placed spawn holds no local checkout, so its receipt names the node's
// <label>-checkout worktree instead: the reviewer sweep resolves its host and
// path from the job record once the job has ended.
func (x *panewireExecutor) keepUnfinishedCheckout(p reviewerPane, reason string) {
	worktree := p.checkout
	if worktree == "" {
		worktree = p.label + reviewerCheckoutSuffix
	}
	c := &reviewerCleanup{x: x, p: p, row: reapReceipt{Action: "reviewer-cleanup", Repo: x.repo.Name, PR: x.pr.Number, Worktree: worktree, Label: p.label}}
	c.record("pending-cleanup", reason)
}

// record writes this cleanup's one result: a reap receipt and the
// reviewer-cleanup audit record. A local checkout stays while its pane may
// still run in it.
func (c *reviewerCleanup) record(result, reason string) {
	reason = fleetevent.Redact(reason)
	if c.local && c.p.keepLocalCheckout != nil {
		*c.p.keepLocalCheckout = !c.paneGone
	}
	c.row.Result, c.row.Reason = result, reason
	_ = c.x.s.reapNote(c.row, false)
	_ = c.x.s.audit(c.x.repo.Name, "reviewer-cleanup", map[string]any{
		"pr":     c.x.pr.Number,
		"head":   c.p.head,
		"pass":   c.p.pass,
		"label":  c.p.label,
		"host":   c.p.placement.Host,
		"pane":   c.p.placement.PaneID,
		"result": result,
		"reason": reason,
	}, false)
}

// localReviewerHost reports whether a placement names a host configured as local.
func (x *panewireExecutor) localReviewerHost(machine string) bool {
	for _, host := range x.s.Config.Hosts {
		if host.Local() && (host.Name == machine || host.MachineID() == machine) {
			return true
		}
	}
	return false
}
