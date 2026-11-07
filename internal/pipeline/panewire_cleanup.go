package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reviewerPane is the reviewer pane one pass spawned: where it runs, and what
// the pass must still confirm before it closes that pane.
type reviewerPane struct {
	label     string
	checkout  string                 // local PR-head checkout the spawn named
	head      string                 // PR head the pass reviewed
	pass      int                    // review pass number
	placement integrations.JobRecord // host and pane id from this spawn's OK line
	confirmed bool                   // the OK line named a host and a pane
	hold      string                 // why the pane stays open; empty when the pass is terminal
	// force closes the pane even while it is working: only a reviewer replaced
	// past its deadline sets it. The pane's output is saved first, and its name
	// and cwd must still match.
	force bool
	// keepLocalCheckout is set while a local pane still runs in the checkout.
	keepLocalCheckout *bool
}

// reviewerPanes is the pane surface a close needs; reap.Exec serves a local
// host and reap.HubHost a remote one.
type reviewerPanes interface {
	Panes(context.Context) ([]reap.Pane, error)
	AllPanes(context.Context) ([]reap.Pane, error)
	Composer(context.Context, reap.Pane) (string, error)
	Capture(context.Context, reap.Pane, string, *time.Time, bool) (reap.Capture, error)
	CloseExact(context.Context, reap.Pane) (string, error)
}

// reviewerCleanup is one cleanup's state. It writes exactly one result, through
// record: a reap receipt and one reviewer-cleanup audit record.
type reviewerCleanup struct {
	x          *panewireExecutor
	p          reviewerPane
	local      bool
	paneClosed bool
	paneGone   bool
	row        reapReceipt
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

// closeReviewerPane runs once a pass has reached a terminal outcome. It closes
// the pane this pass spawned and removes its remote detached worktree through
// the hub's guarded removal. It never changes the review outcome, and it runs
// on a context detached from the pass so an expired deadline cannot skip it.
// It reports whether the pane is closed or gone.
func (x *panewireExecutor) closeReviewerPane(parent context.Context, p reviewerPane) (closed bool) {
	ctx, cancel := reportContext(parent)
	defer cancel()
	c := &reviewerCleanup{x: x, p: p, row: reapReceipt{Action: "reviewer-cleanup", Repo: x.repo.Name, PR: x.pr.Number, Host: p.placement.Host, Pane: p.placement.PaneID, Worktree: p.checkout}}
	defer func() {
		closed = c.paneClosed || c.paneGone
		// A forced close that does not close the pane is a skip, never a pass
		// failure: the replacement has already been decided.
		if p.force && !closed {
			_ = x.s.audit(x.repo.Name, "reviewer-close-skipped", map[string]any{"pr": x.pr.Number, "head": p.head, "pass": p.pass, "label": p.label, "pane": p.placement.PaneID, "reason": c.row.Reason}, false)
		}
	}()
	if !p.confirmed {
		// The pane's host is unknown, so it may still run in the local checkout.
		if p.keepLocalCheckout != nil {
			*p.keepLocalCheckout = true
		}
		c.record("pending-cleanup", "spawn named no host and pane; checkout kept")
		return
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
		c.closeLocal(ctx)
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
	cwd, ok := c.closePane(ctx, host)
	if !ok {
		return
	}
	if c.paneGone && filepath.Base(filepath.Clean(cwd)) != p.label {
		c.record("skipped", "pane gone; remote cwd unavailable or mismatched")
		return
	}
	c.removeWorktree(ctx, host, cwd)
	return
}

// closeLocal closes a pane on this host. The checkout it runs in is removed by
// the caller's deferred cleanup once the pane is closed or gone.
func (c *reviewerCleanup) closeLocal(ctx context.Context) {
	// Jobs names the reviewer's job directory, so a forced close can save the
	// pane's screen there before closing it.
	ops := reap.Exec{Runner: c.x.s.Runner, Host: reap.Host{Name: "local", Jobs: c.x.s.Config.Review.Panewire.Jobs}}
	if _, ok := c.closePane(ctx, ops); !ok {
		return
	}
	if c.paneGone {
		c.record("skipped", "pane already gone")
		return
	}
	c.record("closed", "pane closed; local checkout removed by the pass")
}

// closePane closes the recorded pane when it still carries this pass's name
// and cwd and its composer is empty. It returns the pane's cwd and true once
// the pane is closed or gone. Otherwise it records why the pane stays.
func (c *reviewerCleanup) closePane(ctx context.Context, ops reviewerPanes) (string, bool) {
	panes, err := ops.AllPanes(ctx)
	if err != nil {
		c.record("error", "pane list: "+err.Error())
		return "", false
	}
	var pane *reap.Pane
	for i := range panes {
		if panes[i].ID == c.p.placement.PaneID {
			pane = &panes[i]
			break
		}
	}
	if pane == nil {
		c.paneGone = true
		return c.p.placement.Cwd, true
	}
	c.row.Cwd = pane.Cwd
	if pane.Name == "" || !closableStatus[pane.Status] {
		// The full pane list carries no names on a hub node (herdr's pane
		// list has none), and its status need not be the agent's; the agent
		// list names the same pane id and reports its agent status.
		if err := c.nameFromAgents(ctx, ops, pane); err != nil {
			c.record("error", "agent list: "+err.Error())
			return "", false
		}
	}
	if pane.Name != c.p.label || pane.Cwd == "" || filepath.Base(filepath.Clean(pane.Cwd)) != c.p.label {
		c.record("kept", "pane name/cwd mismatch")
		return "", false
	}
	if c.p.force {
		// A reviewer past its deadline is replaced whatever it is doing, so its
		// screen is saved for the record and the pane closes while it works.
		// Capture answers a pane that is no fleet job (the job dir is missing)
		// with a Skip and no error: such a pane is an operator's and stays open,
		// as in the reap sweep.
		saved, err := ops.Capture(ctx, *pane, c.p.label, nil, true)
		if err != nil {
			c.record("error", "pane screen: "+err.Error())
			return "", false
		}
		// A pane that is gone counts as closed, exactly as it does when the
		// pane list no longer carries it, so the guarded worktree removal still
		// runs. Any other skip keeps the pane open: not-a-job names an
		// operator's pane, and pane-changed no longer carries this pass's.
		switch saved.Skip {
		case "":
		case reap.CapturePaneGone:
			c.paneGone = true
			return pane.Cwd, true
		default:
			c.record("kept", "pane screen skipped: "+saved.Skip)
			return "", false
		}
	} else {
		if !closableStatus[pane.Status] {
			// The hub closes only an idle or done agent; a pane still working is
			// left for the reviewer sweep.
			c.record("kept", "pane is not idle or done: "+pane.Status)
			return "", false
		}
		composer, err := ops.Composer(ctx, *pane)
		if err != nil {
			c.record("error", "composer read: "+err.Error())
			return "", false
		}
		if composer != reap.ComposerEmpty {
			c.record("kept", "composer is not empty or could not be confirmed empty")
			return "", false
		}
	}
	outcome, err := ops.CloseExact(ctx, *pane)
	if err != nil {
		c.record("error", "pane close: "+err.Error())
		return "", false
	}
	if outcome != reap.CloseClosed && outcome != reap.CapturePaneGone {
		c.record("kept", "pane close recheck: "+outcome)
		return "", false
	}
	c.paneClosed = true
	return pane.Cwd, true
}

// closableStatus is the agent status the hub's pane.close accepts.
var closableStatus = map[string]bool{"idle": true, "done": true}

// nameFromAgents fills pane's name from the agent list entry with the same
// pane id and cwd. A missing or different entry leaves the name empty, which
// the identity check then refuses.
func (c *reviewerCleanup) nameFromAgents(ctx context.Context, ops reviewerPanes, pane *reap.Pane) error {
	agents, err := ops.Panes(ctx)
	if err != nil {
		return err
	}
	for _, a := range agents {
		if a.ID == pane.ID && filepath.Clean(a.Cwd) == filepath.Clean(pane.Cwd) {
			if pane.Name == "" {
				pane.Name = a.Name
			}
			pane.Status = a.Status
			return nil
		}
	}
	return nil
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
		c.record("skipped", "worktree already gone")
	default:
		c.record("pending-cleanup", "hub guarded removal refused: "+reason)
	}
}

// keepUnfinishedCheckout handles a spawn that ended without a pane result. It
// closes nothing: the checkout stays with a pending-cleanup receipt that names
// it, so the checkout is not removed under a pane that may still run.
func (x *panewireExecutor) keepUnfinishedCheckout(p reviewerPane, reason string) {
	c := &reviewerCleanup{x: x, p: p, row: reapReceipt{Action: "reviewer-cleanup", Repo: x.repo.Name, PR: x.pr.Number, Worktree: p.checkout}}
	c.record("pending-cleanup", reason)
}

// record writes this cleanup's one result: a reap receipt and the
// reviewer-cleanup audit record. A local checkout stays while its pane may
// still run in it.
func (c *reviewerCleanup) record(result, reason string) {
	reason = fleetevent.Redact(reason)
	if c.local && c.p.keepLocalCheckout != nil {
		*c.p.keepLocalCheckout = !(c.paneClosed || c.paneGone)
	}
	c.row.Result, c.row.Reason = result, reason
	_ = c.x.s.reapNote(c.row, false)
	_ = c.x.s.audit(c.x.repo.Name, "reviewer-cleanup", map[string]any{
		"pr":     c.x.pr.Number,
		"head":   c.p.head,
		"pass":   c.p.pass,
		"label":  c.p.label,
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
