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
	// keepLocalCheckout is set while a local pane still runs in the checkout.
	keepLocalCheckout *bool
}

// reviewerPanes is the pane surface a close needs; reap.Exec serves a local
// host and reap.HubHost a remote one.
type reviewerPanes interface {
	AllPanes(context.Context) ([]reap.Pane, error)
	Composer(context.Context, reap.Pane) (string, error)
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
// cwd is available in stdout on versions that report it directly.
func spawnPlacement(stdout string) (integrations.JobRecord, bool) {
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
					v.Host = p.Host
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

// closeReviewerPane runs once a pass has reached a terminal outcome. It closes
// the pane this pass spawned and removes its remote detached worktree through
// the hub's guarded removal. It never changes the review outcome, and it runs
// on a context detached from the pass so an expired deadline cannot skip it.
func (x *panewireExecutor) closeReviewerPane(parent context.Context, p reviewerPane) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 30*time.Second)
	defer cancel()
	c := &reviewerCleanup{x: x, p: p, row: reapReceipt{Action: "reviewer-cleanup", Repo: x.repo.Name, PR: x.pr.Number, Host: p.placement.Host, Pane: p.placement.PaneID, Worktree: p.checkout}}
	if !p.confirmed {
		c.record("skipped", "spawn did not name a host and pane")
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
}

// closeLocal closes a pane on this host. The checkout it runs in is removed by
// the caller's deferred cleanup once the pane is closed or gone.
func (c *reviewerCleanup) closeLocal(ctx context.Context) {
	if _, ok := c.closePane(ctx, reap.Exec{Runner: c.x.s.Runner}); !ok {
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
	if pane.Name != c.p.label || pane.Cwd == "" || filepath.Base(filepath.Clean(pane.Cwd)) != c.p.label {
		c.record("kept", "pane name/cwd mismatch")
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
	removed, err := host.WorktreeRemove(ctx, tree.Path, c.p.head, c.x.pr.Base.Ref, nil)
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
