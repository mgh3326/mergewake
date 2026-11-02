package pipeline

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/rollout"
)

type rolloutWindow struct{ path, repo string }
type windowReceipt struct {
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// A kernel-independent directory lock excludes concurrent controllers. The
// durable owner survives crashes and failed recovery, so another service cannot
// restart while the previous service's health is uncertain.
func lockRolloutWindow(state, group, repo string) (*rolloutWindow, func(), error) {
	if group == "" {
		return nil, func() {}, nil
	}
	dir := filepath.Join(state, "rollout-windows", group)
	unlock, e := rollout.Lock(filepath.Join(dir, "lock"))
	if e != nil {
		return nil, nil, e
	}
	path := filepath.Join(dir, "active.json")
	var active windowReceipt
	found, e := fsutil.ReadJSON(path, &active)
	if e != nil {
		unlock()
		return nil, nil, e
	}
	if found && active.Repo != repo {
		unlock()
		return nil, nil, fmt.Errorf("rollout window %s held by %s until healthy recovery", group, active.Repo)
	}
	return &rolloutWindow{path, repo}, unlock, nil
}
func (w *rolloutWindow) begin(sha string) error {
	if w == nil {
		return nil
	}
	return fsutil.WriteJSON(w.path, windowReceipt{w.repo, sha})
}
func (w *rolloutWindow) healthy() error {
	if w == nil {
		return nil
	}
	e := os.Remove(w.path)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	return e
}
