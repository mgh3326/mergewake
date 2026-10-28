// Package rollout installs verified files with an on-disk rollback journal.
package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

type Snapshot struct {
	Target string `json:"target"`
	Exists bool   `json:"exists"`
	Mode   uint32 `json:"mode"`
	Backup string `json:"backup"`
}
type Journal struct {
	Root  string     `json:"root"`
	Files []Snapshot `json:"files"`
}
type Result struct {
	Commit    string   `json:"commit"`
	Status    string   `json:"status"`
	Installed []string `json:"installed"`
	Denied    []string `json:"denied"`
}
type Options struct {
	Root, StateDir string
	Allowed        []config.Install
	Deny           []string
	DryRun         bool
}

// Atomic writes beside the target, so rename never crosses filesystems.
func Atomic(name string, data []byte, mode os.FileMode) error {
	if e := os.MkdirAll(filepath.Dir(name), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(name), ".mergewake-*")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(mode); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, name); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(name))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func writeJSON(name string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return Atomic(name, b, 0600)
}
func Lock(dir string) (func(), error) {
	if e := os.MkdirAll(filepath.Dir(dir), 0700); e != nil {
		return nil, e
	}
	if e := os.Mkdir(dir, 0700); e != nil {
		return nil, fmt.Errorf("operation already active or stale lock %s: %w", dir, e)
	}
	return func() { _ = os.Remove(dir) }, nil
}
func rollback(dir string, j Journal) error {
	var errs []error
	for i := len(j.Files) - 1; i >= 0; i-- {
		s := j.Files[i]
		target, e := manifest.Resolve(j.Root, s.Target)
		if e == nil {
			if s.Exists {
				var b []byte
				b, e = os.ReadFile(filepath.Join(dir, s.Backup))
				if e == nil {
					e = Atomic(target, b, os.FileMode(s.Mode))
				}
			} else {
				e = os.Remove(target)
				if os.IsNotExist(e) {
					e = nil
				}
			}
		}
		if e != nil {
			errs = append(errs, fmt.Errorf("restore %s: %w", s.Target, e))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return os.RemoveAll(dir)
}

// Recover is also called before every install. An interrupted attempt is undone
// before a subsequent attempt may begin. Stale locks require operator inspection.
func Recover(root, state string) error {
	dir := filepath.Join(state, "transaction")
	b, e := os.ReadFile(filepath.Join(dir, "journal.json"))
	if os.IsNotExist(e) {
		if _, se := os.Stat(dir); se == nil {
			return os.RemoveAll(dir)
		}
		return nil
	}
	if e != nil {
		return e
	}
	var j Journal
	if e = json.Unmarshal(b, &j); e != nil {
		return e
	}
	abs, e := manifest.CanonicalRoot(root)
	if e != nil {
		return e
	}
	journalRoot, je := manifest.CanonicalRoot(j.Root)
	if je != nil {
		return je
	}
	if journalRoot != abs {
		return errors.New("journal install root differs from configured root")
	}
	for _, s := range j.Files {
		if !manifest.SafePath(s.Target) || !manifest.SafePath(s.Backup) {
			return errors.New("invalid journal path")
		}
	}
	return rollback(dir, j)
}
func Apply(ctx context.Context, m manifest.Manifest, o Options, fetch func(context.Context, string) ([]byte, error), smoke func(context.Context) error) (res Result, err error) {
	res = Result{Commit: m.Commit, Status: "failed", Installed: []string{}, Denied: []string{}}
	if err = m.Validate(); err != nil {
		return
	}
	if o.Root == "" || o.StateDir == "" {
		err = errors.New("install root and state directory are required")
		return
	}
	root, e := manifest.CanonicalRoot(o.Root)
	if e != nil {
		err = e
		return
	}
	if root == string(filepath.Separator) {
		err = errors.New("filesystem root cannot be an install root")
		return
	}
	allowed := map[string]config.Install{}
	for _, s := range o.Allowed {
		allowed[s.Target] = s
	}
	selected := []manifest.File{}
	contents := map[string][]byte{}
	for _, f := range m.Files {
		denied := false
		for _, g := range o.Deny {
			var match bool
			match, e = policy.Match(g, f.Target)
			if e != nil {
				err = e
				return
			}
			denied = denied || match
		}
		if denied {
			res.Denied = append(res.Denied, f.Target)
			continue
		}
		spec, ok := allowed[f.Target]
		mode := uint64(0644)
		if spec.Mode != "" {
			mode, e = strconv.ParseUint(spec.Mode, 8, 32)
			if e != nil {
				err = e
				return
			}
		}
		if !ok || spec.Source != f.Source || mode != uint64(f.Mode) {
			err = fmt.Errorf("manifest target not allowed by host config: %s", f.Target)
			return
		}
		if _, e = manifest.Resolve(root, f.Target); e != nil {
			err = e
			return
		}
		selected = append(selected, f)
	}
	if len(selected) == 0 {
		err = errors.New("no permitted install targets")
		return
	}
	if o.DryRun {
		res.Status = "dry-run"
		for _, f := range selected {
			res.Installed = append(res.Installed, f.Target)
		}
		return
	}
	unlock, e := Lock(filepath.Join(o.StateDir, "lock"))
	if e != nil {
		err = e
		return
	}
	defer unlock()
	if err = Recover(root, o.StateDir); err != nil {
		return
	}
	// Download and verify everything before taking a snapshot or mutating targets.
	for _, f := range selected {
		b, e := fetch(ctx, f.Source)
		if e == nil {
			e = manifest.Verify(f, b)
		}
		if e != nil {
			err = e
			return
		}
		contents[f.Target] = b
	}
	dir := filepath.Join(o.StateDir, "transaction")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return
	}
	j := Journal{Root: root}
	for i, f := range selected {
		s := Snapshot{Target: f.Target, Backup: strconv.Itoa(i) + ".bak"}
		b, mode, e := manifest.ReadRegular(root, f.Target)
		if e == nil {
			s.Exists = true
			s.Mode = uint32(mode)
			e = Atomic(filepath.Join(dir, s.Backup), b, 0600)
		} else if os.IsNotExist(e) {
			e = nil
		}
		if e != nil {
			err = e
			_ = os.RemoveAll(dir)
			return
		}
		j.Files = append(j.Files, s)
	}
	if err = writeJSON(filepath.Join(dir, "journal.json"), j); err != nil {
		return
	}
	undo := func(cause error) {
		if re := rollback(dir, j); re != nil {
			err = errors.Join(cause, fmt.Errorf("rollback failed: %w", re))
			res.Status = "rollback-failed"
		} else {
			res.Status = "rolled-back"
			err = cause
		}
	}
	for _, f := range selected {
		if e = ctx.Err(); e != nil {
			undo(e)
			return
		}
		var target string
		target, e = manifest.Resolve(root, f.Target)
		if e == nil {
			e = Atomic(target, contents[f.Target], os.FileMode(f.Mode))
		}
		if e != nil {
			undo(e)
			return
		}
		res.Installed = append(res.Installed, f.Target)
	}
	if e = smoke(ctx); e != nil {
		undo(fmt.Errorf("smoke: %w", e))
		return
	}
	for _, f := range selected {
		b, mode, e := manifest.ReadRegular(root, f.Target)
		if e == nil {
			e = manifest.Verify(f, b)
		}
		if e == nil && uint32(mode) != f.Mode {
			e = errors.New("installed mode mismatch")
		}
		if e != nil {
			undo(e)
			return
		}
	}
	// Persist the receipt while rollback remains possible.
	res.Status = "success"
	if e = writeJSON(filepath.Join(o.StateDir, "installed.json"), struct {
		Manifest manifest.Manifest `json:"manifest"`
		Result   Result            `json:"result"`
	}{m, res}); e != nil {
		undo(e)
		return
	}
	err = os.RemoveAll(dir)
	return
}
