// Package fsutil holds the durable local-state file operations every role
// shares: atomic replacement, JSON receipts and append-only JSONL logs.
package fsutil

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

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

// WriteJSON atomically replaces name with indented JSON readable only by its owner.
func WriteJSON(name string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return Atomic(name, b, 0600)
}

// ReadJSON decodes name into v. A missing file reports false and no error;
// any other read or decode failure is returned.
func ReadJSON(name string, v any) (bool, error) {
	b, e := os.ReadFile(name)
	if errors.Is(e, fs.ErrNotExist) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return true, json.Unmarshal(b, v)
}

// AppendLine appends one line to name, creating it and its directory owner-only,
// and syncs before returning so a logged decision survives a crash.
func AppendLine(name string, line []byte) error {
	if e := os.MkdirAll(filepath.Dir(name), 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.Write(append(line, '\n'))
	if e == nil {
		e = f.Sync()
	}
	return e
}
