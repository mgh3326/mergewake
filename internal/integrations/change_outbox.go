package integrations

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

type changeObservation struct {
	Repo  string          `json:"repo"`
	PR    int             `json:"pr"`
	Name  string          `json:"name"`
	At    time.Time       `json:"at"`
	Value json.RawMessage `json:"value"`
}

// persist writes the observation to a temp file and renames it into the
// outbox. The temp file is locked before the rename, so the entry is claimed
// from the moment it appears under its .json name. The caller owns the returned
// lock and holds it until the entry is sent and removed, or the process exits.
func (w *ChangeWriter) persist(repo string, pr int, name string, s ChangeSection) (string, *os.File, error) {
	if w.outbox == "" {
		return "", nil, nil
	}
	value, err := json.Marshal(s.Value)
	if err != nil {
		return "", nil, err
	}
	data, err := json.Marshal(changeObservation{Repo: repo, PR: pr, Name: name, At: s.At, Value: value})
	if err != nil {
		return "", nil, err
	}
	if err = os.MkdirAll(w.outbox, 0700); err != nil {
		return "", nil, err
	}
	f, err := os.CreateTemp(w.outbox, ".change-*")
	if err != nil {
		return "", nil, err
	}
	temp := f.Name()
	nameOut := temp + ".json"
	// The temp name does not end in .json, so no replay reads it before the rename.
	if err = fsutil.TryLockFile(f); err == nil {
		if _, err = f.Write(data); err == nil {
			err = f.Sync()
		}
	}
	if err == nil {
		err = os.Rename(temp, nameOut)
	}
	if err != nil {
		_ = f.Close()
		_ = os.Remove(temp)
		return "", nil, err
	}
	return nameOut, f, nil
}

// reload replays observations left by a previous process or a failed send.
// Entries held by another live process, or by this writer, are skipped.
func (w *ChangeWriter) reload() {
	if w.outbox == "" {
		return
	}
	files, err := os.ReadDir(w.outbox)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		w.log.Warn("hk change outbox unreadable")
		return
	}
	ioFailed := false
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".json" {
			continue
		}
		if !w.replayEntry(filepath.Join(w.outbox, f.Name())) {
			ioFailed = true
		}
	}
	if ioFailed {
		w.log.Warn("hk change outbox claim failed")
	}
}

// replayEntry takes over one outbox entry and queues it, unless this writer or
// a live process holds it. It reports false only when the claim failed for an
// I/O reason; a missing entry or one held elsewhere is not a failure.
func (w *ChangeWriter) replayEntry(path string) bool {
	w.mu.Lock()
	if _, own := w.claims[path]; own {
		w.replayLocked(path)
		w.mu.Unlock()
		return true
	}
	w.mu.Unlock()
	claim, err := claimEntry(path)
	if err != nil {
		return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.EWOULDBLOCK)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.holdLocked(path, claim)
	w.replayLocked(path)
	return true
}

// claimEntry opens path and takes its lock without blocking. A sender removes
// an entry only after its send, while still holding the lock, so a missing path
// after the lock means the entry was already sent; it is reported as ErrNotExist.
func claimEntry(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err = fsutil.TryLockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err = os.Stat(path); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// replayLocked queues the claimed entry at path. An unreadable or corrupt entry
// is left on disk and its claim is released.
func (w *ChangeWriter) replayLocked(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		w.releaseLocked(path)
		return
	}
	var rec changeObservation
	if json.Unmarshal(data, &rec) != nil || rec.Repo == "" || rec.PR < 1 || rec.At.IsZero() || len(rec.Value) == 0 {
		w.log.Warn("hk change outbox entry unreadable")
		w.releaseLocked(path)
		return
	}
	w.addLocked(rec.Repo, rec.PR, rec.Name, ChangeSection{At: rec.At, Value: rec.Value}, path)
}

// holdLocked records a claim this writer holds on an outbox entry.
func (w *ChangeWriter) holdLocked(path string, claim *os.File) {
	if w.claims == nil {
		w.claims = map[string]*os.File{}
	}
	w.claims[path] = claim
}

// releaseLocked drops this writer's claim on an outbox entry. Closing the file
// releases the lock, so another process can then take the entry.
func (w *ChangeWriter) releaseLocked(path string) {
	if claim, ok := w.claims[path]; ok {
		_ = claim.Close()
		delete(w.claims, path)
	}
}
