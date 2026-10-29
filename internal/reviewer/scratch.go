package reviewer

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// scratchLock names the file inside a scratch directory on which the owning
// review holds an exclusive flock for its whole lifetime. The kernel drops the
// lock when the owner exits or is killed, so it cannot go stale or be
// confused by pid reuse the way a recorded pid could.
const scratchLock = "owner.lock"

// NewScratch creates a review scratch directory and locks it as live. The
// returned release removes the directory and then drops the lock; a sweep run
// by another review skips the directory until then.
func NewScratch() (string, func(), error) {
	path, err := os.MkdirTemp("", ScratchPattern)
	if err != nil {
		return "", nil, err
	}
	f, err := os.OpenFile(filepath.Join(path, scratchLock), os.O_CREATE|os.O_WRONLY, 0600)
	if err == nil {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	}
	if err != nil {
		if f != nil {
			f.Close()
		}
		os.RemoveAll(path)
		return "", nil, err
	}
	// The pid is informational only; liveness is decided by the lock.
	_, _ = f.WriteString(strconv.Itoa(os.Getpid()) + "\n")
	return path, func() {
		os.RemoveAll(path)
		f.Close()
	}, nil
}

// ScratchPattern names the per-review temporary directory holding the empty
// work directory and adapter control files (private homes, settings).
const ScratchPattern = "mergewake-review-*"

// SweepScratch removes this user's review scratch directories that are older
// than age and whose owner is provably gone. A review removes its own on
// return, but a parent killed by SIGKILL never runs that cleanup, and Kimi's
// private home holds copied credentials. The top-level mtime does not move
// while a review works inside the directory, so age alone cannot tell a dead
// owner from a long review: a directory whose lock is held, or cannot be
// checked, is kept. A directory with no lock file predates NewScratch (or its
// owner died before locking, within microseconds of creating it); only age
// applies to it.
func SweepScratch(root string, age time.Duration, now time.Time) error {
	matches, err := filepath.Glob(filepath.Join(root, ScratchPattern))
	if err != nil {
		return err
	}
	var errs []error
	for _, path := range matches {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || now.Sub(info.ModTime()) < age {
			continue
		}
		if st, ok := info.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != os.Getuid() {
			continue
		}
		release, ok := claimScratch(path)
		if !ok {
			continue
		}
		errs = append(errs, os.RemoveAll(path))
		release()
	}
	return errors.Join(errs...)
}

// claimScratch takes the scratch directory's lock without blocking. It
// reports false when a live review holds it or the lock cannot be examined.
func claimScratch(path string) (func(), bool) {
	f, err := os.OpenFile(filepath.Join(path, scratchLock), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return func() {}, true
	}
	if err != nil {
		return nil, false
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		f.Close()
		return nil, false
	}
	return func() { f.Close() }, true
}
