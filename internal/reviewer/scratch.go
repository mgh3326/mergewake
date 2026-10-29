package reviewer

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// ScratchPattern names the per-review temporary directory holding the empty
// work directory and adapter control files (private homes, settings).
const ScratchPattern = "mergewake-review-*"

// SweepScratch removes this user's review scratch directories older than age.
// A review removes its own on return, but a parent killed by SIGKILL never
// runs that cleanup, and Kimi's private home holds copied credentials. Callers
// pass an age beyond any live review's timeout.
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
		errs = append(errs, os.RemoveAll(path))
	}
	return errors.Join(errs...)
}
