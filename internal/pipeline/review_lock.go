package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var reviewLocks sync.Map

// lockReview serializes local runs through receipt lookup, collection and posting.
// The stable lock file is never removed, so waiters cannot lock different inodes.
func (s *Service) lockReview(ctx context.Context, repo string, pr int) (func(), error) {
	dir := filepath.Join(s.state(repo), "reviewer-jobs")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%d.lock", pr))
	value, _ := reviewLocks.LoadOrStore(path, make(chan struct{}, 1))
	local := value.(chan struct{})
	select {
	case local <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-local }
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		release()
		return nil, err
	}
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close(); release() }, nil
		}
		if err != unix.EWOULDBLOCK && err != unix.EAGAIN {
			_ = f.Close()
			release()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			release()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
