package pipeline

import (
	"errors"
	"fmt"

	"github.com/mgh3326/mergewake/internal/reap"
)

// HostSkip names a host a reap pass could not scan at all because its hub node
// is unreachable. Reason is the hub's own word: node_unavailable, node_timeout
// or unknown.
type HostSkip struct {
	Host   string
	Reason string
}

// hostSkipError is a host-scan failure of an unreachable host. It is a typed
// leaf of the reap result, so the daemon can tell a skipped host from a real
// error without matching message text. Removals on a host never produce it.
type hostSkipError struct {
	skip HostSkip
	err  error
}

func (e *hostSkipError) Error() string { return e.err.Error() }
func (e *hostSkipError) Unwrap() error { return e.err }

// hostScanFailure is the error a failed host scan reports: "<host>: <cause>",
// typed as a host skip when the hub says the host's node is unreachable.
func hostScanFailure(host string, cause error) error {
	wrapped := fmt.Errorf("%s: %w", host, cause)
	var unreachable *reap.HostUnreachable
	if errors.As(cause, &unreachable) {
		return &hostSkipError{skip: HostSkip{Host: host, Reason: unreachable.Reason}, err: wrapped}
	}
	return wrapped
}

// SplitReapHostSkips separates the host skips in a reap failure from every other
// error. rest is nil when the only failures were unreachable hosts, and it keeps
// budget and other leaves as they were. Only the top-level joined leaves are
// searched, so a skip must be a direct leaf of the joined result.
func SplitReapHostSkips(err error) ([]HostSkip, error) {
	var skips []HostSkip
	var rest []error
	var walk func(error)
	walk = func(current error) {
		if current == nil {
			return
		}
		if skip, ok := current.(*hostSkipError); ok {
			skips = append(skips, skip.skip)
			return
		}
		if many, ok := current.(interface{ Unwrap() []error }); ok {
			for _, child := range many.Unwrap() {
				walk(child)
			}
			return
		}
		rest = append(rest, current)
	}
	walk(err)
	return skips, errors.Join(rest...)
}
