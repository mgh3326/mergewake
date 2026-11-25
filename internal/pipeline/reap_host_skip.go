package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/mgh3326/mergewake/internal/reap"
)

// HostSkip names a host a reap pass could not scan at all because the hub cannot
// serve it right now or a call against it timed out. Reason is the hub's own
// word: node_lost, node_unavailable, node_timeout, hub_draining,
// unexpected-response (the hub's front proxy answered 502/503/504 with no hub
// body) or unknown (the hub does not know the machine); or "timeout".
type HostSkip struct {
	Host   string
	Reason string
}

// hostSkipError is a host-scan failure of an unreachable or timed-out host. It is
// a typed leaf of the reap result, so the daemon can tell a skipped host from a
// real error without matching message text. A hub call that failed on a host the
// hub cannot serve is typed the same way wherever its error is collected.
type hostSkipError struct {
	skip HostSkip
	err  error
}

func (e *hostSkipError) Error() string { return e.err.Error() }
func (e *hostSkipError) Unwrap() error { return e.err }

// hostScanFailure is the error a failed host scan reports: "<host>: <cause>",
// typed as a host skip when the hub says the host cannot be served or when the
// inspect timed out. A failure caused by the repository budget is never a host
// skip, on any host kind: it is the budget stop, recorded the way budgetCut
// records every other cut. operation names the call the budget cut.
func hostScanFailure(ctx context.Context, host, operation string, cause error) error {
	wrapped := fmt.Errorf("%s: %w", host, cause)
	if repoBudgetFailure(ctx, cause) {
		// A hub call already recorded its own cut (timedHubOps), so a cause that
		// wraps the budget stop is returned as it is.
		if errors.Is(cause, ErrReapRepoBudget) {
			return wrapped
		}
		return budgetCut(ctx, wrapped, operation)
	}
	if isHostUnreachable(cause) {
		return hostUnreachableFailure(host, wrapped)
	}
	if isTimeout(cause) {
		return &hostSkipError{skip: HostSkip{Host: host, Reason: "timeout"}, err: wrapped}
	}
	return wrapped
}

// hostUnreachableFailure is err, a failure of a hub call against host, typed as
// a host skip when the hub answered that the host cannot be served now. Any
// other error is returned unchanged, so auth and cwd answers stay failures. Only
// the hub's answer is typed here: a timeout is typed by hostScanFailure alone,
// because a budget cut also reads as one.
func hostUnreachableFailure(host string, err error) error {
	var unreachable *reap.HostUnreachable
	if errors.As(err, &unreachable) {
		return &hostSkipError{skip: HostSkip{Host: host, Reason: unreachable.Reason}, err: err}
	}
	return err
}

// repoBudgetFailure reports whether a failed call ended because the repository
// budget ran out: its error wraps the budget stop, or the scan's own context
// ended by the budget. A call cut by its own timeout while the budget still has
// time is not one.
func repoBudgetFailure(ctx context.Context, cause error) bool {
	return errors.Is(cause, ErrReapRepoBudget) || context.Cause(ctx) == ErrReapRepoBudget
}

// isHostUnreachable reports whether err is the hub's answer that a host's node
// cannot be reached.
func isHostUnreachable(err error) bool {
	var unreachable *reap.HostUnreachable
	return errors.As(err, &unreachable)
}

// isTimeout reports whether err is a deadline or a client timeout: a hub call
// that did not answer in time, or a scan cut by its own deadline.
func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout()
}

// SplitReapRepoError splits one repository's reap error for the daemon: the
// budget stop it hit (nil when none), the hosts it skipped, and the failure the
// pass keeps. A skipped host is never kept, so it cannot reappear in the
// repository's error line or its persisted role failure.
func SplitReapRepoError(err error) (budget *ReapBudgetStop, skips []HostSkip, kept error) {
	failures := err
	if errors.As(err, &budget) {
		failures = budget.Failures
	}
	skips, kept = SplitReapHostSkips(failures)
	return budget, skips, kept
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
