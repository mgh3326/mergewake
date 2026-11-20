package pipeline

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/mgh3326/mergewake/internal/reap"
)

// reapPassHostsKey carries one reap pass's host scans to each repository the pass visits.
type reapPassHostsKey struct{}

// reapPassHosts holds one reap pass's host scans by host name. The daemon's reap
// role visits its repositories one after another, and each repository used to
// scan every host again: 25 to 89 s per host per repository in production.
type reapPassHosts struct {
	mu    sync.Mutex
	scans map[string]reapPassScan
}

type reapPassScan struct {
	hs   *reapHost
	errs []error
}

// WithReapPassHosts scopes one reap pass's host scans to ctx. Within the pass a
// host is scanned once, and each later repository reuses that scan.
func WithReapPassHosts(ctx context.Context) context.Context {
	return context.WithValue(ctx, reapPassHostsKey{}, &reapPassHosts{scans: map[string]reapPassScan{}})
}

// reapHostScan is reapScanHost for one repository of a reap pass. Without a pass
// in ctx it is reapScanHost. Within one, a host the pass already scanned answers
// with that scan and its errors, so each repository reports the same host
// failures, and a host that failed stays failed for the pass: its removals stay
// refused, as a fresh scan would refuse them. A scan cut short by the pass's
// context or by a repository budget is not kept, so the next repository scans
// the host itself.
//
// A reused scan can be older than the repository's own work. Each removal path
// re-reads the host's panes first (refreshPanes), so the in-use check sees the
// panes as they are then.
func (s *Service) reapHostScan(ctx context.Context, hub *reap.Hub, h reap.Host) (*reapHost, []error) {
	pass, _ := ctx.Value(reapPassHostsKey{}).(*reapPassHosts)
	if pass == nil {
		return s.reapScanHost(ctx, hub, h)
	}
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if sc, ok := pass.scans[h.Name]; ok {
		return s.rebindHostScan(ctx, hub, sc.hs), sc.errs
	}
	hs, errs := s.reapScanHost(ctx, hub, h)
	budgetCut := slices.ContainsFunc(errs, func(e error) bool { return errors.Is(e, ErrReapRepoBudget) })
	if ctx.Err() == nil && !budgetCut {
		pass.scans[h.Name] = reapPassScan{hs: hs, errs: errs}
	}
	return hs, errs
}

// rebindHostScan answers a repository with its own copy of a pass scan. The
// discovery data is shared; the host operations are not. They are built from
// the hub client and telemetry of the call that uses them: a hub client memoizes
// its agent list, so an operation bound to the first repository's client would
// answer every later refresh from that first read.
func (s *Service) rebindHostScan(ctx context.Context, hub *reap.Hub, scan *reapHost) *reapHost {
	hs := *scan
	h := scan.host
	hs.exec = reap.Exec{Runner: reapRunner(ctx, s.Runner), Host: h}
	hs.ops, hs.viaHub = hs.exec, false
	if hub != nil && !h.Local() {
		hs.ops, hs.viaHub = timedHubOps{Ops: reap.HubHost{Hub: hub, Machine: h.MachineID()}, telemetry: telemetryFrom(ctx)}, true
	}
	return &hs
}
