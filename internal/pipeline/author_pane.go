package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// A PR with no job binding still has an author: the worker whose pane sits in
// a linked worktree of this repository on the PR's head branch. FINDINGS and
// READY find it with the pane scan reap uses, after the job binding and
// before any Notify-Lane trailer or task lane.

// authorScan is one evaluation's host scan. The hosts are read once and the
// result — including a read error — is reused by every route lookup under the
// same context, so FINDINGS and READY for one PR cost one pane list per host.
type authorScan struct {
	once        sync.Once
	hosts       []*reapHost
	failedHosts []string
	readHosts   int
	err         error
}

type authorScanKey struct{}

// WithAuthorScan returns ctx carrying a fresh host scan. A caller that
// evaluates one PR in several steps (findings, then the verdict) wraps its
// context once so the steps share the scan; without it each step scans alone.
func WithAuthorScan(ctx context.Context) context.Context {
	if _, ok := ctx.Value(authorScanKey{}).(*authorScan); ok {
		return ctx
	}
	return context.WithValue(ctx, authorScanKey{}, &authorScan{})
}

// hostScanSinkKey carries the report of pane-scan failures to a pass that
// tolerates them.
type hostScanSinkKey struct{}

// WithHostScanSink returns ctx whose pane-scan failures are also reported to
// sink, once per failed host per evaluation. The failure itself is unchanged:
// the gate, the routes and the takeover still tolerate it as a host scan error
// (see hostScanOnly). The report lets the pass that owns ctx name the host even
// when the verdict stays clean and no error reaches it.
func WithHostScanSink(ctx context.Context, sink func(host string)) context.Context {
	return context.WithValue(ctx, hostScanSinkKey{}, sink)
}

func reportHostScanFailure(ctx context.Context, host string) {
	if sink, ok := ctx.Value(hostScanSinkKey{}).(func(string)); ok && sink != nil {
		sink(host)
	}
}

func authorScanFailures(ctx context.Context) []string {
	if scan, ok := ctx.Value(authorScanKey{}).(*authorScan); ok {
		return append([]string(nil), scan.failedHosts...)
	}
	return nil
}

// hostScanError is one configured host's pane-list failure inside an
// authorScan.
type hostScanError struct{ host string }

func (e hostScanError) Error() string {
	return fmt.Sprintf("author pane scan: host %s: pane list failed", e.host)
}

// hostScanOnly reports whether err consists solely of hostScanError leaves:
// hosts that could not be read while others answered, or every host. Any
// other read error — an hk task or job read, the panewire hub — is not, and
// keeps the caller's retry path.
func hostScanOnly(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(hostScanError); ok {
		return true
	}
	switch u := err.(type) {
	case interface{ Unwrap() []error }:
		for _, e := range u.Unwrap() {
			if !hostScanOnly(e) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return hostScanOnly(u.Unwrap())
	}
	return false
}

// SplitHostScan separates the pane-scan failures of unreadable hosts from every
// other error. rest is what still fails the pass; hosts names the unreadable
// hosts once each, for the one line the pass logs.
func SplitHostScan(err error) (rest error, hosts []string) {
	if err == nil {
		return nil, nil
	}
	if hostScanOnly(err) {
		return nil, uniqueSorted(hostScanNames(err))
	}
	u, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return err, nil
	}
	var keep []error
	var names []string
	for _, e := range u.Unwrap() {
		r, h := SplitHostScan(e)
		if r != nil {
			keep = append(keep, r)
		}
		names = append(names, h...)
	}
	return errors.Join(keep...), uniqueSorted(names)
}

// hostScanNames lists the hosts named by the hostScanError leaves of err.
func hostScanNames(err error) []string {
	switch u := err.(type) {
	case hostScanError:
		return []string{u.host}
	case interface{ Unwrap() []error }:
		var names []string
		for _, e := range u.Unwrap() {
			names = append(names, hostScanNames(e)...)
		}
		return names
	case interface{ Unwrap() error }:
		return hostScanNames(u.Unwrap())
	}
	return nil
}

func uniqueSorted(names []string) []string {
	slices.Sort(names)
	return slices.Compact(names)
}

// authorScanTTL is how long one host's pane scan serves every evaluation that
// asks for it. A burst of notify, delivery and review passes over several PRs
// lands inside it, so the burst costs one pane list per host. A route never
// reads a scan older than this.
var authorScanTTL = 30 * time.Second

// authorHostScanBound caps one host's pane scan on the author path (notify and
// route), so one host can never hold a route longer than this, whatever the
// error shape. It bounds the whole host, not each call: a scan makes one call
// per pane. It must sit above what a healthy scan costs: reap measured 25 to
// 89 s per host (reap_pass_hosts.go), so 120 s leaves room above the top of
// that range. A hung node does not wait for this bound: its first call fails
// at the hub's 45 s client timeout, and the host fails there. Expiry fails the
// host for the TTL, and the route goes on with the hosts that answered.
var authorHostScanBound = 120 * time.Second

// hostScanMu guards Service.hostScans. Like the other Service caches, the map
// is shared by copies of a Service.
var hostScanMu sync.Mutex

// hostScanEntry is one host's scan. done closes once hs and at are set; both
// are written under hostScanMu before that. A scan that failed stays a
// failure for the TTL, never a healthy answer, and the next scan after the TTL
// is its retry. A failed scan cut short by its own evaluation's cancellation
// proves nothing about the host: it is dropped, not kept.
type hostScanEntry struct {
	done    chan struct{}
	hs      *reapHost
	at      time.Time
	dropped bool
}

func (e *hostScanEntry) finished() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

// hostScan returns host h's pane scan: the shared one while it is fresh, or
// one a concurrent evaluation is already running. Otherwise it scans h under
// ctx and shares the result.
func (s *Service) hostScan(ctx context.Context, hub *reap.Hub, h reap.Host) *reapHost {
	for {
		hostScanMu.Lock()
		e := s.hostScans[h.Name]
		if e != nil && e.finished() && s.now().Sub(e.at) >= authorScanTTL {
			e = nil
		}
		if e == nil {
			if s.hostScans == nil {
				s.hostScans = map[string]*hostScanEntry{}
			}
			e = &hostScanEntry{done: make(chan struct{})}
			s.hostScans[h.Name] = e
			hostScanMu.Unlock()
			return s.runHostScan(ctx, hub, h, e)
		}
		hostScanMu.Unlock()
		select {
		case <-e.done:
		case <-ctx.Done():
			return &reapHost{host: h, failed: true}
		}
		// A dropped entry names no answer; ask again, which starts a fresh scan.
		if !e.dropped {
			return e.hs
		}
	}
}

func (s *Service) runHostScan(ctx context.Context, hub *reap.Hub, h reap.Host, e *hostScanEntry) *reapHost {
	// The scan's own deadline is a host failure: the host did not answer in
	// time, so it is kept failed for the TTL. Only the evaluation's own
	// cancellation (ctx) drops a scan, since that says nothing about the host.
	scan, cancel := context.WithTimeout(ctx, authorHostScanBound)
	defer cancel()
	hs, _ := s.reapScanHost(scan, hub, h)
	if scan.Err() != nil && ctx.Err() == nil {
		hs.failed = true
	}
	hostScanMu.Lock()
	e.hs, e.at = hs, s.now()
	if ctx.Err() != nil {
		e.dropped = true
		if s.hostScans[h.Name] == e {
			delete(s.hostScans, h.Name)
		}
	}
	hostScanMu.Unlock()
	close(e.done)
	return hs
}

// authorHosts returns every configured host's pane scan for this evaluation.
// Within one evaluation (ctx from WithAuthorScan) each host is read once; across
// evaluations the scan is shared for authorScanTTL (see hostScan).
func (s *Service) authorHosts(ctx context.Context) ([]*reapHost, error) {
	scan, ok := ctx.Value(authorScanKey{}).(*authorScan)
	if !ok {
		scan = &authorScan{}
	}
	scan.once.Do(func() {
		hub, e := s.reapHub()
		if e != nil {
			scan.err = e
			return
		}
		var errs []error
		for _, h := range s.Config.Hosts {
			hs := s.hostScan(ctx, hub, h)
			if hs.failed {
				errs = append(errs, hostScanError{h.Name})
				scan.failedHosts = append(scan.failedHosts, h.Name)
				reportHostScanFailure(ctx, h.Name)
			} else {
				scan.readHosts++
			}
			scan.hosts = append(scan.hosts, hs)
		}
		scan.err = errors.Join(errs...)
	})
	return scan.hosts, scan.err
}

// authorPane routes to the one live pane on a configured host whose cwd is a
// linked worktree of repo r on pr's head branch, through that pane's job
// record. Zero panes, several panes, or a pane with no usable job lane report
// ok=false so the caller falls through to its next rule; a host or job read
// error is returned so the caller retries instead of guessing.
func (s *Service) authorPane(ctx context.Context, r config.Repo, pr github.Pull) (lane, via string, ok bool, err error) {
	jobs, has := s.Sink.(reapJobs)
	if !has || len(s.Config.Hosts) == 0 || pr.Head.Ref == "" || !sameRepoHead(pr, r.Name) {
		return "", "", false, nil
	}
	hosts, err := s.authorHosts(ctx)
	if err != nil {
		return "", "", false, err
	}
	return s.authorPaneOn(ctx, jobs, hosts, r, pr)
}

// routePane is the author pane a route (findings, notice or worker lane) goes
// to. A scan that lost hosts routes on the hosts that answered, as
// blockLanePartial does: a pane there is the route, named partial in via, and
// none is ok=false, exactly as a full scan with no pane. failedHosts names the
// hosts that could not be read. Any other error is returned.
func (s *Service) routePane(ctx context.Context, r config.Repo, pr github.Pull) (rt authorRoute, ok bool, err error) {
	lane, via, ok, err := s.authorPane(ctx, r, pr)
	if err == nil || !hostScanOnly(err) {
		return authorRoute{lane: lane, via: via}, ok, err
	}
	jobs, has := s.Sink.(reapJobs)
	if !has {
		return authorRoute{}, false, err
	}
	hosts, _ := s.authorHosts(ctx)
	for _, hs := range hosts {
		if hs.failed {
			rt.failedHosts = append(rt.failedHosts, hs.host.Name)
		}
	}
	lane, via, ok, err = s.authorPaneOn(ctx, jobs, hosts, r, pr)
	if err != nil {
		return authorRoute{}, false, err
	}
	rt.lane, rt.via = lane, via+" (partial pane scan)"
	return rt, ok, nil
}

// authorPaneOn is authorPane over an already-read host scan. A failed host
// has no panes, so the hosts that answered still give the route; the caller
// decides whether a scan error beside them is fatal.
func (s *Service) authorPaneOn(ctx context.Context, jobs reapJobs, hosts []*reapHost, r config.Repo, pr github.Pull) (lane, via string, ok bool, err error) {
	var pane, job string
	n := 0
	for _, hs := range hosts {
		if hs.failed {
			continue
		}
		for _, p := range hs.panes {
			w := hs.trees[p.Cwd]
			if p.Cwd == "" || w == nil || w.Branch != pr.Head.Ref || !strings.EqualFold(w.Repo, r.Name) {
				continue
			}
			n++
			pane, job = p.ID, p.Name
		}
	}
	if n != 1 || job == "" {
		return "", "", false, nil
	}
	rec, e := integrations.JobReadOnce(ctx, jobs, job)
	if errors.Is(e, integrations.ErrJobNotFound) {
		return "", "", false, nil
	}
	if e != nil {
		return "", "", false, e
	}
	for _, l := range []string{rec.HubLane, rec.Lane, rec.Label} {
		if laneName.MatchString(l) {
			return l, "author pane " + pane + " job " + job, true, nil
		}
	}
	return "", "", false, nil
}

// authorRoute is an author lane found from the task's bound job, possibly
// superseded by a takeover (see boundAuthorLane). candidates names several
// other live panes when the bound lane was kept. finished names the bound lane
// when its job is terminal and no host shows a pane for it; it is set whether
// or not a takeover then won. unknown is set when a terminal job could not be
// checked against every host.
type authorRoute struct {
	lane, via   string
	candidates  []string
	failedHosts []string
	finished    string
	unknown     bool
}

// boundAuthorLane is the PR author's lane when the task binds a job with a
// usable lane. A bound job is the author only until another live pane on the
// PR's head branch takes the PR over, so the bound lane stands unless exactly
// one other such pane exists; then that pane's job lane wins, via
// "takeover pane <label>". Several other panes keep the bound lane and name
// the candidates for an audit row. A bound job that is terminal and that no
// read host shows a pane for is finished: its lane is not returned (the caller
// falls through to authorPane and the trailer), and the route names it in
// finished so a delivery still on it can be rerouted. A host or job read error
// is returned so the caller retries rather than picking a lane.
func (s *Service) boundAuthorLane(ctx context.Context, r config.Repo, pr github.Pull, task string) (authorRoute, error) {
	// One scan record for the whole route, so the hosts that failed are the
	// ones this route read, and a route with no answering host still names them.
	ctx = WithAuthorScan(ctx)
	job, rec, ok, e := s.boundJob(ctx, task)
	if e != nil || !ok {
		return authorRoute{}, e
	}
	lane := jobLane(rec)
	if lane == "" {
		return authorRoute{}, nil
	}
	finished, unknown, e := s.authorFinished(ctx, job, rec)
	if e != nil {
		return authorRoute{}, e
	}
	bound := authorRoute{lane: lane, via: "task " + task + " job " + job}
	others, e := s.takeoverPanes(ctx, r, pr, job, rec.Label)
	if e != nil {
		// A host whose pane list failed is a partial scan, even when no host
		// answered: the bound lane stands, named partial and unknown, as it does
		// for a scan that answered.
		if !hostScanOnly(e) {
			return authorRoute{}, e
		}
		scan, _ := ctx.Value(authorScanKey{}).(*authorScan)
		if scan != nil {
			bound.failedHosts = append([]string(nil), scan.failedHosts...)
		}
		bound.via += " (partial pane scan)"
	}
	// A partial scan cannot establish that one observed pane is the unique
	// takeover candidate. Keep the bound author lane and report every observed
	// candidate in the partial-route audit.
	if len(bound.failedHosts) > 0 && len(others) > 0 && !finished {
		bound.candidates = others
		bound.unknown = unknown
		return bound, nil
	}
	route, takenOver := bound, false
	switch len(others) {
	case 0:
	case 1:
		jobs := s.Sink.(reapJobs)
		takeover, e := integrations.JobReadOnce(ctx, jobs, others[0])
		if e != nil && !errors.Is(e, integrations.ErrJobNotFound) {
			return authorRoute{}, e
		}
		if e == nil {
			if l := jobLane(takeover); l != "" {
				route = authorRoute{lane: l, via: "takeover pane " + others[0] + partialVia(bound), failedHosts: bound.failedHosts}
				takenOver = true
			}
		}
	default:
		bound.candidates = others
		route = bound
	}
	if finished {
		route.finished = lane
		if !takenOver {
			route.lane, route.via = "", ""
		}
	}
	route.unknown = unknown
	return route, nil
}

// authorFinished reports whether a bound job that is terminal has left no pane
// on the configured hosts. finished is true only when every host was read and
// none shows a pane named for the job, its label, or its recorded pane id —
// the pane id counting only on the host the record names, since a pane id is
// local to its host. unknown is set for a terminal job whose hosts were not
// all read, or with no hosts configured: liveness is then unknown and the
// bound lane stands. A job that is not terminal is never finished.
func (s *Service) authorFinished(ctx context.Context, job string, rec integrations.JobRecord) (finished, unknown bool, err error) {
	if !terminalJob(rec.State) {
		return false, false, nil
	}
	if len(s.Config.Hosts) == 0 {
		return false, true, nil
	}
	hosts, e := s.authorHosts(ctx)
	if len(hosts) == 0 && e != nil {
		return false, false, e
	}
	// The recorded pane id keeps the job live only on the host rec.Host
	// names. An empty host, or one that names no configured host, leaves the
	// id matching on every host: liveness is not given up on a host name
	// mergewake cannot place.
	placed := false
	for _, h := range s.Config.Hosts {
		if authorHostNamed(rec.Host, h) {
			placed = true
			break
		}
	}
	for _, hs := range hosts {
		if hs.failed {
			return false, true, nil
		}
		paneID := rec.PaneID != "" && (!placed || authorHostNamed(rec.Host, hs.host))
		for _, p := range hs.panes {
			if p.Name == job || (rec.Label != "" && p.Name == rec.Label) || (paneID && p.ID == rec.PaneID) {
				return false, false, nil
			}
		}
	}
	if len(hosts) < len(s.Config.Hosts) {
		return false, true, nil
	}
	return true, false, nil
}

// authorHostNamed reports whether a job record's host names this configured
// host: the host's Name or MachineID, or "local" for a host that runs on this
// machine — the rule localReviewerHost uses for reviewer placements.
func authorHostNamed(recorded string, h reap.Host) bool {
	return recorded != "" && (recorded == h.Name || recorded == h.MachineID() || (recorded == "local" && h.Local()))
}

func partialVia(route authorRoute) string {
	if len(route.failedHosts) > 0 {
		return " (partial pane scan)"
	}
	return ""
}

// takeoverPanes names the live panes on the PR's head branch in repo r that
// could be a takeover author: not the bound job's own pane (named by its job
// or its label) and not a mergewake reviewer pane (labelled mwr-…). No hosts,
// or a PR with no head branch in this repository, scans nothing.
func (s *Service) takeoverPanes(ctx context.Context, r config.Repo, pr github.Pull, job, label string) ([]string, error) {
	if len(s.Config.Hosts) == 0 || pr.Head.Ref == "" || !sameRepoHead(pr, r.Name) {
		return nil, nil
	}
	hosts, err := s.authorHosts(ctx)
	var names []string
	for _, hs := range hosts {
		if hs.failed {
			continue
		}
		for _, p := range hs.panes {
			w := hs.trees[p.Cwd]
			if p.Cwd == "" || w == nil || w.Branch != pr.Head.Ref || !strings.EqualFold(w.Repo, r.Name) {
				continue
			}
			if p.Name == "" || p.Name == job || p.Name == label || strings.HasPrefix(p.Name, "mwr-") {
				continue
			}
			names = append(names, p.Name)
		}
	}
	return names, err
}
