package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

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

func authorScanFailures(ctx context.Context) []string {
	if scan, ok := ctx.Value(authorScanKey{}).(*authorScan); ok {
		return append([]string(nil), scan.failedHosts...)
	}
	return nil
}

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
			hs, _ := s.reapScanHost(ctx, hub, h)
			if hs.failed {
				errs = append(errs, fmt.Errorf("author pane scan: host %s: pane list failed", h.Name))
				scan.failedHosts = append(scan.failedHosts, h.Name)
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
	var pane, job string
	n := 0
	for _, hs := range hosts {
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
	rec, e := jobs.Job(ctx, job)
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
		scan, _ := ctx.Value(authorScanKey{}).(*authorScan)
		if scan == nil || scan.readHosts == 0 {
			return authorRoute{}, e
		}
		bound.failedHosts = append([]string(nil), scan.failedHosts...)
		bound.via += " (partial pane scan)"
	}
	route, takenOver := bound, false
	switch len(others) {
	case 0:
	case 1:
		jobs := s.Sink.(reapJobs)
		takeover, e := jobs.Job(ctx, others[0])
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
