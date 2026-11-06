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
)

// A PR with no job binding still has an author: the worker whose pane sits in
// a linked worktree of this repository on the PR's head branch. FINDINGS and
// READY find it with the pane scan reap uses, after the job binding and
// before any Notify-Lane trailer or task lane.

// authorScan is one evaluation's host scan. The hosts are read once and the
// result — including a read error — is reused by every route lookup under the
// same context, so FINDINGS and READY for one PR cost one pane list per host.
type authorScan struct {
	once  sync.Once
	hosts []*reapHost
	err   error
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
