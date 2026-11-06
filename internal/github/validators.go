package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Validators are the ETags of the reads that decide a PR's verdict: the PR, its
// review list and its head's check runs and statuses. Two equal Validators for
// the same PR mean GitHub would have answered each read with a 304. An empty
// field means GitHub gave no ETag for it, so the read says nothing about change.
type Validators struct {
	// Head is the PR's head commit as the PR read returned it.
	Head                  string
	Pull, Reviews, Checks string
}

// Known reports whether every read came with an ETag.
func (v Validators) Known() bool {
	return v.Head != "" && v.Pull != "" && v.Reviews != "" && v.Checks != ""
}

// Changed names the reads whose ETag differs from old, in a fixed order.
func (v Validators) Changed(old Validators) []string {
	var out []string
	if v.Head != old.Head {
		out = append(out, "head")
	}
	if v.Checks != old.Checks {
		out = append(out, "checks")
	}
	if v.Reviews != old.Reviews {
		out = append(out, "reviews")
	}
	if v.Pull != old.Pull {
		out = append(out, "pr")
	}
	return out
}

// etagProbe collects the ETag of every GET answered under a context that
// carries it: the 200's own ETag, or the validator a 304 confirmed.
type etagProbe struct {
	mu   sync.Mutex
	tags []string
}

type etagProbeKey struct{}

func withProbe(ctx context.Context) (context.Context, *etagProbe) {
	p := &etagProbe{}
	return context.WithValue(ctx, etagProbeKey{}, p), p
}

func probeOf(ctx context.Context) *etagProbe {
	p, _ := ctx.Value(etagProbeKey{}).(*etagProbe)
	return p
}

func (p *etagProbe) note(tag string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.tags = append(p.tags, tag)
	p.mu.Unlock()
}

// fingerprint joins the collected ETags; it is empty when nothing was read or
// any read lacked one.
func (p *etagProbe) fingerprint() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.tags) == 0 {
		return ""
	}
	for _, t := range p.tags {
		if t == "" {
			return ""
		}
	}
	return strings.Join(p.tags, ",")
}

// PullValidators revalidates the reads behind one PR's verdict and returns
// their ETags. Each read is a conditional request, so for an unchanged PR it
// costs only 304s; it never reads from the pass memo or the evidence cache.
// The check reads follow the PR's own head, not the caller's.
func (c *Client) PullValidators(ctx context.Context, repo string, n int) (Validators, error) {
	var v Validators
	pullCtx, pull := withProbe(ctx)
	b, e := c.request(pullCtx, "GET", fmt.Sprintf("%s/pulls/%d", base(repo), n), "application/vnd.github+json", nil)
	if e != nil {
		return v, e
	}
	var p Pull
	if e = json.Unmarshal(b, &p); e != nil {
		return v, e
	}
	v.Head, v.Pull = p.Head.SHA, pull.fingerprint()
	reviewCtx, reviews := withProbe(ctx)
	if _, e = list[Review](reviewCtx, c, fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n)); e != nil {
		return v, e
	}
	v.Reviews = reviews.fingerprint()
	checkCtx, checks := withProbe(ctx)
	if _, e = c.checkEvidence(checkCtx, repo, p.Head.SHA, true); e != nil {
		return v, e
	}
	v.Checks = checks.fingerprint()
	return v, nil
}
