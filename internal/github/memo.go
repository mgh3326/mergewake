package github

import (
	"context"
	"strings"
)

// A pass memoizes the mutable reads that repeat inside it (Head, Pull,
// ReviewHistory): the first result stands for the rest of the pass. A read that
// asks for fresh evidence skips the memo and refreshes it, and any write to the
// repository drops the repository's entries, so a decision never reads state
// older than the pass's own writes.

type memoKey struct{ repo, kind, id string }

type passMemo struct {
	entries   map[memoKey]any
	epoch     map[string]uint64 // per repository; "" counts writes to an unknown one
	cacheDown bool
	// Hits and Misses count ObjectCache lookups answered and not answered;
	// MemoHits counts reads served by the memo.
	hits, misses, memoHits int
}

// CacheStats is the pass's cache counters for its github_requests log line.
type CacheStats struct {
	Hits     int `json:"cache_hits"`
	Misses   int `json:"cache_misses"`
	MemoHits int `json:"memo_hits"`
}

func (c *RequestCounts) CacheStats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{c.pass.hits, c.pass.misses, c.pass.memoHits}
}

// passOf is nil outside a pass; every method below accepts nil.
func passOf(ctx context.Context) *RequestCounts {
	c, _ := ctx.Value(requestCountsKey{}).(*RequestCounts)
	return c
}

func (c *RequestCounts) noteCache(hit bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if hit {
		c.pass.hits++
	} else {
		c.pass.misses++
	}
}

func (c *RequestCounts) cacheOff() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pass.cacheDown
}

// tripCache switches the cache off for the pass and reports whether this call
// did it, so the caller logs once. Outside a pass it always reports true.
func (c *RequestCounts) tripCache() bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	was := c.pass.cacheDown
	c.pass.cacheDown = true
	return !was
}

func (c *RequestCounts) epochOf(repo string) uint64 {
	return c.pass.epoch[repo] + c.pass.epoch[""]
}

// invalidate drops the memo of repo, or of every repository when repo is empty.
func (c *RequestCounts) invalidate(repo string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pass.epoch == nil {
		c.pass.epoch = map[string]uint64{}
	}
	c.pass.epoch[repo]++
	for k := range c.pass.entries {
		if repo == "" || k.repo == repo {
			delete(c.pass.entries, k)
		}
	}
}

// memoRepo names the repository a write route touches, or "" when unknown.
func memoRepo(route string) string {
	parts := strings.Split(strings.TrimPrefix(route, "/"), "/")
	if len(parts) >= 3 && parts[0] == "repos" {
		return parts[1] + "/" + parts[2]
	}
	return ""
}

func isFresh(ctx context.Context) bool {
	return ctx.Value(freshRequestKey{}) != nil || ctx.Value(freshEvidenceKey{}) != nil
}

// memoized returns the pass's first successful result for k. Errors are never
// kept. A fresh read always calls read and stores its result.
func memoized[T any](ctx context.Context, k memoKey, read func() (T, error)) (T, error) {
	p := passOf(ctx)
	if p == nil {
		return read()
	}
	if !isFresh(ctx) {
		p.mu.Lock()
		v, ok := p.pass.entries[k]
		if ok {
			p.pass.memoHits++
		}
		p.mu.Unlock()
		if ok {
			return v.(T), nil
		}
	}
	p.mu.Lock()
	epoch := p.epochOf(k.repo)
	p.mu.Unlock()
	v, e := read()
	if e != nil {
		return v, e
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// A write that landed during the read may have made v stale.
	if p.epochOf(k.repo) == epoch {
		if p.pass.entries == nil {
			p.pass.entries = map[memoKey]any{}
		}
		p.pass.entries[k] = v
	}
	return v, nil
}
