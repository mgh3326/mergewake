package github

import (
	"context"
	"strings"
	"sync"
)

// A pass memoizes the mutable reads that repeat inside it (Head, Pull,
// ReviewHistory): the first result stands for the rest of the pass. A read that
// asks for fresh evidence skips the memo and refreshes it. Every write the
// client makes, from any pass, bumps its repository's write epoch; an entry
// holds only while the epoch it was read under is current, so no pass reads
// state older than a write this process made, and a read in flight during a
// write is not kept.

type memoKey struct{ repo, kind, id string }

type memoEntry struct {
	value any
	epoch uint64
}

// passMemo is one pass's memo and cache breaker. A pass that repeats (watch,
// agent) starts a new one each iteration with NewPass.
type passMemo struct {
	mu        sync.Mutex
	entries   map[memoKey]memoEntry
	cacheDown bool
}

type passMemoKey struct{}

// NewPass scopes the read memo and the object-cache breaker to a new pass under
// ctx. Request counts and cache counters stay with the ctx's RequestCounts, so
// accounting runs across passes.
func NewPass(ctx context.Context) context.Context {
	return context.WithValue(ctx, passMemoKey{}, &passMemo{})
}

// memoOf is nil outside a pass; every passMemo method accepts nil.
func memoOf(ctx context.Context) *passMemo {
	m, _ := ctx.Value(passMemoKey{}).(*passMemo)
	return m
}

// writeEpochs counts the writes the client has made, per repository. The empty
// repository counts writes to an unknown one and applies to all.
type writeEpochs struct {
	mu    sync.Mutex
	epoch map[string]uint64
}

func (w *writeEpochs) of(repo string) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.epoch[repo] + w.epoch[""]
}

func (w *writeEpochs) bump(repo string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.epoch == nil {
		w.epoch = map[string]uint64{}
	}
	w.epoch[repo]++
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

func (m *passMemo) cacheOff() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cacheDown
}

// tripCache switches the cache off for the pass and reports whether this call
// did it, so the caller logs once. Outside a pass it always reports true.
func (m *passMemo) tripCache() bool {
	if m == nil {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	was := m.cacheDown
	m.cacheDown = true
	return !was
}

// memoized returns the pass's first successful result for k while no write to
// the repository has happened since. Errors are never kept. A fresh read always
// calls read and stores its result.
func memoized[T any](ctx context.Context, c *Client, k memoKey, read func() (T, error)) (T, error) {
	m := memoOf(ctx)
	if m == nil {
		return read()
	}
	if !isFresh(ctx) {
		m.mu.Lock()
		entry, ok := m.entries[k]
		m.mu.Unlock()
		if ok && entry.epoch == c.writes.of(k.repo) {
			passOf(ctx).noteMemoHit()
			return entry.value.(T), nil
		}
	}
	epoch := c.writes.of(k.repo)
	v, e := read()
	if e != nil {
		return v, e
	}
	// A write that landed during the read may have made v stale.
	if c.writes.of(k.repo) == epoch {
		m.mu.Lock()
		if m.entries == nil {
			m.entries = map[memoKey]memoEntry{}
		}
		m.entries[k] = memoEntry{v, epoch}
		m.mu.Unlock()
	}
	return v, nil
}

// CacheStats is the cache counters for a github_requests log line.
type CacheStats struct {
	Hits     int `json:"cache_hits"`
	Misses   int `json:"cache_misses"`
	MemoHits int `json:"memo_hits"`
}

func (c *RequestCounts) CacheStats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{c.hits, c.misses, c.memoHits}
}

// passOf is nil outside CountRequests; the note methods accept nil.
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
		c.hits++
	} else {
		c.misses++
	}
}

func (c *RequestCounts) noteMemoHit() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.memoHits++
}
