package github

import (
	"context"
	"sync"
	"time"
)

type evidenceKey struct{ repo, kind, id string }
type evidenceEntry struct {
	value   any
	expires time.Time
}
type evidenceCache struct {
	mu         sync.Mutex
	entries    map[evidenceKey]evidenceEntry
	generation map[string]uint64
}
type freshEvidenceKey struct{}
type freshRequestKey struct{}
type uncachedRequestKey struct{}

func freshRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, freshRequestKey{}, true)
}

// pastCache is a fresh request that also sends no If-None-Match, so GitHub
// answers with the current body and never a 304 that replays a cached one.
func pastCache(ctx context.Context) context.Context {
	return context.WithValue(freshRequest(ctx), uncachedRequestKey{}, true)
}

// FreshEvidence forces decision-critical reads through to GitHub.
func FreshEvidence(ctx context.Context) context.Context {
	return context.WithValue(ctx, freshEvidenceKey{}, true)
}

// InvalidateEvidence discards all evidence for a repository on a relevant delivery.
// A generation prevents an in-flight read from restoring invalidated evidence.
func (c *Client) InvalidateEvidence(repo string) {
	p := &c.evidence
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.generation == nil {
		p.generation = map[string]uint64{}
	}
	p.generation[repo]++
	for k := range p.entries {
		if k.repo == repo {
			delete(p.entries, k)
		}
	}
}

func cachedEvidence[T any](ctx context.Context, c *Client, k evidenceKey, read func() (T, error)) (T, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	if !c.Polling || ctx.Value(freshEvidenceKey{}) != nil {
		return read()
	}
	p := &c.evidence
	p.mu.Lock()
	if entry, ok := p.entries[k]; ok && c.now().Before(entry.expires) {
		value := entry.value.(T)
		p.mu.Unlock()
		return value, nil
	}
	generation := p.generation[k.repo]
	p.mu.Unlock()
	value, err := read()
	if err != nil {
		return value, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.generation[k.repo] == generation {
		if p.entries == nil {
			p.entries = map[evidenceKey]evidenceEntry{}
		}
		if len(p.entries) >= 4096 {
			now := c.now()
			for key, entry := range p.entries {
				if !now.Before(entry.expires) {
					delete(p.entries, key)
				}
			}
			if len(p.entries) >= 4096 {
				clear(p.entries)
			}
		}
		p.entries[k] = evidenceEntry{value, c.now().Add(10 * time.Minute)}
	}
	return value, nil
}
