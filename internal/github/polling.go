package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type cachedResponse struct {
	ETag string
	Body []byte
}
type pollingState struct {
	once  sync.Once
	gate  chan struct{}
	mu    sync.Mutex
	cache map[string]cachedResponse
	bytes int
	until time.Time
	polls map[string]time.Time
}

// RateWait exposes the transport's current throttle to loop status/scheduling.
func (c *Client) RateWait() time.Duration {
	c.poll.mu.Lock()
	defer c.poll.mu.Unlock()
	return max(time.Duration(0), time.Until(c.poll.until))
}
func waitUntil(ctx context.Context, until time.Time) error {
	d := time.Until(until)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (c *Client) beginRequest(ctx context.Context, key string) (func(), cachedResponse, error) {
	p := &c.poll
	p.once.Do(func() {
		p.gate = make(chan struct{}, 1)
		p.cache = map[string]cachedResponse{}
		p.polls = map[string]time.Time{}
	})
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, cachedResponse{}, ctx.Err()
	}
	release := func() { <-p.gate }
	p.mu.Lock()
	until := p.until
	if p.polls[key].After(until) {
		until = p.polls[key]
	}
	old := p.cache[key]
	p.mu.Unlock()
	if err := waitUntil(ctx, until); err != nil {
		release()
		return nil, old, err
	}
	return release, old, nil
}
func (c *Client) observeHeaders(h http.Header, code int, key string) {
	p := &c.poll
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	until := now
	if n, e := strconv.ParseInt(h.Get("Retry-After"), 10, 64); e == nil && n >= 0 {
		until = now.Add(time.Duration(min(n, 86400)) * time.Second)
	} else if t, e := http.ParseTime(h.Get("Retry-After")); e == nil {
		until = t
	}
	remaining, e := strconv.Atoi(h.Get("X-RateLimit-Remaining"))
	if e == nil && remaining <= 100 {
		if sec, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64); err == nil {
			reset := time.Unix(sec, 0).Add(time.Second)
			if reset.After(now) {
				delay := reset.Sub(now) / time.Duration(max(1, remaining))
				if now.Add(delay).After(until) {
					until = now.Add(delay)
				}
			}
		}
	}
	if (code == 403 || code == 429) && !until.After(now) {
		until = now.Add(time.Minute)
	}
	if until.After(p.until) {
		p.until = until
	}
	if seconds, e := strconv.Atoi(h.Get("X-Poll-Interval")); e == nil && seconds > 0 {
		p.polls[key] = now.Add(time.Duration(min(seconds, 86400)) * time.Second)
	}
}
func (c *Client) cacheResponse(key, tag string, body []byte, mutation bool) {
	p := &c.poll
	p.mu.Lock()
	defer p.mu.Unlock()
	if mutation {
		p.cache = map[string]cachedResponse{}
		p.bytes = 0
		return
	}
	if tag == "" || len(body) > 4<<20 {
		return
	}
	if p.bytes+len(body) > 32<<20 || len(p.cache) >= 512 {
		p.cache = map[string]cachedResponse{}
		p.bytes = 0
	}
	p.bytes -= len(p.cache[key].Body)
	p.cache[key] = cachedResponse{tag, body}
	p.bytes += len(body)
}

func (c *Client) OpenPulls(ctx context.Context, repo, branch string) ([]Pull, error) {
	return list[Pull](ctx, c, base(repo)+"/pulls?state=open&base="+url.QueryEscape(branch)+"&sort=created&direction=asc")
}

// ReviewClaimed checks durable completion markers. In-flight passes use the
// expiring ReviewLease instead.
func (c *Client) ReviewClaimed(ctx context.Context, repo string, number int, sha string) (bool, error) {
	var out any
	err := c.json(ctx, "GET", fmt.Sprintf("%s/git/ref/heads/mergewake/reviews/%d/%s", base(repo), number, sha), nil, &out)
	if NotFound(err) {
		return false, nil
	}
	return err == nil, err
}

var errUncached304 = errors.New("GitHub returned 304 without a cached representation")
