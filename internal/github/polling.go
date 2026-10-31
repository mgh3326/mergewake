package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

type cachedResponse struct {
	ETag string
	Body []byte
}

// throttle records a pending wait with the reason and HTTP status that caused
// it, so beginRequest can log why it is sleeping. No URL, token or body.
type throttle struct {
	until  time.Time
	reason string
	code   int
}
type pollingState struct {
	once  sync.Once
	gate  chan struct{}
	mu    sync.Mutex
	cache map[string]cachedResponse
	bytes int
	rate  throttle
	polls map[string]throttle
}

// throttleLogThreshold is the shortest wait worth a stderr line.
const throttleLogThreshold = 2 * time.Second

// RateWait exposes the transport's current throttle to loop status/scheduling.
func (c *Client) RateWait() time.Duration {
	c.poll.mu.Lock()
	defer c.poll.mu.Unlock()
	return max(time.Duration(0), time.Until(c.poll.rate.until))
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
// ErrPollSkipped marks a GET dropped because its key still sits inside a
// recorded X-Poll-Interval window with no cached representation to serve.
// Polling callers treat it as "no data this round", never as a failure.
var ErrPollSkipped = errors.New("GitHub request skipped: poll interval pending")

func (p *pollingState) init() {
	p.once.Do(func() {
		p.gate = make(chan struct{}, 1)
		p.cache = map[string]cachedResponse{}
		p.polls = map[string]throttle{}
	})
}

// pollShortcut serves a throttled GET without touching the serializer: a key
// inside its X-Poll-Interval window replays the cached response when one is
// held and is skipped otherwise, so one endpoint's deadline cannot stall
// requests for every other key.
func (c *Client) pollShortcut(key string) (cachedResponse, error) {
	if !c.Polling {
		return cachedResponse{}, nil
	}
	p := &c.poll
	p.init()
	p.mu.Lock()
	deadline, throttled := p.polls[key]
	cached, ok := p.cache[key]
	p.mu.Unlock()
	if !throttled || !deadline.until.After(time.Now()) {
		return cachedResponse{}, nil
	}
	if d := time.Until(deadline.until); d > throttleLogThreshold {
		c.logThrottle(deadline, d)
	}
	if ok {
		return cached, nil
	}
	return cachedResponse{}, ErrPollSkipped
}

func (c *Client) beginRequest(ctx context.Context, key string) (func(), cachedResponse, error) {
	p := &c.poll
	p.init()
	for {
		p.mu.Lock()
		wait := p.rate
		if c.Polling && p.polls[key].until.After(wait.until) {
			wait = p.polls[key]
		}
		old := p.cache[key]
		p.mu.Unlock()
		// Throttle waits happen before the gate is taken: a sleeping key never
		// serialises requests for every other key behind it.
		if d := time.Until(wait.until); d > throttleLogThreshold {
			c.logThrottle(wait, d)
		}
		if err := waitUntil(ctx, wait.until); err != nil {
			return nil, cachedResponse{}, err
		}
		select {
		case p.gate <- struct{}{}:
		case <-ctx.Done():
			return nil, cachedResponse{}, ctx.Err()
		}
		// A request that landed a new throttle while this one slept or queued
		// must not be passed: release the gate and wait out the new deadline.
		p.mu.Lock()
		again := p.rate
		if c.Polling && p.polls[key].until.After(again.until) {
			again = p.polls[key]
		}
		p.mu.Unlock()
		if !again.until.After(time.Now()) {
			return func() { <-p.gate }, old, nil
		}
		<-p.gate
	}
}

// logThrottle reports one throttled wait: the reason, the HTTP status that
// caused it and the wake-up time. It never logs the token, URL query or any
// response body.
func (c *Client) logThrottle(w throttle, d time.Duration) {
	out := c.Stderr
	if out == nil {
		out = os.Stderr
	}
	reason := w.reason
	if reason == "" {
		reason = "rate-limit wait"
	}
	fmt.Fprintf(out, "mergewake: GitHub request throttled: %s (HTTP %d); sleeping %s until %s\n",
		reason, w.code, d.Round(time.Second), w.until.UTC().Format(time.RFC3339))
}
func (c *Client) observeHeaders(h http.Header, code int, key string) {
	p := &c.poll
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	w := throttle{until: now, code: code}
	if n, e := strconv.ParseInt(h.Get("Retry-After"), 10, 64); e == nil && n >= 0 {
		w.until = now.Add(time.Duration(min(n, 86400)) * time.Second)
		w.reason = "Retry-After"
	} else if t, e := http.ParseTime(h.Get("Retry-After")); e == nil {
		w.until = t
		w.reason = "Retry-After"
	}
	remaining, e := strconv.Atoi(h.Get("X-RateLimit-Remaining"))
	if e == nil && remaining <= 100 {
		if sec, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64); err == nil {
			reset := time.Unix(sec, 0).Add(time.Second)
			if reset.After(now) {
				delay := reset.Sub(now) / time.Duration(max(1, remaining))
				if now.Add(delay).After(w.until) {
					w.until = now.Add(delay)
					w.reason = "rate-limit pacing"
				}
			}
		}
	}
	if (code == 403 || code == 429) && !w.until.After(now) {
		w.until = now.Add(time.Minute)
		w.reason = "403-429 backoff"
	}
	if w.until.After(p.rate.until) {
		p.rate = w
	}
	if seconds, e := strconv.Atoi(h.Get("X-Poll-Interval")); e == nil && seconds > 0 {
		p.polls[key] = throttle{until: now.Add(time.Duration(min(seconds, 86400)) * time.Second), reason: "poll interval", code: code}
	}
}
func (c *Client) cacheResponse(key, tag string, body []byte, mutation bool) {
	p := &c.poll
	p.mu.Lock()
	defer p.mu.Unlock()
	// Dropping cached representations also drops their poll windows: a key
	// inside its window with nothing cached is skipped, so a wipe that kept
	// the windows would skip every key a round re-reads after a mutation.
	if mutation {
		p.cache = map[string]cachedResponse{}
		p.polls = map[string]throttle{}
		p.bytes = 0
		return
	}
	if tag == "" || len(body) > 4<<20 {
		return
	}
	if p.bytes+len(body) > 32<<20 || len(p.cache) >= 512 {
		p.cache = map[string]cachedResponse{}
		p.polls = map[string]throttle{}
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
