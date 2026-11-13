package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cachedResponse struct {
	ETag string
	Body []byte
	// Replay marks a body kept without an ETag only because its response set
	// a poll window: it is served inside that window and never revalidated.
	Replay bool
	// Code is 404 for a replayed not-found answer, 0 for a body.
	Code int
}

// throttle records a pending wait with the reason and HTTP status that caused
// it, so beginRequest can log why it is sleeping. No URL, token or body.
type throttle struct {
	until  time.Time
	reason string
	code   int
}
type pollingState struct {
	once sync.Once
	mu   sync.Mutex
	// gates is one semaphore per credential id, created on first use: the user
	// token and every App installation have their own, so a write on one never
	// queues behind reads on another.
	gates map[string]chan struct{}
	cache *responseCache
	// rate is the pending wait and remaining the last X-RateLimit-Remaining,
	// each per credential id: the user token and every App installation have
	// separate budgets, so one running dry never holds up another.
	rate      map[string]throttle
	remaining map[string]int
	polls     map[string]throttle
	// mutations counts, per "owner/repo", the mutations that cleared that
	// repository's cached bodies and poll windows; the "" entry counts those
	// that cleared everything. A read marks the count before it is sent and
	// stores nothing if the count moved while it was in flight.
	mutations map[string]uint64
}

// readMark is a repository's mutation count taken before a request began.
type readMark struct {
	repo string
	n    uint64
}

// markRead takes the mutation count a request for repo must still see when its
// response arrives. Routes outside /repos/{owner}/{repo} have repo "".
func (p *pollingState) markRead(repo string) readMark {
	p.init()
	p.mu.Lock()
	defer p.mu.Unlock()
	return readMark{repo, p.mutationCount(repo)}
}

// overlapped reports whether a mutation cleared the cache after m was taken. The
// caller holds p.mu and checks it in the same critical section as the store it
// guards, so a mutation cannot land between the check and the store.
func (p *pollingState) overlapped(m readMark) bool {
	return p.mutationCount(m.repo) != m.n
}

func (p *pollingState) mutationCount(repo string) uint64 {
	n := p.mutations[""]
	if repo != "" {
		n += p.mutations[repo]
	}
	return n
}

// throttleLogThreshold is the shortest wait worth a stderr line.
const throttleLogThreshold = 2 * time.Second

// githubConcurrency is how many requests one credential may have in flight at
// once. Reads of parallel evaluations used to queue behind a single slot for
// the whole client, so their first reads took minutes.
const githubConcurrency = 4

// RateWait exposes the transport's current throttle to loop status/scheduling.
// It is the wait of the credentials that serve reads: without the App, the user
// token; with it, the App installations, plus the user token while some
// repository reads on it as a fallback. A user budget that only writes draw on
// does not hold passes back, because a write waits on its own credential in
// beginRequest.
func (c *Client) RateWait() time.Duration {
	userReads := c.app == nil || c.app.fallingBack(c.now())
	c.poll.init()
	c.poll.mu.Lock()
	defer c.poll.mu.Unlock()
	var wait time.Duration
	for id, t := range c.poll.rate {
		if id == credUser && !userReads {
			continue
		}
		wait = max(wait, time.Until(t.until))
	}
	return wait
}

// RateRemaining is the last reported X-RateLimit-Remaining per credential kind
// ("user", "app"); with several App installations, the lowest. A credential
// that has not answered yet is absent.
func (c *Client) RateRemaining() map[string]int {
	c.poll.init()
	c.poll.mu.Lock()
	defer c.poll.mu.Unlock()
	out := map[string]int{}
	for id, n := range c.poll.remaining {
		kind, _, _ := strings.Cut(id, ":")
		if low, ok := out[kind]; !ok || n < low {
			out[kind] = n
		}
	}
	return out
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
		p.rate = map[string]throttle{}
		p.remaining = map[string]int{}
		p.gates = map[string]chan struct{}{}
		p.cache = newResponseCache(maxCachedEntries, maxCachedBytes)
		p.polls = map[string]throttle{}
		p.mutations = map[string]uint64{}
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
	cached, ok := p.cache.get(key)
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

// gateFor returns the semaphore of one credential, creating it on first use.
func (p *pollingState) gateFor(cred string) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	gate, ok := p.gates[cred]
	if !ok {
		gate = make(chan struct{}, githubConcurrency)
		p.gates[cred] = gate
	}
	return gate
}

func (c *Client) beginRequest(ctx context.Context, key string, fresh bool, cred string) (func(), cachedResponse, error) {
	p := &c.poll
	p.init()
	gate := p.gateFor(cred)
	for {
		p.mu.Lock()
		wait := p.rate[cred]
		if c.Polling && !fresh && p.polls[key].until.After(wait.until) {
			wait = p.polls[key]
		}
		old, _ := p.cache.get(key)
		p.mu.Unlock()
		// Throttle waits happen before the gate is taken: a sleeping key never
		// serialises requests for every other key behind it.
		if d := time.Until(wait.until); d > throttleLogThreshold {
			c.logThrottle(wait, d)
		}
		if err := waitUntil(ctx, wait.until); err != nil {
			return nil, cachedResponse{}, err
		}
		start := time.Now()
		select {
		case gate <- struct{}{}:
		case <-ctx.Done():
			return nil, cachedResponse{}, ctx.Err()
		}
		recordGateWait(ctx, time.Since(start))
		// A request that landed a new throttle while this one slept or queued
		// must not be passed: release the gate and wait out the new deadline.
		p.mu.Lock()
		again := p.rate[cred]
		if c.Polling && !fresh && p.polls[key].until.After(again.until) {
			again = p.polls[key]
		}
		p.mu.Unlock()
		if !again.until.After(time.Now()) {
			return func() { <-gate }, old, nil
		}
		<-gate
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

// pollingEndpoint identifies GitHub's events and notifications polling routes.
func pollingEndpoint(route string) bool {
	path := strings.SplitN(route, "?", 2)[0]
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 1 {
		return parts[0] == "events" || parts[0] == "notifications"
	}
	if len(parts) == 4 && parts[0] == "repos" {
		return parts[3] == "events" || parts[3] == "notifications"
	}
	if len(parts) == 3 && (parts[0] == "orgs" || parts[0] == "users") {
		return parts[2] == "events" || parts[0] == "users" && parts[2] == "received_events"
	}
	if len(parts) == 4 && parts[0] == "users" && (parts[2] == "events" || parts[2] == "received_events") {
		return parts[3] == "public"
	}
	return len(parts) == 5 && parts[0] == "users" && parts[2] == "events" && parts[3] == "orgs"
}

// observeHeaders records the credential's rate-limit state and, for a polling
// endpoint, the key's poll window. The window is dropped when a mutation cleared
// the repository since mark: it was set by a response that predates the write.
func (c *Client) observeHeaders(cred string, h http.Header, code int, key string, polling bool, mark readMark) {
	p := &c.poll
	p.init()
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
	if e == nil {
		p.remaining[cred] = remaining
	}
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
	if w.until.After(p.rate[cred].until) {
		p.rate[cred] = w
	}
	if seconds, e := strconv.Atoi(h.Get("X-Poll-Interval")); polling && e == nil && seconds > 0 && !p.overlapped(mark) {
		p.polls[key] = throttle{until: now.Add(time.Duration(min(seconds, 86400)) * time.Second), reason: "poll interval", code: code}
	}
}
func (c *Client) cacheResponse(key, tag string, body []byte, mutation bool, mark readMark) {
	p := &c.poll
	p.init()
	p.mu.Lock()
	defer p.mu.Unlock()
	if mutation {
		// Unknown endpoints can affect arbitrary resources; known repository
		// mutations invalidate that repository, including its poll windows.
		endpoint := strings.TrimPrefix(key, strings.Fields(key)[0]+" ")
		endpoint = strings.SplitN(endpoint, " ", 2)[0]
		u, err := url.Parse(endpoint)
		prefix := ""
		var parts []string
		if err == nil {
			parts = strings.Split(strings.Trim(u.Path, "/"), "/")
		}
		if err == nil && len(parts) >= 3 && parts[0] == "repos" {
			prefix = "GET " + u.Scheme + "://" + u.Host + "/repos/" + parts[1] + "/" + parts[2]
		}
		if prefix == "" {
			p.mutations[""]++
		} else {
			p.mutations[normalizeMemoRepo(parts[1]+"/"+parts[2])]++
		}
		var pullNumber string
		if len(parts) >= 5 && (parts[3] == "issues" && parts[4] != "comments" || parts[3] == "pulls") && len(parts) >= 6 && (parts[5] == "comments" || parts[5] == "reviews") {
			if _, err := strconv.Atoi(parts[4]); err == nil {
				pullNumber = parts[4]
			}
		}
		touches := func(k string) bool {
			// GraphQL is a shared POST key, so a repository prefix cannot
			// identify its poll window or cached response.
			if strings.HasPrefix(k, "POST "+c.BaseURL+"/graphql ") {
				return true
			}
			if prefix == "" {
				return true
			}
			if !(strings.HasPrefix(k, prefix+"/") || strings.HasPrefix(k, prefix+"?") || strings.HasPrefix(k, prefix+" ")) {
				return false
			}
			if pullNumber == "" {
				return true
			}
			target := strings.TrimPrefix(k, prefix)
			return strings.HasPrefix(target, " ") || strings.HasPrefix(target, "?") ||
				strings.HasPrefix(target, "/pulls?") || strings.HasPrefix(target, "/pulls ") || strings.HasPrefix(target, "/issues?") || strings.HasPrefix(target, "/issues ") ||
				strings.HasPrefix(target, "/pulls/"+pullNumber+"/") || strings.HasPrefix(target, "/pulls/"+pullNumber+"?") || strings.HasPrefix(target, "/pulls/"+pullNumber+" ") ||
				strings.HasPrefix(target, "/issues/"+pullNumber+"/") || strings.HasPrefix(target, "/issues/"+pullNumber+"?") || strings.HasPrefix(target, "/issues/"+pullNumber+" ")
		}
		for _, k := range p.cache.keys() {
			if touches(k) {
				p.cache.delete(k)
				delete(p.polls, k)
			}
		}
		for k := range p.polls {
			if touches(k) {
				delete(p.polls, k)
			}
		}

		return
	}
	if len(body) > maxCachedBody || p.overlapped(mark) {
		return
	}
	// A response without an ETag is kept only while its poll window runs:
	// without it, a second read of the key in the same round would be skipped
	// every round and the caller (a PR review) would never get its data.
	replay := false
	if tag == "" {
		w, ok := p.polls[key]
		if !ok || !w.until.After(time.Now()) {
			return
		}
		replay = true
	}
	p.store(key, cachedResponse{ETag: tag, Body: body, Replay: replay})
}

// store keeps resp under key. An entry evicted to make room loses its poll
// window with it: a window without a cached response only skips reads.
func (p *pollingState) store(key string, resp cachedResponse) {
	for _, evicted := range p.cache.put(key, resp) {
		delete(p.polls, evicted)
	}
}

// OpenPulls lists open PRs against branch, or against any base when branch
// is empty.
func (c *Client) OpenPulls(ctx context.Context, repo, branch string) ([]Pull, error) {
	route := base(repo) + "/pulls?state=open&sort=created&direction=asc"
	if branch != "" {
		route += "&base=" + url.QueryEscape(branch)
	}
	return list[Pull](ctx, c, route)
}

// ClosedPulls lists the most recently updated closed PRs against branch: one
// page, which the Discord feed reads every pass for merges and closures.
func (c *Client) ClosedPulls(ctx context.Context, repo, branch string) ([]Pull, error) {
	var out []Pull
	route := base(repo) + "/pulls?state=closed&sort=updated&direction=desc&per_page=100&base=" + url.QueryEscape(branch)
	return out, c.json(ctx, "GET", route, nil, &out)
}

// AllClosedPulls lists every closed PR of the repo, merged or not, against any
// base, oldest first. The legacy worktree sweep reads it once: unlike
// MergedPulls it has no recency window. Pagination past GitHub's limit is an
// error, never a partial list.
func (c *Client) AllClosedPulls(ctx context.Context, repo string) ([]Pull, error) {
	return list[Pull](ctx, c, base(repo)+"/pulls?state=closed&sort=created&direction=asc")
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

// cacheNotFound keeps a 404 answer while its key's poll window runs. A
// not-found GET (an unclaimed review ref) is read twice in one daemon round;
// without a replay the second read was skipped every round and the review
// never started. Only the window's own response is replayed, and any mutation
// wipes it with the rest of the cache.
func (c *Client) cacheNotFound(key string, mark readMark) {
	p := &c.poll
	p.init()
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.polls[key]; !ok || !w.until.After(time.Now()) || p.overlapped(mark) {
		return
	}
	p.store(key, cachedResponse{Replay: true, Code: http.StatusNotFound})
}
