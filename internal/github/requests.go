package github

import (
	"context"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
)

type RequestCount struct {
	Role   string `json:"role"`
	Method string `json:"method"`
	Route  string `json:"route"`
	Status int    `json:"status"`
	Count  int    `json:"count"`
}
type requestKey struct {
	method, route string
	status        int
}

// RequestCounts is the accounting of one run of passes: request counts and
// cache counters. The ctx that CountRequests returns carries it.
type RequestCounts struct {
	mu                     sync.Mutex
	role                   string
	counts                 map[requestKey]int
	byCredential           map[string]int
	hits, misses, memoHits int
	notModified            int
}
type requestCountsKey struct{}

// CountRequests attributes all HTTP responses under ctx to this run and starts
// its first pass (see NewPass).
func CountRequests(ctx context.Context, role string) (context.Context, *RequestCounts) {
	c := &RequestCounts{role: role, counts: map[requestKey]int{}, byCredential: map[string]int{}}
	return NewPass(context.WithValue(ctx, requestCountsKey{}, c)), c
}

var numericSegment = regexp.MustCompile(`^[0-9]+$`)
var shaSegment = regexp.MustCompile(`(?i)^[0-9a-f]{40,64}$`)
var doneMarker = regexp.MustCompile(`(?i)^[0-9a-f]{40,64}-done$`)

func routeTemplate(route string) string {
	u, err := url.Parse(strings.TrimPrefix(route, "UPLOAD "))
	if err != nil {
		return "<invalid>"
	}
	parts := strings.Split(u.EscapedPath(), "/")
	for i, part := range parts {
		switch {
		case numericSegment.MatchString(part):
			parts[i] = "{number}"
		case shaSegment.MatchString(part):
			parts[i] = "{sha}"
		case i > 0 && parts[i-1] == "compare":
			left, right, ok := strings.Cut(part, "...")
			if ok && left != "" && right != "" {
				template := func(ref string) string {
					if shaSegment.MatchString(ref) {
						return "{sha}"
					}
					return "{ref}"
				}
				parts[i] = template(left) + "..." + template(right)
			}
		case i > 0 && parts[i-1] == "commits" && strings.HasPrefix(part, "mergewake-") && shaSegment.MatchString(strings.TrimPrefix(part, "mergewake-")):
			parts[i] = "mergewake-{sha}"
		case i > 1 && parts[i-2] == "reviews" && doneMarker.MatchString(part):
			parts[i] = "{sha}-done"
		}
	}
	return strings.Join(parts, "/")
}

// recordRequest counts one HTTP response, or a failed attempt as status 0,
// against the credential kind ("user" or "app") that sent it.
func recordRequest(ctx context.Context, method, route string, status int, credential string) {
	c, ok := ctx.Value(requestCountsKey{}).(*RequestCounts)
	if !ok {
		return
	}
	k := requestKey{method, routeTemplate(route), status}
	c.mu.Lock()
	c.counts[k]++
	c.byCredential[credential]++
	if status == http.StatusNotModified {
		c.notModified++
	}
	c.mu.Unlock()
}

// ByCredential is the request count per credential kind: how much of the run
// the App served and how much the user token did.
func (c *RequestCounts) ByCredential() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.byCredential)
}
func (c *RequestCounts) Snapshot() []RequestCount {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RequestCount, 0, len(c.counts))
	for k, n := range c.counts {
		out = append(out, RequestCount{c.role, k.method, k.route, k.status, n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].Route != out[j].Route {
			return out[i].Route < out[j].Route
		}
		if out[i].Method != out[j].Method {
			return out[i].Method < out[j].Method
		}
		return out[i].Status < out[j].Status
	})
	return out
}
