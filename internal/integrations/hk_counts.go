package integrations

import (
	"context"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// hkRoutes counts every HTTP request this process sends to hk, by route, for
// the life of the process. A loop logs the counts cumulatively, so an interval
// is the difference between two log lines.
var hkRoutes = struct {
	mu sync.Mutex
	n  map[string]int
}{n: map[string]int{}}

// HKRouteCount is the number of requests sent to one hk route.
type HKRouteCount struct {
	Route string `json:"route"`
	Count int    `json:"count"`
}

// HKRouteCounts returns the process's hk request counts by route, sorted by
// route. A route is its method and path with numeric segments as {id}, so
// GET /v1/tasks/42 and GET /v1/tasks/7 are one route.
func HKRouteCounts() []HKRouteCount {
	hkRoutes.mu.Lock()
	defer hkRoutes.mu.Unlock()
	out := make([]HKRouteCount, 0, len(hkRoutes.n))
	for route, n := range hkRoutes.n {
		out = append(out, HKRouteCount{Route: route, Count: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Route < out[j].Route })
	return out
}

// HKCounts is the number of hk requests sent under one context (see CountHK).
type HKCounts struct{ n atomic.Int64 }

// Total is the requests counted so far, attempts included; zero on nil.
func (c *HKCounts) Total() int {
	if c == nil {
		return 0
	}
	return int(c.n.Load())
}

type hkCountsKey struct{}

// CountHK returns ctx carrying a counter of the hk requests built under it. A
// request is counted under the context it was built with.
func CountHK(ctx context.Context) (context.Context, *HKCounts) {
	c := &HKCounts{}
	return context.WithValue(ctx, hkCountsKey{}, c), c
}

// HKRequestTotal is the hk requests counted under ctx so far; zero when ctx
// counts nothing.
func HKRequestTotal(ctx context.Context) int {
	c, _ := ctx.Value(hkCountsKey{}).(*HKCounts)
	return c.Total()
}

var numericPathSegment = regexp.MustCompile(`^[0-9]+$`)

// hkRoute names the route of one request: its method and path, numeric
// segments as {id}, and no query.
func hkRoute(req *http.Request) string {
	parts := strings.Split(req.URL.Path, "/")
	for i, part := range parts {
		if numericPathSegment.MatchString(part) {
			parts[i] = "{id}"
		}
	}
	return req.Method + " " + strings.Join(parts, "/")
}

// hkCountingTransport counts each request attempt before it is sent.
type hkCountingTransport struct{ base http.RoundTripper }

func (t hkCountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	route := hkRoute(req)
	hkRoutes.mu.Lock()
	hkRoutes.n[route]++
	hkRoutes.mu.Unlock()
	if c, ok := req.Context().Value(hkCountsKey{}).(*HKCounts); ok {
		c.n.Add(1)
	}
	return t.base.RoundTrip(req)
}

// countedHK is a copy of hc whose requests are counted. The copy keeps hc's
// transport, redirect policy and timeout.
func countedHK(hc *http.Client) *http.Client {
	cp := *hc
	base := cp.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	cp.Transport = hkCountingTransport{base: base}
	return &cp
}
