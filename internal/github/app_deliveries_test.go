package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// deliveryFake serves GET /app/hook/deliveries. reply gets the 1-based call
// number, so a test scripts each page.
type deliveryFake struct {
	mu    sync.Mutex
	auths []string
	urls  []string
	reply func(n int, w http.ResponseWriter, r *http.Request)
}

func (f *deliveryFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.auths = append(f.auths, r.Header.Get("Authorization"))
	f.urls = append(f.urls, r.Method+" "+r.URL.RequestURI())
	n := len(f.urls)
	f.mu.Unlock()
	f.reply(n, w, r)
}

func (f *deliveryFake) snapshot() (auths, urls []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.auths...), append([]string(nil), f.urls...)
}

func deliveryJSON(guid, at string, status int) string {
	return fmt.Sprintf(`{"id":1,"guid":%q,"delivered_at":%q,"event":"pull_request","action":"opened","redelivery":false,"status_code":%d}`, guid, at, status)
}

func deliveryArray(rows ...string) string { return "[" + strings.Join(rows, ",") + "]" }

// deliveryClient is a client whose only host is the fake; the App is enabled
// unless withApp is false.
func deliveryClient(t *testing.T, fake http.Handler, withApp bool) *Client {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	c := New(appTestUserToken)
	c.BaseURL, c.UploadURL, c.HTTP = srv.URL, srv.URL, srv.Client()
	c.Stderr = io.Discard
	if withApp {
		c.EnableApp(appTestAppID, testAppKey(t))
		// Pages are paced for real; tests record the wait and skip it.
		c.app.pause = func(context.Context, time.Duration) error { return nil }
	}
	return c
}

var since = time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

// Covers brief (a): two linked pages come back whole, each request is a GET
// with per_page=100 and an App JWT, never the user token.
func TestAppHookDeliveriesPagesWithAppJWT(t *testing.T) {
	fake := &deliveryFake{reply: func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?per_page=100&cursor=c2>; rel="next", <http://%s/app/hook/deliveries?per_page=100&cursor=c9>; rel="last"`, r.Host, r.Host))
			fmt.Fprint(w, deliveryArray(deliveryJSON("g1", "2026-11-13T01:00:00Z", 200), deliveryJSON("g2", "2026-11-12T23:00:00Z", 500)))
			return
		}
		fmt.Fprint(w, deliveryArray(deliveryJSON("g3", "2026-11-12T02:00:00Z", 200)))
	}}
	c := deliveryClient(t, fake, true)
	page, err := c.AppHookDeliveries(context.Background(), since)
	if err != nil {
		t.Fatal(err)
	}
	var guids []string
	for _, d := range page.Deliveries {
		guids = append(guids, d.GUID)
	}
	if strings.Join(guids, ",") != "g1,g2,g3" || page.Pages != 2 || page.Truncated {
		t.Fatalf("page = %+v guids=%v", page, guids)
	}
	auths, urls := fake.snapshot()
	if len(urls) != 2 || urls[0] != "GET /app/hook/deliveries?per_page=100" || urls[1] != "GET /app/hook/deliveries?per_page=100&cursor=c2" {
		t.Fatalf("requests = %v", urls)
	}
	for _, a := range auths {
		parts := strings.Split(strings.TrimPrefix(a, "Bearer "), ".")
		if !strings.HasPrefix(a, "Bearer ") || len(parts) != 3 || a == "Bearer "+appTestUserToken {
			t.Fatalf("Authorization = %q, want an App JWT", a)
		}
	}
}

// Covers brief (a): without the App the method errors and sends nothing.
func TestAppHookDeliveriesNeedsTheApp(t *testing.T) {
	hits := 0
	c := deliveryClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }), false)
	if _, err := c.AppHookDeliveries(context.Background(), since); err == nil {
		t.Fatal("no error without the App")
	}
	if hits != 0 {
		t.Fatalf("%d requests sent without the App", hits)
	}
}

// Covers brief (d): stops at the first delivery older than since (no third
// request), paces pages at least a second apart, and never reads past a
// page with no next link.
func TestAppHookDeliveriesStopsAtSinceAndPacesPages(t *testing.T) {
	fake := &deliveryFake{reply: func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?per_page=100&cursor=c2>; rel="next"`, r.Host))
			fmt.Fprint(w, deliveryArray(deliveryJSON("new", "2026-11-12T05:00:00Z", 200)))
			return
		}
		w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?per_page=100&cursor=c3>; rel="next"`, r.Host))
		fmt.Fprint(w, deliveryArray(deliveryJSON("older", "2026-11-11T23:00:00Z", 200)))
	}}
	c := deliveryClient(t, fake, true)
	var waits []time.Duration
	c.app.pause = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	page, err := c.AppHookDeliveries(context.Background(), since)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Deliveries) != 1 || page.Deliveries[0].GUID != "new" || page.Pages != 2 || page.Truncated {
		t.Fatalf("page = %+v", page)
	}
	_, urls := fake.snapshot()
	if len(urls) != 2 {
		t.Fatalf("requests = %v; want the scan to stop at the older delivery", urls)
	}
	if len(waits) != 1 || waits[0] < time.Second {
		t.Fatalf("waits between pages = %v; want one wait of at least 1s", waits)
	}
}

// Covers brief (d): the page bound stops the walk and sets Truncated.
func TestAppHookDeliveriesPageBoundSetsTruncated(t *testing.T) {
	fake := &deliveryFake{reply: func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?per_page=100&cursor=c%d>; rel="next"`, r.Host, n+1))
		fmt.Fprint(w, deliveryArray(deliveryJSON(fmt.Sprintf("g%d", n), "2026-11-12T05:00:00Z", 200)))
	}}
	c := deliveryClient(t, fake, true)
	c.app.maxPages = 2
	page, err := c.AppHookDeliveries(context.Background(), since)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Truncated || page.Pages != 2 || len(page.Deliveries) != 2 {
		t.Fatalf("page = %+v; want truncated after 2 pages", page)
	}
	if _, urls := fake.snapshot(); len(urls) != 2 {
		t.Fatalf("requests = %v; want 2", urls)
	}
}

// Covers brief (d) and (e): a failed page returns no partial page, and the
// error carries neither the App JWT nor the user token.
func TestAppHookDeliveriesFailedPageReturnsNothingAndNoSecrets(t *testing.T) {
	fake := &deliveryFake{reply: func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?per_page=100&cursor=c2>; rel="next"`, r.Host))
			fmt.Fprint(w, deliveryArray(deliveryJSON("g1", "2026-11-13T01:00:00Z", 200)))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}}
	c := deliveryClient(t, fake, true)
	page, err := c.AppHookDeliveries(context.Background(), since)
	if err == nil {
		t.Fatal("no error for a failed page")
	}
	if len(page.Deliveries) != 0 {
		t.Fatalf("partial page returned: %+v", page)
	}
	auths, _ := fake.snapshot()
	for _, a := range auths {
		jwt := strings.TrimPrefix(a, "Bearer ")
		if strings.Contains(err.Error(), jwt) || strings.Contains(err.Error(), appTestUserToken) {
			t.Fatalf("error leaks a credential: %v", err)
		}
	}
}
