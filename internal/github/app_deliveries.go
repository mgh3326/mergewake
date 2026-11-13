package github

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

const (
	appDeliveriesRoute = "/app/hook/deliveries"
	// appDeliveriesMaxPages bounds one walk of the App's delivery log.
	appDeliveriesMaxPages = 1000
	// appDeliveriesPace is the least wait between two pages of the log.
	appDeliveriesPace = time.Second
)

// AppDelivery is one row of the App's webhook delivery log.
type AppDelivery struct {
	ID          int64     `json:"id"`
	GUID        string    `json:"guid"`
	DeliveredAt time.Time `json:"delivered_at"`
	Event       string    `json:"event"`
	Action      string    `json:"action"`
	Redelivery  bool      `json:"redelivery"`
	StatusCode  int       `json:"status_code"`
}

// DeliveryPage is the delivery log from the newest row down to a time bound.
type DeliveryPage struct {
	// Deliveries are the rows delivered at or after since, newest first.
	Deliveries []AppDelivery
	Pages      int
	// Truncated means the walk hit the page bound before reaching since.
	Truncated bool
}

var nextLinkRE = regexp.MustCompile(`<([^>]+)>\s*;\s*rel="next"`)

// AppHookDeliveries reads the App's webhook delivery log, newest first, with
// the App JWT only (never the user token). It walks pages until a row older
// than since, the last page, or the page bound. Pages are at least a second
// apart. The only request it makes is GET.
func (c *Client) AppHookDeliveries(ctx context.Context, since time.Time) (DeliveryPage, error) {
	if c.app == nil {
		return DeliveryPage{}, errors.New("GitHub App not enabled; no delivery log to read")
	}
	a := c.app
	maxPages := a.maxPages
	if maxPages == 0 {
		maxPages = appDeliveriesMaxPages
	}
	pause := a.pause
	if pause == nil {
		pause = waitFor
	}
	var page DeliveryPage
	target := c.BaseURL + appDeliveriesRoute + "?per_page=100"
	for {
		var rows []AppDelivery
		_, header, err := a.send(ctx, c, http.MethodGet, target, appDeliveriesRoute, &rows)
		if err != nil {
			return DeliveryPage{}, err
		}
		page.Pages++
		for _, row := range rows {
			if row.DeliveredAt.Before(since) {
				return page, nil
			}
			page.Deliveries = append(page.Deliveries, row)
		}
		next := nextLink(header)
		if next == "" {
			return page, nil
		}
		if page.Pages >= maxPages {
			page.Truncated = true
			return page, nil
		}
		if !sameOrigin(next, c.BaseURL) {
			return DeliveryPage{}, errors.New("GitHub App delivery log cursor leaves the API host")
		}
		if err := pause(ctx, appDeliveriesPace); err != nil {
			return DeliveryPage{}, err
		}
		target = next
	}
}

// nextLink returns the rel="next" URL of a Link header, or "".
func nextLink(h http.Header) string {
	for _, v := range h.Values("Link") {
		if m := nextLinkRE.FindStringSubmatch(v); m != nil {
			return m[1]
		}
	}
	return ""
}

// sameOrigin reports whether a and b share scheme and host, so the App JWT
// is never sent to a host other than the API's.
func sameOrigin(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)
	return errA == nil && errB == nil && ua.Scheme == ub.Scheme && ua.Host == ub.Host
}

// waitFor sleeps for d or until ctx is done.
func waitFor(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
