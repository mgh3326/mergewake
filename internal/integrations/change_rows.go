package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// UnreconciledChange is one row of hk's GET /v1/changes/unreconciled: a PR hk
// holds open, or holds merged with no merge commit, so the tasks waiting on it
// cannot settle. Only the fields a PR-state sync needs are decoded.
type UnreconciledChange struct {
	ID       int64  `json:"id"`
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	State    string `json:"state"`
}

// UnreconciledChanges reads hk's unreconciled PR rows, oldest first, at most
// limit of them (hk's cap is 1000). The endpoint pages by limit only: it takes
// no after_id, so a reader that must see a list longer than limit rotates over
// the rows it receives. A 404 (an hk without the endpoint) and any other
// non-2xx answer are errors the caller logs and retries.
func (h Handoffkeep) UnreconciledChanges(ctx context.Context, limit int) ([]UnreconciledChange, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		return nil, e
	}
	return c.unreconciledChanges(ctx, limit)
}

func (c *hkJobs) unreconciledChanges(ctx context.Context, limit int) ([]UnreconciledChange, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/changes/unreconciled?limit="+url.QueryEscape(strconv.Itoa(limit)), nil)
	if e != nil {
		return nil, errors.New("invalid change request")
	}
	c.authorize(req)
	resp, e := c.client(8 * time.Second).Do(req)
	if e != nil {
		return nil, errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("handoffkeep HTTP %d (response withheld)", resp.StatusCode)
	}
	var out struct {
		Changes []UnreconciledChange `json:"changes"`
	}
	if e := json.NewDecoder(io.LimitReader(resp.Body, 1<<22)).Decode(&out); e != nil {
		return nil, errors.New("handoffkeep response is not valid JSON")
	}
	return out.Changes, nil
}
