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

// UnreconciledPage is one page of hk's unreconciled change rows.
type UnreconciledPage struct {
	Changes []UnreconciledChange
	// NextAfterID is the after_id of the following page; nil when the
	// answer's next_after_id is null, the list's end.
	NextAfterID *int64
	// Paged reports whether the answer carried next_after_id at all: an hk
	// without after_id paging ignores the parameter and answers without the
	// key, and the reader keeps its one-page rotation.
	Paged bool
}

// UnreconciledChanges reads one page of hk's unreconciled PR rows, oldest
// first: the rows with id > afterID, at most limit of them (hk's cap is
// 1000). hk pages by after_id and answers next_after_id, null at the end;
// an hk without paging ignores after_id, answers the first page without
// the key, and the page's Paged is false. A 404 (an hk without the
// endpoint) and any other non-2xx answer are errors the caller logs and
// retries.
func (h Handoffkeep) UnreconciledChanges(ctx context.Context, afterID int64, limit int) (UnreconciledPage, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		return UnreconciledPage{}, e
	}
	return c.unreconciledChanges(ctx, afterID, limit)
}

func (c *hkJobs) unreconciledChanges(ctx context.Context, afterID int64, limit int) (UnreconciledPage, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/changes/unreconciled?limit="+url.QueryEscape(strconv.Itoa(limit))+"&after_id="+url.QueryEscape(strconv.FormatInt(afterID, 10)), nil)
	if e != nil {
		return UnreconciledPage{}, errors.New("invalid change request")
	}
	c.authorize(req)
	resp, e := c.client(8 * time.Second).Do(req)
	if e != nil {
		return UnreconciledPage{}, errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return UnreconciledPage{}, fmt.Errorf("handoffkeep HTTP %d (response withheld)", resp.StatusCode)
	}
	var out struct {
		Changes     []UnreconciledChange `json:"changes"`
		NextAfterID json.RawMessage      `json:"next_after_id"`
	}
	if e := json.NewDecoder(io.LimitReader(resp.Body, 1<<22)).Decode(&out); e != nil {
		return UnreconciledPage{}, errors.New("handoffkeep response is not valid JSON")
	}
	page := UnreconciledPage{Changes: out.Changes}
	if out.NextAfterID != nil {
		page.Paged = true
		var next *int64
		if e := json.Unmarshal(out.NextAfterID, &next); e == nil {
			page.NextAfterID = next
		}
	}
	return page, nil
}
