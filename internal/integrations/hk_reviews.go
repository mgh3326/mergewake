package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// ReviewRecords reads the review passes hk holds for a PR (`GET
// /v1/changes?pr=`, the change record's reviews[], oldest first). An hk
// without the endpoint (404), without a change for the PR, without passes on
// it, or a host with no hk endpoint configured gives an empty record: "no hk
// data", not an error. Any other failure is an error the caller logs.
func (h Handoffkeep) ReviewRecords(ctx context.Context, repo string, n int) (github.HKChange, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		if errors.Is(e, errEndpointUnconfigured) {
			return github.HKChange{}, nil
		}
		return github.HKChange{}, e
	}
	return c.reviewRecords(ctx, repo, n)
}

func (c *hkJobs) reviewRecords(ctx context.Context, repo string, n int) (github.HKChange, error) {
	pr := fmt.Sprintf("https://github.com/%s/pull/%d", repo, n)
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/changes?pr="+url.QueryEscape(pr), nil)
	if e != nil {
		return github.HKChange{}, errors.New("invalid change request")
	}
	c.authorize(req)
	resp, e := c.client(8 * time.Second).Do(req)
	if e != nil {
		return github.HKChange{}, errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return github.HKChange{}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return github.HKChange{}, fmt.Errorf("handoffkeep HTTP %d (response withheld)", resp.StatusCode)
	}
	var out struct {
		Changes []struct {
			PR      string  `json:"pr"`
			HeadSHA *string `json:"head_sha"`
			Reviews []struct {
				ReviewedHead string     `json:"reviewed_head"`
				PatchID      *string    `json:"patch_id"`
				Round        int        `json:"round"`
				Pass         int        `json:"pass"`
				Lens         string     `json:"lens"`
				Family       *string    `json:"family"`
				Executor     *string    `json:"executor"`
				FinishedAt   *time.Time `json:"finished_at"`
			} `json:"reviews"`
		} `json:"changes"`
	}
	if e := json.NewDecoder(io.LimitReader(resp.Body, 1<<22)).Decode(&out); e != nil {
		return github.HKChange{}, errors.New("handoffkeep response is not valid JSON")
	}
	str := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	for _, ch := range out.Changes {
		if ch.PR != pr || len(ch.Reviews) == 0 {
			continue
		}
		rec := github.HKChange{Head: str(ch.HeadSHA)}
		for _, r := range ch.Reviews {
			rec.Reviews = append(rec.Reviews, github.HKReview{
				Head: r.ReviewedHead, Patch: str(r.PatchID), Round: r.Round, Pass: r.Pass, Lens: r.Lens,
				Family: str(r.Family), Executor: str(r.Executor), FinishedAt: r.FinishedAt,
			})
		}
		return rec, nil
	}
	return github.HKChange{}, nil
}

// AttachReviewHistory lets gh read review history from hk when the sink is
// handoffkeep. Which phase runs is github.HKReviewsPrimary.
func AttachReviewHistory(gh *github.Client, sink Sink) {
	if h, ok := sink.(Handoffkeep); ok {
		gh.UseHK(h, github.HKReviewsPrimary)
	}
}
