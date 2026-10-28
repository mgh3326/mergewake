package github

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FirstPRCI inspects the first attempt of the earliest PR run of each workflow,
// including superseded heads. It never substitutes the current green head for a
// failed initial run. Missing, in-progress, or expired evidence remains unknown.
func (c *Client) FirstPRCI(ctx context.Context, repo string, p Pull, required []string) (*bool, error) {
	type run struct {
		ID         int64     `json:"id"`
		WorkflowID int64     `json:"workflow_id"`
		CreatedAt  time.Time `json:"created_at"`
		Pulls      []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
	}
	all := []run{}
	for page := 1; page <= 100; page++ {
		var v struct {
			Runs []run `json:"workflow_runs"`
		}
		route := base(repo) + "/actions/runs?event=pull_request&per_page=100&page=" + strconv.Itoa(page)
		if p.Head.Ref != "" {
			route += "&branch=" + url.QueryEscape(p.Head.Ref)
		}
		if p.CreatedAt != "" {
			route += "&created=" + url.QueryEscape(">="+p.CreatedAt)
		}
		if err := c.json(ctx, "GET", route, nil, &v); err != nil {
			return nil, err
		}
		for _, r := range v.Runs {
			for _, pr := range r.Pulls {
				if pr.Number == p.Number {
					all = append(all, r)
					break
				}
			}
		}
		if len(v.Runs) < 100 {
			break
		}
		if page == 100 {
			return nil, fmt.Errorf("initial CI pagination limit")
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].ID < all[j].ID
		}
		return all[i].CreatedAt.Before(all[j].CreatedAt)
	})
	seen := map[int64]bool{}
	states := map[string]string{}
	for _, r := range all {
		if seen[r.WorkflowID] {
			continue
		}
		seen[r.WorkflowID] = true
		jobs := []checkRun{}
		for page := 1; page <= 100; page++ {
			var response struct {
				Jobs []checkRun `json:"jobs"`
			}
			err := c.json(ctx, "GET", fmt.Sprintf("%s/actions/runs/%d/attempts/1/jobs?per_page=100&page=%d", base(repo), r.ID, page), nil, &response)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, response.Jobs...)
			if len(response.Jobs) < 100 {
				break
			}
			if page == 100 {
				return nil, fmt.Errorf("initial CI jobs pagination limit")
			}
		}
		for _, j := range jobs {
			for _, name := range required {
				if j.Name == name {
					if j.Status == "completed" {
						states[name] = j.Conclusion
					} else {
						states[name] = "pending"
					}
				}
			}
		}
	}
	if len(required) == 0 {
		return nil, nil
	}
	for _, name := range required {
		state := states[name]
		if state != "" && state != "pending" && state != "success" && state != "skipped" && state != "neutral" {
			v := false
			return &v, nil
		}
	}
	for _, name := range required {
		if states[name] != "success" {
			return nil, nil
		}
	}
	v := true
	return &v, nil
}

// ReviewOutcomes recovers durable severity evidence from the exact-head reviews
// already posted by this reviewer identity, including another host's pass.
func (c *Client) ReviewOutcomes(ctx context.Context, repo string, n int, login string) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	reviews, err := list[Review](ctx, c, fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n))
	if err != nil {
		return nil, err
	}
	heads := map[int64]string{}
	for _, r := range reviews {
		if r.User.Login == login && r.State != "PENDING" && strings.HasPrefix(r.Body, Marker(r.CommitID)+"\n") {
			heads[r.ID] = r.CommitID
			out[r.CommitID] = map[string]int{}
		}
	}
	if len(heads) == 0 {
		return out, nil
	}
	comments, err := list[struct {
		ReviewID int64  `json:"pull_request_review_id"`
		Body     string `json:"body"`
	}](ctx, c, fmt.Sprintf("%s/pulls/%d/comments", base(repo), n))
	if err != nil {
		return nil, err
	}
	for _, comment := range comments {
		head := heads[comment.ReviewID]
		if head == "" {
			continue
		}
		severity := "unspecified"
		for _, name := range []string{"critical", "high", "medium", "low", "blocking", "major", "minor"} {
			if strings.HasPrefix(comment.Body, "<!-- mergewake-severity:"+name+" -->\n") {
				severity = name
				break
			}
		}
		out[head][severity]++
	}
	return out, nil
}

// WatchRevert recovers the watch-created recovery PR even if the daemon crashed
// after GitHub accepted it and before the local ledger was appended.
func (c *Client) WatchRevert(ctx context.Context, repo string, n int, mergeSHA string) (*Pull, error) {
	pulls, err := list[Pull](ctx, c, base(repo)+"/pulls?state=all&head="+url.QueryEscape(strings.Split(repo, "/")[0]+":"+RecoveryBranch))
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("Reverts %s from PR #%d.", mergeSHA, n)
	for _, p := range pulls {
		if strings.HasPrefix(p.Body, prefix) {
			full, err := c.Pull(ctx, repo, p.Number)
			return &full, err
		}
	}
	return nil, nil
}
