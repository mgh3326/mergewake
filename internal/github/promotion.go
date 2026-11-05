package github

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Promotion reads and writes for the promotion owner. Every read bypasses the
// poll cache: certification must see GitHub as it is now.

// BranchRef returns the commit at refs/heads/<name>; ok is false on 404.
func (c *Client) BranchRef(ctx context.Context, repo, name string) (string, bool, error) {
	var r struct {
		Object Ref `json:"object"`
	}
	e := c.json(freshRequest(ctx), "GET", base(repo)+"/git/ref/heads/"+url.PathEscape(name), nil, &r)
	if NotFound(e) {
		return "", false, nil
	}
	if e != nil {
		return "", false, e
	}
	return r.Object.SHA, true, nil
}

// MatchedRef is one ref from git/matching-refs.
type MatchedRef struct {
	Ref    string `json:"ref"`
	Object struct {
		SHA  string `json:"sha"`
		Type string `json:"type"`
	} `json:"object"`
}

// MatchingRefs lists refs under refs/<prefix>.
func (c *Client) MatchingRefs(ctx context.Context, repo, prefix string) ([]MatchedRef, error) {
	return list[MatchedRef](freshRequest(ctx), c, base(repo)+"/git/matching-refs/"+prefix)
}

// TagObject is an annotated tag object.
type TagObject struct {
	Tag     string `json:"tag"`
	Message string `json:"message"`
	Object  struct {
		SHA  string `json:"sha"`
		Type string `json:"type"`
	} `json:"object"`
}

// GitTag reads one annotated tag object.
func (c *Client) GitTag(ctx context.Context, repo, sha string) (TagObject, error) {
	var t TagObject
	e := c.json(freshRequest(ctx), "GET", base(repo)+"/git/tags/"+sha, nil, &t)
	return t, e
}

// CommitParents returns the parents of a commit, first parent first.
func (c *Client) CommitParents(ctx context.Context, repo, sha string) ([]string, error) {
	var r struct {
		Parents []struct {
			SHA string `json:"sha"`
		} `json:"parents"`
	}
	if e := c.json(freshRequest(ctx), "GET", base(repo)+"/git/commits/"+sha, nil, &r); e != nil {
		return nil, e
	}
	out := make([]string, len(r.Parents))
	for i, p := range r.Parents {
		out[i] = p.SHA
	}
	return out, nil
}

// WorkflowRun is the part of a run that binds evidence to a commit.
type WorkflowRun struct {
	ID         int64  `json:"id"`
	WorkflowID int64  `json:"workflow_id"`
	Path       string `json:"path"`
	Event      string `json:"event"`
	HeadBranch string `json:"head_branch"`
	HeadSHA    string `json:"head_sha"`
	RunAttempt int    `json:"run_attempt"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

// PushRuns lists the workflow runs for exactly sha from push events on branch.
// It refuses a partial list.
func (c *Client) PushRuns(ctx context.Context, repo, branch, sha string) ([]WorkflowRun, error) {
	var r struct {
		Total int           `json:"total_count"`
		Runs  []WorkflowRun `json:"workflow_runs"`
	}
	q := url.Values{"head_sha": {sha}, "event": {"push"}, "branch": {branch}, "per_page": {"100"}}
	if e := c.json(freshRequest(ctx), "GET", base(repo)+"/actions/runs?"+q.Encode(), nil, &r); e != nil {
		return nil, e
	}
	if r.Total != len(r.Runs) {
		return nil, fmt.Errorf("workflow runs: got %d of %d", len(r.Runs), r.Total)
	}
	return r.Runs, nil
}

// WorkflowJob is one job of a run attempt.
type WorkflowJob struct {
	Name       string `json:"name"`
	HeadSHA    string `json:"head_sha"`
	RunAttempt int    `json:"run_attempt"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

// AttemptJobs lists every job of one run attempt and refuses a partial list.
func (c *Client) AttemptJobs(ctx context.Context, repo string, run int64, attempt int) ([]WorkflowJob, error) {
	var all []WorkflowJob
	for page := 1; page <= 10; page++ {
		var r struct {
			Total int           `json:"total_count"`
			Jobs  []WorkflowJob `json:"jobs"`
		}
		if e := c.json(freshRequest(ctx), "GET", fmt.Sprintf("%s/actions/runs/%d/attempts/%d/jobs?per_page=100&page=%d", base(repo), run, attempt, page), nil, &r); e != nil {
			return nil, e
		}
		all = append(all, r.Jobs...)
		if len(all) >= r.Total || len(r.Jobs) == 0 {
			if len(all) != r.Total {
				return nil, fmt.Errorf("jobs: got %d of %d", len(all), r.Total)
			}
			return all, nil
		}
	}
	return nil, errors.New("jobs pagination limit; refusing partial data")
}

// CreateAnnotatedTag creates a tag object on a commit, then refs/tags/<name>.
// GitHub rejects the ref if it already exists.
func (c *Client) CreateAnnotatedTag(ctx context.Context, repo, name, target, message string) error {
	var obj struct {
		SHA string `json:"sha"`
	}
	if e := c.json(ctx, "POST", base(repo)+"/git/tags", map[string]string{"tag": name, "message": message, "object": target, "type": "commit"}, &obj); e != nil {
		return e
	}
	if obj.SHA == "" {
		return errors.New("tag object has no sha")
	}
	return c.json(ctx, "POST", base(repo)+"/git/refs", map[string]string{"ref": "refs/tags/" + name, "sha": obj.SHA}, nil)
}

// FastForwardBranch moves refs/heads/<name> to sha with force=false. GitHub's
// API has no expected-old-sha parameter, so this is not a compare-and-swap;
// the caller serializes writers.
func (c *Client) FastForwardBranch(ctx context.Context, repo, name, sha string) error {
	if strings.Contains(name, "..") {
		return errors.New("invalid branch")
	}
	return c.json(ctx, "PATCH", base(repo)+"/git/refs/heads/"+url.PathEscape(name), map[string]any{"sha": sha, "force": false}, nil)
}
