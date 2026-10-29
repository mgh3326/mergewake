package github

import (
	"context"
	"errors"
	"strings"
	"time"
)

const leasePrefix = "mergewake review lease: "

// ReviewLease returns the ref snapshot and whether its reviewer may still be
// running. Legacy claims use their commit date and become reclaimable too.
func (c *Client) ReviewLease(ctx context.Context, repo, branch string, ttl time.Duration) (string, bool, error) {
	sha, err := c.Head(ctx, repo, branch)
	if NotFound(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	commit, err := c.Commit(ctx, repo, sha)
	if err != nil {
		return "", false, err
	}
	started := commit.Commit.Committer.Date
	if strings.HasPrefix(commit.Commit.Message, leasePrefix) {
		started, err = time.Parse(time.RFC3339Nano, strings.TrimPrefix(commit.Commit.Message, leasePrefix))
		if err != nil {
			return "", false, errors.New("invalid review lease timestamp")
		}
	}
	if started.IsZero() {
		return "", false, errors.New("missing review claim timestamp")
	}
	return sha, time.Since(started) < ttl, nil
}

// ClaimReview uses a timestamped empty commit as a renewable ref lease. An
// expired lease advances only by fast-forward; competing renewals are siblings,
// so only one can win. No delete/create gap can admit two reviewers.
func (c *Client) ClaimReview(ctx context.Context, repo, branch, head string, ttl time.Duration) (bool, error) {
	old, active, err := c.ReviewLease(ctx, repo, branch, ttl)
	if err != nil || active {
		return false, err
	}
	parent := old
	if parent == "" {
		parent = head
	}
	commit, err := c.Commit(ctx, repo, parent)
	if err != nil {
		return false, err
	}
	var lease Ref
	err = c.json(ctx, "POST", base(repo)+"/git/commits", map[string]any{
		"message": leasePrefix + time.Now().UTC().Format(time.RFC3339Nano),
		"tree":    commit.Commit.Tree.SHA, "parents": []string{parent},
	}, &lease)
	if err != nil {
		return false, err
	}
	if old == "" {
		err = c.CreateRef(ctx, repo, branch, lease.SHA)
	} else {
		err = c.json(ctx, "PATCH", base(repo)+"/git/refs/heads/"+branch, map[string]any{"sha": lease.SHA, "force": false}, nil)
	}
	if err != nil {
		var api *APIError
		if errors.As(err, &api) && (api.Code == 409 || api.Code == 422) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
