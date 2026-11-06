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
	return sha, c.now().Sub(started) < ttl, nil
}

// ClaimReview uses a timestamped empty commit as a renewable ref lease. An
// expired lease advances only by fast-forward; competing renewals are siblings,
// so only one can win. No delete/create gap can admit two reviewers.
func (c *Client) ClaimReview(ctx context.Context, repo, branch, head string, ttl time.Duration) (bool, error) {
	_, acquired, err := c.ClaimReviewWithSHA(ctx, repo, branch, head, ttl)
	return acquired, err
}

// ClaimReviewWithSHA returns the exact commit created by a successful claim.
func (c *Client) ClaimReviewWithSHA(ctx context.Context, repo, branch, head string, ttl time.Duration) (string, bool, error) {
	old, active, err := c.ReviewLease(ctx, repo, branch, ttl)
	if err != nil || active {
		return "", false, err
	}
	parent := old
	if parent == "" {
		parent = head
	}
	commit, err := c.Commit(ctx, repo, parent)
	if err != nil {
		return "", false, err
	}
	var lease Ref
	err = c.json(ctx, "POST", base(repo)+"/git/commits", map[string]any{
		"message": leasePrefix + c.now().UTC().Format(time.RFC3339Nano),
		"tree":    commit.Commit.Tree.SHA, "parents": []string{parent},
	}, &lease)
	if err != nil {
		return "", false, err
	}
	if old == "" {
		err = c.CreateRef(ctx, repo, branch, lease.SHA)
	} else {
		err = c.json(ctx, "PATCH", base(repo)+"/git/refs/heads/"+branch, map[string]any{"sha": lease.SHA, "force": false}, nil)
	}
	if err != nil {
		var api *APIError
		if errors.As(err, &api) && (api.Code == 409 || api.Code == 422) {
			return "", false, nil
		}
		return "", false, err
	}
	return lease.SHA, true, nil
}

// RenewReviewLease advances only the lease commit the caller owns.
func (c *Client) RenewReviewLease(ctx context.Context, repo, branch, expected string) (string, bool, error) {
	old, err := c.Head(ctx, repo, branch)
	if err != nil || old != expected {
		return "", false, err
	}
	commit, err := c.Commit(ctx, repo, old)
	if err != nil {
		return "", false, err
	}
	var lease Ref
	err = c.json(ctx, "POST", base(repo)+"/git/commits", map[string]any{"message": leasePrefix + c.now().UTC().Format(time.RFC3339Nano), "tree": commit.Commit.Tree.SHA, "parents": []string{old}}, &lease)
	if err != nil {
		return "", false, err
	}
	err = c.json(ctx, "PATCH", base(repo)+"/git/refs/heads/"+branch, map[string]any{"sha": lease.SHA, "force": false}, nil)
	if err != nil {
		var api *APIError
		if errors.As(err, &api) && (api.Code == 409 || api.Code == 422) {
			return "", false, nil
		}
		return "", false, err
	}
	return lease.SHA, true, nil
}

// ReleaseReviewLease deletes the lease branch only while it still points at the
// commit the caller created, so a lease another host renewed is never removed.
// The head is read past the pass memo, by the same unescaped ref path the delete
// uses: the lease branch has slashes, and %2F in the path is not a ref separator. GitHub's ref delete is unconditional, so
// a renewal landing between the read and the delete is the one remaining window;
// the lease then simply expires by its TTL as before.
func (c *Client) ReleaseReviewLease(ctx context.Context, repo, branch, expected string) (bool, error) {
	var r struct {
		Object Ref `json:"object"`
	}
	err := c.json(ctx, "GET", base(repo)+"/git/ref/heads/"+branch, nil, &r)
	if NotFound(err) {
		return false, nil
	}
	if err != nil || expected == "" || r.Object.SHA != expected {
		return false, err
	}
	err = c.json(ctx, "DELETE", base(repo)+"/git/refs/heads/"+branch, nil, nil)
	if err != nil {
		var api *APIError
		if NotFound(err) || (errors.As(err, &api) && api.Code == 422) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
