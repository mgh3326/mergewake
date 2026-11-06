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

// ReleaseReviewLease ends the lease commit the caller owns by fast-forwarding the
// branch to a child commit stamped at the Unix epoch, which ReviewLease reads as
// expired. It is a compare-and-swap like ClaimReviewWithSHA and RenewReviewLease:
// the update is not forced, so if another host renewed the lease the branch no
// longer sits at expected, the child is not a fast-forward, GitHub refuses it
// and the renewed lease stays. GitHub's ref delete has no such precondition, so
// the branch is expired, not deleted; the next claim fast-forwards it.
func (c *Client) ReleaseReviewLease(ctx context.Context, repo, branch, expected string) (bool, error) {
	if expected == "" {
		return false, nil
	}
	commit, err := c.Commit(ctx, repo, expected)
	if NotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var released Ref
	err = c.json(ctx, "POST", base(repo)+"/git/commits", map[string]any{
		"message": leasePrefix + time.Unix(0, 0).UTC().Format(time.RFC3339Nano),
		"tree":    commit.Commit.Tree.SHA, "parents": []string{expected},
	}, &released)
	if err != nil {
		return false, err
	}
	err = c.json(ctx, "PATCH", base(repo)+"/git/refs/heads/"+branch, map[string]any{"sha": released.SHA, "force": false}, nil)
	if err != nil {
		var api *APIError
		if NotFound(err) || (errors.As(err, &api) && (api.Code == 409 || api.Code == 422)) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
