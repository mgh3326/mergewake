package github

import (
	"context"
	"errors"
)

// PathBlob returns the blob SHA of one path in the commit's tree, or "" when
// the path does not exist. It reads immutable Git trees, so a repeat read of
// the same commit is served from cache.
func (c *Client) PathBlob(ctx context.Context, repo, sha, name string) (string, error) {
	commit, e := c.Commit(ctx, repo, sha)
	if e != nil {
		return "", e
	}
	entry, e := c.Entry(ctx, repo, commit.Commit.Tree.SHA, name)
	if e != nil {
		return "", e
	}
	if entry.Type != "blob" {
		return "", nil
	}
	return entry.SHA, nil
}

// Dir lists the regular blobs directly under dir in the commit's tree, with
// full paths. A missing directory is an empty list, not an error.
func (c *Client) Dir(ctx context.Context, repo, sha, dir string) ([]TreeEntry, error) {
	commit, e := c.Commit(ctx, repo, sha)
	if e != nil {
		return nil, e
	}
	entry, e := c.Entry(ctx, repo, commit.Commit.Tree.SHA, dir)
	if e != nil {
		return nil, e
	}
	if entry.Type != "tree" {
		return nil, nil
	}
	t, e := immutable[treeResponse](ctx, c, repo, kindTree, entry.SHA, base(repo)+"/git/trees/"+entry.SHA)
	if e != nil {
		return nil, e
	}
	if t.Truncated {
		return nil, errors.New("truncated Git tree")
	}
	var out []TreeEntry
	for _, v := range t.Tree {
		if v.Type == "blob" {
			v.Path = dir + "/" + v.Path
			out = append(out, v)
		}
	}
	return out, nil
}
