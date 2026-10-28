package github

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

const RecoveryBranch = "mergewake/recovery"

// Revert restores only paths whose current blobs still equal the culprit's
// postimage. Later edits on the same path need an operator rather than a guess.
func (c *Client) Revert(ctx context.Context, repo, branch, head, culprit string, pr int) (string, error) {
	current, e := c.Commit(ctx, repo, head)
	if e != nil {
		return "", e
	}
	bad, e := c.Commit(ctx, repo, culprit)
	if e != nil {
		return "", e
	}
	if len(bad.Parents) != 1 {
		return "", errors.New("automatic revert requires a squash commit")
	}
	before, e := c.Commit(ctx, repo, bad.Parents[0].SHA)
	if e != nil {
		return "", e
	}
	files, e := c.Files(ctx, repo, pr)
	if e != nil {
		return "", e
	}
	p, e := c.Pull(ctx, repo, pr)
	if e != nil {
		return "", e
	}
	if len(files) != p.ChangedFiles {
		return "", errors.New("incomplete revert file list")
	}
	if len(files) == 0 {
		return "", errors.New("empty revert")
	}
	paths := map[string]bool{}
	for _, f := range files {
		paths[f.Path] = true
		if f.Previous != "" {
			paths[f.Previous] = true
		}
		switch f.Kind {
		case "added", "removed", "modified", "renamed":
		default:
			return "", fmt.Errorf("unsupported revert kind %s", f.Kind)
		}
	}
	names := []string{}
	for p := range paths {
		names = append(names, p)
	}
	sort.Strings(names)
	entries := []map[string]any{}
	for _, p := range names {
		post, e := c.Entry(ctx, repo, bad.Commit.Tree.SHA, p)
		if e != nil {
			return "", e
		}
		now, e := c.Entry(ctx, repo, current.Commit.Tree.SHA, p)
		if e != nil {
			return "", e
		}
		old, e := c.Entry(ctx, repo, before.Commit.Tree.SHA, p)
		if e != nil {
			return "", e
		}
		if now.SHA != post.SHA || now.Mode != post.Mode {
			return "", fmt.Errorf("later edit overlaps %s; operator required", p)
		}
		for _, v := range []TreeEntry{old, post} {
			if v.SHA != "" && (v.Type != "blob" || (v.Mode != "100644" && v.Mode != "100755")) {
				return "", errors.New("automatic revert only supports regular files")
			}
		}
		entry := map[string]any{"path": p, "mode": "100644", "type": "blob", "sha": nil}
		if old.SHA != "" {
			entry["sha"] = old.SHA
			entry["mode"] = old.Mode
		}
		entries = append(entries, entry)
	}
	var tree Ref
	if e = c.json(ctx, "POST", base(repo)+"/git/trees", map[string]any{"base_tree": current.Commit.Tree.SHA, "tree": entries}, &tree); e != nil {
		return "", e
	}
	var commit Ref
	if e = c.json(ctx, "POST", base(repo)+"/git/commits", map[string]any{"message": fmt.Sprintf("Revert PR #%d after main CI failure", pr), "tree": tree.SHA, "parents": []string{head}}, &commit); e != nil {
		return "", e
	}
	// A permanent, fixed-name ref is a GitHub-side CAS latch across all hosts.
	if e = c.CreateRef(ctx, repo, RecoveryBranch, commit.SHA); e != nil {
		return "", e
	}
	var pull struct {
		HTMLURL string `json:"html_url"`
	}
	e = c.json(ctx, "POST", base(repo)+"/pulls", map[string]any{"title": fmt.Sprintf("Revert #%d: main CI recovery", pr), "head": RecoveryBranch, "base": branch, "body": fmt.Sprintf("Reverts %s from PR #%d. Review and merge this PR. The recovery latch remains until an operator deletes the recovery branch.", culprit, pr)}, &pull)
	return pull.HTMLURL, e
}
