package github

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const RecoveryBranch = "mergewake/recovery"
const RecoveryTag = "mergewake-recovery-latch"

// RecoveryActive also honors a leftover recovery branch, but the tag survives
// GitHub's automatic deletion of merged PR branches.
func (c *Client) RecoveryActive(ctx context.Context, repo string) (bool, error) {
	var ref any
	e := c.json(ctx, "GET", base(repo)+"/git/ref/tags/"+RecoveryTag, nil, &ref)
	if e == nil {
		return true, nil
	}
	if !NotFound(e) {
		return false, e
	}
	_, e = c.Head(ctx, repo, RecoveryBranch)
	if e == nil {
		return true, nil
	}
	if NotFound(e) {
		return false, nil
	}
	return false, e
}

// Revert restores only paths whose current blobs still equal the culprit's
// postimage. Later edits on the same path need an operator rather than a guess.
func (c *Client) Revert(ctx context.Context, repo, branch, head, culprit string, pr int) (string, error) {
	return c.RevertWithTrailers(ctx, repo, branch, head, culprit, pr, "")
}

func (c *Client) RevertWithTrailers(ctx context.Context, repo, branch, head, culprit string, pr int, trailers string) (string, error) {
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
	// A permanent tag is the CAS latch. A PR branch alone can be auto-deleted.
	if e = c.json(ctx, "POST", base(repo)+"/git/refs", map[string]string{"ref": "refs/tags/" + RecoveryTag, "sha": commit.SHA}, nil); e != nil {
		return "", e
	}
	if e = c.CreateRef(ctx, repo, RecoveryBranch, commit.SHA); e != nil {
		return "", e
	}
	if !strings.HasPrefix(trailers, "Mergewake-Task: ") {
		if trailers != "" {
			trailers += "\n"
		}
		trailers += "Mergewake-Recovery: " + culprit
	}
	var pull struct {
		HTMLURL string `json:"html_url"`
	}
	e = c.json(ctx, "POST", base(repo)+"/pulls", map[string]any{"title": fmt.Sprintf("Revert #%d: main CI recovery", pr), "head": RecoveryBranch, "base": branch, "body": fmt.Sprintf("Reverts %s from PR #%d. Review and merge this PR. The recovery latch clears itself once this PR is merged and the newest main is green; until then it stays.", culprit, pr) + "\n\n" + trailers}, &pull)
	return pull.HTMLURL, e
}

// recoveryLatchSHA is the commit the latch was created at: the tag, or the
// legacy branch alone. Empty means no latch.
func (c *Client) recoveryLatchSHA(ctx context.Context, repo string) (string, error) {
	var ref struct {
		Object Ref `json:"object"`
	}
	e := c.json(ctx, "GET", base(repo)+"/git/ref/tags/"+RecoveryTag, nil, &ref)
	if e == nil {
		return ref.Object.SHA, nil
	}
	if !NotFound(e) {
		return "", e
	}
	sha, e := c.Head(ctx, repo, RecoveryBranch)
	if NotFound(e) {
		return "", nil
	}
	return sha, e
}

// RecoveryRevert returns the revert PR the active latch belongs to, or nil when
// there is none to prove: no latch, or one a person created. A PR belongs to
// the latch when watch opened it (its body names the culprit) from the recovery
// branch and its history holds the commit the latch was created at. Pushes to
// the PR branch after review leave that commit in place; an older recovery's PR
// never holds it.
func (c *Client) RecoveryRevert(ctx context.Context, repo string) (*Pull, error) {
	latch, e := c.recoveryLatchSHA(ctx, repo)
	if e != nil || latch == "" {
		return nil, e
	}
	pulls, e := list[Pull](ctx, c, base(repo)+"/pulls?state=all&head="+url.QueryEscape(strings.Split(repo, "/")[0]+":"+RecoveryBranch))
	if e != nil {
		return nil, e
	}
	sort.Slice(pulls, func(i, j int) bool { return pulls[i].Number > pulls[j].Number })
	for _, p := range pulls {
		if p.Head.Ref != RecoveryBranch || !strings.EqualFold(p.Head.Repo.FullName, repo) || !strings.HasPrefix(p.Body, "Reverts ") {
			continue
		}
		if p.Head.SHA != latch {
			held, e := c.Ancestor(ctx, repo, latch, p.Head.SHA)
			if NotFound(e) {
				continue
			}
			if e != nil {
				return nil, e
			}
			if !held {
				continue
			}
		}
		full, e := c.Pull(ctx, repo, p.Number)
		return &full, e
	}
	return nil, nil
}

// LatestStatus returns the newest commit status named name on sha: its state,
// when it was updated, and whether one exists. It reads the statuses API, not
// check runs, so a re-run an any-ref workflow posts is distinguishable from the
// original run of the same name.
func (c *Client) LatestStatus(ctx context.Context, repo, sha, name string) (CI, time.Time, bool, error) {
	statuses, e := list[status](ctx, c, base(repo)+"/commits/"+sha+"/statuses")
	if e != nil {
		return Pending, time.Time{}, false, e
	}
	var state CI
	var at time.Time
	found := false
	for _, s := range statuses {
		if s.Context != name {
			continue
		}
		if !found || s.UpdatedAt.After(at) {
			state, at, found = statusState(s.State), s.UpdatedAt, true
		}
	}
	return state, at, found, nil
}

func statusState(state string) CI {
	switch state {
	case "success":
		return Green
	case "failure", "error":
		return Red
	}
	return Pending
}

// ClosePull closes an open pull request without merging it.
func (c *Client) ClosePull(ctx context.Context, repo string, n int) error {
	return c.json(ctx, "PATCH", fmt.Sprintf("%s/pulls/%d", base(repo), n), map[string]string{"state": "closed"}, nil)
}

// ClearRecovery deletes the recovery tag and branch. A ref that is already gone
// counts as deleted, so a pass that died halfway finishes on the next one.
func (c *Client) ClearRecovery(ctx context.Context, repo string) error {
	var errs []error
	for _, ref := range []string{"tags/" + RecoveryTag, "heads/" + RecoveryBranch} {
		if e := c.json(ctx, "DELETE", base(repo)+"/git/refs/"+ref, nil, nil); e != nil && !NotFound(e) && !isUnprocessable(e) {
			errs = append(errs, e)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	// A 422 can also mean a ref the rules refuse to delete, so trust only a
	// read that finds the latch gone.
	active, e := c.RecoveryActive(ctx, repo)
	if e == nil && active {
		e = errors.New("recovery latch still present after delete")
	}
	return e
}

// isUnprocessable is GitHub's answer for deleting a ref that does not exist.
func isUnprocessable(e error) bool {
	var a *APIError
	return errors.As(e, &a) && a.Code == 422
}
