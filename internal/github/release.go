package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/mgh3326/mergewake/internal/manifest"
)

const AssetName = "mergewake-manifest.json"

func Tag(sha string) string { return "mergewake-" + sha }

type Asset struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}
type Release struct {
	ID         int64   `json:"id"`
	Tag        string  `json:"tag_name"`
	Draft      bool    `json:"draft"`
	Prerelease bool    `json:"prerelease"`
	Assets     []Asset `json:"assets"`
}

func (c *Client) Publish(ctx context.Context, m manifest.Manifest) error {
	if e := m.Validate(); e != nil {
		return e
	}
	b, e := json.MarshalIndent(m, "", "  ")
	if e != nil {
		return e
	}
	tag := Tag(m.Commit)

	// Reserve an immutable tag before publication. A racing publisher can only
	// succeed if it reserved the same commit; existing tags are never moved.
	var ref struct {
		Object Ref `json:"object"`
	}
	route := base(m.Repo) + "/git/ref/tags/" + tag
	tagErr := c.json(ctx, "GET", route, nil, &ref)
	if NotFound(tagErr) {
		tagErr = c.json(ctx, "POST", base(m.Repo)+"/git/refs", map[string]string{"ref": "refs/tags/" + tag, "sha": m.Commit}, nil)
		if tagErr != nil {
			if e = c.json(ctx, "GET", route, nil, &ref); e != nil {
				return tagErr
			}
		} else {
			ref.Object.SHA = m.Commit
		}
	} else if tagErr != nil {
		return tagErr
	}
	if ref.Object.SHA != m.Commit {
		return errors.New("release tag already points at a different object")
	}
	var r Release
	e = c.json(ctx, "GET", base(m.Repo)+"/releases/tags/"+tag, nil, &r)
	if NotFound(e) {
		e = c.json(ctx, "POST", base(m.Repo)+"/releases", map[string]any{"tag_name": tag, "target_commitish": m.Commit, "name": tag, "draft": true, "make_latest": "false"}, &r)
	}
	if e != nil {
		return e
	}
	// Never replace an existing manifest. Interrupted draft publication can resume.
	found := false
	for _, a := range r.Assets {
		if a.Name == AssetName {
			old, e := c.asset(ctx, m.Repo, a)
			if e != nil {
				return e
			}
			if string(old) != string(b) {
				return errors.New("release manifest already exists with different content")
			}
			found = true
		}
	}
	if !r.Draft && !found {
		return errors.New("published release has no manifest; refusing mutation")
	}
	if !found {
		_, e = c.request(ctx, "POST", fmt.Sprintf("UPLOAD %s/releases/%d/assets?name=%s", base(m.Repo), r.ID, url.QueryEscape(AssetName)), "application/vnd.github+json", b)
		if e != nil {
			return e
		}
	}
	if r.Draft {
		return c.json(ctx, "PATCH", fmt.Sprintf("%s/releases/%d", base(m.Repo), r.ID), map[string]any{"draft": false, "make_latest": "false"}, nil)
	}
	return nil
}
func (c *Client) asset(ctx context.Context, repo string, a Asset) ([]byte, error) {
	if a.Size < 0 || a.Size > 4<<20 {
		return nil, errors.New("manifest asset too large")
	}
	return c.request(ctx, "GET", fmt.Sprintf("%s/releases/assets/%d", base(repo), a.ID), "application/octet-stream", nil)
}
func (c *Client) Latest(ctx context.Context, repo string) (manifest.Manifest, error) {
	releases, e := list[Release](ctx, c, base(repo)+"/releases")
	if e != nil {
		return manifest.Manifest{}, e
	}
	for _, r := range releases {
		if r.Draft || r.Prerelease || !strings.HasPrefix(r.Tag, "mergewake-") {
			continue
		}
		for _, a := range r.Assets {
			if a.Name == AssetName {
				b, e := c.asset(ctx, repo, a)
				if e != nil {
					return manifest.Manifest{}, e
				}
				var m manifest.Manifest
				if e = json.Unmarshal(b, &m); e != nil {
					return m, e
				}
				if e = m.Validate(); e != nil {
					return m, e
				}
				if m.Repo != repo || r.Tag != Tag(m.Commit) {
					return m, errors.New("release identity mismatch")
				}
				commit, e := c.Commit(ctx, repo, r.Tag)
				if e != nil {
					return m, e
				}
				if commit.SHA != m.Commit {
					return m, errors.New("release tag does not point at manifest commit")
				}
				return m, nil
			}
		}
	}
	return manifest.Manifest{}, errors.New("no mergewake release manifest found")
}
