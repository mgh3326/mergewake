package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// MaxCachedObject bounds the body the ObjectCache stores; a larger object is
// read from GitHub every time.
const MaxCachedObject = 1 << 20

const objectCacheTimeout = 3 * time.Second

// Object kinds the cache holds. All are addressed by a Git sha, so a stored
// body never goes stale. A "file" is a blob read through a commit: its key is
// the commit sha plus the path, and its body the decoded bytes.
const (
	kindCommit = "commit"
	kindTree   = "tree"
	kindBlob   = "blob"
	kindFile   = "file"
)

// ObjectCache is a persistent, best-effort store of sha-addressed GitHub
// objects. Path is empty except for kind "file". Any error is treated as a
// miss: the cache never fails or blocks a read.
type ObjectCache interface {
	GetObject(ctx context.Context, repo, kind, sha, path string) (body []byte, found bool, err error)
	PutObject(ctx context.Context, repo, kind, sha, path string, body []byte) error
}

// cacheable reports whether the cache may hold (sha, path): sha must be a full
// hex object id, never a branch name, and path printable ASCII within the
// column.
func cacheable(sha, path string) bool {
	if !shaSegment.MatchString(sha) || len(path) > 255 {
		return false
	}
	for _, r := range path {
		if r < 0x20 || r > 0x7e {
			return false
		}
	}
	return true
}

// cacheGet returns the stored body, or false on a miss or any cache error. A
// failed pass keeps the cache off until the pass ends.
func (c *Client) cacheGet(ctx context.Context, repo, kind, sha, path string) ([]byte, bool) {
	p := passOf(ctx)
	if c.Objects == nil || !cacheable(sha, path) || p.cacheOff() {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, objectCacheTimeout)
	defer cancel()
	body, found, err := c.Objects.GetObject(ctx, repo, kind, strings.ToLower(sha), path)
	if err != nil {
		c.cacheFailed(p, err)
		return nil, false
	}
	return body, found
}

func (c *Client) cachePut(ctx context.Context, repo, kind, sha, path string, body []byte) {
	p := passOf(ctx)
	if c.Objects == nil || !cacheable(sha, path) || len(body) > MaxCachedObject || p.cacheOff() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, objectCacheTimeout)
	defer cancel()
	if err := c.Objects.PutObject(ctx, repo, kind, strings.ToLower(sha), path, body); err != nil {
		c.cacheFailed(p, err)
	}
}

// cacheFailed logs the first cache error of a pass and switches the cache off
// for the rest of it. Without a pass there is no breaker, so every error logs.
func (c *Client) cacheFailed(p *RequestCounts, err error) {
	if !p.tripCache() {
		return
	}
	var out io.Writer = c.Stderr
	if out == nil {
		out = os.Stderr
	}
	fmt.Fprintf(out, "mergewake: GitHub object cache unavailable; reading from GitHub: %v\n", err)
}

// immutable reads a sha-addressed JSON object through the cache.
func immutable[T any](ctx context.Context, c *Client, repo, kind, sha, route string) (T, error) {
	if c.Objects != nil && cacheable(sha, "") {
		var v T
		if body, ok := c.cacheGet(ctx, repo, kind, sha, ""); ok && json.Unmarshal(body, &v) == nil {
			passOf(ctx).noteCache(true)
			return v, nil
		}
		passOf(ctx).noteCache(false)
	}
	var v T
	if e := c.json(ctx, "GET", route, nil, &v); e != nil {
		return v, e
	}
	if c.Objects != nil {
		if body, e := json.Marshal(v); e == nil {
			c.cachePut(ctx, repo, kind, sha, "", body)
		}
	}
	return v, nil
}
