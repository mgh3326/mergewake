package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// A merged pull's merge commit comes from GraphQL: REST pull objects carry no
// merge_commit_sha under this client's API version (2026-03-10), for any
// token. GraphQL still reports it, so one resolver behind Pull and the
// change-state writer fills the field, and callers keep reading
// Pull.MergeCommitSHA.
const mergeCommitQuery = `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){mergedAt mergeCommit{oid}}}}`

// mergeCommit is what the resolver keeps: a merged pull's merge commit and
// merge time. A merged pull's merge commit never changes, so a value stays
// valid for the life of the process.
type mergeCommit struct {
	sha      string
	mergedAt *string
}

// mergeCommits is the process-lifetime cache. A failed read is never kept, so
// a later pass retries it; concurrent reads of one pull share the first call.
type mergeCommits struct {
	mu       sync.Mutex
	known    map[string]mergeCommit
	inflight map[string]*mergeCommitCall
	reported map[string]bool
}

type mergeCommitCall struct {
	done  chan struct{}
	value mergeCommit
	ok    bool
}

func mergeCommitKey(repo string, n int) string {
	return normalizeMemoRepo(repo) + "\x00" + strconv.Itoa(n)
}

// mergeCommitOf fills a merged pull's MergeCommitSHA and, when absent, its
// MergedAt from GraphQL. A pull that is not merged, or whose fields are
// already known, is returned unchanged: nothing is read for it.
func (c *Client) mergeCommitOf(ctx context.Context, repo string, p Pull) Pull {
	if !(p.Merged || p.MergedAt != nil) || (p.MergeCommitSHA != "" && p.MergedAt != nil) {
		return p
	}
	v, ok := c.mergeCommits.resolve(ctx, c, repo, p.Number)
	if !ok {
		// Same answer as an unresolved field always got: the caller keeps or
		// skips this pull this pass, and a later pass asks again.
		return p
	}
	if p.MergeCommitSHA == "" {
		p.MergeCommitSHA = v.sha
	}
	if p.MergedAt == nil {
		p.MergedAt = v.mergedAt
	}
	return p
}

func (m *mergeCommits) resolve(ctx context.Context, c *Client, repo string, n int) (mergeCommit, bool) {
	key := mergeCommitKey(repo, n)
	m.mu.Lock()
	if v, ok := m.known[key]; ok {
		m.mu.Unlock()
		return v, true
	}
	if call, ok := m.inflight[key]; ok {
		m.mu.Unlock()
		select {
		case <-call.done:
			return call.value, call.ok
		case <-ctx.Done():
			return mergeCommit{}, false
		}
	}
	if m.inflight == nil {
		m.inflight = map[string]*mergeCommitCall{}
	}
	call := &mergeCommitCall{done: make(chan struct{})}
	m.inflight[key] = call
	m.mu.Unlock()

	v, e := c.fetchMergeCommit(ctx, repo, n)

	m.mu.Lock()
	if e == nil {
		if m.known == nil {
			m.known = map[string]mergeCommit{}
		}
		m.known[key] = v
	}
	delete(m.inflight, key)
	call.value, call.ok = v, e == nil
	m.mu.Unlock()
	close(call.done)
	if e != nil {
		m.reportFailure(c, repo, n, e)
	}
	return v, e == nil
}

// fetchMergeCommit asks GraphQL for one pull's merge commit. GraphQL reports
// errors inside a 200 answer, so the document's errors field is an error too.
func (c *Client) fetchMergeCommit(ctx context.Context, repo string, n int) (mergeCommit, error) {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return mergeCommit{}, errors.New("repo must be owner/name")
	}
	var result struct {
		Errors []json.RawMessage `json:"errors"`
		Data   *struct {
			Repository *struct {
				PullRequest *struct {
					MergedAt    *string `json:"mergedAt"`
					MergeCommit *struct {
						OID string `json:"oid"`
					} `json:"mergeCommit"`
				} `json:"pullRequest"`
			} `json:"repository"`
		} `json:"data"`
	}
	vars := map[string]any{"owner": parts[0], "name": parts[1], "number": n}
	if e := c.graphql(ctx, repo, mergeCommitQuery, vars, &result); e != nil {
		return mergeCommit{}, e
	}
	if len(result.Errors) > 0 || result.Data == nil || result.Data.Repository == nil || result.Data.Repository.PullRequest == nil {
		return mergeCommit{}, errors.New("GitHub merge commit query failed")
	}
	pr := result.Data.Repository.PullRequest
	if pr.MergeCommit == nil || pr.MergeCommit.OID == "" {
		return mergeCommit{}, errors.New("GitHub merge commit is empty")
	}
	return mergeCommit{sha: pr.MergeCommit.OID, mergedAt: pr.MergedAt}, nil
}

// reportFailure says once per pull per process that the resolver could not
// read a merge commit. It names no token, and replaces the old line that was
// written for every merged pull the REST body lacked.
func (m *mergeCommits) reportFailure(c *Client, repo string, n int, e error) {
	key := mergeCommitKey(repo, n)
	m.mu.Lock()
	if m.reported == nil {
		m.reported = map[string]bool{}
	}
	if m.reported[key] {
		m.mu.Unlock()
		return
	}
	m.reported[key] = true
	m.mu.Unlock()
	out := c.Stderr
	if out == nil {
		out = os.Stderr
	}
	fmt.Fprintf(out, "mergewake: merge commit of %s#%d unresolved: %v\n", repo, n, e)
}
