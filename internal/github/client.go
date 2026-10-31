// Package github implements the GitHub API behind the pipeline's interfaces.
package github

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

type Client struct {
	BaseURL, UploadURL, Token string
	HTTP                      *http.Client
	// Stderr receives throttle-wait log lines; nil means os.Stderr.
	Stderr io.Writer
	// Now stamps and ages review leases; nil means time.Now. Rate-limit waits
	// are real sleeps and always use the wall clock.
	Now func() time.Time
	// Polling honours X-Poll-Interval waits between requests sharing a key.
	// Polling loops (daemon, watch, agent) set it; one-shot commands leave it
	// false so a recorded interval never stalls a single run. Retry-After,
	// 403/429 backoff and rate-limit pacing apply either way.
	Polling bool
	poll    pollingState
}

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func New(token string) *Client {
	return &Client{BaseURL: "https://api.github.com", UploadURL: "https://uploads.github.com", Token: token, HTTP: &http.Client{Timeout: 45 * time.Second}}
}

type APIError struct {
	Code         int
	Method, Path string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("GitHub %s %s: HTTP %d", e.Method, e.Path, e.Code)
}
func NotFound(e error) bool { var a *APIError; return errors.As(e, &a) && a.Code == 404 }
func (c *Client) request(ctx context.Context, method, route, accept string, data []byte) ([]byte, error) {
	endpoint := c.BaseURL + route
	if strings.HasPrefix(route, "UPLOAD ") {
		endpoint = c.UploadURL + strings.TrimPrefix(route, "UPLOAD ")
	}
	key := method + " " + endpoint + " " + accept
	if method == "GET" {
		if hit, e := c.pollShortcut(key); e != nil {
			return nil, e
		} else if hit.ETag != "" || hit.Replay {
			return append([]byte(nil), hit.Body...), nil
		}
	}
	release, cached, e := c.beginRequest(ctx, key)
	if e != nil {
		return nil, e
	}
	defer release()
	req, e := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Accept", accept)
	if method == "GET" && cached.ETag != "" {
		req.Header.Set("If-None-Match", cached.ETag)
	}
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	req.Header.Set("User-Agent", "mergewake")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := c.HTTP.Do(req)
	if e != nil {
		return nil, fmt.Errorf("GitHub %s request failed", method)
	}
	defer resp.Body.Close()
	c.observeHeaders(resp.Header, resp.StatusCode, key)
	if resp.StatusCode == http.StatusNotModified && method == "GET" {
		if cached.ETag == "" {
			return nil, errUncached304
		}
		return append([]byte(nil), cached.Body...), nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{resp.StatusCode, method, strings.Split(route, "?")[0]}
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, manifest.MaxFileSize*2+1))
	if len(b) > manifest.MaxFileSize*2 {
		return nil, errors.New("GitHub response too large")
	}
	if e == nil {
		c.cacheResponse(key, resp.Header.Get("ETag"), b, method != "GET" && route != "/graphql")
	}
	return b, e
}
func (c *Client) json(ctx context.Context, method, route string, in, out any) error {
	var b []byte
	var e error
	if in != nil {
		b, e = json.Marshal(in)
		if e != nil {
			return e
		}
	}
	b, e = c.request(ctx, method, route, "application/vnd.github+json", b)
	if e != nil {
		return e
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}
func base(repo string) string { return "/repos/" + repo }
func list[T any](ctx context.Context, c *Client, route string) ([]T, error) {
	var all []T
	sep := "?"
	if strings.Contains(route, "?") {
		sep = "&"
	}
	for page := 1; page <= 100; page++ {
		var batch []T
		if e := c.json(ctx, "GET", route+sep+"per_page=100&page="+strconv.Itoa(page), nil, &batch); e != nil {
			return nil, e
		}
		all = append(all, batch...)
		if len(batch) < 100 {
			return all, nil
		}
	}
	return nil, errors.New("GitHub pagination limit reached; refusing partial data")
}

type Ref struct {
	SHA  string `json:"sha"`
	Ref  string `json:"ref"`
	Repo struct {
		FullName string `json:"full_name"`
	} `json:"repo"`
}
type Pull struct {
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	ClosedAt       *string `json:"closed_at"`
	HTMLURL        string  `json:"html_url"`
	MergedAt       *string `json:"merged_at"`
	Number         int     `json:"number"`
	State          string  `json:"state"`
	Draft          bool    `json:"draft"`
	Merged         bool    `json:"merged"`
	Title          string  `json:"title"`
	Body           string  `json:"body"`
	Head           Ref     `json:"head"`
	Base           Ref     `json:"base"`
	MergeCommitSHA string  `json:"merge_commit_sha"`
	ChangedFiles   int     `json:"changed_files"`
	Additions      int     `json:"additions"`
	Deletions      int     `json:"deletions"`
	Mergeable      *bool   `json:"mergeable"`
	MergeableState string  `json:"mergeable_state"`
}

func (c *Client) Pull(ctx context.Context, repo string, n int) (Pull, error) {
	var p Pull
	e := c.json(ctx, "GET", fmt.Sprintf("%s/pulls/%d", base(repo), n), nil, &p)
	return p, e
}
func (c *Client) Files(ctx context.Context, repo string, n int) ([]policy.File, error) {
	return list[policy.File](ctx, c, fmt.Sprintf("%s/pulls/%d/files", base(repo), n))
}
func (c *Client) Diff(ctx context.Context, repo string, n int) (string, error) {
	b, e := c.request(ctx, "GET", fmt.Sprintf("%s/pulls/%d", base(repo), n), "application/vnd.github.diff", nil)
	return string(b), e
}
func (c *Client) Merge(ctx context.Context, repo string, n int, sha string) error {
	var result struct {
		Merged bool `json:"merged"`
	}
	e := c.json(ctx, "PUT", fmt.Sprintf("%s/pulls/%d/merge", base(repo), n), map[string]any{"sha": sha, "merge_method": "squash"}, &result)
	if e == nil && !result.Merged {
		return errors.New("GitHub did not merge PR")
	}
	return e
}
func (c *Client) Head(ctx context.Context, repo, branch string) (string, error) {
	var r struct {
		Object Ref `json:"object"`
	}
	e := c.json(ctx, "GET", base(repo)+"/git/ref/heads/"+url.PathEscape(branch), nil, &r)
	return r.Object.SHA, e
}
func (c *Client) CreateRef(ctx context.Context, repo, branch, sha string) error {
	return c.json(ctx, "POST", base(repo)+"/git/refs", map[string]string{"ref": "refs/heads/" + branch, "sha": sha}, nil)
}

type CI string

const (
	Green   CI = "green"
	Red     CI = "red"
	Pending CI = "pending"
)

type checkRun struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}
type status struct {
	ID      int64  `json:"id"`
	Context string `json:"context"`
	State   string `json:"state"`
}

func (c *Client) checkStates(ctx context.Context, repo, sha string) (map[string]CI, error) {
	states := map[string]CI{}
	ids := map[string]int64{}
	for page := 1; page <= 100; page++ {
		var runs struct {
			CheckRuns []checkRun `json:"check_runs"`
		}
		if e := c.json(ctx, "GET", fmt.Sprintf("%s/commits/%s/check-runs?filter=latest&per_page=100&page=%d", base(repo), sha, page), nil, &runs); e != nil {
			return nil, e
		}
		for _, r := range runs.CheckRuns {
			if r.ID < ids[r.Name] {
				continue
			}
			ids[r.Name] = r.ID
			state := Pending
			if r.Status == "completed" {
				switch r.Conclusion {
				case "success":
					state = Green
				case "failure", "timed_out", "action_required", "startup_failure", "stale":
					state = Red
				}
			}
			// Cancelled and skipped conclusions stay unknown rather than red:
			// they cannot mark a red boundary for culprit search or count as
			// confirmed-green evidence.
			states[r.Name] = state
		}
		if len(runs.CheckRuns) < 100 {
			break
		}
		if page == 100 {
			return nil, errors.New("check pagination limit")
		}
	}
	statuses, e := list[status](ctx, c, base(repo)+"/commits/"+sha+"/statuses")
	if e != nil {
		return nil, e
	}
	latest := map[string]status{}
	for _, s := range statuses {
		if latest[s.Context].ID < s.ID {
			latest[s.Context] = s
		}
	}
	for _, name := range slices.Sorted(maps.Keys(latest)) {
		s := latest[name]
		v := Pending
		switch s.State {
		case "success":
			v = Green
		case "failure", "error":
			v = Red
		}
		if old, ok := states[name]; ok && old != v {
			return nil, fmt.Errorf("ambiguous check and status named %q", name)
		}
		states[name] = v
	}
	return states, nil
}

func (c *Client) Check(ctx context.Context, repo, sha string, required []string) (CI, error) {
	if len(required) == 0 {
		return Pending, errors.New("no required checks configured")
	}
	states, e := c.checkStates(ctx, repo, sha)
	if e != nil {
		return Pending, e
	}
	result := Green
	for _, n := range required {
		v, ok := states[n]
		if ok && v == Red {
			return Red, nil
		}
		if !ok || v != Green {
			result = Pending
		}
	}
	return result, nil
}

// CheckStates resolves each required check name to green, red or pending.
// Missing or unknown evidence is pending, matching Check's aggregate result.
func (c *Client) CheckStates(ctx context.Context, repo, sha string, required []string) (map[string]CI, error) {
	states, e := c.checkStates(ctx, repo, sha)
	if e != nil {
		return nil, e
	}
	out := make(map[string]CI, len(required))
	for _, n := range required {
		v, ok := states[n]
		if !ok {
			v = Pending
		}
		out[n] = v
	}
	return out, nil
}

type Review struct {
	ID       int64  `json:"id"`
	Body     string `json:"body"`
	CommitID string `json:"commit_id"`
	State    string `json:"state"`
	User     struct {
		Login string `json:"login"`
	} `json:"user"`
}
type Finding struct {
	Severity string `json:"severity,omitempty"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Body     string `json:"body"`
	// Advisory is the reviewer's claim that the finding lies outside the PR's
	// Acceptance contract. It is honoured only when a contract was supplied.
	Advisory bool `json:"advisory,omitempty"`
}

// AdvisoryMarker tags a posted finding that lies outside the PR's Acceptance
// contract; ThreadSeverity grades such a finding as low, so it never holds a
// merge.
const AdvisoryMarker = "<!-- mergewake-advisory -->"

func Marker(sha string) string { return "<!-- mergewake-review:" + sha + " -->" }

// PassReceipt is the durable per-pass record embedded in each posted review.
type PassReceipt struct {
	Pass       int    `json:"pass"`
	Lens       string `json:"lens,omitempty"`
	Reviewer   string `json:"reviewer,omitempty"`
	Family     string `json:"family,omitempty"`
	SameFamily bool   `json:"same_family"`
	// AnsweredContext counts the already-answered findings the prompt carried,
	// so the effect of that context can be measured later.
	AnsweredContext int `json:"answered_context"`
	// Executor names a non-default pass executor ("runtug"); Fallback records
	// why a configured runtug pass ran headless instead.
	Executor string `json:"executor,omitempty"`
	Fallback string `json:"fallback,omitempty"`
	// Patch is the `git patch-id --stable` of the PR diff the pass reviewed. A
	// later head with the same patch inherits the pass.
	Patch string `json:"patch,omitempty"`
	// Round is the review round of this head: 1 for the first reviewed head,
	// then one more for each later head that carried changed lines. Delta names
	// the previously reviewed head whose interdiff this pass reviewed; empty
	// means the whole diff. Quiet marks a pass limited to critical and high
	// findings (the round budget was spent, or nothing changed).
	Round int    `json:"round,omitempty"`
	Delta string `json:"delta,omitempty"`
	Quiet bool   `json:"quiet,omitempty"`
}

// ReviewedPass is one posted pass together with the head it reviewed.
type ReviewedPass struct {
	Head string
	PassReceipt
}

func PassMarker(r PassReceipt) string {
	b, _ := json.Marshal(r)
	return "<!-- mergewake-pass:" + string(b) + " -->"
}

var passMarker = regexp.MustCompile(`<!-- mergewake-pass:(\{.*?\}) -->`)

// ReviewPasses lists the completed review passes recorded for an exact head,
// oldest first. Pass numbers come from the posted meta marker; legacy reviews
// without one count as sequential passes.
func (c *Client) ReviewPasses(ctx context.Context, repo string, n int, sha, login string) ([]PassReceipt, error) {
	history, e := c.ReviewHistory(ctx, repo, n, login)
	var out []PassReceipt
	for _, p := range history {
		if p.Head == sha {
			out = append(out, p.PassReceipt)
		}
	}
	return out, e
}

// ReviewHistory lists every completed pass the trusted review identity posted
// on the PR, across all heads, oldest first. A review counts only when its body
// carries the marker of the commit it was posted on.
func (c *Client) ReviewHistory(ctx context.Context, repo string, n int, login string) ([]ReviewedPass, error) {
	var out []ReviewedPass
	if login == "" {
		return out, errors.New("review.login is required")
	}
	reviews, e := list[Review](ctx, c, fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n))
	if e != nil {
		return out, e
	}
	next := map[string]int{}
	for _, r := range reviews {
		if r.User.Login != login || r.CommitID == "" || !strings.HasPrefix(r.Body, Marker(r.CommitID)+"\n") {
			continue
		}
		if r.State != "COMMENTED" && r.State != "APPROVED" && r.State != "CHANGES_REQUESTED" {
			continue
		}
		var receipt PassReceipt
		if m := passMarker.FindStringSubmatch(r.Body); len(m) == 2 {
			if e := json.Unmarshal([]byte(m[1]), &receipt); e != nil {
				return out, errors.New("unparseable mergewake pass marker")
			}
		}
		if receipt.Pass <= 0 {
			receipt.Pass = next[r.CommitID] + 1
		}
		next[r.CommitID] = receipt.Pass
		out = append(out, ReviewedPass{Head: r.CommitID, PassReceipt: receipt})
	}
	return out, nil
}
func (c *Client) Reviewed(ctx context.Context, repo string, n int, sha, login string) (bool, error) {
	passes, e := c.ReviewPasses(ctx, repo, n, sha, login)
	return len(passes) > 0, e
}

// ReviewedHead is one head of a PR that mergewake has posted a review on.
type ReviewedHead struct {
	SHA string
	// Round is the round recorded on the head's first pass; 0 for a review that
	// predates round counting.
	Round int
}

// ReviewedHeads lists the distinct heads login has reviewed on the PR, oldest
// first. A head counts from its first posted pass.
func (c *Client) ReviewedHeads(ctx context.Context, repo string, n int, login string) ([]ReviewedHead, error) {
	if login == "" {
		return nil, errors.New("review.login is required")
	}
	reviews, e := list[Review](ctx, c, fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n))
	if e != nil {
		return nil, e
	}
	var out []ReviewedHead
	seen := map[string]bool{}
	for _, r := range reviews {
		if r.User.Login != login || r.CommitID == "" || seen[r.CommitID] || !strings.HasPrefix(r.Body, Marker(r.CommitID)+"\n") {
			continue
		}
		if r.State != "COMMENTED" && r.State != "APPROVED" && r.State != "CHANGES_REQUESTED" {
			continue
		}
		seen[r.CommitID] = true
		head := ReviewedHead{SHA: r.CommitID}
		if m := passMarker.FindStringSubmatch(r.Body); len(m) == 2 {
			var receipt PassReceipt
			if json.Unmarshal([]byte(m[1]), &receipt) == nil {
				head.Round = receipt.Round
			}
		}
		out = append(out, head)
	}
	return out, nil
}

// CompareDiff returns the unified diff from the merge base of base and head to
// head: the PR-only patch head carries against base.
func (c *Client) CompareDiff(ctx context.Context, repo, baseRef, head string) (string, error) {
	b, e := c.request(ctx, "GET", base(repo)+"/compare/"+url.PathEscape(baseRef)+"..."+url.PathEscape(head), "application/vnd.github.diff", nil)
	return string(b), e
}

// CIStarted reports whether any check run or status exists for sha.
func (c *Client) CIStarted(ctx context.Context, repo, sha string) (bool, error) {
	states, e := c.checkStates(ctx, repo, sha)
	return len(states) > 0, e
}

func (c *Client) Unresolved(ctx context.Context, repo string, n int) (int, error) {
	parts := strings.Split(repo, "/")
	cursor := (*string)(nil)
	total := 0
	reviews, e := list[Review](ctx, c, fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n))
	if e != nil {
		return 0, e
	}
	latest := map[string]Review{}
	for _, r := range reviews {
		if (r.State == "APPROVED" || r.State == "CHANGES_REQUESTED" || r.State == "DISMISSED") && latest[r.User.Login].ID < r.ID {
			latest[r.User.Login] = r
		}
	}
	for _, r := range latest {
		if r.State == "CHANGES_REQUESTED" {
			total++
		}
	}

	for page := 0; page < 100; page++ {
		var result struct {
			Errors []json.RawMessage `json:"errors"`
			Data   *struct {
				Repository *struct {
					PullRequest *struct {
						ReviewDecision string `json:"reviewDecision"`
						ReviewThreads  *struct {
							Nodes []struct {
								IsResolved *bool `json:"isResolved"`
							} `json:"nodes"`
							PageInfo *struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
						} `json:"reviewThreads"`
					} `json:"pullRequest"`
				} `json:"repository"`
			} `json:"data"`
		}
		query := `query($owner:String!,$name:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewDecision reviewThreads(first:100,after:$cursor){nodes{isResolved} pageInfo{hasNextPage endCursor}}}}}`
		if e := c.json(ctx, "POST", "/graphql", map[string]any{"query": query, "variables": map[string]any{"owner": parts[0], "name": parts[1], "number": n, "cursor": cursor}}, &result); e != nil {
			return 0, e
		}
		if len(result.Errors) > 0 || result.Data == nil || result.Data.Repository == nil || result.Data.Repository.PullRequest == nil {
			return 0, errors.New("GitHub review resolution query failed")
		}
		pr := result.Data.Repository.PullRequest
		if page == 0 && total == 0 && pr.ReviewDecision == "CHANGES_REQUESTED" {
			total++
		}
		if pr.ReviewThreads == nil || pr.ReviewThreads.PageInfo == nil {
			return 0, errors.New("GitHub returned incomplete review threads")
		}
		for _, t := range pr.ReviewThreads.Nodes {
			if t.IsResolved == nil {
				return 0, errors.New("missing review resolution state")
			}
			if !*t.IsResolved {
				total++
			}
		}
		if !pr.ReviewThreads.PageInfo.HasNextPage {
			return total, nil
		}
		next := pr.ReviewThreads.PageInfo.EndCursor
		if next == "" {
			return 0, errors.New("missing review cursor")
		}
		cursor = &next
	}
	return 0, errors.New("review pagination limit")
}

// FindingBody is the comment text posted for f: the severity and advisory
// markers the merge gate reads, then the finding.
func FindingBody(f Finding) string {
	body := f.Body
	if f.Advisory {
		body = AdvisoryMarker + "\n**Advisory** (outside the PR's Acceptance contract; does not block).\n\n" + body
	}
	if f.Severity != "" {
		body = "<!-- mergewake-severity:" + f.Severity + " -->\n" + body
	}
	return body
}

func (c *Client) PostReview(ctx context.Context, repo string, n int, sha string, findings []Finding, meta PassReceipt) error {
	comments := []map[string]any{}
	for _, f := range findings {
		body := FindingBody(f)
		comments = append(comments, map[string]any{"path": f.Path, "line": f.Line, "side": "RIGHT", "body": body})
	}
	body := Marker(sha) + "\n" + PassMarker(meta) + "\nOne independent diff-only pass (lens: " + meta.Lens + "). Findings are advisory except under blocking policy."
	return c.json(ctx, "POST", fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n), map[string]any{"commit_id": sha, "body": body, "event": "COMMENT", "comments": comments}, nil)
}

type Commit struct {
	SHA     string `json:"sha"`
	Parents []Ref  `json:"parents"`
	Commit  struct {
		Tree    Ref    `json:"tree"`
		Message string `json:"message"`
		Author  struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"author"`
		Committer struct {
			Name  string    `json:"name"`
			Email string    `json:"email"`
			Date  time.Time `json:"date"`
		} `json:"committer"`
	} `json:"commit"`
}

// MergedPulls lists recently updated closed PRs newest-first and keeps those
// merged into branch within the window: the bound is merge recency, so a
// repo with hundreds of old merged PRs does not relist them every pass.
// Pages stop once an item's updated_at predates since; merged_at never
// postdates updated_at, so the page cut cannot drop a PR the window keeps.
// List responses omit detail fields like merge_commit_sha — callers needing
// them must follow up with Pull.
func (c *Client) MergedPulls(ctx context.Context, repo, branch string, since time.Time) ([]Pull, error) {
	var out []Pull
	for page := 1; page <= 10; page++ {
		var batch []Pull
		route := fmt.Sprintf("%s/pulls?state=closed&sort=updated&direction=desc&per_page=100&page=%d", base(repo), page)
		if e := c.json(ctx, "GET", route, nil, &batch); e != nil {
			return nil, e
		}
		stop := false
		for _, p := range batch {
			if t, err := time.Parse(time.RFC3339, p.UpdatedAt); err != nil || t.Before(since) {
				stop = true
			}
			recent := false
			if p.MergedAt != nil {
				if at, e := time.Parse(time.RFC3339, *p.MergedAt); e == nil && !at.Before(since) {
					recent = true
				}
			}
			if recent && p.Base.Ref == branch {
				out = append(out, p)
			}
		}
		if len(batch) < 100 || stop {
			return out, nil
		}
	}
	return out, nil
}

func (c *Client) Commit(ctx context.Context, repo, sha string) (Commit, error) {
	var v Commit
	e := c.json(ctx, "GET", base(repo)+"/commits/"+url.PathEscape(sha), nil, &v)
	return v, e
}
func (c *Client) PRsForCommit(ctx context.Context, repo, sha string) ([]Pull, error) {
	return list[Pull](ctx, c, base(repo)+"/commits/"+sha+"/pulls")
}
func (c *Client) Ancestor(ctx context.Context, repo, older, newer string) (bool, error) {
	var r struct {
		Status          string `json:"status"`
		MergeBaseCommit Ref    `json:"merge_base_commit"`
	}
	e := c.json(ctx, "GET", base(repo)+"/compare/"+url.PathEscape(older)+"..."+url.PathEscape(newer), nil, &r)
	return (r.Status == "ahead" || r.Status == "identical") && r.MergeBaseCommit.SHA == older, e
}

type TreeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

func (c *Client) Entry(ctx context.Context, repo, tree, name string) (TreeEntry, error) {
	if !manifest.SafePath(name) {
		return TreeEntry{}, errors.New("unsafe source path")
	}
	parts := strings.Split(name, "/")
	for i, p := range parts {
		var t struct {
			Tree      []TreeEntry `json:"tree"`
			Truncated bool        `json:"truncated"`
		}
		if e := c.json(ctx, "GET", base(repo)+"/git/trees/"+tree, nil, &t); e != nil {
			return TreeEntry{}, e
		}
		if t.Truncated {
			return TreeEntry{}, errors.New("truncated Git tree")
		}
		found := false
		for _, v := range t.Tree {
			if v.Path == p {
				found = true
				if i == len(parts)-1 {
					v.Path = name
					return v, nil
				}
				if v.Type != "tree" {
					return TreeEntry{}, errors.New("source path crosses non-directory")
				}
				tree = v.SHA
				break
			}
		}
		if !found {
			return TreeEntry{Path: name}, nil
		}
	}
	return TreeEntry{}, errors.New("missing source")
}
func (c *Client) ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
	commit, e := c.Commit(ctx, repo, sha)
	if e != nil {
		return nil, e
	}
	entry, e := c.Entry(ctx, repo, commit.Commit.Tree.SHA, name)
	if e != nil {
		return nil, e
	}
	if entry.SHA == "" {
		return nil, fmt.Errorf("source %s not found in %s@%s", name, repo, sha)
	}
	if entry.Type != "blob" || (entry.Mode != "100644" && entry.Mode != "100755") {
		return nil, fmt.Errorf("source %s in %s@%s is a %s (mode %s); must be a regular Git blob", name, repo, sha, entry.Type, entry.Mode)
	}
	var b struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
		Size     int64  `json:"size"`
	}
	if e = c.json(ctx, "GET", base(repo)+"/git/blobs/"+entry.SHA, nil, &b); e != nil {
		return nil, e
	}
	if b.Encoding != "base64" || b.Size > manifest.MaxFileSize {
		return nil, errors.New("unsupported or oversized Git blob")
	}
	data, e := base64.StdEncoding.DecodeString(strings.ReplaceAll(b.Content, "\n", ""))
	if e == nil && int64(len(data)) != b.Size {
		e = errors.New("Git blob size mismatch")
	}
	return data, e
}
