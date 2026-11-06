// Package github implements the GitHub API behind the pipeline's interfaces.
package github

import (
	"bytes"
	"cmp"
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
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/changestate"
	"github.com/mgh3326/mergewake/internal/github/webhookstate"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

type Client struct {
	Changes                   changestate.Writer
	ChangeLogin               string
	changeHeads               sync.Map
	mergedStateSkipped        atomic.Uint64
	changeJobsMu              sync.Mutex
	changeJobs                map[string]chan struct{}
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
	// Objects, when set, caches sha-addressed objects (commits, trees, blobs,
	// files read through a commit) across passes and processes. Nil means every
	// read goes to GitHub.
	Objects ObjectCache
	// HKTimeout bounds one hk review read; zero means the default.
	HKTimeout time.Duration
	hk        HKReviewSource
	hkPrimary bool
	hkCounts  hkReviewCounters
	// Webhook, when set, holds what verified deliveries carried. Every Pull,
	// head-check and review-history read compares it with the API (shadow) and
	// seeds it from the API; with ServeWebhookState on it answers instead.
	Webhook           *webhookstate.Store
	serveWebhookState bool
	app               *appAuth
	poll              pollingState
	evidence          evidenceCache
	writes            writeEpochs
}

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// ServeWebhookState is the phase switch of docs/webhook-state.md. Off (phase 1),
// stored webhook state is only compared with what the API says. On (phase 2),
// a Pull, head-check or review-history read that is not fresh is answered from
// the stored state when it holds a recent value, and goes to the API when it
// does not. Reads under FreshEvidence always go to the API.
const ServeWebhookState = false

func New(token string) *Client {
	return &Client{BaseURL: "https://api.github.com", UploadURL: "https://uploads.github.com", Token: token, HTTP: &http.Client{Timeout: 45 * time.Second}, serveWebhookState: ServeWebhookState}
}

type APIError struct {
	Code         int
	Method, Path string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("GitHub %s %s: HTTP %d", e.Method, e.Path, e.Code)
}
func NotFound(e error) bool { var a *APIError; return errors.As(e, &a) && a.Code == 404 }

// errAppDenied marks a read the App's installation token was refused (401 or
// 403). The repository now falls back to the user token, and request retries.
var errAppDenied = errors.New("GitHub App request refused")

func (c *Client) request(ctx context.Context, method, route, accept string, data []byte) ([]byte, error) {
	b, e := c.send(ctx, method, route, accept, data, c.credentialFor(ctx, method, route, data))
	if errors.Is(e, errAppDenied) {
		return c.send(ctx, method, route, accept, data, userCredential(c.Token))
	}
	return b, e
}

// graphql sends a GraphQL document about repo. The document decides the
// credential: a query reads with repo's installation token, a mutation with
// the user token.
func (c *Client) graphql(ctx context.Context, repo, query string, variables map[string]any, out any) error {
	return c.json(context.WithValue(ctx, graphqlRepoKey{}, repo), "POST", "/graphql", map[string]any{"query": query, "variables": variables}, out)
}

func (c *Client) send(ctx context.Context, method, route, accept string, data []byte, cred credential) ([]byte, error) {
	endpoint := c.BaseURL + route
	if strings.HasPrefix(route, "UPLOAD ") {
		endpoint = c.UploadURL + strings.TrimPrefix(route, "UPLOAD ")
	}
	// The credential kind is part of the key: a body or poll window recorded
	// under one credential is never replayed for, or revalidated by, the other.
	key := method + " " + endpoint + " " + accept + " " + cred.kind
	fresh := isFresh(ctx)
	if method != "GET" && route != "/graphql" {
		// Whatever the outcome, the write may have changed what any pass memoized.
		defer c.writes.bump(memoRepo(route))
		// Its delivery may not have arrived yet, so nothing stored answers a read.
		defer c.distrustWebhookState(memoRepo(route))
	}
	if method == "GET" && !fresh {
		if hit, e := c.pollShortcut(key); e != nil {
			return nil, e
		} else if hit.Replay && hit.Code == http.StatusNotFound {
			return nil, &APIError{http.StatusNotFound, method, strings.Split(route, "?")[0]}
		} else if hit.ETag != "" || hit.Replay {
			return append([]byte(nil), hit.Body...), nil
		}
	}
	release, cached, e := c.beginRequest(ctx, key, fresh, cred.id)
	if e != nil {
		return nil, e
	}
	defer release()
	req, e := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Accept", accept)
	if method == "GET" && cached.ETag != "" && ctx.Value(uncachedRequestKey{}) == nil {
		req.Header.Set("If-None-Match", cached.ETag)
	}
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	req.Header.Set("User-Agent", "mergewake")
	if cred.token != "" {
		req.Header.Set("Authorization", "Bearer "+cred.token)
	}
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := c.HTTP.Do(req)
	if e != nil {
		recordRequest(ctx, method, route, 0, cred.kind)
		return nil, fmt.Errorf("GitHub %s request failed", method)
	}
	defer resp.Body.Close()
	recordRequest(ctx, method, route, resp.StatusCode, cred.kind)
	if cred.kind == credApp && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
		// A refusal for a missing permission says nothing about the budget; one
		// from an exhausted budget or a secondary limit does, and is recorded.
		if resp.Header.Get("Retry-After") != "" || resp.Header.Get("X-RateLimit-Remaining") == "0" {
			c.observeHeaders(cred.id, resp.Header, resp.StatusCode, key, false)
		}
		c.app.refuse(c, cred, resp.StatusCode)
		return nil, errAppDenied
	}
	c.observeHeaders(cred.id, resp.Header, resp.StatusCode, key, method == "GET" && pollingEndpoint(route))
	if resp.StatusCode == http.StatusNotModified && method == "GET" {
		if cached.ETag == "" {
			return nil, errUncached304
		}
		probeOf(ctx).note(cmp.Or(resp.Header.Get("ETag"), cached.ETag))
		return append([]byte(nil), cached.Body...), nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if method == "GET" && resp.StatusCode == http.StatusNotFound && c.Polling {
			c.cacheNotFound(key)
		}
		return nil, &APIError{resp.StatusCode, method, strings.Split(route, "?")[0]}
	}
	mutation := method != "GET" && route != "/graphql"
	if mutation {
		parts := strings.Split(strings.TrimPrefix(route, "/"), "/")
		if len(parts) >= 3 && parts[0] == "repos" {
			c.InvalidateEvidence(parts[1] + "/" + parts[2])
		}
		c.cacheResponse(key, "", nil, true)
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, manifest.MaxFileSize*2+1))
	if len(b) > manifest.MaxFileSize*2 {
		return nil, errors.New("GitHub response too large")
	}
	if e == nil && !mutation {
		c.cacheResponse(key, resp.Header.Get("ETag"), b, false)
		if method == "GET" {
			probeOf(ctx).note(resp.Header.Get("ETag"))
		}
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
type PullLabel struct {
	Name string `json:"name"`
}
type Pull struct {
	Labels         []PullLabel `json:"labels"`
	CreatedAt      string      `json:"created_at"`
	UpdatedAt      string      `json:"updated_at"`
	ClosedAt       *string     `json:"closed_at"`
	HTMLURL        string      `json:"html_url"`
	MergedAt       *string     `json:"merged_at"`
	Number         int         `json:"number"`
	State          string      `json:"state"`
	Draft          bool        `json:"draft"`
	Merged         bool        `json:"merged"`
	Title          string      `json:"title"`
	Body           string      `json:"body"`
	Head           Ref         `json:"head"`
	Base           Ref         `json:"base"`
	User           PullUser    `json:"user"`
	MergeCommitSHA string      `json:"merge_commit_sha"`
	ChangedFiles   int         `json:"changed_files"`
	Additions      int         `json:"additions"`
	Deletions      int         `json:"deletions"`
	Mergeable      *bool       `json:"mergeable"`
	MergeableState string      `json:"mergeable_state"`
}

func (c *Client) Pull(ctx context.Context, repo string, n int) (Pull, error) {
	return memoized(ctx, c, memoKey{repo, "pull", strconv.Itoa(n)}, func() (Pull, error) {
		if p, ok := c.storedPull(ctx, repo, n); ok {
			return p, nil
		}
		var p Pull
		e := c.json(ctx, "GET", fmt.Sprintf("%s/pulls/%d", base(repo), n), nil, &p)
		if e == nil {
			c.shadowPull(repo, n, p)
			c.changeHeads.Store(repo+"\x00"+strconv.Itoa(n), p.Head.SHA)
			c.writePull(ctx, repo, p, time.Now().UTC(), changeSource(ctx), "")
		}
		return p, e
	})
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
func (c *Client) UpdateBranch(ctx context.Context, repo string, n int, head string) error {
	return c.json(ctx, "PUT", fmt.Sprintf("%s/pulls/%d/update-branch", base(repo), n), map[string]string{"expected_head_sha": head}, nil)
}
func (c *Client) Head(ctx context.Context, repo, branch string) (string, error) {
	return memoized(ctx, c, memoKey{repo, "head", branch}, func() (string, error) {
		var r struct {
			Object Ref `json:"object"`
		}
		e := c.json(ctx, "GET", base(repo)+"/git/ref/heads/"+url.PathEscape(branch), nil, &r)
		return r.Object.SHA, e
	})
}
func (c *Client) CreateRef(ctx context.Context, repo, branch, sha string) error {
	return c.json(ctx, "POST", base(repo)+"/git/refs", map[string]string{"ref": "refs/heads/" + branch, "sha": sha}, nil)
}

// DispatchWorkflow starts a workflow_dispatch run of the named workflow file
// on branch ref, passing inputs.
func (c *Client) DispatchWorkflow(ctx context.Context, repo, workflow, ref string, inputs map[string]string) error {
	return c.json(ctx, "POST", base(repo)+"/actions/workflows/"+url.PathEscape(workflow)+"/dispatches", map[string]any{"ref": ref, "inputs": inputs}, nil)
}

// DispatchRefused reports whether GitHub declined a workflow dispatch for a
// reason a retry cannot fix: missing scope (403), unknown workflow (404), or
// a workflow without a matching workflow_dispatch trigger (422).
func DispatchRefused(e error) bool {
	var a *APIError
	return errors.As(e, &a) && (a.Code == 403 || a.Code == 404 || a.Code == 422)
}

type CI string

const (
	Green   CI = "green"
	Red     CI = "red"
	Pending CI = "pending"
	// Cancelled is a completed run GitHub cancelled: it holds no verdict on the
	// head, and the gate waits for a rerun. Skipped covers skipped and neutral
	// runs, which are not evidence and hold nothing.
	Cancelled CI = "cancelled"
	Skipped   CI = "skipped"
	// Absent is a required name with no check run and no status on the head at
	// all: nothing has been queued, started or reported under it, unlike
	// Pending, where a run exists but has not finished.
	Absent CI = "absent"
)

type checkRun struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	// CompletedAt is when the run finished; the zero time means GitHub did not say.
	CompletedAt time.Time `json:"completed_at"`
	StartedAt   time.Time `json:"started_at"`
	DetailsURL  string    `json:"details_url"`
	CheckSuite  struct {
		ID int64 `json:"id"`
	} `json:"check_suite"`
	App struct {
		Slug string `json:"slug"`
	} `json:"app"`
}
type status struct {
	ID        int64     `json:"id"`
	Context   string    `json:"context"`
	State     string    `json:"state"`
	TargetURL string    `json:"target_url"`
	UpdatedAt time.Time `json:"updated_at"`
}

// checkEvidence is the per-name state of a head's checks plus, for each name
// whose latest run is cancelled, when that run completed.
type checkEvidence struct {
	states    map[string]CI
	cancelled map[string]time.Time
}

func (c *Client) checkStates(ctx context.Context, repo, sha string, preserveVerdicts bool) (map[string]CI, error) {
	ev, e := c.checkEvidence(ctx, repo, sha, preserveVerdicts)
	return ev.states, e
}

func (c *Client) checkEvidence(ctx context.Context, repo, sha string, preserveVerdicts bool) (checkEvidence, error) {
	var all []checkRun
	for page := 1; page <= 100; page++ {
		var runs struct {
			CheckRuns []checkRun `json:"check_runs"`
		}
		if e := c.json(ctx, "GET", fmt.Sprintf("%s/commits/%s/check-runs?filter=latest&per_page=100&page=%d", base(repo), sha, page), nil, &runs); e != nil {
			return checkEvidence{}, e
		}
		all = append(all, runs.CheckRuns...)
		if len(runs.CheckRuns) < 100 {
			break
		}
		if page == 100 {
			return checkEvidence{}, errors.New("check pagination limit")
		}
	}
	statuses, e := list[status](ctx, c, base(repo)+"/commits/"+sha+"/statuses")
	if e != nil {
		return checkEvidence{}, e
	}
	ev, e := foldChecks(all, statuses, preserveVerdicts)
	c.shadowChecks(repo, sha, all, statuses, preserveVerdicts, ev, e)
	if e == nil {
		c.writeChecks(ctx, repo, sha, all, statuses)
	}
	return ev, e
}

// foldChecks reduces a head's check runs and commit statuses to one state per
// check name. It is the one place that judges them, so a read from the API and
// a read from stored webhook state cannot disagree about how.
func foldChecks(runs []checkRun, statuses []status, preserveVerdicts bool) (checkEvidence, error) {
	states := map[string]CI{}
	cancelledAt := map[string]time.Time{}
	ids := map[string]int64{}
	ran := map[string]bool{}
	for _, r := range runs {
		// Two workflows can define a job with the same name (a CI "test" and a
		// release "test" that skips on PRs). A skipped or neutral run never
		// overrides a sibling that actually ran; it only counts when it is the
		// sole run of that name. Per-check gate evidence also preserves a
		// sibling verdict against a cancellation; aggregate Check retains its
		// original latest-run behavior.
		idle := r.Status == "completed" && (r.Conclusion == "skipped" || r.Conclusion == "neutral" || (preserveVerdicts && r.Conclusion == "cancelled"))
		// A skipped or neutral run holds no verdict either, so it never
		// replaces a cancellation: the cancelled run still has to be rerun.
		holdsNothing := r.Conclusion == "skipped" || r.Conclusion == "neutral"
		if idle && (ran[r.Name] || (holdsNothing && states[r.Name] == Cancelled)) {
			continue
		}
		if !idle && !ran[r.Name] {
			ran[r.Name] = true
			ids[r.Name] = 0
		}
		if r.ID < ids[r.Name] {
			continue
		}
		ids[r.Name] = r.ID
		state := Pending
		delete(cancelledAt, r.Name)
		if r.Status == "completed" {
			switch r.Conclusion {
			case "success":
				state = Green
			case "cancelled":
				state = Cancelled
				cancelledAt[r.Name] = r.CompletedAt
			case "skipped", "neutral":
				state = Skipped
			case "failure", "timed_out", "action_required", "startup_failure", "stale":
				state = Red
			}
		}
		// Check treats cancelled evidence as pending for aggregate consumers.
		states[r.Name] = state
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
		// An unknown value (pending, cancelled or skipped) never conflicts with
		// a decisive one of the same name: an any-ref status can settle a commit
		// whose own run was skipped or cancelled. Two decisive values that
		// disagree stay an error; two unknown values read as the status.
		if old, ok := states[name]; ok && old != v {
			decisive := func(x CI) bool { return x == Green || x == Red }
			if decisive(old) && decisive(v) {
				return checkEvidence{}, fmt.Errorf("ambiguous check and status named %q", name)
			}
			if decisive(old) {
				continue
			}
		}
		states[name] = v
		delete(cancelledAt, name)
	}
	return checkEvidence{states, cancelledAt}, nil
}

func (c *Client) Check(ctx context.Context, repo, sha string, required []string) (CI, error) {
	if len(required) == 0 {
		return Pending, errors.New("no required checks configured")
	}
	states, e := c.checkStates(ctx, repo, sha, false)
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

// CheckStates resolves each required name, distinguishing completed runs without
// a verdict from pending evidence. A name with no run or status on the head is
// Absent.
func (c *Client) CheckStates(ctx context.Context, repo, sha string, required []string) (map[string]CI, error) {
	ev, e := c.headChecks(context.WithValue(ctx, requiredChecksKey{}, required), repo, sha)
	if e != nil {
		return nil, e
	}
	out := make(map[string]CI, len(required))
	for _, n := range required {
		v, ok := ev.states[n]
		if !ok {
			v = Absent
		}
		out[n] = v
	}
	return out, nil
}

// CheckSnapshot is CheckStates plus, for each required name whose latest run
// on sha is cancelled, when that run completed (a zero time means GitHub gave
// none). Both come from one read, so a run that replaces a cancelled one
// cannot land between them.
func (c *Client) CheckSnapshot(ctx context.Context, repo, sha string, required []string) (map[string]CI, map[string]time.Time, error) {
	ev, e := c.headChecks(context.WithValue(ctx, requiredChecksKey{}, required), repo, sha)
	if e != nil {
		return nil, nil, e
	}
	states := make(map[string]CI, len(required))
	at := map[string]time.Time{}
	for _, n := range required {
		v, ok := ev.states[n]
		if !ok {
			v = Absent
		}
		states[n] = v
		if t, ok := ev.cancelled[n]; ok {
			at[n] = t
		}
	}
	return states, at, nil
}

func (c *Client) headChecks(ctx context.Context, repo, sha string) (checkEvidence, error) {
	if ev, ok := c.storedHeadChecks(ctx, repo, sha); ok {
		return ev, nil
	}
	return cachedEvidence(ctx, c, evidenceKey{repo, "checks", sha}, func() (checkEvidence, error) { return c.checkEvidence(freshRequest(ctx), repo, sha, true) })
}

type Review struct {
	ID          int64      `json:"id"`
	Body        string     `json:"body"`
	CommitID    string     `json:"commit_id"`
	SubmittedAt *time.Time `json:"submitted_at"`
	State       string     `json:"state"`
	User        struct {
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
	// Anchor says where the finding can be posted. The pipeline sets it after
	// checking the path and line against the diff; a reviewer cannot.
	Anchor Anchor `json:"-"`
}

// Anchor is the place on the PR a finding is posted.
type Anchor string

const (
	// AnchorLine is an inline comment: the line is a new-side line of the diff.
	AnchorLine Anchor = ""
	// AnchorFile is a file-level comment: the path is in the diff, the line is not.
	AnchorFile Anchor = "file"
	// AnchorOutside is a list in the review body: the path is not in the diff.
	AnchorOutside Anchor = "outside"
)

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
	// Executor names a non-default pass executor ("panewire"); Fallback records
	// why a configured panewire pass ran headless instead.
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
	// DiffSource names how the pass obtained a diff the GitHub API refused:
	// "local" is the checkout fallback built after HTTP 406. DiffSummarized
	// lists, one path per line, every file whose content that diff left out (a
	// pure rename, or a block elided for the reviewer budget), so no file is
	// silently dropped. A string keeps PassReceipt comparable.
	DiffSource     string `json:"diff_source,omitempty"`
	DiffSummarized string `json:"diff_summarized,omitempty"`
}

// ReviewedPass is one posted pass together with the head it reviewed.
type ReviewedPass struct {
	Head        string
	SubmittedAt *time.Time
	PassReceipt
	// OutsideBlocking counts the critical, high and blocking findings the pass
	// listed in its review body because their path is not in the diff.
	OutsideBlocking int
}

// OutsideMarker opens the line that records how many of a review's outside-diff
// findings are critical, high or blocking. The gate reads it from the trusted
// review identity's body; finding text is folded onto list lines, so it never
// starts a line with the marker.
const OutsideMarker = "<!-- mergewake-outside-diff:"

var outsideCount = regexp.MustCompile(`(?m)^<!-- mergewake-outside-diff:([0-9]+) -->$`)

// outsideBodyLimit bounds one finding's text in the review body, which GitHub
// caps at 65536 characters for the whole review.
const outsideBodyLimit = 600

func blockingFinding(severity string) bool {
	return severity == "critical" || severity == "high" || severity == "blocking"
}

// oneLine folds s onto one line of at most limit bytes.
func oneLine(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// OutsideBlockingCount is how many of findings are critical, high or blocking,
// not advisory, and outside the diff: the count the review body's marker
// carries for the gate.
func OutsideBlockingCount(findings []Finding) int {
	n := 0
	for _, f := range findings {
		if f.Anchor == AnchorOutside && blockingFinding(f.Severity) && !f.Advisory {
			n++
		}
	}
	return n
}

// OutsideDiffSection is the review-body list of findings whose path is not in
// the diff, with the marker that carries the gate's count. It is empty when
// there are none.
func OutsideDiffSection(findings []Finding) string {
	var lines []string
	blocking := OutsideBlockingCount(findings)
	for _, f := range findings {
		if f.Anchor != AnchorOutside {
			continue
		}
		severity := f.Severity
		if severity == "" {
			severity = "unspecified"
		}
		lines = append(lines, fmt.Sprintf("- `%s:%d` (%s): %s", oneLine(f.Path, 200), f.Line, severity, oneLine(f.Body, outsideBodyLimit)))
	}
	if len(lines) == 0 {
		return ""
	}
	return fmt.Sprintf("\n\nOutside the diff (these paths are not changed by this PR; critical and high hold the merge):\n%s\n%s%d -->", strings.Join(lines, "\n"), OutsideMarker, blocking)
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
	if login == "" {
		return nil, errors.New("review.login is required")
	}
	var history []ReviewedPass
	var e error
	if hk := c.hkPrimaryChange(ctx, repo, n); hasHead(hk.Reviews, sha) {
		history = hkPasses(hk.Reviews)
	} else {
		history, e = c.githubHistory(ctx, repo, n, login)
	}
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
	if login == "" {
		return nil, errors.New("review.login is required")
	}
	if hk := c.hkPrimaryChange(ctx, repo, n); hkServesHistory(hk) {
		return hkPasses(hk.Reviews), nil
	}
	return c.githubHistory(ctx, repo, n, login)
}

// githubHistory reads the history from GitHub; in phase 1 it also compares it
// with hk's record of the PR.
func (c *Client) githubHistory(ctx context.Context, repo string, n int, login string) ([]ReviewedPass, error) {
	out, e := memoized(ctx, c, memoKey{repo, "reviews", strconv.Itoa(n) + " " + login}, func() ([]ReviewedPass, error) {
		history, e := c.reviewHistory(ctx, repo, n, login)
		if e == nil && !c.hkPrimary && c.hk != nil {
			gh := make([]HKReview, len(history))
			for i, p := range history {
				gh[i] = ghAsHK(p)
			}
			c.compareHK(repo, n, c.hkChange(ctx, repo, n).Reviews, gh)
		}
		return history, e
	})
	// The memo's slice is shared: hand each caller its own.
	return slices.Clone(out), e
}

func (c *Client) reviewHistory(ctx context.Context, repo string, n int, login string) ([]ReviewedPass, error) {
	if reviews, ok := c.storedReviews(ctx, repo, n); ok {
		return passesFrom(reviews, login)
	}
	reviews, e := list[Review](ctx, c, fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n))
	if e != nil {
		return nil, e
	}
	c.shadowReviews(repo, n, reviews)
	return passesFrom(reviews, login)
}

// passesFrom lists the passes login posted among reviews, oldest first.
func passesFrom(reviews []Review, login string) ([]ReviewedPass, error) {
	var out []ReviewedPass
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
		pass := ReviewedPass{Head: r.CommitID, SubmittedAt: r.SubmittedAt, PassReceipt: receipt}
		// The marker is the body's last line; an earlier one is a file name
		// from the diff summary, not ours.
		if all := outsideCount.FindAllStringSubmatch(r.Body, -1); len(all) > 0 && strings.HasSuffix(r.Body, all[len(all)-1][0]) {
			pass.OutsideBlocking, _ = strconv.Atoi(all[len(all)-1][1])
		}
		out = append(out, pass)
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
	if hk := c.hkPrimaryChange(ctx, repo, n); hkServesHistory(hk) {
		return hkHeads(hk.Reviews), nil
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
	if !c.hkPrimary && c.hk != nil {
		c.compareHKHeads(repo, n, c.hkChange(ctx, repo, n).Reviews, out)
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
	states, e := c.checkStates(ctx, repo, sha, false)
	return len(states) > 0, e
}

func (c *Client) Unresolved(ctx context.Context, repo string, n int) (int, error) {
	return cachedEvidence(ctx, c, evidenceKey{repo, "unresolved", strconv.Itoa(n)}, func() (int, error) { return c.unresolved(freshRequest(ctx), repo, n) })
}
func (c *Client) unresolved(ctx context.Context, repo string, n int) (int, error) {
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
		if e := c.graphql(ctx, repo, query, map[string]any{"owner": parts[0], "name": parts[1], "number": n, "cursor": cursor}, &result); e != nil {
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
		switch f.Anchor {
		case AnchorOutside:
			continue
		case AnchorFile:
			// The line is not one the PR changed: GitHub takes a comment on the
			// file, which is still a thread the gate counts.
			comments = append(comments, map[string]any{"path": f.Path, "subject_type": "file", "body": fmt.Sprintf("%s\n\n(Line %d is not changed by this PR.)", body, f.Line)})
		default:
			comments = append(comments, map[string]any{"path": f.Path, "line": f.Line, "side": "RIGHT", "body": body})
		}
	}
	body := Marker(sha) + "\n" + PassMarker(meta) + "\nOne independent diff-only pass (lens: " + meta.Lens + "). Findings are advisory except under blocking policy."
	if meta.DiffSource != "" {
		body += "\n\nDiff source: " + meta.DiffSource + " checkout fallback; GitHub refused the PR diff, so the patch was built from the reviewed head and base locally."
	}
	if meta.DiffSummarized != "" {
		body += "\n\nListed without content (pure renames or over the local diff budget):\n" + meta.DiffSummarized
	}
	body += OutsideDiffSection(findings)
	err := c.json(ctx, "POST", fmt.Sprintf("%s/pulls/%d/reviews", base(repo), n), map[string]any{"commit_id": sha, "body": body, "event": "COMMENT", "comments": comments}, nil)
	if err == nil {
		c.writePostedFindings(repo, n, sha, findings, meta)
	}
	return err
}

// IssueComment is one comment on a PR's conversation.
type IssueComment struct {
	Body string   `json:"body"`
	User PullUser `json:"user"`
}

func (c *Client) IssueComments(ctx context.Context, repo string, n int) ([]IssueComment, error) {
	return list[IssueComment](ctx, c, fmt.Sprintf("%s/issues/%d/comments", base(repo), n))
}

func (c *Client) PostIssueComment(ctx context.Context, repo string, n int, body string) error {
	return c.json(ctx, "POST", fmt.Sprintf("%s/issues/%d/comments", base(repo), n), map[string]any{"body": body}, nil)
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
	return immutable[Commit](ctx, c, repo, kindCommit, sha, base(repo)+"/commits/"+url.PathEscape(sha))
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

type treeResponse struct {
	Tree      []TreeEntry `json:"tree"`
	Truncated bool        `json:"truncated"`
}
type blobResponse struct {
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
	Size     int64  `json:"size"`
}

func (c *Client) Entry(ctx context.Context, repo, tree, name string) (TreeEntry, error) {
	if !manifest.SafePath(name) {
		return TreeEntry{}, errors.New("unsafe source path")
	}
	parts := strings.Split(name, "/")
	for i, p := range parts {
		t, e := immutable[treeResponse](ctx, c, repo, kindTree, tree, base(repo)+"/git/trees/"+tree)
		if e != nil {
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
	if c.Objects != nil && cacheable(sha, name) {
		if body, ok := c.cacheGet(ctx, repo, kindFile, sha, name); ok {
			passOf(ctx).noteCache(true)
			return body, nil
		}
		passOf(ctx).noteCache(false)
	}
	data, e := c.readFile(ctx, repo, sha, name)
	if e == nil {
		c.cachePut(ctx, repo, kindFile, sha, name, data)
	}
	return data, e
}
func (c *Client) readFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
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
	b, e := immutable[blobResponse](ctx, c, repo, kindBlob, entry.SHA, base(repo)+"/git/blobs/"+entry.SHA)
	if e != nil {
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

// PullUser is a PR's author account; Type is "Bot" for app accounts such as
// Renovate.
type PullUser struct {
	Login string `json:"login"`
	Type  string `json:"type"`
}

// PRFresh is one GraphQL read of a PR's current state: its head, the check
// rollup of that head, the unresolved review threads and GitHub's mergeable
// state. It is evidence for a caller that does not trust the daemon's last
// pass; it never feeds the merge gate.
type PRFresh struct {
	Head string `json:"head"`
	// ChecksRollup is GitHub's status check rollup state for the head
	// (SUCCESS, FAILURE, PENDING, ERROR, EXPECTED), empty when none ran.
	ChecksRollup      string `json:"checks_rollup"`
	UnresolvedThreads int    `json:"unresolved_threads"`
	// ThreadsCapped is set when the PR has more threads than one query reads,
	// so UnresolvedThreads counts only the first 100.
	ThreadsCapped bool `json:"unresolved_threads_capped,omitempty"`
	// Mergeable is MERGEABLE, CONFLICTING or UNKNOWN; MergeState is GitHub's
	// merge state status (CLEAN, BEHIND, BLOCKED, DIRTY, ...).
	Mergeable  string    `json:"mergeable"`
	MergeState string    `json:"merge_state"`
	ObservedAt time.Time `json:"observed_at"`
}

// PRFresh makes exactly one GraphQL request, bypassing every cache.
func (c *Client) PRFresh(ctx context.Context, repo string, n int) (PRFresh, error) {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 {
		return PRFresh{}, errors.New("repo must be owner/name")
	}
	const query = `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){headRefOid mergeable mergeStateStatus reviewThreads(first:100){nodes{isResolved} pageInfo{hasNextPage}} commits(last:1){nodes{commit{statusCheckRollup{state}}}}}}}`
	var result struct {
		Errors []json.RawMessage `json:"errors"`
		Data   *struct {
			Repository *struct {
				PullRequest *struct {
					HeadRefOid       string `json:"headRefOid"`
					Mergeable        string `json:"mergeable"`
					MergeStateStatus string `json:"mergeStateStatus"`
					ReviewThreads    struct {
						Nodes []struct {
							IsResolved bool `json:"isResolved"`
						} `json:"nodes"`
						PageInfo struct {
							HasNextPage bool `json:"hasNextPage"`
						} `json:"pageInfo"`
					} `json:"reviewThreads"`
					Commits struct {
						Nodes []struct {
							Commit struct {
								StatusCheckRollup *struct {
									State string `json:"state"`
								} `json:"statusCheckRollup"`
							} `json:"commit"`
						} `json:"nodes"`
					} `json:"commits"`
				} `json:"pullRequest"`
			} `json:"repository"`
		} `json:"data"`
	}
	vars := map[string]any{"owner": parts[0], "name": parts[1], "number": n}
	if e := c.json(freshRequest(ctx), "POST", "/graphql", map[string]any{"query": query, "variables": vars}, &result); e != nil {
		return PRFresh{}, e
	}
	if len(result.Errors) > 0 || result.Data == nil || result.Data.Repository == nil || result.Data.Repository.PullRequest == nil {
		return PRFresh{}, errors.New("GitHub fresh status query failed")
	}
	pr := result.Data.Repository.PullRequest
	out := PRFresh{Head: pr.HeadRefOid, Mergeable: pr.Mergeable, MergeState: pr.MergeStateStatus, ThreadsCapped: pr.ReviewThreads.PageInfo.HasNextPage, ObservedAt: c.now().UTC()}
	for _, t := range pr.ReviewThreads.Nodes {
		if !t.IsResolved {
			out.UnresolvedThreads++
		}
	}
	if len(pr.Commits.Nodes) > 0 && pr.Commits.Nodes[0].Commit.StatusCheckRollup != nil {
		out.ChecksRollup = pr.Commits.Nodes[0].Commit.StatusCheckRollup.State
	}
	return out, nil
}
