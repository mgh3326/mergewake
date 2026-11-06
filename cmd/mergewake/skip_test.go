package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

const skipBase = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// fakePR is one PR of the fake GitHub: its head and the bodies GitHub serves
// for it. updatedAt stays fixed, as it does on GitHub when a check run
// completes or a review thread is resolved.
type fakePR struct {
	number    int
	head      string
	updatedAt string
	runs      []string // names of completed check runs
	reviews   int
}

// skipFixture is a fake GitHub that serves every GET with an ETag derived from
// its body and answers a matching If-None-Match with 304, plus the daemon-side
// pieces the skip rule reads.
type skipFixture struct {
	t     *testing.T
	mu    sync.Mutex
	prs   []*fakePR
	base  string
	log   []string // "METHOD path status"
	srv   *httptest.Server
	s     *pipeline.Service
	gh    *github.Client
	repo  config.Repo
	skips *prSkips
	dirty *daemon.Dirty
	clock time.Time
	out   *syncBuffer
	logs  *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *syncBuffer) take() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.b.String()
	b.b.Reset()
	return s
}

func headOf(n int) string { return strings.Repeat(fmt.Sprintf("%x", n), 40)[:40] }

func newSkipFixture(t *testing.T, numbers ...int) *skipFixture {
	t.Helper()
	f := &skipFixture{t: t, base: skipBase, clock: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC), out: &syncBuffer{}, logs: &syncBuffer{}}
	for _, n := range numbers {
		f.prs = append(f.prs, &fakePR{number: n, head: headOf(n), updatedAt: "2026-11-06T11:00:00Z", runs: []string{"CI"}})
	}
	policy := []byte("version: 1\nenabled: false\nrequired_checks: [CI]\n")
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		status := 200
		body, ok := f.route(r, policy)
		switch {
		case !ok:
			f.t.Errorf("unexpected GitHub route: %s %s", r.Method, r.URL.String())
			status = 500
			http.Error(w, "unexpected route", status)
		case r.Method == "GET":
			sum := sha256.Sum256([]byte(body))
			tag := `"` + hex.EncodeToString(sum[:8]) + `"`
			w.Header().Set("ETag", tag)
			if r.Header.Get("If-None-Match") == tag {
				status = 304
				w.WriteHeader(status)
			} else {
				fmt.Fprint(w, body)
			}
		default:
			fmt.Fprint(w, body)
		}
		f.log = append(f.log, fmt.Sprintf("%s %s %d", r.Method, r.URL.RequestURI(), status))
	}))
	t.Cleanup(f.srv.Close)
	f.repo = config.Repo{Name: "o/one", Branch: "main", Policy: "policy.yaml"}
	cfg := config.Config{StateDir: t.TempDir(), Repos: []config.Repo{f.repo}, WatchLimit: 10, Review: config.Review{Login: "reviewer", Timeout: "60s"}}
	f.gh = github.New("")
	f.gh.BaseURL = f.srv.URL
	f.gh.Polling = true
	f.gh.Stderr = io.Discard
	f.s = &pipeline.Service{Config: cfg, GH: f.gh, Runner: integrations.Exec{}, Sink: integrations.Noop{}, Notifier: integrations.Noop{}, Out: f.out}
	f.dirty = &daemon.Dirty{}
	f.skips = &prSkips{Dirty: f.dirty, Log: slog.New(slog.NewJSONHandler(f.logs, nil)), Now: func() time.Time { return f.clock }}
	return f
}

func (f *skipFixture) pr(n int) *fakePR {
	for _, p := range f.prs {
		if p.number == n {
			return p
		}
	}
	return nil
}

func (f *skipFixture) pullJSON(p *fakePR) string {
	return fmt.Sprintf(`{"number":%d,"state":"open","title":"change","body":"Mergewake-Author-Family: anthropic","updated_at":%q,"head":{"sha":%q,"ref":"b%d"},"base":{"ref":"main","sha":%q},"user":{"login":"author","type":"User"},"changed_files":1,"mergeable":true,"mergeable_state":"clean"}`, p.number, p.updatedAt, p.head, p.number, f.base)
}

// route returns the body for a request. Callers hold f.mu.
func (f *skipFixture) route(r *http.Request, policy []byte) (string, bool) {
	path := r.URL.Path
	seg := strings.Split(strings.Trim(path, "/"), "/")
	byHead := func(sha string) *fakePR {
		for _, p := range f.prs {
			if p.head == sha {
				return p
			}
		}
		return nil
	}
	switch {
	case path == "/graphql":
		sent, _ := io.ReadAll(r.Body)
		head := ""
		for _, p := range f.prs {
			if bytes.Contains(sent, []byte(fmt.Sprintf(`"number":%d`, p.number))) || bytes.Contains(sent, []byte(fmt.Sprintf("number:%d", p.number))) {
				head = p.head
			}
		}
		return `{"data":{"repository":{"pullRequest":{"author":{"login":"author","__typename":"User"},"headRefOid":"` + head + `","reviewDecision":"","reviews":{"nodes":[]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`, true
	case strings.HasSuffix(path, "/pulls"):
		if r.URL.Query().Get("state") == "closed" {
			return `[]`, true
		}
		var all []string
		for _, p := range f.prs {
			all = append(all, f.pullJSON(p))
		}
		return "[" + strings.Join(all, ",") + "]", true
	case len(seg) == 5 && seg[3] == "pulls" && f.pr(atoi(seg[4])) != nil:
		return f.pullJSON(f.pr(atoi(seg[4]))), true
	case len(seg) == 6 && seg[3] == "pulls" && seg[5] == "reviews" && f.pr(atoi(seg[4])) != nil:
		pr := f.pr(atoi(seg[4]))
		review := fmt.Sprintf(`[{"id":1,"commit_id":%q,"state":"COMMENTED","body":%q,"user":{"login":"reviewer"}}]`, pr.head, github.Marker(pr.head)+"\nno findings")
		return review + strings.Repeat(" ", pr.reviews), true
	case strings.HasSuffix(path, "/git/ref/heads/main"):
		return fmt.Sprintf(`{"object":{"sha":%q}}`, f.base), true
	case strings.Contains(path, "/git/ref/heads/mergewake/"):
		return `{}`, true
	case strings.HasSuffix(path, "/git/trees/tree"):
		return `{"tree":[{"path":"policy.yaml","mode":"100644","type":"blob","sha":"blob"}],"truncated":false}`, true
	case strings.HasSuffix(path, "/git/blobs/blob"):
		return fmt.Sprintf(`{"content":%q,"encoding":"base64","size":%d}`, base64.StdEncoding.EncodeToString(policy), len(policy)), true
	case strings.HasSuffix(path, "/actions/runs"):
		return `{"workflow_runs":[]}`, true
	case len(seg) == 6 && seg[3] == "commits" && seg[5] == "check-runs" && byHead(seg[4]) != nil:
		var runs []string
		for i, name := range byHead(seg[4]).runs {
			runs = append(runs, fmt.Sprintf(`{"id":%d,"name":%q,"status":"completed","conclusion":"success"}`, i+1, name))
		}
		return `{"check_runs":[` + strings.Join(runs, ",") + `]}`, true
	case strings.HasSuffix(path, "/files"):
		return `[{"filename":"docs/note.md","status":"modified","additions":1,"deletions":0}]`, true
	case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/comments"):
		return `[]`, true
	case strings.Contains(path, "/compare/"):
		return fmt.Sprintf(`{"status":"ahead","merge_base_commit":{"sha":%q}}`, f.base), true
	case len(seg) == 5 && seg[3] == "commits":
		return `{"commit":{"tree":{"sha":"tree"}}}`, true
	}
	return "", false
}

func atoi(s string) int {
	n := 0
	fmt.Sscan(s, &n)
	return n
}

// passResult is what one pr pass cost and decided.
type passResult struct {
	// charged lists every response that was not a 304, as "METHOD route status".
	charged []string
	// writes counts non-GET requests other than GraphQL reads.
	graphql, writes int
	// audit is what the pass printed (one JSON record per land decision).
	audit string
	// skipped and evaluated come from the pass log; reasons maps a PR to why it
	// was evaluated.
	skipped, evaluated int
	reasons            map[int][]string
}

func (f *skipFixture) pass() passResult {
	f.t.Helper()
	ctx, counts := github.CountRequests(context.Background(), "pr")
	if err := pollPRsSkipping(ctx, f.s, f.gh, f.repo, pipeline.Options{Shadow: true}, f.skips); err != nil {
		f.t.Fatalf("pass: %v", err)
	}
	var res passResult
	for _, c := range counts.Snapshot() {
		if c.Status == http.StatusNotModified {
			continue
		}
		res.charged = append(res.charged, fmt.Sprintf("%s %s %d ×%d", c.Method, c.Route, c.Status, c.Count))
		switch {
		case c.Route == "/graphql":
			res.graphql += c.Count
		case c.Method != "GET":
			res.writes += c.Count
		}
	}
	slices.Sort(res.charged)
	res.audit = f.out.take()
	res.reasons = map[int][]string{}
	for _, line := range strings.Split(f.logs.take(), "\n") {
		var rec struct {
			Msg       string
			Skipped   int      `json:"prs_skipped"`
			Evaluated int      `json:"prs_evaluated"`
			Detail    []string `json:"evaluated"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Msg != "pr pass" {
			continue
		}
		res.skipped, res.evaluated = rec.Skipped, rec.Evaluated
		for _, d := range rec.Detail {
			n, why, _ := strings.Cut(d, ":")
			res.reasons[atoi(n)] = strings.Split(why, "+")
		}
	}
	return res
}

func (r passResult) String() string {
	return fmt.Sprintf("skipped=%d evaluated=%d reasons=%v graphql=%d charged=%v", r.skipped, r.evaluated, r.reasons, r.graphql, r.charged)
}

func hasReason(r passResult, n int, reason string) bool { return slices.Contains(r.reasons[n], reason) }

// (a) Nothing changed: the second pass skips the PR. The only GitHub traffic is
// 304 revalidation: no charged request, no GraphQL, no write, and nothing the
// land decision would print.
func TestUnchangedPRIsSkippedOnTheNextPass(t *testing.T) {
	f := newSkipFixture(t, 1)
	first := f.pass()
	t.Logf("first:  %s", first)
	if first.evaluated != 1 || !hasReason(first, 1, "first") || first.audit == "" || first.graphql == 0 {
		t.Fatalf("the first pass must evaluate the PR in full: %s", first)
	}
	second := f.pass()
	t.Logf("second: %s", second)
	if second.skipped != 1 || second.evaluated != 0 {
		t.Fatalf("PR was not skipped: %s", second)
	}
	if len(second.charged) != 0 || second.graphql != 0 || second.writes != 0 {
		t.Fatalf("a skipped PR cost a charged request: %s", second)
	}
	if second.audit != "" {
		t.Fatalf("a skipped PR printed a decision: %s", second.audit)
	}
}

// (b) A check run completes: its ETag changes while the PR, its updated_at and
// its review list do not. The PR is evaluated.
func TestCheckRunCompletionEvaluatesThePR(t *testing.T) {
	f := newSkipFixture(t, 1)
	f.pass()
	f.pr(1).runs = append(f.pr(1).runs, "lint")
	res := f.pass()
	t.Logf("%s", res)
	if res.evaluated != 1 || !hasReason(res, 1, "checks") || hasReason(res, 1, "pr") || hasReason(res, 1, "reviews") {
		t.Fatalf("a completed check run did not evaluate the PR, or blamed the wrong read: %s", res)
	}
	if res.audit == "" || res.graphql == 0 {
		t.Fatalf("the PR was marked evaluated but not decided again: %s", res)
	}
}

// (c) A review thread is resolved: GitHub shows nothing in REST, only the
// webhook names the PR. Every REST read answers 304; the PR is evaluated.
func TestResolvedThreadWebhookEvaluatesThePR(t *testing.T) {
	f := newSkipFixture(t, 1, 2)
	f.pass()
	f.dirty.Mark("o/one", 1, "")
	res := f.pass()
	t.Logf("%s", res)
	if res.evaluated != 1 || res.skipped != 1 || !hasReason(res, 1, "webhook") || len(res.reasons[1]) != 1 {
		t.Fatalf("the webhook alone must evaluate PR 1 and skip PR 2: %s", res)
	}
	if res.graphql == 0 || res.audit == "" {
		t.Fatalf("PR 1 was not decided again: %s", res)
	}
	// The mark is spent: the next quiet pass skips both.
	if next := f.pass(); next.skipped != 2 {
		t.Fatalf("the evaluation did not clear the mark: %s", next)
	}
}

// A webhook that lands while the PR is being evaluated is not lost.
func TestWebhookDuringEvaluationKeepsThePRDirty(t *testing.T) {
	f := newSkipFixture(t, 1)
	plan := f.skips.plan(context.Background(), f.gh, "o/one", 1, skipBase, nil)
	f.skips.record("o/one", 1, plan, true)
	f.dirty.Mark("o/one", 0, headOf(1))
	if p := f.skips.plan(context.Background(), f.gh, "o/one", 1, skipBase, nil); p.skip || !slices.Contains(p.reasons, "webhook") {
		t.Fatalf("a mark newer than the evaluation start was ignored: %+v", p)
	}
}

// (e) Everything answers 304 but the last evaluation is an hour old: evaluate.
func TestEvaluationOlderThanSixtyMinutesIsNotSkipped(t *testing.T) {
	f := newSkipFixture(t, 1)
	f.pass()
	f.clock = f.clock.Add(skipMaxAge - time.Second)
	if res := f.pass(); res.skipped != 1 {
		t.Fatalf("a 59-minute-old evaluation must still skip: %s", res)
	}
	f.clock = f.clock.Add(time.Second)
	res := f.pass()
	t.Logf("%s", res)
	if res.evaluated != 1 || !hasReason(res, 1, "age") || len(res.reasons[1]) != 1 {
		t.Fatalf("an hour-old evaluation was skipped: %s", res)
	}
	// The age runs from the last full evaluation, not from a skip.
	if next := f.pass(); next.skipped != 1 {
		t.Fatalf("a fresh evaluation did not skip: %s", next)
	}
}

// (f) A new head evaluates the PR.
func TestNewHeadEvaluatesThePR(t *testing.T) {
	f := newSkipFixture(t, 1)
	f.pass()
	f.pr(1).head = "dddddddddddddddddddddddddddddddddddddddd"
	res := f.pass()
	t.Logf("%s", res)
	if res.evaluated != 1 || !hasReason(res, 1, "head") {
		t.Fatalf("a new head did not evaluate the PR: %s", res)
	}
}

// Each ETag the rule reads decides on its own, and so does the base branch.
func TestEachChangedReadEvaluatesThePR(t *testing.T) {
	for _, c := range []struct {
		name, reason string
		change       func(*skipFixture)
	}{
		{"pr", "pr", func(f *skipFixture) { f.pr(1).updatedAt = "2026-11-06T11:30:00Z" }},
		{"reviews", "reviews", func(f *skipFixture) { f.pr(1).reviews++ }},
		{"base", "base", func(f *skipFixture) { f.base = strings.Repeat("e", 40) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newSkipFixture(t, 1)
			f.pass()
			c.change(f)
			if res := f.pass(); res.evaluated != 1 || !hasReason(res, 1, c.reason) {
				t.Fatalf("%s", res)
			}
		})
	}
}

// A PR whose evaluation failed is evaluated again, even if nothing changed.
func TestFailedEvaluationIsNotRemembered(t *testing.T) {
	f := newSkipFixture(t, 1)
	plan := f.skips.plan(context.Background(), f.gh, "o/one", 1, skipBase, nil)
	f.skips.record("o/one", 1, plan, false)
	if p := f.skips.plan(context.Background(), f.gh, "o/one", 1, skipBase, nil); p.skip || !slices.Contains(p.reasons, "first") {
		t.Fatalf("an unsettled evaluation was remembered: %+v", p)
	}
}

// Without a webhook receiver there is no skipping at all.
func TestNoSkipsWithoutWebhookReceiver(t *testing.T) {
	f := newSkipFixture(t, 1)
	f.skips = nil
	f.pass()
	if res := f.pass(); res.graphql == 0 || res.audit == "" {
		t.Fatalf("a daemon without webhooks skipped a PR: %s", res)
	}
}
