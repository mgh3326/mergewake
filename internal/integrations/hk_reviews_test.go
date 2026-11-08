package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

const (
	hkRepo  = "example/project"
	hkPRNum = 7
	hkHeadA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hkHeadB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hkLogin = "review-bot"
)

type hkPass struct {
	head   string
	pass   int
	lens   string
	round  int
	family string
}

// fakeHK serves the change contract (`GET /v1/changes?pr=`) the reader calls.
type fakeHK struct {
	mu      sync.Mutex
	status  int
	body    string
	hang    chan struct{}
	queries []string
}

func (f *fakeHK) serve(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.queries = append(f.queries, r.URL.Path+"?pr="+r.URL.Query().Get("pr"))
		f.mu.Unlock()
		if f.hang != nil {
			select {
			case <-f.hang:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(f.status)
		w.Write([]byte(f.body))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
}

func hkChangeBody(head string, passes ...hkPass) string {
	var reviews []map[string]any
	for _, p := range passes {
		reviews = append(reviews, map[string]any{"reviewed_head": p.head, "round": p.round, "pass": p.pass, "lens": p.lens, "family": p.family, "critical": 0})
	}
	b, _ := json.Marshal(map[string]any{"changes": []any{map[string]any{
		"pr": fmt.Sprintf("https://github.com/%s/pull/%d", hkRepo, hkPRNum), "head_sha": head, "reviews": reviews,
	}}})
	return string(b)
}

func newFakeHK(t *testing.T, head string, passes ...hkPass) *fakeHK {
	f := &fakeHK{status: 200, body: hkChangeBody(head, passes...)}
	f.serve(t)
	return f
}

// fakeGitHub serves the reviews mergewake posted and counts the list requests.
type fakeGitHub struct {
	mu      sync.Mutex
	reviews int
	posted  []hkPass
}

func (g *fakeGitHub) client(t *testing.T) (*github.Client, *strings.Builder) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/repos/%s/pulls/%d/reviews", hkRepo, hkPRNum) {
			http.NotFound(w, r)
			return
		}
		g.mu.Lock()
		g.reviews++
		g.mu.Unlock()
		var out []github.Review
		for i, p := range g.posted {
			rv := github.Review{ID: int64(i + 1), CommitID: p.head, State: "COMMENTED"}
			rv.User.Login = hkLogin
			rv.Body = github.Marker(p.head) + "\n" + github.PassMarker(github.PassReceipt{Pass: p.pass, Lens: p.lens, Family: p.family, Round: p.round})
			out = append(out, rv)
		}
		json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	logs := &strings.Builder{}
	return &github.Client{BaseURL: srv.URL, HTTP: srv.Client(), Stderr: logs}, logs
}

func (g *fakeGitHub) requests() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reviews
}

// hkLines are the hk_review_history log lines the client wrote.
func hkLines(logs *strings.Builder) []map[string]any {
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "hk_review_history" {
			out = append(out, m)
		}
	}
	return out
}

var twoPasses = []hkPass{
	{hkHeadA, 1, "bugs", 1, "openai"},
	{hkHeadA, 2, "design", 1, "anthropic"},
}

func historyOf(t *testing.T, c *github.Client) []github.ReviewedPass {
	t.Helper()
	h, e := c.ReviewHistory(github.NewPass(context.Background()), hkRepo, hkPRNum, hkLogin)
	if e != nil {
		t.Fatalf("ReviewHistory: %v", e)
	}
	return h
}

// (a) hk holds the same passes as GitHub: no mismatch line, one hit.
func TestHKReviewHistoryShadowSamePassesNoMismatch(t *testing.T) {
	newFakeHK(t, hkHeadA, twoPasses...)
	gh := &fakeGitHub{posted: twoPasses}
	c, logs := gh.client(t)
	AttachReviewHistory(c, Handoffkeep{})
	h := historyOf(t, c)
	if len(h) != 2 {
		t.Fatalf("history %d passes, want 2", len(h))
	}
	if lines := hkLines(logs); len(lines) != 0 {
		t.Fatalf("mismatch lines for identical passes: %v", lines)
	}
	if s := c.HKReviewStats(); s != (github.HKReviewStats{Hits: 1}) {
		t.Fatalf("stats %+v, want one hit", s)
	}
	heads, e := c.ReviewedHeads(context.Background(), hkRepo, hkPRNum, hkLogin)
	if e != nil || len(heads) != 1 || heads[0].SHA != hkHeadA || len(hkLines(logs)) != 0 {
		t.Fatalf("heads %v %v, lines %v", heads, e, hkLines(logs))
	}
}

// (b) hk lacks a pass GitHub has: one mismatch line, the gate's answer is still
// GitHub's.
func TestHKReviewHistoryShadowMissingPassOneMismatchGateUnchanged(t *testing.T) {
	newFakeHK(t, hkHeadA, twoPasses[0])
	gh := &fakeGitHub{posted: twoPasses}
	c, logs := gh.client(t)
	AttachReviewHistory(c, Handoffkeep{})
	h := historyOf(t, c)
	if len(h) != 2 || h[1].Pass != 2 || h[1].Lens != "design" {
		t.Fatalf("gate saw %+v, want GitHub's two passes", h)
	}
	lines := hkLines(logs)
	if len(lines) != 1 {
		t.Fatalf("mismatch lines %v, want 1", lines)
	}
	want := map[string]any{"pr": float64(hkPRNum), "head": hkHeadA, "pass": float64(2), "lens": "design", "field": "presence", "hk": "absent", "github": "present"}
	for k, v := range want {
		if lines[0][k] != v {
			t.Fatalf("line %v: %s = %v, want %v", lines[0], k, lines[0][k], v)
		}
	}
	if s := c.HKReviewStats(); s.Hits != 1 || s.Mismatches != 1 {
		t.Fatalf("stats %+v", s)
	}
}

// A differing field is one line naming both values.
func TestHKReviewHistoryShadowFieldMismatch(t *testing.T) {
	newFakeHK(t, hkHeadA, hkPass{hkHeadA, 1, "bugs", 2, "openai"}, twoPasses[1])
	c, logs := (&fakeGitHub{posted: twoPasses}).client(t)
	AttachReviewHistory(c, Handoffkeep{})
	historyOf(t, c)
	lines := hkLines(logs)
	if len(lines) != 1 || lines[0]["field"] != "round" || lines[0]["hk"] != "2" || lines[0]["github"] != "1" {
		t.Fatalf("lines %v", lines)
	}
}

// (c) No hk data is not an error and not a log line: GitHub answers.
func TestHKReviewHistoryNoHKData(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"404 (hk without the endpoint)": {404, `not found`},
		"no change for the PR":          {200, `{"changes":[]}`},
		"change without reviews":        {200, hkChangeBody(hkHeadA)},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeHK{status: tc.status, body: tc.body}
			f.serve(t)
			gh := &fakeGitHub{posted: twoPasses}
			c, logs := gh.client(t)
			AttachReviewHistory(c, Handoffkeep{})
			if h := historyOf(t, c); len(h) != 2 || gh.requests() != 1 {
				t.Fatalf("history %d passes, %d GitHub requests", len(h), gh.requests())
			}
			if lines := hkLines(logs); len(lines) != 0 {
				t.Fatalf("log lines for no hk data: %v", lines)
			}
			if s := c.HKReviewStats(); s != (github.HKReviewStats{Misses: 1}) {
				t.Fatalf("stats %+v, want one miss", s)
			}
			if len(f.queries) != 1 || f.queries[0] != "/v1/changes?pr=https://github.com/example/project/pull/7" {
				t.Fatalf("hk queries %v", f.queries)
			}
		})
	}
}

// No hk endpoint configured on the host is also "no hk data".
func TestHKReviewHistoryUnconfiguredEndpoint(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	gh := &fakeGitHub{posted: twoPasses}
	c, logs := gh.client(t)
	AttachReviewHistory(c, Handoffkeep{})
	if h := historyOf(t, c); len(h) != 2 || len(hkLines(logs)) != 0 {
		t.Fatalf("history %d, lines %v", len(h), hkLines(logs))
	}
}

// (d) A slow hk costs one log line, not the pass.
func TestHKReviewHistoryTimeoutDoesNotFailPass(t *testing.T) {
	f := &fakeHK{hang: make(chan struct{})}
	f.serve(t)
	t.Cleanup(func() { close(f.hang) })
	gh := &fakeGitHub{posted: twoPasses}
	c, logs := gh.client(t)
	c.HKTimeout = 50 * time.Millisecond
	AttachReviewHistory(c, Handoffkeep{})
	start := time.Now()
	h := historyOf(t, c)
	if len(h) != 2 || gh.requests() != 1 {
		t.Fatalf("history %d passes, %d GitHub requests", len(h), gh.requests())
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("pass waited %v on hk", time.Since(start))
	}
	lines := hkLines(logs)
	if len(lines) != 1 || lines[0]["error"] == nil {
		t.Fatalf("log lines %v, want one error line", lines)
	}
	if s := c.HKReviewStats(); s != (github.HKReviewStats{Misses: 1}) {
		t.Fatalf("stats %+v", s)
	}
}

// An hk that answers 500 is the same: one line, GitHub used.
func TestHKReviewHistoryServerErrorDoesNotFailPass(t *testing.T) {
	(&fakeHK{status: 500, body: "boom"}).serve(t)
	gh := &fakeGitHub{posted: twoPasses}
	c, logs := gh.client(t)
	AttachReviewHistory(c, Handoffkeep{})
	if h := historyOf(t, c); len(h) != 2 || len(hkLines(logs)) != 1 {
		t.Fatalf("history %d, lines %v", len(h), hkLines(logs))
	}
}

// (e) Phase 2, enabled here only: hk holding the head serves without listing
// GitHub's reviews; hk lacking the head reads GitHub.
func TestHKReviewHistoryPhase2(t *testing.T) {
	t.Run("hk has the head", func(t *testing.T) {
		ctx := github.NewPass(context.Background())
		newFakeHK(t, hkHeadA, twoPasses...)
		gh := &fakeGitHub{posted: twoPasses}
		c, logs := gh.client(t)
		c.UseHK(Handoffkeep{}, true)
		h, e := c.ReviewHistory(ctx, hkRepo, hkPRNum, hkLogin)
		passes, e2 := c.ReviewPasses(ctx, hkRepo, hkPRNum, hkHeadA, hkLogin)
		heads, e3 := c.ReviewedHeads(ctx, hkRepo, hkPRNum, hkLogin)
		if e != nil || e2 != nil || e3 != nil || len(h) != 2 || len(passes) != 2 || len(heads) != 1 || heads[0].SHA != hkHeadA {
			t.Fatalf("history %v %v, passes %v %v, heads %v %v", h, e, passes, e2, heads, e3)
		}
		if h[0].Round != 1 || h[1].Lens != "design" || h[1].Family != "anthropic" {
			t.Fatalf("hk passes lost fields: %+v", h)
		}
		if gh.requests() != 0 || len(hkLines(logs)) != 0 {
			t.Fatalf("%d GitHub review requests, lines %v", gh.requests(), hkLines(logs))
		}
	})
	t.Run("hk lacks the head", func(t *testing.T) {
		ctx := github.NewPass(context.Background())
		newFakeHK(t, hkHeadB, hkPass{hkHeadB, 1, "bugs", 2, "openai"})
		gh := &fakeGitHub{posted: twoPasses}
		c, _ := gh.client(t)
		c.UseHK(Handoffkeep{}, true)
		passes, e := c.ReviewPasses(ctx, hkRepo, hkPRNum, hkHeadA, hkLogin)
		if e != nil || len(passes) != 2 || gh.requests() != 1 {
			t.Fatalf("passes %v %v, %d GitHub requests", passes, e, gh.requests())
		}
	})
	t.Run("hk has no change", func(t *testing.T) {
		ctx := github.NewPass(context.Background())
		(&fakeHK{status: 404, body: "x"}).serve(t)
		gh := &fakeGitHub{posted: twoPasses}
		c, _ := gh.client(t)
		c.UseHK(Handoffkeep{}, true)
		h, e := c.ReviewHistory(ctx, hkRepo, hkPRNum, hkLogin)
		heads, e2 := c.ReviewedHeads(ctx, hkRepo, hkPRNum, hkLogin)
		if e != nil || e2 != nil || len(h) != 2 || len(heads) != 1 || gh.requests() != 2 {
			t.Fatalf("history %v %v heads %v %v, %d requests", h, e, heads, e2, gh.requests())
		}
	})
	t.Run("hk down", func(t *testing.T) {
		ctx := github.NewPass(context.Background())
		(&fakeHK{status: 500, body: "x"}).serve(t)
		gh := &fakeGitHub{posted: twoPasses}
		c, _ := gh.client(t)
		c.UseHK(Handoffkeep{}, true)
		if h, e := c.ReviewHistory(ctx, hkRepo, hkPRNum, hkLogin); e != nil || len(h) != 2 || gh.requests() != 1 {
			t.Fatalf("history %v %v, %d requests", h, e, gh.requests())
		}
	})
}

// (f) The shipped default is phase 1: GitHub is listed even when hk has the
// head, and only a handoffkeep sink gets hk attached.
func TestHKReviewHistoryShippedDefaultIsShadow(t *testing.T) {
	if github.HKReviewsPrimary {
		t.Fatal("phase 2 is on by default")
	}
	newFakeHK(t, hkHeadA, twoPasses...)
	gh := &fakeGitHub{posted: twoPasses}
	c, _ := gh.client(t)
	AttachReviewHistory(c, Handoffkeep{})
	historyOf(t, c)
	if gh.requests() != 1 || c.HKReviewStats().Hits != 1 {
		t.Fatalf("%d GitHub requests, stats %+v: want GitHub read and hk compared", gh.requests(), c.HKReviewStats())
	}

	noHK, _ := (&fakeGitHub{posted: twoPasses}).client(t)
	AttachReviewHistory(noHK, Noop{})
	historyOf(t, noHK)
	if s := noHK.HKReviewStats(); s != (github.HKReviewStats{}) {
		t.Fatalf("hk read without a handoffkeep sink: %+v", s)
	}
}
