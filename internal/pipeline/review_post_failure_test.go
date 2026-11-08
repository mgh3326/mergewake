package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// workflowDiff has the shape of the diff in the evidence: one workflow file
// whose hunks cover new-file lines 16-46 and 70-77, so line 62 is not in any.
func workflowDiff() string {
	hunk := func(start, n int) string {
		var b strings.Builder
		fmt.Fprintf(&b, "@@ -%d,%d +%d,%d @@\n", start, n, start, n)
		for i := 0; i < n; i++ {
			b.WriteString("+line\n")
		}
		return b.String()
	}
	return "diff --git a/.github/workflows/main.yml b/.github/workflows/main.yml\n--- a/.github/workflows/main.yml\n+++ b/.github/workflows/main.yml\n" + hunk(16, 31) + hunk(70, 8)
}

const stateRiskFinding = `{"findings":[{"path":".github/workflows/main.yml","line":62,"body":"the deploy job runs before its guard","severity":"high"}]}`

// githubShapedGH posts through the real GitHub client against a server that
// holds a create-review request to the documented contract: a review comment
// carries a path, a body and a line or position. The comment fields the
// endpoint does not list, such as subject_type, are not honored, so a comment
// without a line is refused with 422.
type githubShapedGH struct {
	*fakeGH
	client *github.Client
	mu     sync.Mutex
	posted []map[string]any
	refuse []string
}

func newGitHubShapedGH(t *testing.T, g *fakeGH) *githubShapedGH {
	t.Helper()
	x := &githubShapedGH{fakeGH: g}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.mu.Lock()
		defer x.mu.Unlock()
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var review map[string]any
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
			t.Error(err)
		}
		comments, _ := review["comments"].([]any)
		for _, c := range comments {
			m := c.(map[string]any)
			if m["line"] == nil && m["position"] == nil {
				x.refuse = append(x.refuse, fmt.Sprint(m["path"]))
				w.WriteHeader(http.StatusUnprocessableEntity)
				fmt.Fprint(w, `{"message":"Unprocessable Entity"}`)
				return
			}
		}
		x.posted = append(x.posted, review)
		fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(server.Close)
	x.client = github.New("placeholder")
	x.client.BaseURL, x.client.UploadURL, x.client.HTTP = server.URL, server.URL, server.Client()
	return x
}

func (x *githubShapedGH) PostReview(ctx context.Context, repo string, n int, sha string, f []github.Finding, meta github.PassReceipt) error {
	if err := x.client.PostReview(ctx, repo, n, sha, f, meta); err != nil {
		return err
	}
	return x.fakeGH.PostReview(ctx, repo, n, sha, f, meta)
}

// twoPassRound is the evidence round: a blocking change, two passes, the
// correctness pass clean and the state-risk pass finding a line outside the
// hunks of a file the PR changes.
func twoPassRound(t *testing.T) (*Service, *fakeGH, *githubShapedGH, config.Repo) {
	t.Helper()
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", byLabel: map[string]string{"-p2-": stateRiskFinding}}
	s, g, r := panewireSetup(t, runner)
	g.blocking = true
	g.diffs = map[string]string{shaA: workflowDiff()}
	gh := newGitHubShapedGH(t, g)
	s.GH = gh
	return s, g, gh, r
}

// (a) A finding on a line outside the hunks of a changed file is listed in the
// review body, as one on a path the PR does not touch is: both passes post, and
// the count the gate reads carries it.
func TestReviewPostsPassWhoseFindingIsOutsideTheHunks(t *testing.T) {
	s, g, gh, r := twoPassRound(t)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("review: %v (refused comments for %v)", err, gh.refuse)
	}
	if len(gh.posted) != 2 || len(g.passes[shaA]) != 2 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("posted=%d passes=%v done=%v", len(gh.posted), g.passes[shaA], g.refs)
	}
	var body string
	for _, review := range gh.posted {
		if b := review["body"].(string); strings.Contains(b, "state-risk") {
			body = b
			if n := len(review["comments"].([]any)); n != 0 {
				t.Fatalf("the out-of-hunk finding was sent as %d review comments", n)
			}
		}
	}
	if !strings.Contains(body, "`.github/workflows/main.yml:62` (high)") || !strings.Contains(body, "<!-- mergewake-outside-diff:1 -->") {
		t.Fatalf("state-risk review body=%q", body)
	}
	if got := github.OutsideBlockingCount(g.postedFindings); got != 1 {
		t.Fatalf("blocking outside findings=%d, want 1", got)
	}
}

// refusedPostGH posts pass 1 and refuses pass 2 with err, as in the evidence.
type refusedPostGH struct {
	*fakeGH
	err error
}

func (g refusedPostGH) PostReview(ctx context.Context, repo string, n int, sha string, f []github.Finding, meta github.PassReceipt) error {
	if meta.Pass == 2 {
		return g.err
	}
	return g.fakeGH.PostReview(ctx, repo, n, sha, f, meta)
}

// (b) A pass that was collected and then fails to post leaves one failure row
// per attempt with the redacted error, counts the failure, and the escalation
// notice carries the clipped last error.
func TestReviewPostFailureIsLoggedAndEscalated(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", byLabel: map[string]string{"-p2-": stateRiskFinding}}
	s, g, r := panewireSetup(t, runner)
	g.blocking = true
	g.diffs = map[string]string{shaA: workflowDiff()}
	long := "GitHub POST /repos/example/project/pulls/7/reviews: HTTP 422 for dev@example.com " + strings.Repeat("detail ", 100)
	s.GH = refusedPostGH{g, errors.New(long)}
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 1}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("want the post error")
	}
	if st := retryStateOf(t, s, r); st.Failures != 1 || !st.Escalated || !strings.Contains(st.LastError, "HTTP 422") {
		t.Fatalf("state=%+v", st)
	}
	rows := auditRows(t, s, r, "review-failed")
	if len(rows) != 1 {
		t.Fatalf("review-failed rows=%v", rows)
	}
	row := rows[0]
	text, _ := row["error"].(string)
	if row["pr"] != float64(7) || row["head"] != shaA || row["pass"] != float64(2) || !strings.HasPrefix(fmt.Sprint(row["label"]), "mwr-7-"+shaA[:7]+"-p2-") || !strings.Contains(text, "HTTP 422") || strings.Contains(text, "dev@example.com") || len(text) > 400 {
		t.Fatalf("row=%v", row)
	}
	n := s.Notifier.(*fakeNotifier)
	if n.count != 1 || !strings.Contains(n.events[0].Text, "review failed 1 times") || !strings.Contains(n.events[0].Text, "last error: GitHub POST") || !strings.Contains(n.events[0].Text, "HTTP 422") || strings.Contains(n.events[0].Text, "dev@example.com") || len(n.events[0].Text) > 600 {
		t.Fatalf("notice=%+v", n.events)
	}
	// The next attempt fails the same way and says so again: a pass collected
	// and never posted is not retried in silence.
	c.t = retryStateOf(t, s, r).Next
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("second attempt: want the post error")
	}
	if rows := auditRows(t, s, r, "review-failed"); len(rows) != 2 {
		t.Fatalf("after the second attempt: %v", rows)
	}
}
