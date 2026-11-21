package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// pendingConflictGH posts pass 2 through the real GitHub client, against a
// server that answers the way GitHub does when the posting user already holds
// a pending review on the pull request.
type pendingConflictGH struct {
	*fakeGH
	client   *github.Client
	attempts *atomic.Int32
}

func (g pendingConflictGH) PostReview(ctx context.Context, repo string, n int, sha string, f []github.Finding, meta github.PassReceipt) error {
	if meta.Pass == 2 {
		return g.client.PostReview(ctx, repo, n, sha, f, meta)
	}
	return g.fakeGH.PostReview(ctx, repo, n, sha, f, meta)
}

func TestReviewPostPendingConflictIsNamedAndBacksOff(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprint(w, `{"message":"Unprocessable Entity","errors":["User can only have one pending review per pull request"],"documentation_url":"https://docs.github.com/rest"}`)
	}))
	t.Cleanup(server.Close)
	client := github.New("placeholder")
	client.BaseURL, client.UploadURL, client.HTTP = server.URL, server.URL, server.Client()

	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", byLabel: map[string]string{"p2t": stateRiskFinding}}
	s, g, r := panewireSetup(t, runner)
	g.blocking = true
	g.diffs = map[string]string{shaA: workflowDiff()}
	s.GH = pendingConflictGH{fakeGH: g, client: client}
	s.Config.Review.Login = "example-bot"
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 1}

	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("want the post error")
	}
	const named = "a pending review by example-bot on this PR blocks posting; the PR's owner submits or deletes it"
	st := retryStateOf(t, s, r)
	if st.Failures != 1 || !st.Escalated || st.LastError != named {
		t.Fatalf("state=%+v", st)
	}
	if !st.Next.After(c.t) {
		t.Fatalf("next=%v, want after %v (the backoff applies)", st.Next, c.t)
	}
	rows := auditRows(t, s, r, "review-failed")
	if len(rows) != 1 || !strings.Contains(fmt.Sprint(rows[0]["error"]), "User can only have one pending review per pull request") {
		t.Fatalf("review-failed rows=%v", rows)
	}
	n := s.Notifier.(*fakeNotifier)
	if n.count != 1 || !strings.Contains(n.events[0].Text, "review failed 1 times") || !strings.Contains(n.events[0].Text, named) {
		t.Fatalf("notice=%+v", n.events)
	}
	// Before the backoff ends, a retry does not post again.
	before := attempts.Load()
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != before {
		t.Fatalf("attempts during backoff: %d -> %d", before, got)
	}
}
