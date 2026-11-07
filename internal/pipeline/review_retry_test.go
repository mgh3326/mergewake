package pipeline

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

func TestRetryReviewBackoffRestartAndHeadReset(t *testing.T) {
	s, g, _, n, r := reviewSetup(t)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.Now = c.now
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 3}
	g.pull.Body = "Mergewake-Author-Profile: unknown@v1"
	// An invalid family fails before claiming a pass, allowing a deterministic retry.
	s.Config.Review.DefaultFamily = ""
	path := filepath.Join(s.state(r.Name), "review-retry-7.json")
	check := func(want int, delay time.Duration) {
		t.Helper()
		var st reviewRetryState
		if _, err := fsutil.ReadJSON(path, &st); err != nil || st.Failures != want || !st.Next.Equal(c.t.Add(delay)) {
			t.Fatalf("state=%+v err=%v", st, err)
		}
	}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("expected failed review")
	}
	check(1, time.Minute)
	// New service instance sees the same persisted deadline.
	restarted := *s
	s = &restarted
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	check(1, time.Minute)
	c.t = c.t.Add(time.Minute)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("expected failed review")
	}
	check(2, 2*time.Minute)
	if n.count != 0 {
		t.Fatalf("premature escalation: %v", n.events)
	}
	c.t = c.t.Add(2 * time.Minute)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("expected failed review")
	}
	check(3, 2*time.Minute)
	if n.count != 1 {
		t.Fatalf("expected one escalation: %v", n.events)
	}
	g.pull.Head.SHA = shaC
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("expected new head failure")
	}
	check(1, time.Minute)
	if n.count != 1 {
		t.Fatal("new head escalated immediately")
	}
}

func TestRetryReviewStaleDoneAndLease(t *testing.T) {
	for _, staleLease := range []bool{false, true} {
		t.Run(map[bool]string{false: "done", true: "expired lease"}[staleLease], func(t *testing.T) {
			s, g, _, _, r := reviewSetup(t)
			s.Config.Review.DefaultFamily = "openai"
			g.refs = map[string]bool{doneRef(7, shaA): true}
			if staleLease {
				ref := claimRef(7, shaA, 1)
				g.refs[ref] = true
				g.leaseTimes = map[string]time.Time{ref: time.Now().Add(-time.Hour)}
			}
			if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
				t.Fatal(err)
			}
			if g.postCount != 1 {
				t.Fatalf("no review posted: %d", g.postCount)
			}
			if len(g.passes[shaA]) != 1 {
				t.Fatal("missing receipt")
			}
		})
	}
}

func TestRetryReviewShadowDoesNotWrite(t *testing.T) {
	for _, o := range []Options{{DryRun: true}, {Shadow: true}} {
		s, g, _, n, r := reviewSetup(t)
		s.Config.Review.DefaultFamily = "openai"
		g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Family: "anthropic"}}}
		if err := s.RetryReview(context.Background(), r, g.pull, o); err != nil {
			t.Fatal(err)
		}
		var st reviewRetryState
		found, err := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), &st)
		if err != nil || found || g.claimCount != 0 || n.count != 0 {
			t.Fatalf("acted in shadow: %v %v %d %d", found, err, g.claimCount, n.count)
		}
	}
}

func TestRetryReviewLocalFallbackWithStaleMarker(t *testing.T) {
	root, base, head := seedLargeDiffRepo(t)
	s, g, _, _, r := reviewSetup(t)
	runner := &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}
	s.Runner = runner
	s.Config.Review.DefaultFamily = "openai"
	r.Root = root
	g.diffErr = &github.APIError{Code: 406, Method: "GET", Path: "/pull"}
	g.pull.Base = github.Ref{Ref: "main", SHA: base}
	g.pull.Head = github.Ref{SHA: head, Ref: "feature"}
	g.pull.Head.Repo.FullName = r.Name
	g.pull.ChangedFiles = 2
	g.files = []policy.File{{Path: "tool", Kind: "modified"}, {Path: "renamed", Previous: "old", Kind: "renamed"}}
	g.refs = map[string]bool{doneRef(7, head): true, claimRef(7, head, 1): true}
	g.leaseTimes = map[string]time.Time{claimRef(7, head, 1): time.Now().Add(-time.Hour)}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(g.passes[head]) != 1 || g.passes[head][0].DiffSource != "local" {
		t.Fatalf("passes=%+v", g.passes[head])
	}
}

func TestFleetAuthorDisposition(t *testing.T) {
	thread := github.ReviewThread{Comments: []github.ReviewComment{
		{Author: "reviewer", Body: "<!-- mergewake-severity:high --> finding"},
		{Author: "fleet-account[bot]", AuthorType: "Bot", Body: "Disposition: dismiss: covered by the test"},
	}}
	if got := blockDisposition(thread, true, nil); got != "" {
		t.Fatalf("unconfigured account accepted: %q", got)
	}
	if got := blockDisposition(thread, true, nil, []string{"FLEET-ACCOUNT"}); got != "dismiss" {
		t.Fatalf("configured account rejected: %q", got)
	}
	thread.Comments[1].Body = "<!-- mergewake-severity:high --> another finding"
	if got := blockDisposition(thread, true, nil, []string{"fleet-account"}); got != "" {
		t.Fatal("finding counted as reply")
	}
}

func TestFleetAuthorGateIntegration(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	markReviewed(g)
	g.feedback.Threads = []github.ReviewThread{{Comments: []github.ReviewComment{
		{Author: "review-bot", Body: "<!-- mergewake-severity:high --> finding"},
		{Author: "fleet[bot]", AuthorType: "Bot", Body: "Dismiss: the fixture demonstrates this is safe"},
	}}}
	p, err := s.Policy(context.Background(), r, shaB)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if err != nil {
		t.Fatal(err)
	}
	if before.Review.Findings.OpenBlocking != 1 {
		t.Fatalf("unconfigured disposition accepted: %+v", before)
	}
	s.Config.Review.FleetAuthors = []string{"fleet[bot]"}
	after, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if err != nil {
		t.Fatal(err)
	}
	if after.Review.Findings.OpenBlocking != 0 || after.Review.Findings.DismissedBlocking != 1 {
		t.Fatalf("configured disposition rejected: %+v", after)
	}
}

func TestRetryReviewCompletedWithoutAuthorFamilyDoesNotEscalate(t *testing.T) {
	s, g, _, n, r := reviewSetup(t)
	g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Family: "anthropic"}}}
	g.refs = map[string]bool{doneRef(7, shaA): true}
	s.Config.Daemon.PR.ReviewRetry.Failures = 1
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 || g.postCount != 0 {
		t.Fatalf("completed review retried or escalated: %d %d", g.postCount, n.count)
	}
}

func TestRetryReviewWaitsForLiveLeaseThenRecovers(t *testing.T) {
	s, g, _, n, r := reviewSetup(t)
	s.Config.Review.DefaultFamily = "openai"
	ref := claimRef(7, shaA, 1)
	g.refs = map[string]bool{ref: true}
	g.leaseTimes = map[string]time.Time{ref: time.Now()}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewPending) {
		t.Fatalf("live lease: err=%v, want ErrReviewPending", err)
	}
	if g.postCount != 0 || n.count != 0 {
		t.Fatalf("live lease retried or escalated: %d %d", g.postCount, n.count)
	}
	g.leaseTimes[ref] = time.Now().Add(-time.Hour)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 || n.count != 0 {
		t.Fatalf("expired lease not recovered: %d %d", g.postCount, n.count)
	}
}

func TestFleetAuthorSelfReply(t *testing.T) {
	for _, account := range []struct{ first, reply, kind string }{
		{"FLEET-AUTHOR", "fleet-author", "User"},
		{"fleet-author[bot]", "FLEET-AUTHOR[bot]", "Bot"},
	} {
		t.Run(account.kind, func(t *testing.T) {
			thread := github.ReviewThread{Comments: []github.ReviewComment{
				{Author: account.first, Body: "finding requires a disposition"},
				{Author: account.reply, AuthorType: account.kind, Body: "Dismiss: already covered by a focused test"},
			}}
			fleet := []string{"fleet-author"}
			if got := blockDisposition(thread, false, nil, fleet); got != "" {
				t.Fatalf("own finding accepted a disposition: got %q, want empty", got)
			}
			if answered(thread, false, fleet) {
				t.Fatal("own finding accepted its author's answer")
			}
			// Mergewake's review identity deliberately permits same-login dispositions.
			if got := blockDisposition(thread, true, nil, fleet); got != "dismiss" {
				t.Fatalf("own-review exception lost: got %q", got)
			}
			if !answered(thread, true, fleet) {
				t.Fatal("own-review exception lost its answer")
			}
		})
	}
}
