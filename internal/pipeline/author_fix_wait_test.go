package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestLandAuthorFixWait(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	posted := now.Add(-time.Minute)
	for _, tc := range []struct {
		name       string
		wait       string
		live       bool
		reply      bool
		advance    time.Duration
		wantMerges int
		wantReason string
	}{
		{name: "live author waits within bound", wait: "30m", live: true, wantReason: "author_fixing"},
		{name: "live author merges after bound", wait: "30m", live: true, advance: 30 * time.Minute, wantMerges: 1},
		{name: "author reply clears wait", wait: "30m", live: true, reply: true, wantMerges: 1},
		{name: "no live author merges at once", wait: "30m", wantMerges: 1},
		{name: "zero disables wait", wait: "0s", live: true, wantMerges: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			s.Config.Daemon.PR.AuthorFixWait = tc.wait
			s.Now = func() time.Time { return now.Add(tc.advance) }
			g.enabled = true
			g.ci = github.Green
			g.policyExtra = "review_gate: {land: graded}\n"
			g.pull.User.Login = "dev"
			g.pull.Body = "Mergewake-Task: 42\n"
			markReviewed(g)
			comment := github.ReviewComment{Author: "review-bot", AuthorType: "Bot", Body: "<!-- mergewake-severity:medium -->\nfinding", CommitSHA: shaA, OriginalCommitSHA: shaA, CreatedAt: posted}
			thread := github.ReviewThread{ID: "thread-1", Comments: []github.ReviewComment{comment}}
			if tc.reply {
				thread.Comments = append(thread.Comments, github.ReviewComment{Author: "dev", AuthorType: "User", Body: "Disposition: Fixed in work", CreatedAt: now})
			}
			g.feedback.Threads = []github.ReviewThread{thread}
			g.findings = 1
			if tc.live {
				s.Sink = &jobSink{
					jobs: map[string]string{"42": "author-job"},
					recs: map[string]integrations.JobRecord{"author-job": {State: "running", Label: "author-job"}},
				}
			}

			err := s.Land(context.Background(), r, 7, Options{Observed: true})
			if err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != tc.wantMerges {
				t.Fatalf("merge count = %d, want %d", g.mergeCount, tc.wantMerges)
			}
			if tc.wantReason != "" {
				// Land audit carries the exact pending verdict reason.
				if !strings.Contains(strings.Join(auditLines(t, s, r.Name), "\n"), tc.wantReason) {
					t.Fatalf("audit does not name %q", tc.wantReason)
				}
			}
		})
	}
}

func TestNewestUnansweredFindingUsesCurrentHeadAndLatestPost(t *testing.T) {
	posted := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	thread := func(id, head string, at time.Time, replies ...github.ReviewComment) github.ReviewThread {
		comments := []github.ReviewComment{{Author: "review-bot", CommitSHA: head, OriginalCommitSHA: head, CreatedAt: at}}
		comments = append(comments, replies...)
		return github.ReviewThread{ID: id, Comments: comments}
	}
	fb := github.ReviewFeedback{Threads: []github.ReviewThread{
		thread("old", shaA, posted.Add(time.Minute)),
		thread("answered", shaB, posted.Add(2*time.Minute), github.ReviewComment{Author: "dev"}),
		thread("newest", shaB, posted.Add(3*time.Minute)),
	}}
	if got := newestUnansweredFinding(fb, shaB, "dev"); !got.Equal(posted.Add(3 * time.Minute)) {
		t.Fatalf("newest finding time = %v", got)
	}
	if got := newestUnansweredFinding(fb, shaC, "dev"); !got.IsZero() {
		t.Fatalf("new head retained old finding time %v", got)
	}
}
