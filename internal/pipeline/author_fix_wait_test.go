package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

func TestLandAuthorFixWait(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	posted := now.Add(-time.Minute)
	for _, tc := range []struct {
		name       string
		wait       string
		live       bool
		jobState   string
		pane       string
		paneState  string
		replyBy    string
		resolved   bool
		advance    time.Duration
		wantMerges int
		wantReason string
	}{
		{name: "live author waits within bound", wait: "30m", live: true, wantReason: "author_fixing"},
		{name: "live author merges after bound", wait: "30m", live: true, advance: 30 * time.Minute, wantMerges: 1},
		{name: "same-login author reply clears wait", wait: "30m", live: true, replyBy: "dev", wantMerges: 1},
		{name: "any reply clears wait", wait: "30m", live: true, replyBy: "reviewer", wantMerges: 1},
		{name: "resolved finding clears wait", wait: "30m", live: true, resolved: true, wantMerges: 1},
		{name: "no live author merges at once", wait: "30m", wantMerges: 1},
		{name: "zero disables wait", wait: "0s", live: true, wantMerges: 1},
		{name: "terminal bound job ignores its lingering head pane", wait: "30m", jobState: "completed", pane: "author-job", wantMerges: 1},
		{name: "terminal bound job waits for live takeover pane", wait: "30m", jobState: "completed", pane: "takeover-job", paneState: "running", wantReason: "author_fixing"},
		{name: "unbound live head pane waits", wait: "30m", pane: "head-pane", paneState: "running", wantReason: "author_fixing"},
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
			// The PR author, finding author and (in the same-login case) replier
			// are the same identity, as on this fleet's actual PRs.
			comment := github.ReviewComment{Author: "dev", AuthorType: "User", Body: "<!-- mergewake-severity:medium -->\nfinding", CommitSHA: shaA, OriginalCommitSHA: shaA, CreatedAt: posted}
			thread := github.ReviewThread{ID: "thread-1", IsResolved: tc.resolved, Comments: []github.ReviewComment{comment}}
			if tc.replyBy != "" {
				thread.Comments = append(thread.Comments, github.ReviewComment{Author: tc.replyBy, AuthorType: "User", Body: "Disposition: Fixed in work", CreatedAt: now})
			}
			g.feedback.Threads = []github.ReviewThread{thread}
			g.findings = 1
			if tc.live || tc.jobState != "" || tc.pane != "" {
				sink := &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
				if tc.live || tc.jobState != "" {
					state := tc.jobState
					if state == "" {
						state = "running"
					}
					sink.jobs["42"] = "author-job"
					sink.recs["author-job"] = integrations.JobRecord{State: state, Label: "author-job"}
				}
				if tc.pane != "" {
					if tc.pane != "author-job" {
						state := tc.paneState
						if state == "" {
							state = "running"
						}
						sink.recs[tc.pane] = integrations.JobRecord{State: state, Label: tc.pane + "-lane"}
					}
					s.Runner = noMergeGitRunner{next: authorHost(aPane{"w1:p1", tc.pane, "/wt/a", "feature", projectOrigin})}
					s.Config.Hosts = []reap.Host{{Name: "local"}}
				}
				s.Sink = sink
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
		comments := []github.ReviewComment{{Author: "dev", Body: "<!-- mergewake-severity:medium --> finding", CommitSHA: head, OriginalCommitSHA: head, CreatedAt: at}}
		comments = append(comments, replies...)
		return github.ReviewThread{ID: id, Comments: comments}
	}
	fb := github.ReviewFeedback{Threads: []github.ReviewThread{
		thread("old", shaA, posted.Add(time.Minute)),
		thread("answered", shaB, posted.Add(2*time.Minute), github.ReviewComment{Author: "reviewer"}),
		thread("newest", shaB, posted.Add(3*time.Minute)),
		{ID: "unmarked", Comments: []github.ReviewComment{{Author: "dev", Body: "<!-- mergewake-severity: -->", CommitSHA: shaB, OriginalCommitSHA: shaB, CreatedAt: posted.Add(4 * time.Minute)}}},
	}}
	if got := newestUnansweredFinding(fb, shaB); !got.Equal(posted.Add(3 * time.Minute)) {
		t.Fatalf("newest finding time = %v", got)
	}
	if got := newestUnansweredFinding(fb, shaC); !got.IsZero() {
		t.Fatalf("new head retained old finding time %v", got)
	}
}
