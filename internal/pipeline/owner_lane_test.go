package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A human-judgment notice goes to the repo's design-session lane when
// repos[].lane names one, and to the desk when it does not. Author-bound
// events keep their job lane; the fourth test covers that.
func TestOwnerLaneRoutesHumanJudgmentNotices(t *testing.T) {
	for _, tt := range []struct{ name, repoLane, want string }{
		{"repo lane", "x-design", "x-design"},
		{"no repo lane", "", "operator-desk"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("review failed", func(t *testing.T) {
				s, g, _, n, r := reviewSetup(t)
				s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
				r.Lane = tt.repoLane
				c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
				s.Now = c.now
				s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "2m", Failures: 1}
				g.pull.Body = "Mergewake-Author-Profile: unknown@v1"
				s.Config.Review.DefaultFamily = ""
				if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
					t.Fatal("want a review failure")
				}
				if n.count != 1 || n.events[0].Lane != tt.want || !strings.Contains(n.events[0].Text, "review failed") {
					t.Fatalf("events: %+v", n.events)
				}
			})

			t.Run("review result lost", func(t *testing.T) {
				runner := &panewireRunner{}
				s, _, r := panewireSetup(t, runner)
				s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
				r.Lane = tt.repoLane
				runner.sink.missing = &integrations.JobRecord{State: "completed"}
				if err := s.Review(context.Background(), r, 7, "", Options{}); err == nil {
					t.Fatal("want a lost result")
				}
				n := s.Notifier.(*fakeNotifier)
				if n.count != 1 || n.events[0].Lane != s.deskLane() || !strings.Contains(n.events[0].Text, "result was lost") {
					t.Fatalf("events: %+v", n.events)
				}
			})

			t.Run("out-of-scope findings owner", func(t *testing.T) {
				s, g, n, _, r, now := deliverSetup(t)
				r.Lane = tt.repoLane
				g.feedback.Threads = []github.ReviewThread{
					finding("T1", "review-bot", "<!-- mergewake-severity:medium -->\n<!-- mergewake-advisory -->\n**Advisory** outside the contract\nstyle", 3),
					ownFinding("T2", "high", 5),
				}
				deliver(t, s, r)
				// Author-bound copies keep the job lane; the owner is where
				// escalations go.
				if n.count != 2 {
					t.Fatalf("events: %+v", n.events)
				}
				for _, e := range n.events {
					if e.Lane != "w-7" {
						t.Fatalf("author-bound delivery moved: %+v", e)
					}
				}
				ledger, e := s.loadDeliveries(r.Name)
				if e != nil || len(ledger) != 2 {
					t.Fatalf("ledger: %v %+v", e, ledger)
				}
				for _, d := range ledger {
					if d.Owner != tt.want {
						t.Fatalf("%s delivery owner %q, want %q", d.Scope, d.Owner, tt.want)
					}
				}
				*now = now.Add(2 * time.Hour)
				deliver(t, s, r)
				if n.count != 3 {
					t.Fatalf("no escalation: %+v", n.events)
				}
				esc := n.events[2]
				if esc.Lane != tt.want || esc.Type != "decision" || !strings.Contains(esc.Text, "findings still open") {
					t.Fatalf("escalation: %+v", esc)
				}
			})

			t.Run("review blocked", func(t *testing.T) {
				f := blockSetup(t)
				f.s.Config.Notifier.Lane, f.s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
				f.r.Lane = tt.repoLane
				if e := f.review(t); e == nil {
					t.Fatal("want a blocked review")
				}
				if f.n.count != 1 || f.n.events[0].Lane != tt.want || f.n.events[0].Type != "decision" || !strings.Contains(f.n.events[0].Text, "blocked: missing-task-trailer") {
					t.Fatalf("events: %+v", f.n.events)
				}
			})

			t.Run("aged check failure", func(t *testing.T) {
				s, g, n, _, r, _ := deliverSetup(t)
				r.Lane = tt.repoLane
				for _, head := range []string{shaA, shaB, shaC} {
					g.pull.Head.SHA = head
					if e := s.ageCheckFailures(context.Background(), r, g.pull, []string{"check_failed:unit"}); e != nil {
						t.Fatal(e)
					}
				}
				if n.count != 1 || n.events[0].Lane != tt.want || !strings.Contains(n.events[0].Text, "still failing after") {
					t.Fatalf("events: %+v", n.events)
				}
			})
		})
	}
}

// A repo lane does not take over author-bound events: FINDINGS and BLOCKED for
// a PR bound to a job still go to that job's lane.
func TestOwnerLaneKeepsAuthorBoundEventsOnTheJobLane(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	r.Lane = "x-design"
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	if n.count != 1 || n.events[0].Lane != "w-7" || n.events[0].Type != "findings" {
		t.Fatalf("findings: %+v", n.events)
	}
	lane, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "blocked", "blocking_findings:1")
	if e != nil || !sent || lane != "w-7" {
		t.Fatalf("blocked route: %q %t %v", lane, sent, e)
	}
	if last := n.events[len(n.events)-1]; last.Lane != "w-7" || last.Type != "blocked" {
		t.Fatalf("blocked event: %+v", last)
	}
}
