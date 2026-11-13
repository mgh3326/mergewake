package pipeline

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// deferSink is the hk task sink the defer gate meets: it counts TaskJob reads
// and refuses an id the real client rejects before any call
// (integrations.taskClient), which hk would never see.
type deferSink struct {
	jobSink
	taskReads int
}

func (s *deferSink) TaskJob(ctx context.Context, task string) (string, string, error) {
	s.taskReads++
	if _, e := strconv.ParseInt(task, 10, 64); !integrations.TaskID.MatchString(task) || e != nil {
		return "", "", errors.New("task ID must be a positive integer")
	}
	return s.jobSink.TaskJob(ctx, task)
}

// deferGateThread is an unanswered triage finding by the review identity with
// one human reply.
func deferGateThread(id, reply string) github.ReviewThread {
	return github.ReviewThread{ID: id, Path: "tool", Line: 3, Comments: []github.ReviewComment{
		{Author: "review-bot", AuthorType: "User", Body: "finding", CommitSHA: shaA, OriginalCommitSHA: shaA, URL: "https://github.example/pr/7#discussion_" + id},
		{Author: "dev", AuthorType: "User", Body: reply},
	}}
}

// deferGateSetup is a fully reviewed, green, mergeable head under the graded
// gate with the defer task sink attached and task 2481 bound to a job.
func deferGateSetup(t *testing.T) (*Service, *fakeGH, *deferSink, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	markReviewed(g)
	g.pull.Body = "Mergewake-Task: 2481\n"
	sink := &deferSink{jobSink: jobSink{jobs: map[string]string{"2481": "w-7"}}}
	s.Sink = sink
	return s, g, sink, r
}

func gateOnce(t *testing.T, s *Service, r config.Repo, g *fakeGH) (gateVerdict, error) {
	t.Helper()
	p, e := s.Policy(context.Background(), r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	return s.mergeGate(context.Background(), r, g.pull, p, shaB)
}

// A defer naming an id hk could never look up — not a positive integer, or
// too large for int64 — is an absent target: the thread stays unanswered as
// for a missing task, the gate does not fail, and no task read is made for it.
func TestGateDeferInvalidTaskIDIsAbsent(t *testing.T) {
	for _, id := range []string{"0", "007", "99999999999999999999"} {
		t.Run(id, func(t *testing.T) {
			s, g, sink, r := deferGateSetup(t)
			g.feedback.Threads = []github.ReviewThread{deferGateThread("T1", "Disposition: defer "+id)}
			v, e := gateOnce(t, s, r, g)
			if e != nil {
				t.Fatalf("gate failed on an invalid defer target: %v", e)
			}
			if reasons(v) != "triage_unanswered:1" {
				t.Fatalf("reasons=%q, want the thread unanswered", reasons(v))
			}
			if sink.taskReads != 0 {
				t.Fatalf("TaskJob called %d times for an invalid defer target", sink.taskReads)
			}
		})
	}
}

// failOnceCommitGH fails the first Commit read, as a transient GitHub error
// would, then answers from the embedded fake.
type failOnceCommitGH struct {
	*fakeGH
	failed bool
}

func (g *failOnceCommitGH) Commit(ctx context.Context, repo, sha string) (github.Commit, error) {
	if !g.failed {
		g.failed = true
		return github.Commit{}, errors.New("commit read failed")
	}
	return g.fakeGH.Commit(ctx, repo, sha)
}

// A failed read of the PR's own task is not cached: the next defer judged in
// the same pass reads again. While the read fails the defer is a lookup
// failure, never "not own" — here the retry succeeds and the defer to the
// PR's own task stays unanswered.
func TestGateDeferOwnTaskReadRetriedInSamePass(t *testing.T) {
	s, g, sink, r := deferGateSetup(t)
	// The own task comes from the head commit, so its read can fail.
	g.pull.Body = ""
	g.messages = map[string]string{shaA: "subject\n\nMergewake-Task: 2481\n"}
	s.GH = &failOnceCommitGH{fakeGH: g}
	defer2481 := "Disposition: defer 2481"
	g.feedback.Threads = []github.ReviewThread{
		// The first own-task read fails on this defer; the later ordinary
		// reply still answers the thread, so the pass goes on.
		{ID: "T1", Path: "tool", Line: 3, Comments: []github.ReviewComment{
			{Author: "review-bot", AuthorType: "User", Body: "finding", CommitSHA: shaA, OriginalCommitSHA: shaA, URL: "https://github.example/pr/7#discussion_T1"},
			{Author: "dev", AuthorType: "User", Body: defer2481},
			{Author: "dev", AuthorType: "User", Body: "I applied the fix"},
		}},
		// The retried read finds the own task, so this defer does not count.
		deferGateThread("T2", defer2481),
	}
	v, e := gateOnce(t, s, r, g)
	if e != nil {
		t.Fatalf("gate failed: %v", e)
	}
	if reasons(v) != "triage_unanswered:1" {
		t.Fatalf("reasons=%q, want the own-task defer unanswered", reasons(v))
	}
	if sink.taskReads != 0 {
		t.Fatalf("TaskJob called %d times; an own-task defer must not look the task up", sink.taskReads)
	}
}
