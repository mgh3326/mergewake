package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

const fleetBody = "Mergewake-Author-Family: anthropic\n"

// commentGH keeps PR comments in memory, posted under the review identity.
type commentGH struct {
	*fakeGH
	comments []github.IssueComment
	postErr  error
	posts    int
}

func (g *commentGH) IssueComments(context.Context, string, int) ([]github.IssueComment, error) {
	return g.comments, nil
}

func (g *commentGH) PostIssueComment(_ context.Context, _ string, _ int, body string) error {
	if g.postErr != nil {
		return g.postErr
	}
	g.posts++
	g.comments = append(g.comments, github.IssueComment{Body: body, User: github.PullUser{Login: "review-bot"}})
	return nil
}

// failingTaskSink cannot read the task behind a trailer.
type failingTaskSink struct{ *taskJobSink }

func (failingTaskSink) TaskJob(context.Context, string) (string, string, error) {
	return "", "", errors.New("hk unavailable")
}

type blockFixture struct {
	s      *Service
	g      *fakeGH
	gh     *commentGH
	n      *fakeNotifier
	r      config.Repo
	runner *panewireRunner
}

// blockSetup is a PR opened by a fleet account, with no task trailer.
func blockSetup(t *testing.T) blockFixture {
	t.Helper()
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	gh := &commentGH{fakeGH: g}
	n := &fakeNotifier{}
	s.GH, s.Notifier = gh, n
	s.Config.Notifier.FallbackLane = "desk-lane"
	s.Config.Review.FleetAuthors = []string{"fleet-bot"}
	g.pull.User.Login = "fleet-bot"
	g.pull.Body = fleetBody
	return blockFixture{s, g, gh, n, r, runner}
}

func (f blockFixture) blocked(t *testing.T, pr int) string {
	t.Helper()
	state, e := f.s.ledger().Read()
	if e != nil {
		t.Fatal(e)
	}
	return state.Latest[outcomes.Key(f.r.Name, pr)].ReviewBlocked[f.g.pull.Head.SHA]
}

func (f blockFixture) review(t *testing.T) error {
	t.Helper()
	return f.s.Review(context.Background(), f.r, f.g.pull.Number, "", Options{})
}

func TestFleetPRWithoutTrailerBlocksVisibly(t *testing.T) {
	for name, login := range map[string]string{"fleet author": "Fleet-Bot", "review login": "review-bot[bot]"} {
		t.Run(name, func(t *testing.T) {
			f := blockSetup(t)
			f.g.pull.User.Login = login
			if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
				t.Fatalf("want a blocked review: %v", e)
			}
			if got := f.blocked(t, 7); got != "blocked: missing-task-trailer" {
				t.Fatalf("outcome row: %q", got)
			}
			if len(f.gh.comments) != 1 {
				t.Fatalf("comments: %+v", f.gh.comments)
			}
			for _, want := range []string{"blocked: missing-task-trailer", "Add one line to the PR body", "Mergewake-Task: <id>", "`(grade X, Tn)`", reviewBlockKey(shaA)} {
				if !strings.Contains(f.gh.comments[0].Body, want) {
					t.Fatalf("comment lacks %q:\n%s", want, f.gh.comments[0].Body)
				}
			}
			if f.n.count != 1 || f.n.events[0].Lane != "desk-lane" || f.n.events[0].Type != "decision" || !strings.Contains(f.n.events[0].Text, "blocked: missing-task-trailer") {
				t.Fatalf("events: %+v", f.n.events)
			}
			// No done marker, no claim, no reviewer, no pass.
			if f.g.refs[doneRef(7, shaA)] || f.g.claimCount != 0 || f.g.postCount != 0 || len(f.runner.spawns) != 0 || f.runner.headless != 0 {
				t.Fatalf("refs=%v claims=%d posts=%d spawns=%d headless=%d", f.g.refs, f.g.claimCount, f.g.postCount, len(f.runner.spawns), f.runner.headless)
			}
			p, e := f.s.Policy(context.Background(), f.r, shaB)
			if e != nil {
				t.Fatal(e)
			}
			v, e := f.s.mergeGate(context.Background(), f.r, f.g.pull, p, shaB)
			if e != nil {
				t.Fatal(e)
			}
			if v.State != "blocked" || !strings.Contains(strings.Join(v.Reasons, ","), "review_missing") {
				t.Fatalf("gate: %+v", v)
			}
		})
	}
}

func TestFleetTaskWithoutMarkerBlocksVisibly(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
	f.runner.sink.title = "Fix the thing"
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("want a blocked review: %v", e)
	}
	if got := f.blocked(t, 7); got != "blocked: task-missing-T-marker" {
		t.Fatalf("outcome row: %q", got)
	}
	if len(f.gh.comments) != 1 || !strings.Contains(f.gh.comments[0].Body, "Add `(grade X, Tn)` to the title of hk task 42, for example `(grade B, T1)`.") {
		t.Fatalf("comments: %+v", f.gh.comments)
	}
	if f.n.count != 1 || !strings.Contains(f.n.events[0].Text, "blocked: task-missing-T-marker") {
		t.Fatalf("events: %+v", f.n.events)
	}
	if f.g.refs[doneRef(7, shaA)] || f.g.claimCount != 0 || len(f.runner.spawns) != 0 {
		t.Fatalf("refs=%v claims=%d spawns=%v", f.g.refs, f.g.claimCount, f.runner.spawns)
	}
}

// RetryReview is the daemon's entry: a rerun on the head stays quiet and is no
// reviewer failure, however many polls the author takes.
func TestBlockedRerunPostsNoSecondComment(t *testing.T) {
	f := blockSetup(t)
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1h", Maximum: "2h", Failures: 2}
	for range 4 {
		if e := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if f.gh.posts != 1 {
		t.Fatalf("comment posted %d times, want 1 per head:\n%+v", f.gh.posts, f.gh.comments)
	}
	if f.n.count != 1 {
		t.Fatalf("desk events: %+v", f.n.events)
	}
	if f.g.refs[doneRef(7, shaA)] {
		t.Fatal("done marker written for a blocked head")
	}

	// A new head is a new block.
	f.g.pull.Head.SHA = shaC
	if e := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	if f.gh.posts != 2 || f.n.count != 2 || f.blocked(t, 7) != blockMissingTrailer {
		t.Fatalf("posts=%d events=%+v", f.gh.posts, f.n.events)
	}
}

func TestBlockedHeadReasonChangeKeepsOneComment(t *testing.T) {
	f := blockSetup(t)
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
	f.runner.sink.title = "Fix the thing"
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.gh.posts != 1 || f.n.count != 2 || f.blocked(t, 7) != blockMissingMarker {
		t.Fatalf("posts=%d events=%+v row=%q", f.gh.posts, f.n.events, f.blocked(t, 7))
	}
}

func TestAddingTrailerLetsReviewProceed(t *testing.T) {
	f := blockSetup(t)
	if e := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
	if e := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(f.runner.spawns) != 1 || f.g.postCount != 1 || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d posts=%d refs=%v", len(f.runner.spawns), f.g.postCount, f.g.refs)
	}
	if got := f.blocked(t, 7); got != "" {
		t.Fatalf("outcome row still blocked: %q", got)
	}
	if f.gh.posts != 1 || f.n.count != 1 {
		t.Fatalf("posts=%d events=%+v", f.gh.posts, f.n.events)
	}
}

func TestNonFleetAuthorKeepsFallback(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.User.Login = "outside-contributor"
	if e := f.review(t); e != nil {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || f.n.count != 0 || f.blocked(t, 7) != "" {
		t.Fatalf("posts=%d events=%+v row=%q", f.gh.posts, f.n.events, f.blocked(t, 7))
	}
	got := f.g.passes[shaA]
	if len(got) != 1 || f.runner.headless != 1 || got[0].Fallback != "panewire: no Mergewake-Task trailer" || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("passes=%+v headless=%d refs=%v", got, f.runner.headless, f.g.refs)
	}
}

// Only a positive answer blocks: a read error stays with the fallback.
func TestUnreadableEvidenceDoesNotBlock(t *testing.T) {
	for name, arrange := range map[string]func(blockFixture){
		"head commit": func(f blockFixture) { f.g.commitErr = map[string]error{shaA: errors.New("GitHub down")} },
		"task": func(f blockFixture) {
			f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
			f.s.Sink = failingTaskSink{f.runner.sink}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := blockSetup(t)
			arrange(f)
			if e := f.review(t); errors.Is(e, ErrReviewBlocked) {
				t.Fatal(e)
			}
			if f.gh.posts != 0 || f.n.count != 0 || f.blocked(t, 7) != "" {
				t.Fatalf("posts=%d events=%+v", f.gh.posts, f.n.events)
			}
		})
	}
}

func TestHeadlessExecutorDoesNotBlock(t *testing.T) {
	f := blockSetup(t)
	f.s.Config.Review.Executor = "headless"
	if e := f.review(t); e != nil {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || f.g.postCount != 1 {
		t.Fatalf("comments=%d passes=%d", f.gh.posts, f.g.postCount)
	}
}

func TestRepoWithoutRootDoesNotBlock(t *testing.T) {
	f := blockSetup(t)
	f.r.Root = ""
	if e := f.review(t); e != nil {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || f.g.postCount != 1 {
		t.Fatalf("comments=%d passes=%d", f.gh.posts, f.g.postCount)
	}
}

func TestCompleteHeadNeedsNoTrailer(t *testing.T) {
	f := blockSetup(t)
	f.g.passes = map[string][]github.PassReceipt{shaA: {{Pass: 1, Lens: "default", Family: "openai"}}}
	if e := f.review(t); e != nil {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("comments=%d refs=%v", f.gh.posts, f.g.refs)
	}
}

func TestBlockDedupeIgnoresOtherAuthors(t *testing.T) {
	f := blockSetup(t)
	f.gh.comments = []github.IssueComment{{Body: reviewBlockKey(shaA), User: github.PullUser{Login: "someone-else"}}}
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.gh.posts != 1 {
		t.Fatalf("another account's comment suppressed the notice: %d", f.gh.posts)
	}
}

func TestBlockCommentFailureStillTellsDeskAndRetries(t *testing.T) {
	f := blockSetup(t)
	f.gh.postErr = errors.New("GitHub down")
	e := f.review(t)
	if e == nil || errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("a failed comment must stay a retryable error: %v", e)
	}
	if f.n.count != 1 {
		t.Fatalf("events: %+v", f.n.events)
	}
	f.gh.postErr = nil
	if e = f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.gh.posts != 1 || f.n.count != 1 {
		t.Fatalf("posts=%d events=%+v", f.gh.posts, f.n.events)
	}
}

func TestBlockEventFailureRetriesWithoutSecondComment(t *testing.T) {
	f := blockSetup(t)
	f.n.err = errors.New("hub down")
	if e := f.review(t); e == nil || errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("a failed event must stay a retryable error: %v", e)
	}
	f.n.err = nil
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.gh.posts != 1 || f.n.count != 1 {
		t.Fatalf("posts=%d events=%+v", f.gh.posts, f.n.events)
	}
}

func TestBlockDryRunAndShadowTellNobody(t *testing.T) {
	for name, tt := range map[string]struct {
		o   Options
		row string
	}{
		"dry run": {Options{DryRun: true}, ""},
		"shadow":  {Options{Shadow: true}, blockMissingTrailer},
	} {
		t.Run(name, func(t *testing.T) {
			f := blockSetup(t)
			var out strings.Builder
			f.s.Out = &out
			if e := f.s.RetryReview(context.Background(), f.r, f.g.pull, tt.o); e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(out.String(), `"action":"review-blocked"`) {
				t.Fatal(out.String())
			}
			if f.gh.posts != 0 || f.n.count != 0 || f.g.refs[doneRef(7, shaA)] || f.blocked(t, 7) != tt.row {
				t.Fatalf("posts=%d events=%+v row=%q", f.gh.posts, f.n.events, f.blocked(t, 7))
			}
		})
	}
}

func TestStallAlarmSkipsBlockedHead(t *testing.T) {
	f := blockSetup(t)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	f.s.Now = c.now
	f.s.Config.Daemon.PR.StallAfter = "30m"
	prs := []github.Pull{f.g.pull}
	if e := f.s.RetryReview(context.Background(), f.r, f.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	if e := f.s.CheckStalls(context.Background(), f.r, prs, Options{}); e != nil {
		t.Fatal(e)
	}
	c.t = c.t.Add(2 * time.Hour)
	if e := f.s.CheckStalls(context.Background(), f.r, prs, Options{}); e != nil {
		t.Fatal(e)
	}
	if f.n.count != 1 {
		t.Fatalf("a blocked head raised a stall alarm: %+v", f.n.events)
	}
}
