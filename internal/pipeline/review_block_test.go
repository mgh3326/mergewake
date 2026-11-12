package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/reap"
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
	if f.gh.posts != 2 || f.n.count != 1 || f.blocked(t, 7) != blockMissingTrailer {
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

// A non-fleet author is not blocked; without a task trailer its pass fails
// and is retried, with no headless review.
func TestNonFleetAuthorWithoutTaskFailsPass(t *testing.T) {
	f := blockSetup(t)
	f.g.pull.User.Login = "outside-contributor"
	e := f.review(t)
	if errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || f.n.count != 0 || f.blocked(t, 7) != "" {
		t.Fatalf("posts=%d events=%+v row=%q", f.gh.posts, f.n.events, f.blocked(t, 7))
	}
	assertPanewirePassFailed(t, f.s, f.g, f.r, f.runner, e, "no Mergewake-Task trailer")
}

// Only a positive answer blocks: a read error stays with the executor's skip.
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

func TestRepoWithoutRootDoesNotBlock(t *testing.T) {
	f := blockSetup(t)
	f.r.Root = ""
	e := f.review(t)
	if errors.Is(e, ErrReviewBlocked) || f.gh.posts != 0 {
		t.Fatalf("err=%v comments=%d", e, f.gh.posts)
	}
	assertPanewirePassFailed(t, f.s, f.g, f.r, f.runner, e, "repo root unset")
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

// A pass that arrives after the block, on this head or on an earlier head with
// the same patch, leaves no stale outcome.
func TestLaterPassClearsBlockedOutcome(t *testing.T) {
	diff, _ := (&fakeGH{}).Diff(context.Background(), "", 0)
	for name, passes := range map[string]map[string][]github.PassReceipt{
		"own pass":       {shaA: {{Pass: 1, Lens: "default", Family: "openai"}}},
		"inherited pass": {shaC: {{Pass: 1, Lens: "default", Family: "openai", Patch: reviewPatchID(diff)}}},
	} {
		t.Run(name, func(t *testing.T) {
			f := blockSetup(t)
			if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
				t.Fatal(e)
			}
			f.g.passes = passes
			if e := f.review(t); e != nil {
				t.Fatal(e)
			}
			if got := f.blocked(t, 7); got != "" {
				t.Fatalf("outcome row still blocked: %q", got)
			}
			if f.gh.posts != 1 || !f.g.refs[doneRef(7, shaA)] {
				t.Fatalf("posts=%d refs=%v", f.gh.posts, f.g.refs)
			}
		})
	}
}

// blockRouter answers the herdr and worktree reads of the author-pane scan
// from lister and every other command from the panewire fake.
type blockRouter struct {
	lister *scriptRunner
	pane   *panewireRunner
}

func (r blockRouter) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if _, ok := r.lister.outs[key(name, args...)]; ok || name == "herdr" {
		return r.lister.Run(ctx, name, args, input, dir)
	}
	return r.pane.Run(ctx, name, args, input, dir)
}

// withAuthorPane puts one live worker on the PR's branch (feat/thing, the head
// of blockSetup) on a local host. Its job w-9 has lane author-route.
func (f blockFixture) withAuthorPane(t *testing.T) *scriptRunner {
	t.Helper()
	lister := authorHost(aPane{"pane-author", "w-9", "/wt/a", "feat/thing", projectOrigin})
	f.s.Runner = blockRouter{lister, f.runner}
	f.s.Config.Hosts = []reap.Host{{Name: "local"}}
	f.runner.sink.jobs["w-9"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-9", HubLane: "author-route"}
	return lister
}

// (a) A missing trailer is the author's to fix: with a live author pane on the
// PR's branch the block goes to that pane's lane, not the repo's owner lane.
func TestAuthorFixableBlockGoesToTheAuthorPane(t *testing.T) {
	f := blockSetup(t)
	f.withAuthorPane(t)
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.n.count != 1 {
		t.Fatalf("events: %+v", f.n.events)
	}
	ev := f.n.events[0]
	if ev.Lane != "author-route" || ev.Type != "decision" || !strings.HasSuffix(ev.Text, "| lane=author-route via author pane pane-author job w-9") {
		t.Fatalf("event lane %q text %q", ev.Lane, ev.Text)
	}
	if f.gh.posts != 1 {
		t.Fatalf("comments posted %d times, want 1", f.gh.posts)
	}
}

// (b) An unknown family is fixed by the author too: with no pane, the valid
// Notify-Lane trailer names the author's lane.
func TestAuthorFamilyBlockGoesToTheNotifyLaneTrailer(t *testing.T) {
	f := familyBlockSetup(t)
	f.g.pull.Body = "Mergewake-Task: 42\nMergewake-Notify-Lane: trailer-route\n"
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if got := f.blocked(t, 7); got != blockUnknownFamily {
		t.Fatalf("outcome row: %q", got)
	}
	if f.n.count != 1 || f.n.events[0].Lane != "trailer-route" || !strings.HasSuffix(f.n.events[0].Text, "| lane=trailer-route via notify-lane trailer") {
		t.Fatalf("events: %+v", f.n.events)
	}
}

// (c) No author, no trailer, no task: the owner lane gets it, and the desk
// gets nothing when the repo names a lane.
func TestAuthorlessBlockGoesToOwnerLaneNotDesk(t *testing.T) {
	f := blockSetup(t)
	f.r.Lane = "owner-route"
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.n.count != 1 {
		t.Fatalf("events: %+v", f.n.events)
	}
	ev := f.n.events[0]
	if ev.Lane != "owner-route" || !strings.HasSuffix(ev.Text, "| lane=owner-route via owner lane") {
		t.Fatalf("event lane %q text %q", ev.Lane, ev.Text)
	}
	desk := f.s.Config.Notifier.FallbackLane
	for _, e := range f.n.events {
		if e.Lane == desk {
			t.Fatalf("desk got the block: %+v", f.n.events)
		}
	}
}

// (d) The task title and the secret-like content belong to a human: they stay
// with the owner lane even when a live author pane could take them.
func TestTaskAndSecretBlocksStayWithOwnerLane(t *testing.T) {
	for name, arrange := range map[string]func(*testing.T, blockFixture){
		"task-missing-T-marker": func(t *testing.T, f blockFixture) {
			f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
			f.runner.sink.title = "Fix the thing"
		},
		"secret-like-content": func(t *testing.T, f blockFixture) {
			f.g.pull.Body = "Mergewake-Task: 42\n" + fleetBody
			if e := f.s.recordReviewBlock(f.r.Name, 7, shaA, blockSecretLike, false); e != nil {
				t.Fatal(e)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := blockSetup(t)
			f.r.Lane = "owner-route"
			f.withAuthorPane(t)
			arrange(t, f)
			if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
				t.Fatal(e)
			}
			if f.n.count != 1 {
				t.Fatalf("events: %+v", f.n.events)
			}
			ev := f.n.events[0]
			if ev.Lane != "owner-route" || !strings.HasSuffix(ev.Text, "| lane=owner-route via owner lane") {
				t.Fatalf("event lane %q text %q", ev.Lane, ev.Text)
			}
		})
	}
}

// (e) A pane read error sends nothing and records no sent key, and the next
// pass with a working lister sends once. The comment is still posted once.
func TestAuthorRouteReadErrorSendsNothingThenRetries(t *testing.T) {
	f := blockSetup(t)
	lister := f.withAuthorPane(t)
	lister.errs = map[string]error{key("herdr", "agent", "list"): errors.New("herdr down")}
	if e := f.review(t); e == nil || errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("a failed pane read must stay a retryable error: %v", e)
	}
	if f.n.count != 0 {
		t.Fatalf("events after a read error: %+v", f.n.events)
	}
	lister.errs = nil
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.n.count != 1 || f.n.events[0].Lane != "author-route" {
		t.Fatalf("events: %+v", f.n.events)
	}
	if f.gh.posts != 1 {
		t.Fatalf("comments posted %d times, want 1", f.gh.posts)
	}
}

// (f) The same (PR, head, reason) twice is one event, even when routed to the
// author pane.
func TestAuthorFixableBlockSendsOnce(t *testing.T) {
	f := blockSetup(t)
	f.withAuthorPane(t)
	for range 2 {
		if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
			t.Fatal(e)
		}
	}
	if f.n.count != 1 || f.gh.posts != 1 {
		t.Fatalf("events=%+v posts=%d", f.n.events, f.gh.posts)
	}
}
