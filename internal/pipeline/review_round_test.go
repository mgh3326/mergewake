package pipeline

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// Round one's patch: line 1 of tool changes.
const patchOne = "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1,2 +1,2 @@\n-old\n+new\n keep\n"

// Round two adds a second hunk (new-side lines 10-12) to the same file.
const patchTwo = patchOne + "@@ -10,2 +10,3 @@\n ctx\n+added line\n ctx2\n"

// roundGH serves round history, the previous patch and the current diff, and
// records what PostReview was given.
type roundGH struct {
	*fakeGH
	heads  []github.ReviewedHead
	full   string
	old    string
	posted [][]github.Finding
	ciUp   *bool
}

func (g *roundGH) Diff(context.Context, string, int) (string, error) { return g.full, nil }
func (g *roundGH) ReviewedHeads(context.Context, string, int, string) ([]github.ReviewedHead, error) {
	return g.heads, nil
}
func (g *roundGH) CompareDiff(context.Context, string, string, string) (string, error) {
	return g.old, nil
}
func (g *roundGH) PostReview(ctx context.Context, repo string, n int, sha string, f []github.Finding, meta github.PassReceipt) error {
	g.posted = append(g.posted, f)
	return g.fakeGH.PostReview(ctx, repo, n, sha, f, meta)
}
func (g *roundGH) CIStarted(context.Context, string, string) (bool, error) {
	return g.ciUp == nil || *g.ciUp, nil
}

// roundRunner answers every reviewer call with out and keeps the prompts.
type roundRunner struct {
	*fakeRunner
	out    string
	prompt string
}

func (r *roundRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "scopefuel" {
		return r.fakeRunner.Run(ctx, command, args, input, dir)
	}
	r.prompt = input
	return []byte(r.out), nil
}

func roundSetup(t *testing.T, heads []github.ReviewedHead, old, full, out string) (*Service, *roundGH, *roundRunner, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	rg := &roundGH{fakeGH: g, heads: heads, old: old, full: full}
	run := &roundRunner{fakeRunner: &fakeRunner{}, out: out}
	s.GH, s.Runner = rg, run
	return s, rg, run, r
}

func fjson(line int, severity, body string) string {
	return `{"path":"tool","line":` + strconv.Itoa(line) + `,"body":"` + body + `","severity":"` + severity + `"}`
}

func fsjson(items ...string) string { return `{"findings":[` + strings.Join(items, ",") + `]}` }

var priorHeads = []github.ReviewedHead{{SHA: shaB, Round: 1}}

func TestRoundOneReviewsTheWholeDiff(t *testing.T) {
	s, g, run, r := roundSetup(t, nil, "", patchTwo, fsjson())
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(run.prompt, "+new") || !strings.Contains(run.prompt, "+added line") || strings.Contains(run.prompt, "REVIEW ROUND") {
		t.Fatal(run.prompt)
	}
	if got := g.passes[g.pull.Head.SHA]; len(got) != 1 || got[0].Round != 1 || got[0].Delta != "" || got[0].Quiet {
		t.Fatal(got)
	}
}

func TestRoundTwoPromptHasOnlyTheInterdiffAndPriorFindings(t *testing.T) {
	prior := thread("tool", 1, false,
		say("review-bot", "<!-- mergewake-severity:medium -->\nThe old line is wrong"),
		say("pr-author", "won't fix: intended"))
	open := thread("tool", 2, false, say("review-bot", "<!-- mergewake-severity:low -->\nkeep line is brittle"))
	s, g, run, r := roundSetup(t, priorHeads, patchOne, patchTwo, fsjson())
	g.feedback.Threads = []github.ReviewThread{prior, open}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	diffAt := strings.Index(run.prompt, "UNTRUSTED DIFF:")
	delta := run.prompt[diffAt:]
	if !strings.Contains(delta, "+added line") || strings.Contains(delta, "+new") || strings.Contains(delta, "-old") {
		t.Fatalf("round two must see only the interdiff:\n%s", delta)
	}
	ctx := run.prompt[:diffAt]
	for _, want := range []string{
		"REVIEW ROUND 2 (delta)", "since the last reviewed head bbbbbbb",
		"PRIOR FINDINGS (rounds 1-1", "tool:1 (medium)", "The old line is wrong", "disposition: answered: won't fix: intended",
		"tool:2 (low)", "disposition: none yet (still open)", "Do not reopen",
	} {
		if !strings.Contains(ctx, want) {
			t.Fatalf("missing %q in:\n%s", want, ctx)
		}
	}
	// The earlier findings are listed once, not again as "already answered".
	if strings.Contains(ctx, "ALREADY ANSWERED") || strings.Contains(ctx, "mergewake-severity") {
		t.Fatal(ctx)
	}
	got := g.passes[g.pull.Head.SHA]
	if len(got) != 1 || got[0].Round != 2 || got[0].Delta != shaB {
		t.Fatal(got)
	}
}

func TestDuplicateOfAnExistingThreadIsNotPosted(t *testing.T) {
	prior := thread("tool", 1, true,
		say("review-bot", "<!-- mergewake-severity:high -->\nThe old line is wrong and leaks a file handle"),
		say("pr-author", "fixed"))
	out := fsjson(
		// same path, same substance, different line: a duplicate
		fjson(11, "high", "The old line is wrong, it leaks a file handle"),
		// same path, different substance: kept
		fjson(11, "high", "added line is never validated before use"),
	)
	s, g, _, r := roundSetup(t, priorHeads, patchOne, patchTwo, out)
	g.feedback.Threads = []github.ReviewThread{prior}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.posted) != 1 || len(g.posted[0]) != 1 || !strings.Contains(g.posted[0][0].Body, "never validated") {
		t.Fatalf("%+v", g.posted)
	}
}

func TestDuplicateAcrossPassesOfOneRunIsNotPosted(t *testing.T) {
	s, g, _, r := roundSetup(t, nil, "", patchTwo, fsjson(fjson(1, "high", "the same bug in the same file twice over")))
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.posted) != 2 || len(g.posted[0]) != 1 || len(g.posted[1]) != 0 {
		t.Fatalf("%+v", g.posted)
	}
}

func TestContractMakesOutsideFindingsAdvisory(t *testing.T) {
	body := "Summary\n\n## Acceptance (1:1 with Do)\n1. the parser rejects empty input\n\n## Notes\nx"
	out := fsjson(
		strings.Replace(fjson(1, "medium", "style of the helper is inconsistent"), "}", `,"advisory":true}`, 1),
		strings.Replace(fjson(1, "high", "unrelated race in the cache layer"), "}", `,"advisory":true}`, 1),
		strings.Replace(fjson(11, "critical", "secret written to the log"), "}", `,"advisory":true}`, 1),
		fjson(11, "high", "empty input is accepted by the parser"),
	)
	s, g, run, r := roundSetup(t, nil, "", patchTwo, out)
	g.pull.Body = body
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(run.prompt, "ACCEPTANCE CONTRACT (from the PR description") || !strings.Contains(run.prompt, "the parser rejects empty input") || strings.Contains(run.prompt, "## Notes") {
		t.Fatal(run.prompt)
	}
	got := g.posted[0]
	if len(got) != 4 {
		t.Fatalf("%+v", got)
	}
	if !got[0].Advisory || got[0].Severity != "medium" {
		t.Fatalf("out-of-contract medium: %+v", got[0])
	}
	if !got[1].Advisory || got[1].Severity != "medium" {
		t.Fatalf("out-of-contract high is capped at medium: %+v", got[1])
	}
	if got[2].Advisory || got[2].Severity != "critical" {
		t.Fatalf("critical stays critical: %+v", got[2])
	}
	if got[3].Advisory || got[3].Severity != "high" {
		t.Fatalf("in-contract finding is untouched: %+v", got[3])
	}
	// As posted, the advisory medium grades advisory at the merge gate (no
	// answer needed), while the critical still blocks and an in-contract medium
	// would still need an answer.
	gate := (policy.ReviewGate{}).WithOwnLogin("review-bot")
	grade := func(f github.Finding) string {
		return gate.Grade(policy.ReviewFinding{Author: "review-bot", OwnReview: true, Severity: github.ThreadSeverity("review-bot", github.FindingBody(f))}, "routine")
	}
	if g := grade(got[0]); g != policy.GradeAdvisory {
		t.Fatalf("advisory medium graded %s", g)
	}
	if g := grade(got[2]); g != policy.GradeBlock {
		t.Fatalf("critical graded %s", g)
	}
	if g := grade(github.Finding{Severity: "medium", Body: "x"}); g != policy.GradeTriage {
		t.Fatalf("in-contract medium graded %s", g)
	}
}

func TestAdvisoryClaimIsIgnoredWithoutAContract(t *testing.T) {
	out := fsjson(strings.Replace(fjson(1, "high", "a real bug"), "}", `,"advisory":true}`, 1))
	s, g, run, r := roundSetup(t, nil, "", patchTwo, out)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(run.prompt, "ACCEPTANCE CONTRACT") {
		t.Fatal(run.prompt)
	}
	if f := g.posted[0][0]; f.Advisory || f.Severity != "high" {
		t.Fatalf("%+v", f)
	}
}

type bodySink struct {
	fakeSink
	body string
}

func (b *bodySink) TaskBody(_ context.Context, task string) (string, error) {
	if task != "12" {
		return "", nil
	}
	return b.body, nil
}

func TestContractComesFromTheLinkedTask(t *testing.T) {
	s, g, run, r := roundSetup(t, nil, "", patchTwo, fsjson())
	g.pull.Body = "no acceptance here\n\nMergewake-Task: 12\n"
	s.Sink = &bodySink{body: "intro\n## Acceptance\n- exits 0 on success\n"}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(run.prompt, "(from the linked task 12") || !strings.Contains(run.prompt, "exits 0 on success") {
		t.Fatal(run.prompt)
	}
}

func TestAcceptanceSectionHeadings(t *testing.T) {
	for _, tt := range []struct{ body, want string }{
		{"## Acceptance\n- a\n## Next\nb", "## Acceptance\n- a"},
		{"### acceptance criteria:\n- a", "### acceptance criteria:\n- a"},
		{"## Acceptance (1:1 with Do)\n1. a", "## Acceptance (1:1 with Do)\n1. a"},
		{"## Acceptable risk\nx", ""},
		{"```\n## Acceptance\nx\n```", ""},
		{"nothing", ""},
	} {
		if got := acceptanceSection(tt.body); got != tt.want {
			t.Errorf("%q: got %q want %q", tt.body, got, tt.want)
		}
	}
}

func TestRoundFourReportsOnlyCriticalAndHigh(t *testing.T) {
	heads := []github.ReviewedHead{{SHA: shaC, Round: 1}, {SHA: shaD, Round: 2}, {SHA: shaB, Round: 3}}
	out := fsjson(
		fjson(11, "medium", "a medium finding on the delta"),
		fjson(11, "low", "a low finding on the delta"),
		fjson(11, "high", "a high finding on the delta"),
		fjson(11, "critical", "a critical finding on the delta"),
	)
	s, g, run, r := roundSetup(t, heads, patchOne, patchTwo, out)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.posted[0]) != 2 || g.posted[0][0].Severity != "high" || g.posted[0][1].Severity != "critical" {
		t.Fatalf("%+v", g.posted[0])
	}
	if !strings.Contains(run.prompt, "REVIEW ROUND 4 (delta)") || !strings.Contains(run.prompt, "REVIEW BUDGET SPENT: this PR has had 3 full review rounds") {
		t.Fatal(run.prompt)
	}
	if got := g.passes[g.pull.Head.SHA]; got[0].Round != 4 || !got[0].Quiet {
		t.Fatal(got)
	}
}

func TestRoundThreeStillReportsMedium(t *testing.T) {
	heads := []github.ReviewedHead{{SHA: shaC, Round: 1}, {SHA: shaB, Round: 2}}
	s, g, run, r := roundSetup(t, heads, patchOne, patchTwo, fsjson(fjson(11, "medium", "a medium finding on the delta")))
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.posted[0]) != 1 || strings.Contains(run.prompt, "BUDGET") {
		t.Fatalf("%+v\n%s", g.posted[0], run.prompt)
	}
	if got := g.passes[g.pull.Head.SHA]; got[0].Round != 3 || got[0].Quiet {
		t.Fatal(got)
	}
}

func TestBudgetIsConfigurable(t *testing.T) {
	heads := []github.ReviewedHead{{SHA: shaB, Round: 1}}
	s, g, _, r := roundSetup(t, heads, patchOne, patchTwo, fsjson(fjson(11, "medium", "a medium finding")))
	s.Config.Review.BudgetRounds = 1
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.posted[0]) != 0 {
		t.Fatalf("%+v", g.posted[0])
	}
	s, g, _, r = roundSetup(t, []github.ReviewedHead{{SHA: shaC, Round: 8}}, patchOne, patchTwo, fsjson(fjson(11, "medium", "a medium finding")))
	s.Config.Review.BudgetRounds = -1
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.posted[0]) != 1 {
		t.Fatalf("budget off: %+v", g.posted[0])
	}
}

func TestUnchangedPatchDoesNotSpendARound(t *testing.T) {
	heads := []github.ReviewedHead{{SHA: shaB, Round: 3}}
	s, g, run, r := roundSetup(t, heads, patchTwo, patchTwo, fsjson(fjson(1, "medium", "a medium"), fjson(1, "high", "a high")))
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(run.prompt, "No hunk changed since the last reviewed head") || !strings.Contains(run.prompt, "+new") {
		t.Fatal(run.prompt)
	}
	if len(g.posted[0]) != 1 || g.posted[0][0].Severity != "high" {
		t.Fatalf("%+v", g.posted[0])
	}
	if got := g.passes[g.pull.Head.SHA]; got[0].Round != 3 || !got[0].Quiet {
		t.Fatal(got)
	}
}

func TestSecondPassOfARoundKeepsItsRound(t *testing.T) {
	// The current head already has a posted pass, so its recorded round stands.
	heads := []github.ReviewedHead{{SHA: shaB, Round: 1}, {SHA: shaA, Round: 2}}
	s, g, run, r := roundSetup(t, heads, patchOne, patchTwo, fsjson())
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(run.prompt, "REVIEW ROUND 2 (delta)") || !strings.Contains(run.prompt, "bbbbbbb") {
		t.Fatal(run.prompt)
	}
	if got := g.passes[g.pull.Head.SHA]; got[0].Round != 2 || got[0].Delta != shaB {
		t.Fatal(got)
	}
}

func TestReviewsBeforeRoundCountingCountOneRoundEach(t *testing.T) {
	heads := []github.ReviewedHead{{SHA: shaC}, {SHA: shaD}, {SHA: shaB}}
	s, g, _, r := roundSetup(t, heads, patchOne, patchTwo, fsjson())
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if got := g.passes[g.pull.Head.SHA]; got[0].Round != 4 || !got[0].Quiet {
		t.Fatal(got)
	}
}

type badRangeGH struct{ *roundGH }

func (badRangeGH) CompareDiff(context.Context, string, string, string) (string, error) {
	return "", context.DeadlineExceeded
}

func TestUnreadablePreviousPatchFallsBackToTheWholeDiff(t *testing.T) {
	s, g, run, r := roundSetup(t, priorHeads, "", patchTwo, fsjson())
	s.GH = badRangeGH{g}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(run.prompt, "+new") || !strings.Contains(run.prompt, "REVIEW ROUND 2. The diff below is the whole PR") {
		t.Fatal(run.prompt)
	}
	if got := g.passes[g.pull.Head.SHA]; got[0].Round != 2 || got[0].Delta != "" {
		t.Fatal(got)
	}
}

func TestInterdiff(t *testing.T) {
	// A rebase moves line numbers and context only: nothing is new.
	moved := "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -41,2 +41,2 @@\n-old\n+new\n other context\n"
	if got := interdiff(patchOne, moved); got != "" {
		t.Fatalf("rebased patch must be empty, got %q", got)
	}
	if got := interdiff(patchOne, patchOne); got != "" {
		t.Fatal(got)
	}
	want := "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -10,2 +10,3 @@\n ctx\n+added line\n ctx2\n"
	if got := interdiff(patchOne, patchTwo); got != want {
		t.Fatalf("got %q", got)
	}
	// A new file and an edited hunk are both kept; an untouched file is dropped.
	other := "diff --git a/other b/other\n--- a/other\n+++ b/other\n@@ -1 +1 @@\n-a\n+b\n"
	edited := strings.Replace(patchOne, "+new", "+newer", 1)
	got := interdiff(patchOne+other, edited+other+"diff --git a/fresh b/fresh\nnew file mode 100644\n--- /dev/null\n+++ b/fresh\n@@ -0,0 +1 @@\n+x\n")
	if !strings.Contains(got, "+newer") || !strings.Contains(got, "b/fresh") || strings.Contains(got, "b/other") {
		t.Fatal(got)
	}
	// Files without hunks (mode change) compare by their whole block.
	mode := "diff --git a/run.sh b/run.sh\nold mode 100644\nnew mode 100755\n"
	if got := interdiff(patchOne+mode, patchOne+mode); got != "" {
		t.Fatal(got)
	}
	if got := interdiff(patchOne, patchOne+mode); got != mode {
		t.Fatal(got)
	}
}

func TestSameSubstance(t *testing.T) {
	for _, tt := range []struct {
		a, b string
		want bool
	}{
		{"The old line is wrong and leaks a file handle", "the old line is wrong, it leaks a file handle", true},
		{"leaks a file handle on every call to Open in the retry loop", "leaks a file handle", false},
		{"returns an error without closing the response body in the retry loop on a 429", "closing the response body is skipped when the retry loop returns an error on a 429", true},
		{"missing nil check on the config pointer", "off by one in the page cursor", false},
		{"", "anything", false},
	} {
		if got := sameSubstance(tt.a, tt.b); got != tt.want {
			t.Errorf("%q vs %q: got %v", tt.a, tt.b, got)
		}
	}
}

func TestRuntugBriefCarriesTheDelta(t *testing.T) {
	x := &runtugExecutor{repo: config.Repo{Name: "example/project"}, pr: github.Pull{Number: 7, Head: github.Ref{SHA: shaA}, Base: github.Ref{Ref: "main", SHA: shaB}}, diff: "diff --git a/tool b/tool\n+x\n", delta: true, round: 2, context: "ACCEPTANCE CONTRACT"}
	got := x.brief(&passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
	for _, want := range []string{"this is review round 2: review only the delta patch", "## Delta to review", "+x", "optional boolean `advisory`"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	x.delta, x.context = false, ""
	if got = x.brief(&passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai"); strings.Contains(got, "Delta to review") || !strings.Contains(got, "git diff "+shaB+"..."+shaA) || !strings.Contains(got, "No other keys.") {
		t.Fatal(got)
	}
}

// --- heartbeat ---

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func stallSetup(t *testing.T) (*Service, *roundGH, *fakeNotifier, *clock, config.Repo) {
	t.Helper()
	s, g, _, n, r := setup(t)
	s.Config.Notifier.Lane = "operator-desk"
	c := &clock{t: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	s.Now = c.now
	return s, &roundGH{fakeGH: g}, n, c, r
}

func TestStalledPRAlarmsOnce(t *testing.T) {
	s, g, n, c, r := stallSetup(t)
	s.GH = g
	prs := []github.Pull{g.pull}
	poll := func() {
		t.Helper()
		if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	poll()
	c.t = c.t.Add(29 * time.Minute)
	poll()
	if n.count != 0 {
		t.Fatalf("alarm inside the threshold: %v", n.events)
	}
	c.t = c.t.Add(2 * time.Minute)
	poll()
	if n.count != 1 {
		t.Fatalf("want one alarm, got %v", n.events)
	}
	ev := n.events[0]
	if ev.Lane != "operator-desk" || !strings.Contains(ev.Text, "STALLED review example/project#7 head=aaaaaaa") || !strings.Contains(ev.Text, "31m0s") || !strings.Contains(ev.Text, "threshold 30m0s") {
		t.Fatal(ev)
	}
	c.t = c.t.Add(3 * time.Hour)
	poll()
	if n.count != 1 {
		t.Fatalf("a stalled head alarms once, got %v", n.events)
	}
	// A restarted daemon keeps the clock and the sent record.
	s2 := *s
	if e := s2.CheckStalls(context.Background(), r, prs, Options{}); e != nil || n.count != 1 {
		t.Fatal(e, n.events)
	}
	// A new head starts a new clock.
	prs[0].Head.SHA = shaC
	poll()
	c.t = c.t.Add(31 * time.Minute)
	poll()
	if n.count != 2 || !strings.Contains(n.events[1].Text, "head=ccccccc") {
		t.Fatal(n.events)
	}
}

func TestReviewedOrNotReadyPRsDoNotAlarm(t *testing.T) {
	s, g, n, c, r := stallSetup(t)
	s.GH = g
	started := false
	reviewed := g.pull
	reviewed.Number, reviewed.Head.SHA = 8, shaB
	g.refs = map[string]bool{claimRef(8, shaB, 1): true}
	draft := g.pull
	draft.Number, draft.Draft = 9, true
	noCI := g.pull
	noCI.Number, noCI.Head.SHA = 10, shaD
	prs := []github.Pull{reviewed, draft}
	poll := func() {
		t.Helper()
		if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	poll()
	c.t = c.t.Add(5 * time.Hour)
	poll()
	if n.count != 0 {
		t.Fatalf("a claimed review or a draft must not alarm: %v", n.events)
	}
	// CI that has not started keeps the clock stopped.
	g.ciUp = &started
	prs = []github.Pull{noCI}
	poll()
	c.t = c.t.Add(5 * time.Hour)
	poll()
	if n.count != 0 {
		t.Fatalf("no CI yet must not alarm: %v", n.events)
	}
	// Once CI starts the clock starts then, not retroactively.
	g.ciUp = nil
	poll()
	c.t = c.t.Add(29 * time.Minute)
	poll()
	if n.count != 0 {
		t.Fatal(n.events)
	}
	c.t = c.t.Add(2 * time.Minute)
	poll()
	if n.count != 1 {
		t.Fatal(n.events)
	}
}

func TestStallAlarmSwitchesAndDryRun(t *testing.T) {
	s, g, n, c, r := stallSetup(t)
	s.GH = g
	prs := []github.Pull{g.pull}
	run := func(o Options) {
		t.Helper()
		if e := s.CheckStalls(context.Background(), r, prs, o); e != nil {
			t.Fatal(e)
		}
		c.t = c.t.Add(time.Hour)
		if e := s.CheckStalls(context.Background(), r, prs, o); e != nil {
			t.Fatal(e)
		}
	}
	run(Options{DryRun: true})
	s.Config.Daemon.PR.StallAfter = "off"
	run(Options{})
	if n.count != 0 {
		t.Fatalf("%v", n.events)
	}
	s.Config.Daemon.PR.StallAfter = "10m"
	run(Options{Shadow: true})
	if n.count != 1 || !strings.Contains(n.events[0].Text, "threshold 10m0s") {
		t.Fatalf("shadow mode still alarms: %v", n.events)
	}
}

func TestFailedStallAlarmRetries(t *testing.T) {
	s, g, n, c, r := stallSetup(t)
	s.GH = g
	prs := []github.Pull{g.pull}
	_ = s.CheckStalls(context.Background(), r, prs, Options{})
	c.t = c.t.Add(time.Hour)
	n.err = context.DeadlineExceeded
	if e := s.CheckStalls(context.Background(), r, prs, Options{}); e == nil {
		t.Fatal("a failed delivery is an error")
	}
	n.err = nil
	if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil || n.count != 1 {
		t.Fatal(e, n.events)
	}
}

func TestStallWindowConfig(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want time.Duration
	}{{"", 30 * time.Minute}, {"45m", 45 * time.Minute}, {"off", 0}, {"junk", 0}} {
		if got := (config.PRLoop{StallAfter: tt.in}).StallWindow(); got != tt.want {
			t.Errorf("%q: got %v want %v", tt.in, got, tt.want)
		}
	}
}
