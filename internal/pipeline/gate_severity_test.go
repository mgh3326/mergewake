package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

const (
	gradedPolicy = "review_gate: {land: graded}\n"
	fastLane     = "fast_lane: {max_changed_lines: 50}\n"
)

// gradedThread is a review thread opened by author with the given severity marker
// on commit; replies are (login, actor type) pairs posted after the finding.
func gradedThread(author, severity string, resolved bool, commit string, replies ...[2]string) github.ReviewThread {
	body := "finding"
	if severity != "" {
		body = "<!-- mergewake-severity:" + severity + " -->\n" + body
	}
	cs := []github.ReviewComment{{Author: author, CommitSHA: commit, OriginalCommitSHA: commit, Body: body}}
	for _, r := range replies {
		cs = append(cs, github.ReviewComment{Author: r[0], AuthorType: r[1], Body: "reply"})
	}
	return github.ReviewThread{IsResolved: resolved, Comments: cs}
}

var human = [2]string{"dev", "User"}

// verdict gates a fully reviewed, green head under the extra policy text, with
// lines changed and the given threads. The raw unresolved count is set to the
// number of unresolved threads, as GitHub would report it.
func verdict(t *testing.T, extra string, lines int, threads ...github.ReviewThread) gateVerdict {
	t.Helper()
	return verdictSized(t, extra, lines, lines, threads...)
}

// verdictSized is verdict where the PR reports prLines changed lines while its
// file list sums to fileLines.
func verdictSized(t *testing.T, extra string, prLines, fileLines int, threads ...github.ReviewThread) gateVerdict {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = extra
	g.pull.Additions, g.pull.Deletions = prLines-prLines/2, prLines/2
	g.files = []policy.File{{Path: "tool", Kind: "modified", Additions: fileLines - fileLines/2, Deletions: fileLines / 2}}
	markReviewed(g)
	g.feedback.Threads = threads
	for _, th := range threads {
		if !th.IsResolved {
			g.findings++
		}
	}
	p, e := s.Policy(context.Background(), r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if e != nil {
		t.Fatal(e)
	}
	return v
}

func reasons(v gateVerdict) string { return strings.Join(v.Reasons, ";") }

func TestGateBlockClass(t *testing.T) {
	for _, tt := range []struct {
		name, want string
		th         github.ReviewThread
	}{
		{"unresolved high", "blocking_findings:1", gradedThread("review-bot", "high", false, shaB)},
		{"unresolved critical", "blocking_findings:1", gradedThread("review-bot", "critical", false, shaB)},
		{"reply does not clear high", "blocking_findings:1", gradedThread("review-bot", "high", false, shaB, human)},
		{"resolved high is resolved on the current head", "", gradedThread("review-bot", "high", true, shaA, human)},
		{"resolved high is resolved after a new head", "", gradedThread("review-bot", "high", true, shaB, human)},
		{"resolved critical is resolved", "", gradedThread("review-bot", "critical", true, shaB)},
		{"third-party unresolved critical", "blocking_findings:1", gradedThread("coderabbitai[bot]", "critical", false, shaB)},
		{"third-party critical resolved on the current head is resolved", "", gradedThread("coderabbitai[bot]", "critical", true, shaA, human)},
		{"third-party critical resolved after a new head is resolved", "", gradedThread("coderabbitai[bot]", "critical", true, shaB, human)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reasons(verdict(t, gradedPolicy, 0, tt.th)); got != tt.want {
				t.Fatalf("reasons=%q", got)
			}
		})
	}
}

func TestGateTriageClass(t *testing.T) {
	for _, tt := range []struct {
		name, want string
		th         github.ReviewThread
	}{
		{"own medium never holds", "", gradedThread("review-bot", "medium", false, shaB)},
		{"own low never holds", "", gradedThread("review-bot", "low", false, shaB)},
		{"missing severity needs an answer", "triage_unanswered:1", gradedThread("review-bot", "", false, shaB)},
		{"operator reply under the review login answers its own finding", "", gradedThread("review-bot", "", false, shaA, [2]string{"review-bot", "User"})},
		{"bot reply does not answer", "triage_unanswered:1", gradedThread("review-bot", "", false, shaA, [2]string{"ci[bot]", "Bot"})},
		{"human reply answers without a new head", "", gradedThread("review-bot", "", false, shaA, human)},
		{"resolved unrated finding on the current head is not held", "", gradedThread("review-bot", "", true, shaA)},
		{"third-party P2 needs an answer", "triage_unanswered:1", gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)},
		{"third-party P2 with a reply (fix SHA, dismissal or follow-up) has a disposition", "", gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaA, human)},
		{"resolved third-party P2 has a disposition", "", gradedThread("chatgpt-codex-connector[bot]", "p2", true, shaA)},
		{"third-party Major ranks with high: an answer alone does not clear it", "blocking_findings:1", gradedThread("coderabbitai[bot]", "major", false, shaA, human)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reasons(verdict(t, gradedPolicy, 0, tt.th)); got != tt.want {
				t.Fatalf("reasons=%q", got)
			}
		})
	}
}

func TestGateAdvisoryNeverBlocks(t *testing.T) {
	v := verdict(t, gradedPolicy, 0, gradedThread("someone-else", "", false, shaA), gradedThread("someone-else", "high", false, shaA))
	if v.State != "ready" || reasons(v) != "" {
		t.Fatalf("%+v", v)
	}
}

func TestGateReportsBothGradedReasons(t *testing.T) {
	v := verdict(t, gradedPolicy, 0,
		gradedThread("review-bot", "high", false, shaB),
		gradedThread("review-bot", "critical", false, shaB),
		gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB))
	if v.State != "blocked" || reasons(v) != "blocking_findings:2;triage_unanswered:1" {
		t.Fatalf("%+v", v)
	}
}

func TestGateGradedKeepsChangesRequested(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	markReviewed(g)
	g.feedback.ChangesRequested = 1
	p, _ := s.Policy(context.Background(), r, shaB)
	v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if e != nil || reasons(v) != "blocking_findings:1" {
		t.Fatalf("%v %+v", e, v)
	}
}

// Without review_gate or fast_lane the raw count decides, whatever the
// severity of what is open.
func TestGateReviewGateWithoutLandKeepsRawCount(t *testing.T) {
	for _, extra := range []string{"review_gate: {}\n", "review_gate: {required_bots: [coderabbitai]}\n"} {
		v := verdict(t, extra, 0, gradedThread("review-bot", "low", false, shaA))
		if reasons(v) != "threads_open:1" {
			t.Fatalf("%q: %+v", extra, v)
		}
	}
	// A review_gate section still feeds the fast lane's rules once it applies.
	v := verdict(t, "review_gate: {}\n"+fastLane, 10, gradedThread("review-bot", "medium", false, shaA))
	if reasons(v) != "" {
		t.Fatalf("%+v", v)
	}
}

func TestGateUngradedPolicyKeepsRawCount(t *testing.T) {
	v := verdict(t, "", 0, gradedThread("review-bot", "low", false, shaA))
	if reasons(v) != "threads_open:1" {
		t.Fatalf("%+v", v)
	}
	v = verdict(t, "", 0, gradedThread("review-bot", "low", true, shaA))
	if reasons(v) != "" {
		t.Fatalf("a resolved own finding is resolved: %+v", v)
	}
}

func TestGateFastLane(t *testing.T) {
	for _, tt := range []struct {
		name, extra, want string
		lines             int
		th                github.ReviewThread
	}{
		{"small PR ignores unanswered triage", fastLane + gradedPolicy, "", 50, gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)},
		{"small PR without a review_gate section", fastLane, "", 50, gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)},
		{"small PR is still stopped by block-class", fastLane, "blocking_findings:1", 50, gradedThread("review-bot", "high", false, shaB)},
		{"small PR accepts a block-class finding resolved on the head", fastLane, "", 5, gradedThread("review-bot", "high", true, shaA, human)},
		{"small PR still needs an answer for an unreadable own severity", fastLane, "triage_unanswered:1", 10, gradedThread("review-bot", "", false, shaB)},
		{"small PR accepts an answer for an unreadable own severity", fastLane, "", 10, gradedThread("review-bot", "", false, shaB, human)},
		{"over the limit keeps triage", fastLane + gradedPolicy, "triage_unanswered:1", 51, gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)},
		{"over the limit without review_gate keeps the raw count", fastLane, "threads_open:1", 51, gradedThread("review-bot", "low", false, shaB)},
		{"no fast_lane keeps triage", gradedPolicy, "triage_unanswered:1", 10, gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)},
		{"a blocking path never takes the lane", fastLane + gradedPolicy + "blocking:\n  - paths: ['**']\n    reason: effects\n", "triage_unanswered:1", 10, gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reasons(verdict(t, tt.extra, tt.lines, tt.th)); got != tt.want {
				t.Fatalf("reasons=%q", got)
			}
		})
	}
}

// The fast lane trusts the PR's line totals only when the file list sums to
// the same additions and deletions; any mismatch refuses the lane.
func TestGateFastLaneNeedsVerifiedLines(t *testing.T) {
	medium := gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB)
	for _, tt := range []struct {
		name, want         string
		prLines, fileLines int
	}{
		{"totals match", "", 10, 10},
		{"PR reports fewer lines than the files", "triage_unanswered:1", 10, 500},
		{"PR reports more lines than the files", "triage_unanswered:1", 10, 4},
		{"PR totals zero while files carry lines", "triage_unanswered:1", 0, 30},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reasons(verdictSized(t, fastLane+gradedPolicy, tt.prLines, tt.fileLines, medium)); got != tt.want {
				t.Fatalf("reasons=%q", got)
			}
		})
	}
	// Additions and deletions are compared separately, not just their sum.
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = fastLane + gradedPolicy
	g.pull.Additions, g.pull.Deletions = 6, 4
	g.files = []policy.File{{Path: "tool", Kind: "modified", Additions: 4, Deletions: 6}}
	markReviewed(g)
	g.feedback.Threads = []github.ReviewThread{medium}
	p, _ := s.Policy(context.Background(), r, shaB)
	v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if e != nil || reasons(v) != "triage_unanswered:1" {
		t.Fatalf("%v %+v", e, v)
	}
}

// End to end: an answered triage finding lands without a new head, an
// unanswered one holds the merge.
func TestLandGradedTriageAnswered(t *testing.T) {
	for _, answered := range []bool{false, true} {
		s, g, _, _, r := setup(t)
		g.enabled = true
		g.policyExtra = gradedPolicy
		markReviewed(g)
		var replies [][2]string
		if answered {
			replies = append(replies, human)
		}
		g.feedback.Threads = []github.ReviewThread{gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaA, replies...)}
		g.findings = 1
		if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
			t.Fatal(err)
		}
		if want := map[bool]int{false: 0, true: 1}[answered]; g.mergeCount != want {
			t.Fatalf("answered=%v merges=%d", answered, g.mergeCount)
		}
	}
}

// outdated returns th marked outdated.
func outdated(th github.ReviewThread) github.ReviewThread {
	th.IsOutdated = true
	return th
}

// verdictRechecked is verdict with the given authors having reviewed this head.
func verdictRechecked(t *testing.T, rechecked map[string]bool, ths ...github.ReviewThread) gateVerdict {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	markReviewed(g)
	g.feedback.Threads = ths
	g.feedback.RecheckedAuthors = rechecked
	p, _ := s.Policy(context.Background(), r, shaB)
	v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if e != nil {
		t.Fatal(e)
	}
	return v
}

func TestGateOutdatedFindings(t *testing.T) {
	cr := "coderabbitai[bot]"
	for _, tt := range []struct {
		name, want string
		rechecked  map[string]bool
		th         github.ReviewThread
	}{
		{"own outdated block still holds", "blocking_findings:1", nil, outdated(gradedThread("review-bot", "high", false, shaB))},
		{"own outdated unrated finding still needs an answer", "triage_unanswered:1", nil, outdated(gradedThread("review-bot", "", false, shaB))},
		{"third-party outdated block holds until its reviewer rechecks", "blocking_findings:1", nil, outdated(gradedThread(cr, "critical", false, shaB))},
		{"third-party outdated block clears once rechecked", "", map[string]bool{"coderabbitai": true}, outdated(gradedThread(cr, "critical", false, shaB))},
		{"third-party outdated major holds until its reviewer rechecks", "blocking_findings:1", nil, outdated(gradedThread(cr, "major", false, shaB))},
		{"third-party outdated major clears once rechecked", "", map[string]bool{"coderabbitai": true}, outdated(gradedThread(cr, "major", false, shaB))},
		{"third-party outdated p2 still needs an answer", "triage_unanswered:1", nil, outdated(gradedThread("chatgpt-codex-connector[bot]", "p2", false, shaB))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reasons(verdictRechecked(t, tt.rechecked, tt.th)); got != tt.want {
				t.Fatalf("reasons=%q", got)
			}
		})
	}
	// ignore_outdated: false keeps every outdated finding.
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.policyExtra = "review_gate: {land: graded, ignore_outdated: false}\n"
	markReviewed(g)
	g.feedback.RecheckedAuthors = map[string]bool{"coderabbitai": true}
	g.feedback.Threads = []github.ReviewThread{outdated(gradedThread(cr, "critical", false, shaB))}
	p, _ := s.Policy(context.Background(), r, shaB)
	v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if e != nil || reasons(v) != "blocking_findings:1" {
		t.Fatalf("%v %+v", e, v)
	}
}

// A thread with no comment cannot be graded: unresolved it holds, as the raw
// count would, and resolved it is ignored.
func TestGateCommentlessThread(t *testing.T) {
	if got := reasons(verdict(t, gradedPolicy, 0, github.ReviewThread{})); got != "blocking_findings:1" {
		t.Fatalf("unresolved commentless thread: %q", got)
	}
	if got := reasons(verdict(t, gradedPolicy, 0, github.ReviewThread{IsResolved: true})); got != "" {
		t.Fatalf("resolved commentless thread: %q", got)
	}
}

// A follow-up finding posted under the review login (it carries a severity
// marker) is not an answer to the earlier own-review finding.
func TestGateOwnFindingFollowUpFindingIsNotAnAnswer(t *testing.T) {
	th := gradedThread("review-bot", "", false, shaA)
	th.Comments = append(th.Comments, github.ReviewComment{Author: "review-bot", AuthorType: "User", Body: "<!-- mergewake-severity:low -->\nanother finding"})
	if v := verdict(t, "review_gate:\n  land: graded\n", 400, th); reasons(v) != "triage_unanswered:1" {
		t.Fatalf("reasons=%q", reasons(v))
	}
}

// crBody is a CodeRabbit inline finding as CodeRabbit posts it: the severity
// badge and label ahead of the title, no mergewake marker.
func crBody(icon, label string) string {
	return "<!-- coderabbit-ai-generated:v1 -->\n\n**<picture><img src=\"https://storage.googleapis.com/coderabbit_public_assets/review-comment-icons-v1/" + icon + "-16.svg\" width=\"16\" height=\"16\" align=\"absmiddle\" alt=\"\"></picture> " + label + "** · **Functional Correctness** · **Quick win**\n\n**Classify the failure.**\n\nDetails."
}

// thread is an unresolved review thread opened by author with body on head,
// followed by (author, body) replies from human accounts.
func gateThread(author, body string, resolved bool, replies ...[2]string) github.ReviewThread {
	cs := []github.ReviewComment{{Author: author, CommitSHA: shaB, OriginalCommitSHA: shaB, Body: body}}
	for _, r := range replies {
		cs = append(cs, github.ReviewComment{Author: r[0], AuthorType: "User", Body: r[1]})
	}
	return github.ReviewThread{IsResolved: resolved, Comments: cs}
}

// The severity-aware gate table (hk 2481 acceptance 4): mergewake
// critical/high and CodeRabbit Critical/Major hold until fixed or answered with
// a reasoned disposition; mergewake medium/low and CodeRabbit Minor/Nitpick
// never hold and are not counted as unanswered; an answered-but-unresolved
// high with a disposition is answered, not unanswered.
func TestGateSeverityTable(t *testing.T) {
	const cr = "coderabbitai[bot]"
	own := func(sev string) string { return "<!-- mergewake-severity:" + sev + " -->\nthe finding" }
	for _, tt := range []struct {
		name, want                                    string
		th                                            github.ReviewThread
		openBlocking, openTriage, answered, dismissed int
	}{
		{"own high unanswered blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false), 1, 0, 0, 0},
		{"own critical unanswered blocks", "blocking_findings:1", gateThread("review-bot", own("critical"), false), 1, 0, 0, 0},
		{"CodeRabbit Critical unanswered blocks", "blocking_findings:1", gateThread(cr, crBody("critical", "Critical"), false), 1, 0, 0, 0},
		{"CodeRabbit Major unanswered blocks", "blocking_findings:1", gateThread(cr, crBody("major", "Major"), false), 1, 0, 0, 0},
		{"CodeRabbit Minor unanswered neither blocks nor counts", "", gateThread(cr, crBody("amber", "Minor"), false), 0, 0, 0, 0},
		{"CodeRabbit Nitpick unanswered neither blocks nor counts", "", gateThread(cr, crBody("nitpick", "Nitpick"), false), 0, 0, 0, 0},
		{"own medium unanswered neither blocks nor counts", "", gateThread("review-bot", own("medium"), false), 0, 0, 0, 0},
		{"own low unanswered neither blocks nor counts", "", gateThread("review-bot", own("low"), false), 0, 0, 0, 0},
		{"answered-but-unresolved high with a fix is answered", "", gateThread("review-bot", own("high"), false, [2]string{"review-bot", "Fixed in 81149ea: the budget is spent after delivery."}), 0, 0, 1, 0},
		{"answered-but-unresolved high with a reasoned dismissal is answered", "", gateThread("review-bot", own("high"), false, [2]string{"dev", "Dismiss: the path is test-only; see config_test.go:41."}), 0, 0, 1, 1},
		{"fleet-review dismissal counts", "", gateThread("review-bot", own("high"), false, [2]string{"dev", "Disposition: dismiss with evidence: config_test.go:41 proves the path is test-only."}), 0, 0, 1, 1},
		{"fleet-review fixed commit counts", "", gateThread("review-bot", own("high"), false, [2]string{"review-bot", "Disposition: fixed in 81149ea: budget spent after delivery."}), 0, 0, 1, 0},
		{"fleet-review fixed shorthand counts", "", gateThread("review-bot", own("high"), false, [2]string{"review-bot", "Disposition: fixed 81149ea."}), 0, 0, 1, 0},
		{"fleet-review defer still blocks high", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Disposition: defer to hk 2490."}), 1, 0, 0, 0},
		{"fleet-review bare dismiss still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Disposition: dismiss."}), 1, 0, 0, 0},
		{"fleet-review unverified fix still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Disposition: fixed."}), 1, 0, 0, 0},
		{"fleet-review bot dismissal still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"ci[bot]", "Disposition: dismiss: see config_test.go:41."}), 1, 0, 0, 0},
		{"later objection supersedes fleet dismissal", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Disposition: dismiss: see config_test.go:41."}, [2]string{"qa", "That evidence is wrong."}), 1, 0, 0, 0},
		{"CodeRabbit Critical fixed by the author is answered", "", gateThread(cr, crBody("critical", "Critical"), false, [2]string{"dev", "Fixed in 6d7d76d."}), 0, 0, 1, 0},
		{"CodeRabbit Major fixed by the author is answered unresolved", "", gateThread(cr, crBody("major", "Major"), false, [2]string{"dev", "Fixed in 81149ea."}), 0, 0, 1, 0},
		{"CodeRabbit Major dismissed with a reason is answered unresolved", "", gateThread(cr, crBody("major", "Major"), false, [2]string{"dev", "Dismiss: the field is never user input; see parse.go:12."}), 0, 0, 1, 1},
		{"CodeRabbit Major deferred still blocks", "blocking_findings:1", gateThread(cr, crBody("major", "Major"), false, [2]string{"dev", "Deferred to hk 2490."}), 1, 0, 0, 0},
		{"high acknowledged without a disposition still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Looking into it."}), 1, 0, 0, 0},
		{"high dismissed without a reason still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Dismissed."}), 1, 0, 0, 0},
		{"high fixed without a commit still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Fixed."}), 1, 0, 0, 0},
		{"high deferred still blocks", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Deferred to hk 2490."}), 1, 0, 0, 0},
		{"high disposition from a bot does not count", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"ci[bot]", "Fixed in 81149ea."}), 1, 0, 0, 0},
		{"a later non-disposition answer supersedes a fix", "blocking_findings:1", gateThread("review-bot", own("high"), false, [2]string{"dev", "Fixed in 81149ea."}, [2]string{"qa", "That fix is wrong: the bug is still there."}), 1, 0, 0, 0},
		{"a later fix supersedes an acknowledgement", "", gateThread("review-bot", own("high"), false, [2]string{"dev", "Looking into it."}, [2]string{"dev", "Fixed in 81149ea."}), 0, 0, 1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := verdict(t, gradedPolicy, 0, tt.th)
			if got := reasons(v); got != tt.want {
				t.Fatalf("reasons=%q want %q", got, tt.want)
			}
			d := v.Review.Findings
			if d.OpenBlocking != tt.openBlocking || d.OpenTriage != tt.openTriage || d.Answered != tt.answered || d.DismissedBlocking != tt.dismissed {
				t.Fatalf("dispositions %+v", d)
			}
		})
	}
}

// A fix reply clears a block finding only when the commit it names exists,
// comes after the head the finding was posted on, and is in the PR head's
// history: "Fixed in deadbee" is not a fix. A lookup GitHub cannot answer
// holds the verdict instead of passing it.
func TestGateFixClaimMustBeARealCommitInThePR(t *testing.T) {
	own := "<!-- mergewake-severity:high -->\nthe finding"
	const fix = "81149ea"
	for _, tt := range []struct {
		name, reply, want string
		tweak             func(*fakeGH)
		fails             bool
	}{
		{"a commit after the finding, in the head, clears", "Fixed in " + fix + ".", "", nil, false},
		{"a commit GitHub does not know still blocks", "Fixed in deadbee.", "blocking_findings:1", func(g *fakeGH) {
			g.commitErr = map[string]error{"deadbee": &github.APIError{Code: 422, Method: "GET", Path: "/commits"}}
		}, false},
		{"a 404 commit still blocks", "Fixed in deadbee.", "blocking_findings:1", func(g *fakeGH) {
			g.commitErr = map[string]error{"deadbee": &github.APIError{Code: 404, Method: "GET", Path: "/commits"}}
		}, false},
		{"a commit outside the PR head's history still blocks", "Fixed in " + fix + ".", "blocking_findings:1", func(g *fakeGH) {
			g.notAncestor = map[[2]string]bool{{fix, shaA}: true}
		}, false},
		{"a commit from before the finding still blocks", "Fixed in " + fix + ".", "blocking_findings:1", func(g *fakeGH) {
			g.notAncestor = map[[2]string]bool{{shaB, fix}: true}
		}, false},
		{"the head the finding was posted on is no fix", "Fixed in " + shaB[:12] + ".", "blocking_findings:1", func(g *fakeGH) {
			g.commitErr = nil
		}, false},
		{"any one real commit among several tokens clears", "Fixed in deadbee, then " + fix + ".", "", func(g *fakeGH) {
			g.commitErr = map[string]error{"deadbee": &github.APIError{Code: 422}}
		}, false},
		{"a GitHub outage holds the verdict", "Fixed in " + fix + ".", "", func(g *fakeGH) {
			g.commitErr = map[string]error{fix: errors.New("GitHub GET /commits: HTTP 502")}
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.policyExtra = gradedPolicy
			markReviewed(g)
			g.feedback.Threads = []github.ReviewThread{gateThread("review-bot", own, false, [2]string{"dev", tt.reply})}
			g.findings = 1
			if tt.tweak != nil {
				tt.tweak(g)
			}
			p, e := s.Policy(context.Background(), r, shaB)
			if e != nil {
				t.Fatal(e)
			}
			v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
			if tt.fails {
				if e == nil {
					t.Fatalf("an unverifiable fix passed: %+v", v)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if got := reasons(v); got != tt.want {
				t.Fatalf("reasons=%q want %q", got, tt.want)
			}
		})
	}
}

func TestOwnMediumTriageGateAndNotify(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	sink.recs["w-7"] = integrations.JobRecord{Role: "worker", Label: "author-lane", OwnerLane: "operator-desk", State: "completed"}
	g.enabled = true
	g.policyExtra = gradedPolicy + "blocking:\n  - paths: ['**']\n    reason: effects\n"
	g.files = []policy.File{{Path: "tool", Kind: "modified"}}
	g.feedback.Threads = []github.ReviewThread{gradedThread("review-bot", "medium", false, shaA)}
	g.findings = 1
	p, err := s.Policy(context.Background(), r, shaB)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if err != nil || v.State != "blocked" || reasons(v) != "triage_unanswered:1" {
		t.Fatalf("verdict: %+v, error: %v", v, err)
	}
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.events) != 1 || n.events[0].Lane != "author-lane" || !strings.Contains(n.events[0].Text, "triage_unanswered:1") {
		t.Fatalf("events: %+v", n.events)
	}
}

func TestOwnMediumTriageAnswered(t *testing.T) {
	for _, body := range []string{"Fixed in abc1234", "Dismissed: already covered", "Defer to task 42"} {
		t.Run(body, func(t *testing.T) {
			th := gradedThread("review-bot", "medium", false, shaB)
			th.Comments = append(th.Comments, github.ReviewComment{Author: "dev", AuthorType: "User", Body: body})
			v := verdict(t, gradedPolicy+"blocking:\n  - paths: ['**']\n    reason: effects\n", 10, th)
			if v.State != "ready" || reasons(v) != "" {
				t.Fatalf("%+v", v)
			}
		})
	}
}
