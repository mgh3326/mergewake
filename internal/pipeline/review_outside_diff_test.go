package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

const outsideToolDiff = "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1,2 +1,2 @@\n-old\n+new\n keep\n"

// reviewWith runs one panewire pass whose reviewer answers result against
// outsideToolDiff, and returns what was posted.
func reviewWith(t *testing.T, result string) (*Service, *fakeGH, string) {
	t.Helper()
	runner := &panewireRunner{findings: result, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.diffs = map[string]string{shaA: outsideToolDiff}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	return s, g, filepath.Join(s.state(r.Name), "decisions.jsonl")
}

func anchors(g *fakeGH) []string {
	var out []string
	for _, f := range g.postedFindings {
		out = append(out, fmt.Sprintf("%s:%d:%q", f.Path, f.Line, f.Anchor))
	}
	return out
}

// (a) One finding on a changed line and one on an unchanged line of the same
// file: the pass is posted, the second listed in the review body.
func TestOutsideDiffLineIsListedInTheBody(t *testing.T) {
	_, g, _ := reviewWith(t, `{"findings":[{"path":"tool","line":1,"body":"in the diff","severity":"high"},{"path":"tool","line":40,"body":"unchanged line","severity":"high"}]}`)
	if g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("posts=%d refs=%v", g.postCount, g.refs)
	}
	got := anchors(g)
	if len(got) != 2 || got[0] != `tool:1:""` || got[1] != `tool:40:"outside"` {
		t.Fatalf("anchors=%v", got)
	}
}

// (b) A finding on a file the PR does not touch: the pass is posted and the
// finding rides in the review body; a low one does not hold the gate.
func TestOutsideDiffPathLowIsPostedAndDoesNotHold(t *testing.T) {
	_, g, _ := reviewWith(t, `{"findings":[{"path":"docs/other.md","line":3,"body":"stale doc","severity":"low"}]}`)
	if g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("posts=%d refs=%v", g.postCount, g.refs)
	}
	if got := anchors(g); len(got) != 1 || got[0] != `docs/other.md:3:"outside"` {
		t.Fatalf("anchors=%v", got)
	}
	if got := reasons(outsideVerdict(t, 0, nil)); got != "" {
		t.Fatalf("reasons=%q", got)
	}
}

// outsideGH answers the history with a posted pass that listed blocking
// outside-diff findings, and the PR's issue comments.
type outsideGH struct {
	*fakeGH
	history  []github.ReviewedPass
	comments []github.IssueComment
}

func (g *outsideGH) ReviewHistory(context.Context, string, int, string) ([]github.ReviewedPass, error) {
	return g.history, nil
}
func (g *outsideGH) IssueComments(context.Context, string, int) ([]github.IssueComment, error) {
	return g.comments, nil
}

// outsideVerdict gates a green, reviewed head whose pass on the head listed
// blocking outside-diff findings (on the gated head, or on the earlier head heads names, beside a clean pass pair on the gated one).
func outsideVerdict(t *testing.T, blocking int, comments []github.IssueComment, heads ...string) gateVerdict {
	t.Helper()
	return outsideVerdictFor(t, blocking, "", comments, heads...)
}

// outsideVerdictFor is outsideVerdict where the receipts carry receiptPatch
// instead of the current patch id when it is not empty.
func outsideVerdictFor(t *testing.T, blocking int, receiptPatch string, comments []github.IssueComment, heads ...string) gateVerdict {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.files = []policy.File{{Path: "tool", Kind: "modified", Additions: 1}}
	g.pull.Additions = 1
	markReviewed(g)
	passHead := shaA
	if len(heads) > 0 {
		passHead = heads[0]
	}
	patch := s.patchIDOf(context.Background(), r.Name, 7, shaA)
	if receiptPatch != "" {
		patch = receiptPatch
	}
	history := []github.ReviewedPass{
		{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Lens: "correctness", Patch: patch}},
		{Head: shaA, PassReceipt: github.PassReceipt{Pass: 2, Lens: "state-risk", Patch: patch}},
	}
	if passHead == shaA {
		history[0].OutsideBlocking = blocking
	} else {
		history = append(history, github.ReviewedPass{Head: passHead, PassReceipt: github.PassReceipt{Pass: 1, Lens: "correctness", Patch: patch}, OutsideBlocking: blocking})
	}
	s.GH = &outsideGH{fakeGH: g, comments: comments, history: history}
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

// (c) The same with severity high holds the head.
func TestOutsideDiffHighHoldsTheHead(t *testing.T) {
	_, g, _ := reviewWith(t, `{"findings":[{"path":"docs/other.md","line":3,"body":"wrong claim","severity":"high"}]}`)
	if got := anchors(g); len(got) != 1 || got[0] != `docs/other.md:3:"outside"` {
		t.Fatalf("anchors=%v", got)
	}
	v := outsideVerdict(t, 1, nil)
	if v.State != "blocked" || reasons(v) != "finding_outside_diff:1" {
		t.Fatalf("state=%s reasons=%q", v.State, reasons(v))
	}
	if !authorAction(reasons(v)) {
		t.Fatal("finding_outside_diff is not an author action")
	}
}

func TestOutsideDiffHoldClears(t *testing.T) {
	dismissal := func(login, typ, body string) []github.IssueComment {
		return []github.IssueComment{{Body: body, User: github.PullUser{Login: login, Type: typ}}}
	}
	for _, tt := range []struct {
		name     string
		comments []github.IssueComment
		heads    []string
		want     string
	}{
		{"held", nil, nil, "finding_outside_diff:1"},
		{"a person's dismissal naming the head clears it", dismissal("desk", "User", "Disposition: dismiss: the claim is about a file this PR leaves alone ("+shaA[:7]+")."), nil, ""},
		{"a comment that is not a dismissal holds", dismissal("desk", "User", shaA[:7]+" was dismissed."), nil, "finding_outside_diff:1"},
		{"a dismissal naming no head holds", dismissal("desk", "User", "Disposition: dismiss: not ours."), nil, "finding_outside_diff:1"},
		{"a dismissal naming another head holds", dismissal("desk", "User", "Disposition: dismiss: not ours. "+shaB[:7]), nil, "finding_outside_diff:1"},
		{"a bot's dismissal holds", dismissal("ci[bot]", "Bot", "Disposition: dismiss: not ours. "+shaA[:7]), nil, "finding_outside_diff:1"},
		{"the review identity cannot dismiss itself", dismissal("review-bot", "User", "Disposition: dismiss: not ours. "+shaA[:7]), nil, "finding_outside_diff:1"},
		// A finding listed on an earlier head does not hold a new head, which
		// carries its own pass.
		{"an earlier head's finding does not hold the new head", nil, []string{shaB}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reasons(outsideVerdict(t, 1, tt.comments, tt.heads...)); got != tt.want {
				t.Fatalf("reasons=%q want %q", got, tt.want)
			}
		})
	}
}

// (d) One finding with an empty body among valid ones: only it is dropped,
// with an audit record, and the pass is posted with the rest.
func TestOutsideDiffEmptyBodyIsDroppedAndAudited(t *testing.T) {
	s, g, audit := reviewWith(t, `{"findings":[{"path":"tool","line":1,"body":"first","severity":"low"},{"path":"docs/x.md","line":9,"body":"  \n","severity":"high"},{"path":"tool","line":2,"body":"third","severity":"low"}]}`)
	if g.postCount != 1 || len(g.postedFindings) != 2 || g.postedFindings[0].Body != "first" || g.postedFindings[1].Body != "third" {
		t.Fatalf("posts=%d findings=%+v", g.postCount, g.postedFindings)
	}
	_, rows := readLines(t, audit)
	var dropped []map[string]any
	for _, row := range rows {
		if row["command"] == "review-finding-dropped" {
			dropped = append(dropped, row["value"].(map[string]any))
		}
	}
	if len(dropped) != 1 {
		t.Fatalf("dropped audit=%v", dropped)
	}
	want := map[string]any{"pr": float64(7), "pass": float64(1), "path": "docs/x.md", "line": float64(9), "reason": "empty body"}
	for k, v := range want {
		if dropped[0][k] != v {
			t.Errorf("%s=%v want %v", k, dropped[0][k], v)
		}
	}
	if dropped[0]["label"] == "" || dropped[0]["label"] == nil {
		t.Errorf("no label: %v", dropped[0])
	}
	_ = s
}

func TestOutsideDiffOverLongBodyIsDropped(t *testing.T) {
	diff := "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1 +1 @@\n-x\n+y\n"
	long := strings.Repeat("x", 4001)
	kept, dropped, e := parseFindings([]byte(`{"findings":[{"path":"a","line":1,"body":"`+long+`"},{"path":"a","line":1,"body":"`+long[:4000]+`"}]}`), diff)
	if e != nil || len(kept) != 1 || len(dropped) != 1 || dropped[0].Reason != "body over 4000 bytes" {
		t.Fatalf("kept=%d dropped=%+v err=%v", len(kept), dropped, e)
	}
}

// (e) A malformed document still rejects the whole result.
func TestMalformedResultIsStillRejected(t *testing.T) {
	diff := outsideToolDiff
	many := strings.Repeat(`{"path":"tool","line":1,"body":"b"},`, 50) + `{"path":"tool","line":1,"body":"b"}`
	for name, body := range map[string]string{
		"unknown field":   `{"findings":[{"path":"tool","line":1,"body":"b","surprise":1}]}`,
		"51 findings":     `{"findings":[` + many + `]}`,
		"bad severity":    `{"findings":[{"path":"tool","line":1,"body":"ok"},{"path":"tool","line":1,"body":"b","severity":"urgent"}]}`,
		"not JSON":        `findings`,
		"trailing data":   `{"findings":[]} x`,
		"findings absent": `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if kept, dropped, e := parseFindings([]byte(body), diff); e == nil {
				t.Fatalf("accepted: kept=%v dropped=%v", kept, dropped)
			}
		})
	}
	// 50 is the limit, not 49.
	if _, _, e := parseFindings([]byte(`{"findings":[`+many[:len(many)-len(`,{"path":"tool","line":1,"body":"b"}`)]+`]}`), diff); e != nil {
		t.Fatalf("50 findings rejected: %v", e)
	}
	// The reviewer cannot choose an anchor.
	if _, _, e := parseFindings([]byte(`{"findings":[{"path":"tool","line":1,"body":"b","anchor":"file"}]}`), diff); e == nil {
		t.Fatal("anchor accepted from the reviewer")
	}
}

// Only receipts for the current patch count; a head that inherited its review
// (same patch id) carries the inherited pass's outside-diff findings, and an
// unrelated head's pass does not.
func TestOutsideBlockingFollowsInheritedPatch(t *testing.T) {
	history := []github.ReviewedPass{
		{Head: shaB, PassReceipt: github.PassReceipt{Pass: 1, Patch: "p1"}, OutsideBlocking: 2},
		{Head: "c", PassReceipt: github.PassReceipt{Pass: 1, Patch: "p2"}, OutsideBlocking: 5},
		{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Patch: "p1"}, OutsideBlocking: 1},
	}
	if got := outsideBlocking(history, shaA, "p1", shaB); got != 3 {
		t.Fatalf("inherited: %d", got)
	}
	if got := outsideBlocking(history, shaA, "p1", ""); got != 1 {
		t.Fatalf("not inherited: %d", got)
	}
	// The base moved under the same head: the receipt is for another patch.
	if got := outsideBlocking(history, shaA, "p3", ""); got != 0 {
		t.Fatalf("stale patch counted: %d", got)
	}
	// An unreadable diff leaves the patch unknown; the hold stays.
	if got := outsideBlocking(history, shaA, "", ""); got != 1 {
		t.Fatalf("unknown patch: %d", got)
	}
}

// The base moving under a fixed head changes the patch: a pass recorded for the
// old patch no longer holds the head.
func TestOutsideDiffHoldIgnoresAnotherPatch(t *testing.T) {
	if got := reasons(outsideVerdictFor(t, 1, "an-older-patch", nil)); got == "finding_outside_diff:1" {
		t.Fatalf("a stale-patch receipt held the head: %q", got)
	}
}
