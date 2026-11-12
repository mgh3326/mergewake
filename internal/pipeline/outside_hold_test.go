package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// outsidePass is one posted pass carrying outside-diff findings.
func outsidePass(head, patch string, pass, round int, findings ...github.OutsideFinding) github.ReviewedPass {
	return github.ReviewedPass{
		Head:        head,
		PassReceipt: github.PassReceipt{Pass: pass, Lens: "correctness", Patch: patch, Round: round},
		Outside:     findings,
	}
}

func holdFinding(path string, line int, severity, body string) github.OutsideFinding {
	return github.OutsideFinding{
		Ref:      github.OutsideRef(path, line, severity, false, body),
		Path:     path,
		Line:     line,
		Severity: severity,
		Body:     body,
	}
}

// holdVerdict gates the fixture head shaA. history lists the PR's posted
// passes oldest first. When reviewed is set the current head carries its own
// completed pass pair carrying currentFindings, so its review settles;
// otherwise it is unreviewed and review_missing joins the verdict.
func holdVerdict(t *testing.T, history []github.ReviewedPass, comments []github.IssueComment, reviewed bool, currentFindings ...github.OutsideFinding) gateVerdict {
	t.Helper()
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.files = []policy.File{{Path: "tool", Kind: "modified", Additions: 1}}
	g.pull.Additions = 1
	patch := s.patchIDOf(context.Background(), r.Name, 7, shaA)
	if reviewed {
		markReviewed(g)
		outside := currentFindings
		history = append(history,
			github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Lens: "correctness", Patch: patch, Round: 2}, Outside: outside},
			github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 2, Lens: "state-risk", Patch: patch, Round: 2}},
		)
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

func holdComment(login, typ, body string) []github.IssueComment {
	return []github.IssueComment{{Body: body, User: github.PullUser{Login: login, Type: typ}}}
}

func openOutsideRefs(v gateVerdict) []string {
	var out []string
	for _, f := range v.Outside {
		if f.State == "open" {
			out = append(out, f.Ref)
		}
	}
	return out
}

// (a) A high outside-diff finding posted on an earlier head still holds after
// a new head is pushed and not yet reviewed.
func TestOutsideHoldSurvivesNewHead(t *testing.T) {
	f := holdFinding("docs/x.md", 3, "high", "the stale claim about the config")
	history := []github.ReviewedPass{outsidePass(shaC, "p-old", 1, 1, f)}
	v := holdVerdict(t, history, nil, false)
	if got := reasons(v); !strings.Contains(got, "finding_outside_diff:1") {
		t.Fatalf("reasons=%q", got)
	}
	refs := openOutsideRefs(v)
	if len(refs) != 1 || refs[0] != f.Ref {
		t.Fatalf("open=%v want %s", refs, f.Ref)
	}
}

// (b) An accepted answer naming the finding clears the hold; anything else
// leaves it.
func TestOutsideHoldAnswerForms(t *testing.T) {
	f := holdFinding("docs/x.md", 3, "high", "the stale claim about the config")
	for _, tt := range []struct {
		name     string
		comments []github.IssueComment
		wantHold bool
	}{
		{"Fixed in a verified commit", holdComment("pr-author", "User", "Fixed in bbbbbbb: "+f.Ref+" the flag is gone"), false},
		{"Dismiss with evidence", holdComment("pr-author", "User", "Dismiss: "+f.Ref+" — the file is generated, not source"), false},
		{"Disposition defer a task", holdComment("pr-author", "User", "Disposition: defer 4253 — "+f.Ref), false},
		{"names it by location", holdComment("pr-author", "User", "Dismiss: docs/x.md:3 is intentional"), false},
		{"names it by title", holdComment("pr-author", "User", "Dismiss: the stale claim about the config is intended"), false},
		{"a reply without an accepted form holds", holdComment("pr-author", "User", "noted, will look: "+f.Ref), true},
		{"an accepted form naming nothing holds", holdComment("pr-author", "User", "Dismiss: unrelated thread"), true},
		{"a bot cannot answer", holdComment("ci[bot]", "Bot", "Dismiss: "+f.Ref), true},
		{"the review identity cannot answer itself", holdComment("review-bot", "User", "Dismiss: "+f.Ref), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			history := []github.ReviewedPass{outsidePass(shaC, "p-old", 1, 1, f)}
			v := holdVerdict(t, history, tt.comments, false)
			got := reasons(v)
			if tt.wantHold && !strings.Contains(got, "finding_outside_diff:1") {
				t.Fatalf("reasons=%q want the hold", got)
			}
			if !tt.wantHold && strings.Contains(got, "finding_outside_diff") {
				t.Fatalf("reasons=%q want the hold cleared", got)
			}
		})
	}
}

// (c) A completed review pass on a later head that does not report the finding
// again clears it; one that reports the same content again keeps it open.
func TestOutsideHoldReReviewedClears(t *testing.T) {
	f := holdFinding("docs/x.md", 3, "high", "the stale claim about the config")
	history := []github.ReviewedPass{outsidePass(shaC, "p-old", 1, 1, f)}
	v := holdVerdict(t, history, nil, true)
	if got := reasons(v); strings.Contains(got, "finding_outside_diff") {
		t.Fatalf("reasons=%q want the hold cleared by re-review", got)
	}
	if v.State != "ready" {
		t.Fatalf("state=%s reasons=%q", v.State, reasons(v))
	}

	// The new head's own pass reports the same content again: same ref, still
	// open, still holding.
	v = holdVerdict(t, history, nil, true, f)
	if got := reasons(v); !strings.Contains(got, "finding_outside_diff:1") {
		t.Fatalf("re-report reasons=%q want the hold", got)
	}
}

// (d) The next round's reviewer prompt carries the still-open outside-diff
// finding, marked as outside the diff.
func TestRoundTwoPromptCarriesOpenOutsideFinding(t *testing.T) {
	f := holdFinding("docs/x.md", 3, "high", "the stale claim about the config")
	s, rg, run, r := roundSetup(t, []github.ReviewedHead{{SHA: shaC, Round: 1}}, patchOne, patchTwo, fsjson())
	s.GH = &holdRoundGH{roundGH: rg, history: []github.ReviewedPass{outsidePass(shaC, "p-1", 1, 1, f)}}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	ctx := run.prompt[:strings.Index(run.prompt, "## Delta to review")]
	for _, want := range []string{
		"docs/x.md:3", "outside the diff", "the stale claim about the config", "none yet (still open)",
	} {
		if !strings.Contains(ctx, want) {
			t.Fatalf("missing %q in:\n%s", want, ctx)
		}
	}
}

// holdRoundGH serves a fixed review history and issue comments to Review.
type holdRoundGH struct {
	*roundGH
	history  []github.ReviewedPass
	comments []github.IssueComment
}

func (g *holdRoundGH) ReviewHistory(context.Context, string, int, string) ([]github.ReviewedPass, error) {
	return g.history, nil
}
func (g *holdRoundGH) IssueComments(context.Context, string, int) ([]github.IssueComment, error) {
	return g.comments, nil
}

// (e) hk 3190 stands: a pass on the current head carrying a different patch id
// does not count as the head's own, so its outside findings neither hold nor
// are tracked.
func TestOutsideHoldIgnoresStalePatchReceipt(t *testing.T) {
	f := holdFinding("docs/x.md", 3, "high", "the stale claim about the config")
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.files = []policy.File{{Path: "tool", Kind: "modified", Additions: 1}}
	g.pull.Additions = 1
	markReviewed(g)
	patch := s.patchIDOf(context.Background(), r.Name, 7, shaA)
	history := []github.ReviewedPass{
		outsidePass(shaA, "an-older-patch", 1, 2, f),
		outsidePass(shaA, patch, 2, 2),
	}
	s.GH = &outsideGH{fakeGH: g, history: history}
	p, e := s.Policy(context.Background(), r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if e != nil {
		t.Fatal(e)
	}
	if got := reasons(v); strings.Contains(got, "finding_outside_diff") {
		t.Fatalf("reasons=%q want no outside hold", got)
	}
	if len(v.Outside) != 0 {
		t.Fatalf("tracked %+v want none", v.Outside)
	}
}

// (f) Every state change of an outside-diff finding leaves an audit line:
// open when first posted, answered by a person's comment, re-reviewed when a
// later completed pass does not report it again.
func TestOutsideHoldAuditStates(t *testing.T) {
	s, g, _, r := notifySetup(t)
	f := holdFinding("docs/x.md", 3, "high", "the stale claim about the config")
	history := []github.ReviewedPass{outsidePass(shaC, "p-old", 1, 1, f)}
	oh := &outsideGH{fakeGH: g, history: history}
	s.GH = oh

	outsideRows := func() []map[string]any {
		b, e := os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl"))
		if e != nil {
			t.Fatal(e)
		}
		return auditRecords(t, b, "outside-finding")
	}

	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if rows := outsideRows(); len(rows) != 1 || rows[0]["state"] != "open" || rows[0]["ref"] != f.Ref {
		t.Fatalf("rows=%v", rows)
	}

	// A person answers it by ref; the hold lifts and the answer is audited.
	oh.comments = holdComment("pr-author", "User", "Dismiss: "+f.Ref+" — generated file")
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if rows := outsideRows(); len(rows) != 2 || rows[1]["state"] != "answered" || rows[1]["via"] != "comment" {
		t.Fatalf("rows=%v", rows)
	}

	// The new head's completed review does not report it again: re-reviewed.
	patch := s.patchIDOf(context.Background(), r.Name, 7, shaA)
	oh.history = append(history,
		github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Lens: "correctness", Patch: patch, Round: 2}},
		github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 2, Lens: "state-risk", Patch: patch, Round: 2}},
	)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if rows := outsideRows(); len(rows) != 3 || rows[2]["state"] != "re-reviewed" || rows[2]["via"] != "rereview" {
		t.Fatalf("rows=%v", rows)
	}
}
