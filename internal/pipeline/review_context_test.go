package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

func thread(path string, line int, resolved bool, comments ...github.ReviewComment) github.ReviewThread {
	return github.ReviewThread{Path: path, Line: line, IsResolved: resolved, Comments: comments}
}

func say(author, body string) github.ReviewComment {
	return github.ReviewComment{Author: author, Body: body}
}

func TestReviewContextIncludesAnsweredFindingsAndDecisions(t *testing.T) {
	body := "Summary\n\n## Operator decisions\n- quota_exclude stays as is\n- no retry on 429\n\n## Test plan\nrun it\n"
	fb := github.ReviewFeedback{Threads: []github.ReviewThread{
		thread("a.go", 3, true, say("review-bot", "<!-- mergewake-severity:high -->\nquota_exclude is unused"), say("pr-author", "intended: operator decision")),
		thread("b.go", 9, false, say("review-bot", "only the bot spoke here")),
		thread("c.go", 4, false, say("review-bot", "first"), say("review-bot", "second by the same bot")),
	}}
	rc := buildReviewContext(body, fb)
	if rc.answered != 1 || rc.truncated {
		t.Fatal(rc.answered, rc.truncated)
	}
	for _, want := range []string{"ALREADY ANSWERED ON THIS PR", "a.go:3 (resolved)", "quota_exclude is unused", "intended: operator decision", "new, concrete reason", "quote the answer you dispute", "OPERATOR DECISIONS", "- quota_exclude stays as is", "- no retry on 429"} {
		if !strings.Contains(rc.text, want) {
			t.Fatalf("missing %q in:\n%s", want, rc.text)
		}
	}
	for _, unwanted := range []string{"b.go", "c.go", "mergewake-severity", "Test plan"} {
		if strings.Contains(rc.text, unwanted) {
			t.Fatalf("unexpected %q in:\n%s", unwanted, rc.text)
		}
	}
}

func TestReviewContextEmptyWithoutAnswersOrDecisions(t *testing.T) {
	fb := github.ReviewFeedback{Threads: []github.ReviewThread{thread("a.go", 1, false, say("review-bot", "alone"))}}
	if rc := buildReviewContext("plain body\n## Operator notes\nx", fb); rc.text != "" || rc.answered != 0 {
		t.Fatal(rc)
	}
}

func TestOperatorDecisionsSection(t *testing.T) {
	for _, tt := range []struct{ name, body, want string }{
		{"any level", "# Operator decisions\nkeep\n# Next\nno", "# Operator decisions\nkeep"},
		{"case and colon", "### operator Decisions:\nkeep", "### operator Decisions:\nkeep"},
		{"subsections stay", "## Operator decisions\na\n### Detail\nb\n## After\nc", "## Operator decisions\na\n### Detail\nb"},
		{"crlf", "## Operator decisions\r\nkeep\r\n## End\r\n", "## Operator decisions\nkeep"},
		{"fenced heading is text", "```\n## Operator decisions\nfake\n```\n", ""},
		{"other fence marker inside a block", "```\n~~~\n```\n## Operator decisions\nkeep", "## Operator decisions\nkeep"},
		{"short fence inside a longer one", "````\n```\n## Operator decisions\nfake\n```\n````\n## Operator decisions\nkeep", "## Operator decisions\nkeep"},
		{"tilde fence does not close a backtick fence", "```\n~~~\n## Operator decisions\nfake\n", ""},
		{"closing fence may be longer", "```\ncode\n`````\n## Operator decisions\nkeep", "## Operator decisions\nkeep"},
		{"bare empty heading ends the section", "## Operator decisions\nkeep\n##\nafter", "## Operator decisions\nkeep"},
		{"hash without space is text", "## Operator decisions\nkeep\n#tag\nstill", "## Operator decisions\nkeep\n#tag\nstill"},
		{"setext level 1", "Operator decisions\n=====\nkeep\nSub\n----\nmore\nNext\n====\nno", "Operator decisions\n=====\nkeep\nSub\n----\nmore"},
		{"setext level 2 ends a level 2 section", "## Operator decisions\nkeep\nNext\n---\nno", "## Operator decisions\nkeep"},
		{"rule after a blank line is not a heading", "## Operator decisions\nkeep\n\n---\nmore", "## Operator decisions\nkeep\n\n---\nmore"},
		{"list item over a rule is not a heading", "## Operator decisions\n- keep\n---\nmore", "## Operator decisions\n- keep\n---\nmore"},
		{"absent", "no such section", ""},
	} {
		if got := operatorDecisions(tt.body); got != tt.want {
			t.Errorf("%s: got %q want %q", tt.name, got, tt.want)
		}
	}
}

func TestReviewContextBounds(t *testing.T) {
	var fb github.ReviewFeedback
	for i := 0; i < 60; i++ {
		fb.Threads = append(fb.Threads, thread(fmt.Sprintf("f%02d.go", i), i+1, false,
			say("review-bot", strings.Repeat("finding ", 100)), say("pr-author", strings.Repeat("answer ", 100))))
	}
	body := "## Operator decisions\n" + strings.Repeat("settled ", 2000)
	rc := buildReviewContext(body, fb)
	if len(rc.text) > contextBudget || !rc.truncated || rc.answered == 0 || rc.answered >= 60 {
		t.Fatal(len(rc.text), rc.truncated, rc.answered)
	}
	if !strings.Contains(rc.text, "Operator decisions truncated") || !strings.Contains(rc.text, fmt.Sprintf("%d of 60 answered findings omitted", 60-rc.answered)) {
		t.Fatal(rc.text[len(rc.text)-300:])
	}
	// The decisions section, notice included, stays within its own cap.
	if at, end := strings.Index(rc.text, "## Operator decisions"), strings.Index(rc.text, "\n\nALREADY ANSWERED"); at < 0 || end < 0 || end-at > decisionsBudget {
		t.Fatal(at, end)
	}
	// Newest first: the last thread leads, the oldest is the one dropped.
	if !strings.Contains(rc.text, "f59.go:60") || strings.Contains(rc.text, "f00.go:1 ") {
		t.Fatal("not newest first")
	}
	// Quoted excerpts are bounded.
	for _, l := range strings.Split(rc.text, "\n") {
		if strings.HasPrefix(l, "   finding: ") && len([]rune(l)) > len("   finding: ")+excerptLimit+1 {
			t.Fatal(len(l))
		}
	}
}

func TestReviewPromptCarriesContextAndReceiptRecordsIt(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.pull.Body = "## Operator decisions\nkeep quota_exclude"
	g.feedback.Threads = []github.ReviewThread{
		thread("a.go", 3, true, say("other-bot", "quota_exclude flagged"), say("pr-author", "intended")),
		thread("b.go", 2, false, say("review-bot", "bot alone")),
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	runner := s.Runner.(*fakeRunner)
	diffAt := strings.Index(runner.input, "UNTRUSTED DIFF:")
	for _, want := range []string{"a.go:3", "keep quota_exclude"} {
		at := strings.Index(runner.input, want)
		if at < 0 || at > diffAt {
			t.Fatalf("%q missing or after the diff marker:\n%s", want, runner.input)
		}
	}
	if strings.Contains(runner.input, "b.go") {
		t.Fatal("bot-only thread leaked into the prompt")
	}
	if got := g.passes[g.pull.Head.SHA]; len(got) != 1 || got[0].AnsweredContext != 1 {
		t.Fatal(got)
	}
}

func TestReviewPromptUnchangedWithoutContext(t *testing.T) {
	s, _, _, _, r := setup(t)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(s.Runner.(*fakeRunner).input, "PR CONTEXT") {
		t.Fatal("context added to a PR with nothing answered")
	}
}

func TestWithContextNeverDropsContext(t *testing.T) {
	if got := withContext("head\nUNTRUSTED DIFF:\n", "CTX\n"); got != "head\nCTX\nUNTRUSTED DIFF:\n" {
		t.Fatal(got)
	}
	if got := withContext("no marker", "CTX\n"); got != "CTX\nno marker" {
		t.Fatal(got)
	}
	if got := withContext("p", ""); got != "p" {
		t.Fatal(got)
	}
}

func TestReviewContextKeepsDecisionsWhenThreadReadFails(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.pull.Body = "## Operator decisions\nkeep quota_exclude"
	s.GH = &feedbackFailGH{g}
	var out strings.Builder
	s.Out = &out
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	in := s.Runner.(*fakeRunner).input
	if !strings.Contains(in, "keep quota_exclude") || strings.Contains(in, "ALREADY ANSWERED") {
		t.Fatal(in)
	}
	if !strings.Contains(out.String(), `"context_error"`) || !strings.Contains(out.String(), `"answered_context":0`) {
		t.Fatal(out.String())
	}
}

type feedbackFailGH struct{ *fakeGH }

func (feedbackFailGH) ReviewFeedback(context.Context, string, int) (github.ReviewFeedback, error) {
	return github.ReviewFeedback{}, fmt.Errorf("graphql down")
}

func TestReviewContextQuotesLatestReplyIncludingRebuttal(t *testing.T) {
	fb := github.ReviewFeedback{Threads: []github.ReviewThread{
		thread("a.go", 3, false, say("review-bot", "flag"), say("pr-author", "intended"), say("review-bot", "still wrong: the answer ignores X")),
		thread("b.go", 4, false, say("review-bot", "flag"), say("pr-author", "intended"), say("pr-author", "   ")),
	}}
	rc := buildReviewContext("", fb)
	if rc.answered != 2 {
		t.Fatal(rc.answered)
	}
	// Newest thread first: b.go keeps the last nonempty comment (the answer).
	b, a := strings.Index(rc.text, "b.go:4"), strings.Index(rc.text, "a.go:3")
	if b < 0 || a < b {
		t.Fatal(rc.text)
	}
	if !strings.Contains(rc.text, "latest reply (by the finding's author, disputing the answer): still wrong: the answer ignores X") {
		t.Fatal(rc.text)
	}
	if strings.Contains(rc.text[b:a], "disputing") || !strings.Contains(rc.text[b:a], "latest reply: intended") {
		t.Fatal(rc.text)
	}
}

// slowFeedbackGH blocks the thread read until its context ends.
type slowFeedbackGH struct{ *fakeGH }

func (slowFeedbackGH) ReviewFeedback(ctx context.Context, _ string, _ int) (github.ReviewFeedback, error) {
	<-ctx.Done()
	return github.ReviewFeedback{}, ctx.Err()
}

func TestSlowFeedbackReadDoesNotSpendTheReviewDeadline(t *testing.T) {
	old := feedbackReadLimit
	feedbackReadLimit = 30 * time.Millisecond
	defer func() { feedbackReadLimit = old }()
	s, g, _, _, r := setup(t)
	g.pull.Body = "## Operator decisions\nkeep quota_exclude"
	s.GH = slowFeedbackGH{g}
	var out strings.Builder
	s.Out = &out
	start := time.Now()
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if time.Since(start) > 3*time.Second || g.postCount != 1 {
		t.Fatal(time.Since(start), g.postCount)
	}
	if !strings.Contains(out.String(), "context deadline exceeded") || !strings.Contains(s.Runner.(*fakeRunner).input, "keep quota_exclude") {
		t.Fatal(out.String())
	}
}

func TestFeedbackTimeoutIsBoundedByReviewTimeout(t *testing.T) {
	if feedbackTimeout(time.Hour) != feedbackReadLimit || feedbackTimeout(8*time.Second) != 2*time.Second {
		t.Fatal(feedbackTimeout(time.Hour), feedbackTimeout(8*time.Second))
	}
}
