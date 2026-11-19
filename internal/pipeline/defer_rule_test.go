package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// The defer rule rides with an author delivery whose head has no open
// critical/high finding. It is text only: the event ID stays the one the named
// threads give, so the rule never changes a delivery's identity.
func TestDeferRuleRidesOnlyWhenNoCriticalOrHighIsOpen(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "medium", 3)}
	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("events=%d, want one", n.count)
	}
	e := n.events[0]
	for _, want := range []string{
		"rule: this head has no open critical/high finding",
		"defer each open medium/low finding to ONE follow-up hk task for this PR",
		`title "PR #7 follow-ups: ..."`,
		`answer each open medium thread "disposition: defer hk <id>"`,
		"push nothing more for them",
		"the PR merges at this head once required checks pass",
		"a failed required check is still fixed in the PR",
	} {
		if !strings.Contains(e.Text, want) {
			t.Fatalf("missing %q in %q", want, e.Text)
		}
	}
	sum := sha256.Sum256([]byte("T1\n"))
	if want := "mergewake-findings-example/project-7-" + hex.EncodeToString(sum[:6]); e.ID != want {
		t.Fatalf("event id %q, want %q: the rule must not change the id", e.ID, want)
	}

	s, g, n, _, r, _ = deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3), ownFinding("T2", "medium", 5)}
	deliver(t, s, r)
	if n.count != 1 || strings.Contains(n.events[0].Text, "rule:") {
		t.Fatalf("a head with an open high carried the defer rule: %+v", n.events)
	}
}

// On a blocking-class PR a medium finding is triage for the graded gate, which
// holds on it until something disposes it. A reply deferring it to an existing
// hk task other than the PR's own, "disposition: defer hk <id>" or the plain
// "deferred to hk <id>", disposes it. A defer to the PR's own task does not.
func TestDeferReplyDisposesMediumOnBlockingClass(t *testing.T) {
	exists := func(string) (bool, error) { return true, nil }
	noFix := func(string, string) bool { return false }
	triageWith := func(replies ...string) int {
		t.Helper()
		var comments []github.ReviewComment
		for _, body := range replies {
			comments = append(comments, github.ReviewComment{Author: "worker", AuthorType: "User", Body: body})
		}
		fb := github.ReviewFeedback{Threads: []github.ReviewThread{ownFinding("T1", "medium", 3, comments...)}}
		rule := &deferTaskRule{own: "7", exists: exists, seen: map[string]bool{}, failed: map[string]error{}}
		g := policy.ReviewGate{}.WithOwnLogin("review-bot")
		_, triage := gradeThreadsWithDefer(fb, g, "review-bot", "blocking", false, noFix, rule)
		if rule.err != nil {
			t.Fatalf("defer lookup error: %v", rule.err)
		}
		return triage
	}
	if got := triageWith(); got != 1 {
		t.Fatalf("unanswered medium on a blocking-class PR: triage=%d, want 1", got)
	}
	for _, reply := range []string{"disposition: defer hk 42", "deferred to hk 42"} {
		if got := triageWith(reply); got != 0 {
			t.Fatalf("reply %q left the medium holding the gate: triage=%d", reply, got)
		}
	}
	if got := triageWith("disposition: defer hk 7"); got != 1 {
		t.Fatalf("a defer to the PR's own task disposed the medium: triage=%d, want 1", got)
	}
}
