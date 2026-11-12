package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// holdThread is an unresolved review thread with id, opened by author with body
// on commit, followed by human replies in order.
func holdThread(id, author, body, commit, url string, replies ...string) github.ReviewThread {
	cs := []github.ReviewComment{{Author: author, CommitSHA: commit, OriginalCommitSHA: commit, Body: body, URL: url}}
	for _, r := range replies {
		cs = append(cs, github.ReviewComment{Author: "dev", AuthorType: "User", Body: r})
	}
	return github.ReviewThread{ID: id, Path: "tool/a.go", Line: 12, Comments: cs}
}

// mwHigh is a mergewake high finding on commit; the fake PR head is shaA and
// shaC stands in for an earlier head.
func mwHigh(id, commit, url string, replies ...string) github.ReviewThread {
	return holdThread(id, "review-bot", "<!-- mergewake-severity:high -->\nfinding", commit, url, replies...)
}

// notifyBlocked runs NotifyPR on a graded, reviewed head holding threads and
// returns the last event text.
func notifyBlocked(t *testing.T, threads ...github.ReviewThread) string {
	t.Helper()
	s, g, n, r := notifySetup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	g.feedback.Threads = threads
	for _, th := range threads {
		if !th.IsResolved {
			g.findings++
		}
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 {
		t.Fatalf("events=%+v", n.events)
	}
	return lastEvent(t, n)
}

// (a) The BLOCKED event lists every thread holding the head, names the reply
// that is not an accepted disposition, and does not claim the latest pass is
// clean while a thread is posted on the head.
func TestBlockedNoticeListsHoldingThreads(t *testing.T) {
	const cr = "coderabbitai[bot]"
	ev := notifyBlocked(t,
		mwHigh("t1", shaC, "https://example.test/pull/7#r1", "Update (fixed the nil path)"),
		mwHigh("t2", shaA, "https://example.test/pull/7#r2"),
		holdThread("t3", cr, crBody("major", "Major"), shaA, "https://example.test/pull/7#r3"),
	)
	for _, want := range []string{
		"blocking_findings:3",
		"https://example.test/pull/7#r1",
		"https://example.test/pull/7#r2",
		"https://example.test/pull/7#r3",
		"Update (fixed the nil path)",
		"reply not accepted",
		"Fixed in",
		"Dismiss:",
	} {
		if !strings.Contains(ev, want) {
			t.Fatalf("event missing %q:\n%s", want, ev)
		}
	}
	if strings.Contains(ev, "raised no new findings") {
		t.Fatalf("clean-pass claim while a thread is posted on the head:\n%s", ev)
	}
	if strings.Count(ev, "reply not accepted") != 1 {
		t.Fatalf("not-accepted note on more than the one replied thread:\n%s", ev)
	}
}

// (b) When the head's latest review pass raised nothing new and only threads
// from an earlier head hold, the event says so explicitly.
func TestBlockedNoticeCleanLatestPassSaysSo(t *testing.T) {
	ev := notifyBlocked(t,
		mwHigh("t1", shaC, "https://example.test/pull/7#r1"),
	)
	if !strings.Contains(ev, "blocking_findings:1") {
		t.Fatalf("%s", ev)
	}
	if !strings.Contains(ev, "latest review pass on "+shaA[:7]+" raised no new findings") {
		t.Fatalf("clean latest pass not stated:\n%s", ev)
	}
}

// (c) More than ten holding threads list the first ten and count the rest.
func TestBlockedNoticeCapsListAtTen(t *testing.T) {
	var threads []github.ReviewThread
	for i := 1; i <= 12; i++ {
		threads = append(threads, mwHigh(fmt.Sprintf("t%02d", i), shaA, fmt.Sprintf("https://example.test/pull/7#r%02d", i)))
	}
	ev := notifyBlocked(t, threads...)
	if !strings.Contains(ev, "https://example.test/pull/7#r10") {
		t.Fatalf("tenth thread not listed:\n%s", ev)
	}
	if strings.Contains(ev, "https://example.test/pull/7#r11") {
		t.Fatalf("eleventh thread listed:\n%s", ev)
	}
	if !strings.Contains(ev, "and 2 more") {
		t.Fatalf("overflow count missing:\n%s", ev)
	}
}

// (d) The reason vocabulary and the event id stay as they were: the id still
// carries the head and the summary is still the bare reason list.
func TestBlockedNoticeKeepsReasonsAndID(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.enabled = true
	g.policyExtra = gradedPolicy
	g.feedback.Threads = []github.ReviewThread{mwHigh("t1", shaA, "https://example.test/pull/7#r1")}
	g.findings = 1
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	ev := n.events[len(n.events)-1]
	if !strings.Contains(ev.ID, shaA) {
		t.Fatalf("event id lost the head: %q", ev.ID)
	}
	if strings.Contains(ev.ID, "reply") || strings.Contains(ev.ID, "https") {
		t.Fatalf("detail leaked into event id: %q", ev.ID)
	}
	if !strings.Contains(ev.Text, "BLOCKED example/project#7 head="+shaA[:7]+" | blocking_findings:1") {
		t.Fatalf("reason summary changed:\n%s", ev.Text)
	}
}

// (e) The notice never exceeds the room it is given: the list drops threads
// before the fixed text, and the notice is empty when the fixed text alone
// does not fit.
func TestHoldingNoticeFitsRoom(t *testing.T) {
	var holds []holdingThread
	for i := 1; i <= 12; i++ {
		holds = append(holds, holdingThread{
			Ref: fmt.Sprintf("https://example.test/pull/7#r%02d", i), Author: "review-bot",
			Severity: "high", Location: "tool/a.go:12", ReplyBy: "dev", ReplyBody: strings.Repeat("x", 200),
		})
	}
	v := gateVerdict{Holds: holds, PassClean: true}
	for _, room := range []int{0, 200, 400, 800, 1600, 2000} {
		if got := holdingNotice(v, shaA, room); len(got) > room {
			t.Fatalf("room %d: notice is %d bytes:\n%s", room, len(got), got)
		}
	}
	if got := holdingNotice(v, shaA, 2000); !strings.Contains(got, "a block-class thread clears") || !strings.Contains(got, "raised no new findings") {
		t.Fatalf("fixed text missing at room 2000:\n%s", got)
	}
	if got := holdingNotice(v, shaA, 100); got != "" {
		t.Fatalf("notice at room 100 should be empty:\n%s", got)
	}
}
