package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

func TestBlockNoticeOncePerPRUntilBodyChanges(t *testing.T) {
	f := blockSetup(t)
	b := reviewBlock{reason: blockMissingTrailer, fix: "add task trailer"}
	if err := f.s.notifyReviewBlock(context.Background(), f.r, f.g.pull, b); err != nil {
		t.Fatal(err)
	}
	f.g.pull.Head.SHA = shaB
	if err := f.s.notifyReviewBlock(context.Background(), f.r, f.g.pull, b); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 1 {
		t.Fatalf("same block was delivered %d times, want 1", f.n.count)
	}
	if rows := auditRows(t, f.s, f.r, "review-blocked-suppressed"); len(rows) != 1 {
		t.Fatalf("suppressed notice audit rows = %v, want one", rows)
	}
	f.g.pull.Body += "\nbody changed"
	if err := f.s.notifyReviewBlock(context.Background(), f.r, f.g.pull, b); err != nil {
		t.Fatal(err)
	}
	if f.n.count != 2 {
		t.Fatalf("changed body notices = %d, want 2", f.n.count)
	}
}

func TestWorkerDeferAcceptsExistingOtherTaskAndRejectsOwnTask(t *testing.T) {
	thread := ownFinding("T1", "low", 3, github.ReviewComment{Author: "worker", AuthorType: "User", Body: "Disposition: defer 42"})
	exists := func(id string) bool { return id == "42" || id == "7" }
	rule := &deferTaskRule{own: "7", exists: exists}
	if !answeredWithDefer(thread, false, rule) {
		t.Fatal("worker-filed task disposition was not accepted")
	}
	thread.Comments[1].Body = "Disposition: defer 7"
	if answeredWithDefer(thread, false, rule) {
		t.Fatal("PR's own task disposition was accepted")
	}
}

func TestLostReviewBurstNotifiesOneDesignIncident(t *testing.T) {
	s, g, n, r := notifySetup(t)
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	for i := 1; i <= 3; i++ {
		plan := &passPlan{pass: i, lens: "default"}
		lost := &reviewResultLost{label: "reviewer", cause: errors.New("result file missing")}
		if err := s.notifyReviewResultLost(context.Background(), r, g.pull, plan, lost); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Minute)
	}
	if n.count != 1 || n.events[0].Lane != s.deskLane() {
		t.Fatalf("burst notifications = %+v, want one design lane event", n.events)
	}
	if rows := auditRows(t, s, r, "review-result-lost-suppressed"); len(rows) != 2 {
		t.Fatalf("suppressed incident audit rows = %v, want two", rows)
	}
}

func TestWatchHoldsNotifyOncePerRedEpisode(t *testing.T) {
	s, _, _, n, r := setup(t)
	for i, head := range []string{"red-a", "red-b", "red-c"} {
		result := watchResult{Head: head, Action: "hold", Reason: "main is red"}
		if err := s.reportWatch(context.Background(), r, result, head+":hold", Options{}); err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			if rows := auditRows(t, s, r, "watch-hold-suppressed"); len(rows) != 2 {
				t.Fatalf("suppressed hold audit rows = %v, want two", rows)
			}
		}
	}
	if n.count != 1 {
		t.Fatalf("red episode notifications = %d, want 1", n.count)
	}
	if err := s.clearWatchHoldEpisode(r.Name); err != nil {
		t.Fatal(err)
	}
	if err := s.reportWatch(context.Background(), r, watchResult{Head: "green-then-red", Action: "hold", Reason: "new red"}, "green-then-red:hold", Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 2 {
		t.Fatalf("next red episode notifications = %d, want 2", n.count)
	}
}

func TestRoundFactsRecordNewCodeAndRepeatedThread(t *testing.T) {
	prior := []github.ReviewThread{{Path: "a.go", Line: 5, Comments: []github.ReviewComment{{Body: "<!-- mergewake-severity:high -->\nSame bug title"}}}}
	findings := []github.Finding{{Path: "a.go", Line: 5, Severity: "high", Body: "Same bug title"}}
	rp := roundPlan{round: 2, addedAfterFirstDiff: "diff --git a/a.go b/a.go\n+++ b/a.go\n@@ -4,0 +5,1 @@\n+new code\n"}
	facts := roundFindingFacts(findings, prior, rp)
	if len(facts) != 1 || !facts[0].AddedAfterFirst || !facts[0].RepeatsEarlier {
		t.Fatalf("finding facts = %+v, want new and repeated", facts)
	}
	record := passRecord(github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}, github.PassReceipt{Pass: 1, Round: 2, Lens: "default"}, findings, "", time.Now(), time.Now(), facts)
	if len(record.FindingFacts) != 1 || !record.FindingFacts[0].AddedAfterFirst || !record.FindingFacts[0].RepeatsEarlier {
		t.Fatalf("land record finding facts = %+v", record.FindingFacts)
	}
	s, _, _, _, r := setup(t)
	if err := s.auditRoundFindingFacts(r.Name, 7, 2, facts); err != nil {
		t.Fatal(err)
	}
	if rows := auditRows(t, s, r, "review-round-finding"); len(rows) != 1 || rows[0]["added_after_first_head"] != true || rows[0]["repeats_earlier_thread"] != true {
		t.Fatalf("finding audit rows = %v", rows)
	}
}

func TestConflictUsesAuthorRouteThenOneDelayedOwnerNotice(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneA)
	g := s.GH.(*fakeGH)
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	r := testRepoFor(s)
	r.Lane = s.Config.Notifier.Lane
	n := newEscNotifier()
	s.Notifier = n
	s.Config.Daemon.PR.Notify.ConflictOwnerAfter = "30m"
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	route, err := s.findingsRoute(context.Background(), r, g.pull)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.escs(); len(got) != 1 || got[0].Lane != route.lane {
		t.Fatalf("initial conflict route = %+v, want author route %q", got, route.lane)
	}
	now = now.Add(29 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.escs()) != 1 {
		t.Fatalf("owner notice arrived before threshold: %+v", n.escs())
	}
	now = now.Add(time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := n.escs(); len(got) != 1 || len(n.accepted) != 2 || n.accepted[1].Lane != s.ownerLane(r) {
		t.Fatalf("author and owner notifications = %+v, want one author and one owner event", n.accepted)
	}
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.accepted) != 2 {
		t.Fatalf("owner was notified more than once for the PR: %+v", n.accepted)
	}
	if rows := auditRows(t, s, r, "conflict-esc-owner-suppressed"); len(rows) != 1 {
		t.Fatalf("suppressed owner audit rows = %v, want one", rows)
	}
}
