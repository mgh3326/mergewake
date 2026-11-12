package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
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
	rule := &deferTaskRule{own: "7", exists: func(id string) (bool, error) { return exists(id), nil }, seen: map[string]bool{}, failed: map[string]error{}}
	if !answeredWithDefer(thread, false, rule) {
		t.Fatal("worker-filed task disposition was not accepted")
	}
	thread.Comments[1].Body = "Disposition: defer 7"
	if answeredWithDefer(thread, false, rule) {
		t.Fatal("PR's own task disposition was accepted")
	}
	thread.Comments[1].Body = "Disposition: defer abc"
	if answeredWithDefer(thread, false, rule) {
		t.Fatal("malformed defer disposition was accepted as an ordinary answer")
	}
	thread.Comments = ownFinding("T1", "low", 3,
		github.ReviewComment{Author: "worker", AuthorType: "User", Body: "I applied the fix"},
		github.ReviewComment{Author: "worker", AuthorType: "User", Body: "Disposition: defer abc"},
	).Comments
	if !answeredWithDefer(thread, false, rule) {
		t.Fatal("an earlier ordinary answer was lost after an invalid defer reply")
	}
}

func TestWorkerDeferTaskReadErrorIsRetained(t *testing.T) {
	thread := ownFinding("T1", "low", 3, github.ReviewComment{Author: "worker", AuthorType: "User", Body: "Disposition: defer 42"})
	rule := &deferTaskRule{own: "7", seen: map[string]bool{}, failed: map[string]error{}, exists: func(string) (bool, error) {
		return false, errors.New("handoffkeep unavailable")
	}}
	if answeredWithDefer(thread, false, rule) || rule.err == nil {
		t.Fatal("task lookup error was treated as an absent task")
	}
}

func TestWorkerDeferMissingTaskIsUnansweredWithoutHoldingGate(t *testing.T) {
	thread := ownFinding("T1", "low", 3, github.ReviewComment{Author: "worker", AuthorType: "User", Body: "Disposition: defer 42"})
	rule := &deferTaskRule{own: "7", seen: map[string]bool{}, failed: map[string]error{}, exists: func(string) (bool, error) {
		return false, errors.New("handoffkeep task not_found: 42")
	}}
	answered := answeredWithDefer(thread, false, rule)
	if answered || rule.err != nil {
		t.Fatalf("missing task disposition should remain unanswered without a lookup failure: %+v", rule)
	}
}

func TestWorkerDeferLookupErrorDoesNotEraseEarlierAnswer(t *testing.T) {
	thread := ownFinding("T1", "low", 3,
		github.ReviewComment{Author: "worker", AuthorType: "User", Body: "I applied the fix"},
		github.ReviewComment{Author: "worker", AuthorType: "User", Body: "Disposition: defer 42"},
	)
	rule := &deferTaskRule{own: "7", seen: map[string]bool{}, failed: map[string]error{}, exists: func(string) (bool, error) {
		return false, errors.New("handoffkeep unavailable")
	}}
	answered := answeredWithDefer(thread, false, rule)
	if !answered || rule.err != nil {
		t.Fatalf("later lookup error erased an earlier answer: answered=%t err=%v", answered, rule.err)
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
	s, _, _, r, _ := feedSetup(t)
	sink := &fakeSink{}
	s.Sink = sink
	n := newEscNotifier()
	s.Notifier = n
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
	if len(n.accepted) != 1 {
		t.Fatalf("red episode notifications = %d, want 1", len(n.accepted))
	}
	if sink.records != 3 {
		t.Fatalf("red episode sink records = %d, want one per held head", sink.records)
	}
	if err := s.reportWatch(context.Background(), r, watchResult{Head: "red-c", Action: "hold", Reason: "main is red"}, "red-c:hold", Options{}); err != nil {
		t.Fatal(err)
	}
	if sink.records != 3 {
		t.Fatalf("same held head wrote another sink record: %d", sink.records)
	}
	if got := outboxLen(t, s.Feed); got != 3 {
		t.Fatalf("red-head watch feed entries = %d, want 3", got)
	}
	if err := s.clearWatchHoldEpisode(r.Name); err != nil {
		t.Fatal(err)
	}
	nextAt := feedNow.Add(time.Second)
	s.Now = func() time.Time { return nextAt }
	if err := s.reportWatch(context.Background(), r, watchResult{Head: "red-a", Action: "hold", Reason: "main is red"}, "red-a:hold", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.accepted) != 2 {
		t.Fatalf("next red episode notifications = %d, want 2", len(n.accepted))
	}
	if n.accepted[0].ID == n.accepted[1].ID {
		t.Fatalf("same-head red episode reused event id %q", n.accepted[0].ID)
	}
}

func TestRoundFactsRecordNewCodeAndRepeatedThread(t *testing.T) {
	prior := []github.ReviewThread{{Path: "a.go", Line: 5, Comments: []github.ReviewComment{{Body: "<!-- mergewake-severity:high -->\nSame bug title"}}}}
	findings := []github.Finding{{Path: "a.go", Line: 5, Severity: "high", Body: "Same bug title"}}
	rp := roundPlan{round: 2, firstDiffKnown: true, addedAfterFirst: map[string]map[int]bool{"a.go": {5: true}}}
	facts := roundFindingFacts(findings, prior, rp, true)
	if len(facts) != 1 || facts[0].AddedAfterFirst == nil || !*facts[0].AddedAfterFirst || facts[0].RepeatsEarlier == nil || !*facts[0].RepeatsEarlier {
		t.Fatalf("finding facts = %+v, want new and repeated", facts)
	}
	prior[0].Comments[0].Body = "<!-- mergewake-severity:high -->\nDifferent finding title"
	if facts := roundFindingFacts(findings, prior, rp, true); len(facts) != 1 || facts[0].RepeatsEarlier == nil || !*facts[0].RepeatsEarlier {
		t.Fatalf("same path and line should count as an overlapping earlier thread: %+v", facts)
	}
	prior[0].Path = "elsewhere.go"
	prior[0].Line = 99
	prior[0].Comments[0].Body = "<!-- mergewake-severity:high -->\nSame bug title"
	if facts := roundFindingFacts(findings, prior, rp, true); len(facts) != 1 || facts[0].RepeatsEarlier == nil || !*facts[0].RepeatsEarlier {
		t.Fatalf("same-title thread should count as repeated across paths: %+v", facts)
	}
	unknown := roundFindingFacts(findings, prior, roundPlan{round: 2}, false)
	if unknown[0].AddedAfterFirst != nil || unknown[0].RepeatsEarlier != nil {
		t.Fatalf("unknown round facts must remain unknown: %+v", unknown)
	}
	first := "diff --git a/a.go b/a.go\n+++ b/a.go\n@@ -4,0 +5,1 @@\n+old line\n"
	current := "diff --git a/a.go b/a.go\n+++ b/a.go\n@@ -4,0 +5,2 @@\n+old line\n+new line\n"
	newLines := diffNewAddedLocations(first, current)
	if newLines["a.go"][5] || !newLines["a.go"][6] {
		t.Fatalf("line-level first-head comparison = %+v", newLines)
	}
	record := passRecord(github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}, github.PassReceipt{Pass: 1, Round: 2, Lens: "default"}, findings, "", time.Now(), time.Now(), facts)
	if record.FindingFacts == nil || len(*record.FindingFacts) != 1 || (*record.FindingFacts)[0].AddedAfterFirst == nil || !*(*record.FindingFacts)[0].AddedAfterFirst || (*record.FindingFacts)[0].RepeatsEarlier == nil || !*(*record.FindingFacts)[0].RepeatsEarlier {
		t.Fatalf("land record finding facts = %+v", record.FindingFacts)
	}
	s, _, _, _, r := setup(t)
	if err := s.auditRoundFindingFacts(r.Name, 7, shaA, 1, 2, facts); err != nil {
		t.Fatal(err)
	}
	if err := s.auditRoundFindingFacts(r.Name, 7, shaA, 1, 2, facts); err != nil {
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
		t.Fatalf("same delivered head sent another notice: %+v", n.accepted)
	}
	if rows := auditRows(t, s, r, "conflict-esc-suppressed"); len(rows) != 1 {
		t.Fatalf("suppressed author notice audits = %v, want one", rows)
	}
	g.pull.Head.SHA = shaB
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.accepted) != 3 {
		t.Fatalf("new head notice after owner follow-up = %+v", n.accepted)
	}
	now = now.Add(30 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := auditRows(t, s, r, "conflict-esc-owner-suppressed"); len(rows) != 2 {
		t.Fatalf("suppressed owner audit rows = %v, want one per head", rows)
	}
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := auditRows(t, s, r, "conflict-esc-owner-suppressed"); len(rows) != 2 {
		t.Fatalf("same head wrote repeated owner suppression audits: %v", rows)
	}
}

func TestConflictPersistsOwnerClockWhenHeadReturns(t *testing.T) {
	s, g, _, r := workerNotifySetup(t)
	n := newEscNotifier()
	s.Notifier = n
	s.Config.Daemon.PR.Notify.ConflictOwnerAfter = "30m"
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	g.pull.Head.SHA = shaB
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	g.pull.Head.SHA = shaA
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.accepted) != 3 || n.accepted[2].Lane != s.ownerLane(r) {
		t.Fatalf("head return owner notice = %+v, want one follow-up", n.accepted)
	}
}

func TestConflictDeliveredOwnerSentSkipsRouteReads(t *testing.T) {
	rig := newEscRig(t)
	rig.escalate(t)
	if len(rig.n.accepted) != 1 || !strings.Contains(rig.n.accepted[0].Text, "author route gone") {
		t.Fatalf("initial owner delivery = %+v", rig.n.accepted)
	}
	pulls := rig.g.pullCalls
	sink := &hkDownSink{}
	rig.s.Sink = sink
	rig.escalate(t)
	if rig.g.pullCalls != pulls || sink.reads != 0 {
		t.Fatalf("delivered head route reads: pull calls %d→%d, hk reads=%d", pulls, rig.g.pullCalls, sink.reads)
	}
}

func TestConflictOwnerFollowupPinsAndAuditsFailure(t *testing.T) {
	s, g, _, r := workerNotifySetup(t)
	n := newEscNotifier()
	s.Notifier = n
	s.Config.Daemon.PR.Notify.ConflictOwnerAfter = "30m"
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	ownerLane := s.ownerLane(r)
	n.fail[ownerLane] = errors.New("delivery ambiguous")
	now = now.Add(30 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("owner delivery failure was not returned")
	}
	if rows := auditRows(t, s, r, "conflict-esc-failed"); len(rows) != 1 || rows[0]["kind"] != "owner" {
		t.Fatalf("owner failure audit = %+v", rows)
	}
	delete(n.fail, ownerLane)
	r.Lane = ownerLane + "-changed"
	now = now.Add(2 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.accepted) != 2 || n.accepted[1].Lane != ownerLane {
		t.Fatalf("owner retry moved destination: %+v", n.accepted)
	}
}

func TestConflictOwnerPendingMovesWithHead(t *testing.T) {
	s, g, _, r := workerNotifySetup(t)
	n := newEscNotifier()
	s.Notifier = n
	s.Config.Daemon.PR.Notify.ConflictOwnerAfter = "30m"
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	ownerLane := s.ownerLane(r)
	n.fail[ownerLane] = errors.New("delivery ambiguous")
	now = now.Add(30 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("owner delivery failure was not returned")
	}
	g.pull.Head.SHA = shaB
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("second owner delivery failure was not returned")
	}
	var ownerAttempts []integrations.Event
	for _, event := range n.attempts {
		if strings.Contains(event.ID, "-owner-") {
			ownerAttempts = append(ownerAttempts, event)
		}
	}
	if len(ownerAttempts) != 2 || !strings.Contains(ownerAttempts[1].Text, "head="+short7(shaB)) || ownerAttempts[1].Lane != ownerLane || ownerAttempts[0].ID == ownerAttempts[1].ID {
		t.Fatalf("owner retries after head change = %+v, want separate A and B notices", ownerAttempts)
	}
}

func TestConflictResolutionClearsPendingOwnerNotice(t *testing.T) {
	s, g, _, r := workerNotifySetup(t)
	n := newEscNotifier()
	s.Notifier = n
	s.Config.Daemon.PR.Notify.ConflictOwnerAfter = "30m"
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	ownerLane := s.ownerLane(r)
	n.fail[ownerLane] = errors.New("delivery ambiguous")
	now = now.Add(30 * time.Minute)
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("owner delivery failure was not returned")
	}
	file := s.conflictEscFile(r.Name, g.pull.Number)
	state, err := readConflictEsc(file)
	if err != nil || state.OwnerPending == nil {
		t.Fatalf("pending owner notice before resolution = %+v, err=%v", state.OwnerPending, err)
	}
	yes := true
	g.pull.Mergeable, g.pull.MergeableState = &yes, "clean"
	if err = s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	state, err = readConflictEsc(file)
	if err != nil || state.OwnerPending != nil || state.OwnerPendingHead != "" || state.OwnerPendingReason != "" {
		t.Fatalf("pending owner notice after resolution = %+v, err=%v", state, err)
	}
}

func TestConflictAuthorRouteGoneAfterDeliveryNotifiesOwner(t *testing.T) {
	s, g, _, r := workerNotifySetup(t)
	n := newEscNotifier()
	s.Notifier = n
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	s.Config.Daemon.PR.Notify.ConflictOwnerAfter = "30m"
	g.pull.Body = ""
	s.Sink = &fakeSink{}
	if err := s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(n.accepted) != 2 || n.accepted[1].Lane != s.ownerLane(r) {
		t.Fatalf("owner notice after author route disappeared = %+v", n.accepted)
	}
}

func TestConflictPinnedAuthorRetrySkipsRouteLookup(t *testing.T) {
	s, _, _ := takeoverSetup(t, paneA)
	g := s.GH.(*fakeGH)
	r := testRepoFor(s)
	g.pull.Body = "Mergewake-Task: 2481\n"
	no := false
	g.pull.Mergeable, g.pull.MergeableState = &no, "dirty"
	at := time.Now().UTC()
	s.Now = func() time.Time { return at }
	route, err := s.findingsRoute(context.Background(), r, g.pull)
	if err != nil {
		t.Fatal(err)
	}
	n := newEscNotifier()
	s.Notifier = n
	n.fail[route.lane] = errors.New("delivery ambiguous")
	if err = s.EscalateConflict(context.Background(), r, g.pull, Options{}); err == nil {
		t.Fatal("first delivery failure was not returned")
	}
	broken := &hkDownSink{}
	s.Sink = broken
	// Keep the retry inside the pinned author's route after its binding becomes
	// unreadable; only the destination already persisted in state may be used.
	for i := 0; i < 2; i++ {
		at = at.Add(2 * time.Minute)
		if i == 1 {
			delete(n.fail, route.lane)
		}
		if err = s.EscalateConflict(context.Background(), r, g.pull, Options{}); err != nil && i == 1 {
			t.Fatal(err)
		}
	}
	if broken.reads != 0 || len(n.accepted) != 1 || n.accepted[0].Lane != route.lane {
		t.Fatalf("pinned retry read the route or moved lanes: reads=%d events=%+v", broken.reads, n.accepted)
	}
}
