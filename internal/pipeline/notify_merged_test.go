package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mergedPull is the PR as the land path holds it: head shaA, merged at shaB.
func withMergeSHA(g *fakeGH) {
	g.pull.MergeCommitSHA = shaB
}

// (a) A MERGED with a Mergewake-Notify-Lane trailer goes once to that lane, as a
// merged event, with the merge sha in its text and a sent pr-notify row.
func TestMergedNotifiesTrailerLane(t *testing.T) {
	s, g, n, r := notifySetup(t)
	g.pull.Body = "Summary\n\nMergewake-Notify-Lane: x-design\n"
	withMergeSHA(g)
	lane, via, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title)
	if e != nil || !sent || lane != "x-design" || via != "notify-lane trailer" {
		t.Fatalf("lane=%q via=%q sent=%t err=%v", lane, via, sent, e)
	}
	if n.count != 1 {
		t.Fatalf("events=%+v", n.events)
	}
	ev := n.events[0]
	want := "[mergewake] MERGED example/project#7 head=aaaaaaa merge=bbbbbbb | Add feature | lane=x-design via notify-lane trailer"
	if ev.Lane != "x-design" || ev.Type != "merged" || ev.Text != want {
		t.Fatalf("event=%+v", ev)
	}
	if ev.ID != "mergewake-merged-example/project-7-"+shaA {
		t.Fatalf("id=%q", ev.ID)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["state"] != "merged" || rows[0]["sent"] != true || rows[0]["lane"] != "x-design" ||
		rows[0]["via"] != "notify-lane trailer" || rows[0]["suppressed"] != nil {
		t.Fatalf("decisions=%v", rows)
	}
	if !sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) {
		t.Fatal("sent key not recorded")
	}
}

// (b) With no trailer, the repo's design lane receives the MERGED.
func TestMergedNotifiesRepoLane(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	withMergeSHA(g)
	lane, via, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title)
	if e != nil || !sent || lane != "repo-design" || via != "repo lane" {
		t.Fatalf("lane=%q via=%q sent=%t err=%v", lane, via, sent, e)
	}
	if n.count != 1 || n.events[0].Lane != "repo-design" || n.events[0].Type != "merged" {
		t.Fatalf("events=%+v", n.events)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["sent"] != true || rows[0]["via"] != "repo lane" {
		t.Fatalf("decisions=%v", rows)
	}
}

// (c) With no trailer and no repo lane, nothing is sent: the row says "no design
// lane", and the desk never receives the MERGED.
func TestMergedNoDesignLaneNeverReachesDesk(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = ""
	withMergeSHA(g)
	lane, via, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title)
	if e != nil || sent {
		t.Fatalf("sent=%t err=%v", sent, e)
	}
	if n.count != 0 {
		t.Fatalf("desk or other lane received MERGED: %+v", n.events)
	}
	if lane != "" || via != "" {
		t.Fatalf("lane=%q via=%q", lane, via)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || rows[0]["suppressed"] != "no design lane" || rows[0]["sent"] != false || rows[0]["state"] != "merged" {
		t.Fatalf("decisions=%v", rows)
	}
	if !sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) {
		t.Fatal("sent key not recorded")
	}
}

// (d) The same (PR, head) MERGED processed twice sends one event and logs one row.
func TestMergedSentOncePerPRHead(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	withMergeSHA(g)
	for i := 0; i < 2; i++ {
		if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil {
			t.Fatal(e)
		}
	}
	if n.count != 1 {
		t.Fatalf("events=%+v", n.events)
	}
	if rows := prNotifyRows(t, s, r); len(rows) != 1 {
		t.Fatalf("decisions=%v", rows)
	}
}

// (e) A failed send leaves no sent key, and the next pass sends exactly once.
func TestMergedFailedSendRetriesNextPass(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	withMergeSHA(g)
	n.err = errors.New("lane offline")
	_, _, sent, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title)
	if !errors.Is(e, errNotifyDelivery) || sent {
		t.Fatalf("sent=%t err=%v", sent, e)
	}
	if sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) {
		t.Fatal("sent key recorded after a failed send")
	}
	if n.count != 0 {
		t.Fatalf("events=%+v", n.events)
	}
	n.err = nil
	if _, _, sent, e = s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil || !sent {
		t.Fatalf("retry sent=%t err=%v", sent, e)
	}
	if _, _, sent, e = s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil || sent {
		t.Fatalf("third pass sent=%t err=%v", sent, e)
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "MERGED") {
		t.Fatalf("events=%+v", n.events)
	}
}

// The MERGED text is bounded like the other lane events.
func TestMergedTextIsBounded(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	withMergeSHA(g)
	g.pull.Title = strings.Repeat("x", 5000)
	if _, _, _, e := s.laneEvent(context.Background(), r, g.pull, "merged", g.pull.Title); e != nil {
		t.Fatal(e)
	}
	if n.count != 1 || len(n.events[0].Text) > findingsLaneText {
		t.Fatalf("len=%d", len(n.events[0].Text))
	}
}

type failOnceOutput struct {
	io.Writer
	fail bool
}

func (w *failOnceOutput) Write(p []byte) (int, error) {
	if w.fail {
		w.fail = false
		return 0, errors.New("audit output unavailable")
	}
	return w.Writer.Write(p)
}

func TestMergedAuditFailureRetriesRowWithoutSending(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	withMergeSHA(g)
	s.Out = &failOnceOutput{Writer: io.Discard, fail: true}
	if err := s.RetryMergedCandidate(context.Background(), r, g.pull, g.pull.Title); err == nil {
		t.Fatal("audit failure was hidden")
	}
	if n.count != 1 {
		t.Fatalf("first sends=%d", n.count)
	}
	s.Out = io.Discard
	if err := s.RetryMergedPending(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("retry resent notice: %d", n.count)
	}
	if rows := prNotifyRows(t, s, r); len(rows) != 1 || rows[0]["sent"] != true {
		t.Fatalf("retry did not write exactly one successful row: %v", rows)
	}
	if !sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) {
		t.Fatal("key not saved")
	}
}

func TestMergedNoLaneAuditFailureRetriesSuppressionRow(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = ""
	s.Out = &failOnceOutput{Writer: io.Discard, fail: true}
	if err := s.RetryMergedCandidate(context.Background(), r, g.pull, g.pull.Title); err == nil {
		t.Fatal("audit failure was hidden")
	}
	s.Out = io.Discard
	if err := s.RetryMergedPending(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 || !sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)) {
		t.Fatalf("sends=%d key=%t", n.count, sentKeyHas(t, s, r, verdictKey("merged", 7, shaA)))
	}
}

func TestMergedPendingPersistsAndExpiresOnce(t *testing.T) {
	s, g, n, r := notifySetup(t)
	now := time.Now().UTC()
	s.Now = func() time.Time { return now }
	r.Lane = "repo-design"
	n.err = errors.New("lane offline")
	if err := s.RetryMergedCandidate(context.Background(), r, g.pull, g.pull.Title); err == nil {
		t.Fatal("expected notifier failure")
	}
	s2 := *s
	if _, err := os.Stat(filepath.Join(s2.state(r.Name), "pr-notify-merged-pending.json")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Hour)
	n.err = nil
	if err := s2.RetryMergedPending(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	rows := prNotifyRows(t, &s2, r)
	if len(rows) != 1 || rows[0]["suppressed"] != "gave up after 24 hours" {
		t.Fatalf("rows=%v", rows)
	}
	if err := s2.RetryMergedPending(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if len(prNotifyRows(t, &s2, r)) != 1 {
		t.Fatal("gave-up row repeated")
	}
}

func TestMergedNotificationUsesRefreshedBodyAndGatedHead(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	updated := g.pull
	updated.Body = "Mergewake-Notify-Lane: changed-lane\n"
	updated.Head.SHA = shaC
	updated.MergeCommitSHA = shaB
	// Land refreshes the PR body after merge but holds the head gated before it.
	updated.Head.SHA = shaA
	if err := s.RetryMergedCandidate(context.Background(), r, updated, "gated"); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || n.events[0].Lane != "changed-lane" || n.events[0].ID != fmt.Sprintf("mergewake-merged-%s-7-%s", r.Name, shaA) || !strings.Contains(n.events[0].Text, "head=aaaaaaa") {
		t.Fatalf("events=%+v", n.events)
	}
}

func TestLandMergedFailureRetriesFromPendingOnSweep(t *testing.T) {
	s, g, _, n, r := setup(t)
	g.enabled = true
	markReviewed(g)
	r.Lane = "repo-design"
	n.err = errors.New("lane offline")
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 || len(auditRows(t, s, r, "land-notify-error")) != 1 {
		t.Fatalf("merges=%d land-notify-error=%v", g.mergeCount, auditRows(t, s, r, "land-notify-error"))
	}
	n.err = nil
	if err := s.RetryMergedPending(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || n.events[0].ID != "mergewake-merged-example/project-7-"+shaA {
		t.Fatalf("events=%+v", n.events)
	}
	if rows := prNotifyRows(t, s, r); len(rows) != 1 || rows[0]["head"] != shaA {
		t.Fatalf("pr-notify rows=%v", rows)
	}
	mergedAt := time.Now().UTC().Format(time.RFC3339)
	g.pull.Merged, g.pull.State, g.pull.MergedAt = true, "closed", &mergedAt
	if err := s.ObserveMergedCandidate(context.Background(), r, g.pull); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("post-Land closed evaluation duplicated MERGED: %+v", n.events)
	}
}

func TestLandMergedLaneReadFailureStaysPending(t *testing.T) {
	s, g, _, n, r := setup(t)
	g.enabled = true
	markReviewed(g)
	r.Lane = "repo-design"
	g.commitErr = map[string]error{shaA: errors.New("commit read failed")}
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if g.mergeCount != 1 || len(auditRows(t, s, r, "land-notify-error")) != 1 {
		t.Fatalf("merges=%d land-notify-error=%v", g.mergeCount, auditRows(t, s, r, "land-notify-error"))
	}
	g.commitErr = nil
	if err := s.RetryMergedPending(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || n.events[0].Lane != "repo-design" || !strings.Contains(n.events[0].Text, "head=aaaaaaa") {
		t.Fatalf("events=%+v", n.events)
	}
}

func TestLandUsesPostMergeBodyAndGatedHead(t *testing.T) {
	s, g, _, n, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.nextBodyPull = 3 // initial read, merge recheck, then the post-merge read
	g.nextBody = "Mergewake-Notify-Lane: fresh-lane\n"
	if err := s.Land(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || n.events[0].Lane != "fresh-lane" || n.events[0].ID != "mergewake-merged-example/project-7-"+shaA || !strings.Contains(n.events[0].Text, "head=aaaaaaa") {
		t.Fatalf("events=%+v", n.events)
	}
}

func TestMergedRecentExternalCandidateAndLandCandidateShareSentKey(t *testing.T) {
	s, g, n, r := notifySetup(t)
	r.Lane = "repo-design"
	now := time.Now().UTC()
	s.Now = func() time.Time { return now }
	mergedAt := now.Add(-time.Hour).Format(time.RFC3339)
	g.pull.Merged, g.pull.State, g.pull.MergedAt = true, "closed", &mergedAt
	if err := s.ObserveMergedCandidate(context.Background(), r, g.pull); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryMergedCandidate(context.Background(), r, g.pull, g.pull.Title); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("merged PR got duplicate notices: %+v", n.events)
	}
	mergedAt = now.Add(-25 * time.Hour).Format(time.RFC3339)
	g.pull.Head.SHA, g.pull.MergedAt = shaC, &mergedAt
	if err := s.ObserveMergedCandidate(context.Background(), r, g.pull); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 {
		t.Fatalf("old external merge was notified: %+v", n.events)
	}
}
