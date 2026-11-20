package pipeline

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// nhGH is a PR whose review history is explicit: heads reviewed in
// order, each with the time its last pass was posted, and a current head that
// has no pass of its own yet.
type nhGH struct {
	*roundRuleGH
	history     []github.ReviewedPass
	historyErr  error
	commentsErr error
	// onComments runs when the PR's comments are read.
	onComments func()
}

func (g *nhGH) ReviewHistory(context.Context, string, int, string) ([]github.ReviewedPass, error) {
	if g.historyErr != nil {
		return nil, g.historyErr
	}
	return slices.Clone(g.history), nil
}

func (g *nhGH) IssueComments(ctx context.Context, repo string, n int) ([]github.IssueComment, error) {
	if g.onComments != nil {
		g.onComments()
	}
	if g.commentsErr != nil {
		return nil, g.commentsErr
	}
	return g.roundRuleGH.IssueComments(ctx, repo, n)
}

var nhT0 = time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

func nhHead(i int) string { return strings.Repeat(string(rune('0'+i)), 40) }

func nhPass(i int) github.ReviewedPass {
	at := nhT0.Add(time.Duration(i) * time.Hour)
	return github.ReviewedPass{
		Head:        nhHead(i),
		SubmittedAt: &at,
		PassReceipt: github.PassReceipt{Pass: 1, Patch: "patch-" + string(rune('0'+i)), Round: i},
	}
}

// nhFinding is a finding posted by the pass of head i, at its time.
func nhFinding(id, severity string, i int, replies ...github.ReviewComment) github.ReviewThread {
	t := ownFinding(id, severity, 10+i, replies...)
	t.Comments[0].CreatedAt = nhT0.Add(time.Duration(i) * time.Hour)
	return t
}

// nhReply is a reply posted d after head i's pass.
func nhReply(i int, d time.Duration, body string) github.ReviewComment {
	return github.ReviewComment{Author: "pr-author", Body: body, CreatedAt: nhT0.Add(time.Duration(i)*time.Hour + d)}
}

// newerHeadSetup is the handoffkeep#482 shape: heads 1..rounds fully reviewed
// (each by one pass), the last of them ended with a high open, and the PR's
// current head is a newer one nobody has reviewed. The gate is evaluated on it.
func newerHeadSetup(t *testing.T, rounds int) (*Service, *nhGH, func() gateVerdict, config.Repo) {
	return newerHeadSetupPolicy(t, rounds, policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}})
}

func newerHeadSetupPolicy(t *testing.T, rounds int, p policy.Policy) (*Service, *nhGH, func() gateVerdict, config.Repo) {
	t.Helper()
	s, g, _, _, _, r := roundRuleSetup(t, 1)
	hh := &nhGH{roundRuleGH: g}
	s.GH = hh
	for i := 1; i <= rounds; i++ {
		hh.history = append(hh.history, nhPass(i))
	}
	// Each reviewed head is done; the current head has no pass and no marker.
	g.fakeGH.refs = map[string]bool{}
	for i := 1; i <= rounds; i++ {
		g.fakeGH.refs[doneRef(g.pull.Number, nhHead(i))] = true
	}
	g.fakeGH.reviewed = false
	g.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", rounds)}
	return s, hh, func() gateVerdict {
		t.Helper()
		v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
		if err != nil {
			t.Fatalf("mergeGate err: %v", err)
		}
		return v
	}, r
}

func nhHeld(v gateVerdict) bool { return slices.Contains(v.Reasons, "round_hold") }

// handoffkeep#482: round 7 ended with high findings open, the author pushed
// right after, and the new head had no review yet.
func TestRoundHoldNewerHead_PendingHeadAfterRoundWithHighIsHeld(t *testing.T) {
	_, _, gate, _ := newerHeadSetup(t, 7)
	v := gate()
	if !nhHeld(v) || v.State != "blocked" {
		t.Fatalf("pending head after a round-7 head with a high open: state %q reasons %v, want blocked with round_hold", v.State, v.Reasons)
	}
	if len(v.RoundHold) != 1 || v.RoundHold[0].ID != "T1" {
		t.Fatalf("hold names %v, want T1", v.RoundHold)
	}
}

func TestRoundHoldNewerHead_BelowTheHoldRoundIsNotHeld(t *testing.T) {
	_, _, gate, _ := newerHeadSetup(t, 6)
	if v := gate(); nhHeld(v) {
		t.Fatalf("round 6 head with a high open held a pending head: %v", v.Reasons)
	}
}

func TestRoundHoldNewerHead_NoHighOpenIsNotHeld(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "low", 7), nhFinding("T2", "medium", 7)}
	if v := gate(); nhHeld(v) {
		t.Fatalf("round 7 head with no critical/high open held a pending head: %v", v.Reasons)
	}
}

// A round 7 whose first pass is posted on the new head is not a completed
// round: round 6 ended with the high, so six rounds completed.
func TestRoundHoldNewerHead_PartialRoundDoesNotCount(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 6)
	// Two passes are needed for a blocking change; one is posted on the pending
	// head. Its patch would be round 7 if partial heads counted.
	hh.history = append(hh.history, github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Patch: "patch-7"}})
	if v := gate(); nhHeld(v) {
		t.Fatalf("round 6 completed and round 7 half posted held the PR: %v", v.Reasons)
	}
}

// A high carried from an earlier head, still open at the end of round 7 and
// first posted before it, is among the findings the round ended with.
func TestRoundHoldNewerHead_CarriedFindingCounts(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 5)}
	if v := gate(); !nhHeld(v) {
		t.Fatalf("a high carried into round 7 did not hold the pending head: %v", v.Reasons)
	}
}

// A finding the review posts on the pending head's own pass is not the
// completed head's.
func TestRoundHoldNewerHead_FindingOfTheNewerPassDoesNotCount(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	t1 := nhFinding("T1", "high", 8) // first posted after round 7 ended
	hh.feedback.Threads = []github.ReviewThread{t1}
	if v := gate(); nhHeld(v) {
		t.Fatalf("a high posted after round 7 ended held the pending head: %v", v.Reasons)
	}
}

// The author's reply, posted after round 7 ended, does not release the hold:
// the round ended with the finding open. A reply posted before it disposed of
// the finding then.
func TestRoundHoldNewerHead_ReplyAfterTheRoundDoesNotDispose(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 7, nhReply(7, 40*time.Second, "Dismiss: the code line disproves it"))}
	if v := gate(); !nhHeld(v) {
		t.Fatalf("a dismissal posted after round 7 ended released the hold: %v", v.Reasons)
	}
}

func TestRoundHoldNewerHead_ReplyBeforeTheRoundEndedDisposed(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 7, nhReply(7, -time.Minute, "Dismiss: the code line disproves it"))}
	if v := gate(); nhHeld(v) {
		t.Fatalf("a finding dismissed before round 7 ended held the pending head: %v", v.Reasons)
	}
}

// The release phrase still lifts the hold for the PR.
func TestRoundHoldNewerHead_ReleaseCommentLiftsIt(t *testing.T) {
	s, hh, gate, r := newerHeadSetup(t, 7)
	if v := gate(); !nhHeld(v) {
		t.Fatalf("not held: %v", v.Reasons)
	}
	hh.issueComments = append(hh.issueComments, github.IssueComment{User: github.PullUser{Login: "operator", Type: "User"}, Body: "Mergewake-Round-Hold: release"})
	if v := gate(); nhHeld(v) {
		t.Fatalf("still held after release: %v", v.Reasons)
	}
	if s.isRoundHoldActive(context.Background(), r, hh.pull) {
		t.Fatal("review still held after release")
	}
}

// Trap: a failed pass-history or issue-comment read must not erase or skip the
// hold the gate recorded.
func TestRoundHoldNewerHead_FailedReadsKeepTheRecordedHold(t *testing.T) {
	s, hh, gate, r := newerHeadSetup(t, 7)
	if v := gate(); !nhHeld(v) {
		t.Fatalf("not held: %v", v.Reasons)
	}
	// The comments read fails: still held, and no release is read from nothing.
	hh.commentsErr = errors.New("comments unavailable")
	if v := gate(); !nhHeld(v) {
		t.Fatalf("failed comments read dropped the hold: %v", v.Reasons)
	}
	if !s.isRoundHoldActive(context.Background(), r, hh.pull) {
		t.Fatal("failed comments read let the review run on a held PR")
	}
	// The history read fails: the gate errors, never a verdict that passes, and
	// the saved hold stands for the pass after.
	hh.commentsErr = nil
	hh.historyErr = errors.New("history unavailable")
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	if v, err := s.mergeGate(context.Background(), r, hh.pull, p, shaB); err == nil && v.State == "ready" {
		t.Fatalf("failed history read produced a passing verdict: %+v", v)
	}
	if st := s.loadRoundRuleState(r.Name, hh.pull.Number); !st.held() {
		t.Fatal("failed history read erased the saved hold")
	}
	hh.historyErr = nil
	// The newer head's review comes back clean with nothing open: the hold owed
	// for the earlier head is not cleared by it.
	hh.feedback.Threads = nil
	hh.history = append(hh.history, github.ReviewedPass{Head: shaA, PassReceipt: github.PassReceipt{Pass: 1, Patch: "patch-8"}})
	hh.fakeGH.refs[doneRef(hh.pull.Number, shaA)] = true
	if v := gate(); !nhHeld(v) {
		t.Fatalf("a later reviewed head with nothing open cleared the hold: %v", v.Reasons)
	}
}

// Trap: a first read of the comments that fails must still record the hold, and
// the baseline taken later must not turn a release comment into a loss.
func TestRoundHoldNewerHead_FailedCommentsOnFirstPassStillHolds(t *testing.T) {
	s, hh, gate, r := newerHeadSetup(t, 7)
	hh.commentsErr = errors.New("comments unavailable")
	if v := gate(); !nhHeld(v) {
		t.Fatalf("not held when the comments read failed: %v", v.Reasons)
	}
	if st := s.loadRoundRuleState(r.Name, hh.pull.Number); !st.held() || st.baselineCaptured() {
		t.Fatalf("state after failed first read: held %v baseline %v, want held without baseline", st.held(), st.baselineCaptured())
	}
	hh.commentsErr = nil
	if v := gate(); !nhHeld(v) {
		t.Fatalf("not held after the comments came back: %v", v.Reasons)
	}
}

// Trap: a same-patch rebase or a clean base merge of the head must not bypass
// the hold. The rebased head inherits round 7's review: it is reviewed, with
// the high disposed of since by a dismissal.
func TestRoundHoldNewerHead_SamePatchHeadDoesNotBypass(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	// shaA carries round 7's patch: it inherits that review with no pass of its
	// own and no done marker.
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 7, nhReply(7, 40*time.Second, "Dismiss: the code line disproves it"))}
	diff, _ := hh.fakeGH.Diff(context.Background(), "", 0)
	hh.history[len(hh.history)-1].Patch = reviewPatchID(diff)
	v := gate()
	if v.Review.Inherited == "" {
		t.Fatalf("head did not inherit round 7's review: %+v", v.Review)
	}
	if !nhHeld(v) {
		t.Fatalf("a same-patch head bypassed the round hold: %v", v.Reasons)
	}
}

// Trap: promoting the hold must not strand an outstanding consult. The hold is
// saved on a fresh copy of the state: a consult row settled while the comments
// were read survives it.
func TestRoundHoldNewerHead_HoldKeepsAnOutstandingConsult(t *testing.T) {
	s, hh, gate, r := newerHeadSetup(t, 7)
	// The consult is requested while the gate reads the comments.
	hh.onComments = func() {
		_, _ = s.updateRoundRuleState(r.Name, hh.pull.Number, func(st *roundRuleState) bool {
			st.HubConsultOwed, st.HubConsultPosted, st.HubConsultID, st.HubConsultHead = true, true, "consult-9", nhHead(7)
			return true
		})
	}
	if v := gate(); !nhHeld(v) {
		t.Fatalf("not held: %v", v.Reasons)
	}
	st := s.loadRoundRuleState(r.Name, hh.pull.Number)
	if !st.held() || !st.HubConsultOwed || st.HubConsultID != "consult-9" || !st.HubConsultPosted {
		t.Fatalf("hold stranded the consult row: held %v owed %v id %q posted %v", st.held(), st.HubConsultOwed, st.HubConsultID, st.HubConsultPosted)
	}
}

// Trap: a failed state write must not make the hold fail open: the pass blocks
// on what it derived, and the next pass records the hold again.
func TestRoundHoldNewerHead_FailedStateWriteStillBlocks(t *testing.T) {
	s, _, gate, r := newerHeadSetup(t, 7)
	// A directory where the state file goes makes the write fail.
	file := s.roundRuleFile(r.Name, 7)
	if err := os.MkdirAll(file, 0o700); err != nil {
		t.Fatal(err)
	}
	v := gate()
	if !nhHeld(v) || v.State != "blocked" {
		t.Fatalf("failed state write let the pass through: state %q reasons %v", v.State, v.Reasons)
	}
	if !auditHas(t, s, r.Name, "round-hold-state-error") {
		t.Fatal("failed state write not audited")
	}
	if err := os.RemoveAll(file); err != nil {
		t.Fatal(err)
	}
	if v := gate(); !nhHeld(v) {
		t.Fatalf("hold lost after the write recovered: %v", v.Reasons)
	}
	if st := s.loadRoundRuleState(r.Name, 7); !st.held() {
		t.Fatal("hold not recorded after the write recovered")
	}
}

// One decision event per PR: the pending head's hold sends it once, naming the
// findings the round ended with even after a push disposed of them.
func TestRoundHoldNewerHead_OneDecisionEvent(t *testing.T) {
	s, hh, _, r := newerHeadSetup(t, 7)
	for i := 0; i < 3; i++ {
		if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
			t.Fatalf("NotifyPR: %v", err)
		}
	}
	_ = hh
	n := s.Notifier.(*ackNotifier)
	count := 0
	for _, e := range n.events {
		if e.Type == "decision" && strings.Contains(e.Text, "round-hold") {
			count++
			if !strings.Contains(e.Text, "T1") {
				t.Errorf("decision does not name the finding: %s", e.Text)
			}
		}
	}
	if count != 1 {
		t.Fatalf("decision events = %d, want 1: %+v", count, n.events)
	}
}

// Each reviewed head is judged by its own required pass count: a push that
// adds a guarded path (two passes needed now) does not make the routine
// one-pass rounds before it incomplete.
func TestRoundHoldNewerHead_CompletenessIsPerHead(t *testing.T) {
	// The pending head touches a guarded path: it needs two passes.
	_, _, gate, _ := newerHeadSetupPolicy(t, 7, policy.Policy{
		ReviewGate: &policy.ReviewGate{Land: "graded"},
		Blocking:   []policy.Rule{{Paths: []string{"**"}, Reason: "guarded"}},
	})
	if v := gate(); !nhHeld(v) {
		t.Fatalf("one-pass routine rounds stopped counting when the new head needs two passes: %v", v.Reasons)
	}
}

// And the inverse: a head that needed two passes and has one, no completion
// marker, is not a completed round, whatever the current head needs.
func TestRoundHoldNewerHead_UnfinishedHeadIsNotARound(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	delete(hh.fakeGH.refs, doneRef(hh.pull.Number, nhHead(7)))
	if v := gate(); nhHeld(v) {
		t.Fatalf("a head without its completion marker counted a round: %v", v.Reasons)
	}
}

// A fix reply made before round 7 ended is checked against round 7's head: a
// later push that rewrote the fix commit away does not reopen the finding.
func TestRoundHoldNewerHead_FixIsVerifiedAgainstTheRoundsHead(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 7, nhReply(7, -time.Minute, "Fixed in "+shaC[:7]))}
	// The current head no longer contains the fix commit; round 7's head does.
	hh.fakeGH.notAncestor = map[[2]string]bool{{shaC, shaA}: true}
	if v := gate(); nhHeld(v) {
		t.Fatalf("a fix verified at round 7 was reopened by a later push: %v", v.Reasons)
	}
}

// The latest completed review is chosen from every pass, base merges included:
// a conflict-resolving base merge reviewed after a valid dismissal is the
// newest completed head, and it ended with nothing open.
func TestRoundHoldNewerHead_BaseMergeReviewIsTheLatest(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 7)
	merge := nhHead(8)
	second := strings.Repeat("e", 40)
	hh.fakeGH.parents2 = map[string][]string{merge: {nhHead(7), second}}
	hh.fakeGH.pull.Base.SHA = shaB
	p := nhPass(8)
	p.Patch = "patch-8"
	hh.history = append(hh.history, p)
	hh.fakeGH.refs[doneRef(hh.pull.Number, merge)] = true
	// Dismissed after round 7 ended, before the merge's review.
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 7, nhReply(7, 30*time.Minute, "Dismiss: the code line disproves it"))}
	if v := gate(); nhHeld(v) {
		t.Fatalf("round 7's snapshot held a PR whose later reviewed head had nothing open: %v", v.Reasons)
	}
}

// An inherited review with a partial round before it is not round 7.
func TestRoundHoldNewerHead_InheritedHeadUsesCompletedRounds(t *testing.T) {
	_, hh, gate, _ := newerHeadSetup(t, 6)
	// An abandoned seventh patch with one pass and no marker.
	hh.history = append(hh.history, nhPass(7))
	diff, _ := hh.fakeGH.Diff(context.Background(), "", 0)
	hh.history[5].Patch = reviewPatchID(diff) // the current head inherits round 6
	hh.feedback.Threads = []github.ReviewThread{nhFinding("T1", "high", 6)}
	v := gate()
	if v.Review.Inherited == "" {
		t.Fatalf("head did not inherit: %+v", v.Review)
	}
	if nhHeld(v) {
		t.Fatalf("six completed rounds plus a partial seventh held an inheriting head: %v", v.Reasons)
	}
}

// A release posted while the comments could not be read is not lost when the
// baseline is taken afterwards.
func TestRoundHoldNewerHead_ReleaseDuringCommentOutageCounts(t *testing.T) {
	s, hh, gate, r := newerHeadSetup(t, 7)
	hh.commentsErr = errors.New("comments unavailable")
	if v := gate(); !nhHeld(v) {
		t.Fatalf("not held: %v", v.Reasons)
	}
	held := s.loadRoundRuleState(r.Name, hh.pull.Number).HoldAt
	hh.issueComments = append(hh.issueComments, github.IssueComment{
		User: github.PullUser{Login: "operator", Type: "User"}, Body: "Mergewake-Round-Hold: release",
		CreatedAt: held.Add(time.Minute),
	})
	hh.commentsErr = nil
	if v := gate(); nhHeld(v) {
		t.Fatalf("a release posted during the outage was lost: %v", v.Reasons)
	}
}
