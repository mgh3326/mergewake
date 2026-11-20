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

// nhGH is a PR whose comment reads and review history reads can fail or run a
// hook, over the review fixture.
type nhGH struct {
	*fakeGH
	commentsErr error
	historyErr  error
	// onComments runs when the PR's comments are read.
	onComments func()
}

func (g *nhGH) IssueComments(ctx context.Context, repo string, n int) ([]github.IssueComment, error) {
	if g.onComments != nil {
		g.onComments()
	}
	if g.commentsErr != nil {
		return nil, g.commentsErr
	}
	return g.fakeGH.IssueComments(ctx, repo, n)
}

func (g *nhGH) ReviewHistory(ctx context.Context, repo string, n int, login string) ([]github.ReviewedPass, error) {
	if g.historyErr != nil {
		return nil, g.historyErr
	}
	return g.fakeGH.ReviewHistory(ctx, repo, n, login)
}

func nhHead(i int) string { return strings.Repeat(string(rune('0'+i)), 40) }

const nhHigh = `{"findings":[{"severity":"high","path":"tool","line":2,"body":"A concrete bug"}]}`

// nhSetup is a PR whose `prior` earlier heads were fully reviewed (one pass
// each, a marker each) and whose current head, shaA, is reviewed for real by
// Review: the reviewer returns findings. So Review completes round prior+1.
func nhSetup(t *testing.T, prior int, findings string) (*Service, *nhGH, config.Repo) {
	t.Helper()
	s, g, _, _, r := reviewSetup(t)
	hh := &nhGH{fakeGH: g}
	s.GH = hh
	s.Runner.(*fakeRunner).pane.findings = findings
	g.diffs = map[string]string{shaA: toolDiff("1111111..2222222", 1, "new"), shaB: toolDiff("3333333..4444444", 1, "newer")}
	g.passes = map[string][]github.PassReceipt{}
	g.refs = map[string]bool{}
	for i := 1; i <= prior; i++ {
		g.passes[nhHead(i)] = []github.PassReceipt{{Pass: 1, Patch: "patch-" + string(rune('0'+i)), Round: i}}
		g.refs[doneRef(7, nhHead(i))] = true
	}
	return s, hh, r
}

func nhReview(s *Service, r config.Repo, o Options) error {
	return s.Review(context.Background(), r, 7, "openai", o)
}

// nhPush moves the PR to a newer head nobody has reviewed.
func nhPush(hh *nhGH) {
	hh.pull.Head.SHA = shaB
	hh.pull.Base.SHA = shaC
}

func nhGate(t *testing.T, s *Service, hh *nhGH, r config.Repo) gateVerdict {
	t.Helper()
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	v, err := s.mergeGate(context.Background(), r, hh.pull, p, shaC)
	if err != nil {
		t.Fatalf("mergeGate: %v", err)
	}
	return v
}

func nhHeld(v gateVerdict) bool { return slices.Contains(v.Reasons, "round_hold") }

func nhSaved(s *Service, r config.Repo) *roundRuleState {
	st := s.loadRoundRuleState(r.Name, 7)
	return &st
}

func nhDecisions(s *Service) int {
	count := 0
	for _, e := range s.Notifier.(*fakeNotifier).events {
		if e.Type == "decision" && strings.Contains(e.Text, "round-hold") {
			count++
		}
	}
	return count
}

// handoffkeep#482: round 7 ends with a high posted, the author pushes right
// after, and the new head has no review yet. The hold was saved when round 7
// ended, so the new head's review does not start and the gate holds it.
func TestRoundHoldNewerHead_PendingHeadAfterRoundSevenHighIsHeld(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatalf("round 7 review: %v", err)
	}
	if !nhSaved(s, r).held() {
		t.Fatal("round 7 ended with a high and saved no hold")
	}
	if !hh.refs[doneRef(7, shaA)] {
		t.Fatal("round 7 head has no completion marker")
	}
	nhPush(hh)
	posts := hh.postCount
	if err := nhReview(s, r, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("review of the newer head: %v, want ErrReviewHeld", err)
	}
	if hh.postCount != posts {
		t.Fatal("a review was posted on the held PR")
	}
	if v := nhGate(t, s, hh, r); !nhHeld(v) || v.State != "blocked" {
		t.Fatalf("gate on the newer head: %q %v, want blocked with round_hold", v.State, v.Reasons)
	}
}

func TestRoundHoldNewerHead_BelowTheHoldRoundSavesNothing(t *testing.T) {
	s, _, r := nhSetup(t, 5, nhHigh) // the review is round 6
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	if nhSaved(s, r).held() {
		t.Fatal("round 6 saved a hold")
	}
}

func TestRoundHoldNewerHead_RoundWithoutHighSavesNothing(t *testing.T) {
	s, _, r := nhSetup(t, 6, `{"findings":[{"severity":"low","path":"tool","line":2,"body":"nit"}]}`)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	if nhSaved(s, r).held() {
		t.Fatal("a round with no critical/high saved a hold")
	}
}

// A high still open from an earlier head, though the round posts none, is open
// when the round ends.
func TestRoundHoldNewerHead_CarriedHighHolds(t *testing.T) {
	s, hh, r := nhSetup(t, 6, `{"findings":[]}`)
	hh.feedback.Threads = []github.ReviewThread{ownFinding("T0", "high", 5)}
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	if !nhSaved(s, r).held() {
		t.Fatal("a carried high open at round 7 saved no hold")
	}
}

// The feedback read has not caught up with the post (no thread visible): the
// posted finding alone is the evidence. (nhSetup's feedback has no threads.)
func TestRoundHoldNewerHead_PostedHighHoldsWhileFeedbackLags(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if len(hh.feedback.Threads) != 0 {
		t.Fatal("fixture feedback should have no threads")
	}
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	if !nhSaved(s, r).held() {
		t.Fatal("a lagging feedback read hid the posted high")
	}
}

// Resolving the thread after the round ended, before any gate pass, releases
// nothing: the hold was saved when the round ended.
func TestRoundHoldNewerHead_ResolutionAfterTheRoundDoesNotRelease(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	t1 := ownFinding("T1", "high", 2)
	t1.IsResolved = true
	hh.feedback.Threads = []github.ReviewThread{t1}
	nhPush(hh)
	if err := nhReview(s, r, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("review after the resolution: %v, want ErrReviewHeld", err)
	}
	if v := nhGate(t, s, hh, r); !nhHeld(v) {
		t.Fatalf("a resolution after the round released the hold: %v", v.Reasons)
	}
}

func TestRoundHoldNewerHead_DryRunSavesNothing(t *testing.T) {
	s, _, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if nhSaved(s, r).held() {
		t.Fatal("a dry run saved a hold")
	}
}

// Trap: a failed pass-history or issue-comment read must not erase or skip the
// saved hold. A later head reviewed clean with nothing open does not clear it.
func TestRoundHoldNewerHead_FailedReadsAndACleanLaterHeadKeepTheHold(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	nhPush(hh)
	hh.commentsErr = errors.New("comments unavailable")
	if v := nhGate(t, s, hh, r); !nhHeld(v) {
		t.Fatalf("failed comments read dropped the hold: %v", v.Reasons)
	}
	if !s.isRoundHoldActive(context.Background(), r, hh.pull) {
		t.Fatal("failed comments read let the review run")
	}
	hh.commentsErr = nil
	hh.historyErr = errors.New("history unavailable")
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	if v, err := s.mergeGate(context.Background(), r, hh.pull, p, shaC); err == nil && v.State == "ready" {
		t.Fatalf("failed history read produced a passing verdict: %+v", v)
	}
	if !nhSaved(s, r).held() {
		t.Fatal("failed history read erased the saved hold")
	}
	hh.historyErr = nil
	// The newer head is later reviewed clean with nothing open.
	hh.passes[shaB] = []github.PassReceipt{{Pass: 1, Patch: "patch-9", Round: 8}}
	hh.refs[doneRef(7, shaB)] = true
	hh.feedback.Threads = nil
	if v := nhGate(t, s, hh, r); !nhHeld(v) {
		t.Fatalf("a later reviewed head with nothing open cleared the hold: %v", v.Reasons)
	}
}

// Trap: a same-patch rebase or a clean base merge of the head does not bypass a
// saved hold: the hold is the PR's.
func TestRoundHoldNewerHead_SamePatchHeadDoesNotBypass(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	nhPush(hh)
	hh.diffs[shaB] = hh.diffs[shaA] // the rebased head carries round 7's patch
	hh.pull.Base.SHA = shaC
	v := nhGate(t, s, hh, r)
	if v.Review.Inherited == "" {
		t.Fatalf("head did not inherit round 7's review: %+v", v.Review)
	}
	if !nhHeld(v) {
		t.Fatalf("a same-patch head bypassed the hold: %v", v.Reasons)
	}
}

// Trap: saving the hold must not strand an outstanding consult: a row settled
// while the comments were read survives it.
func TestRoundHoldNewerHead_HoldKeepsAnOutstandingConsult(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	hh.onComments = func() {
		_, _ = s.updateRoundRuleState(r.Name, 7, func(st *roundRuleState) bool {
			st.HubConsultOwed, st.HubConsultPosted, st.HubConsultID, st.HubConsultHead = true, true, "consult-9", nhHead(6)
			return true
		})
	}
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	st := nhSaved(s, r)
	if !st.held() || !st.HubConsultOwed || st.HubConsultID != "consult-9" || !st.HubConsultPosted {
		t.Fatalf("hold stranded the consult row: held %v owed %v id %q posted %v", st.held(), st.HubConsultOwed, st.HubConsultID, st.HubConsultPosted)
	}
}

// Trap: a failed state write must not make the hold fail open. The review stops
// before the completion marker, so a later review finds every pass posted,
// reaches the round's end again and saves the hold.
func TestRoundHoldNewerHead_FailedStateWriteStopsBeforeTheMarker(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	file := s.roundRuleFile(r.Name, 7)
	if err := os.MkdirAll(file, 0o700); err != nil { // a directory where the file goes
		t.Fatal(err)
	}
	if err := nhReview(s, r, Options{}); err == nil {
		t.Fatal("a failed hold write did not stop the review")
	}
	if hh.refs[doneRef(7, shaA)] {
		t.Fatal("the head was marked done with no saved hold")
	}
	if !auditHas(t, s, r.Name, "round-hold-state-error") {
		t.Fatal("failed write not audited")
	}
	if err := os.RemoveAll(file); err != nil {
		t.Fatal(err)
	}
	posts := hh.postCount
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if hh.postCount != posts {
		t.Fatal("the retry posted the pass again")
	}
	if !nhSaved(s, r).held() || !hh.refs[doneRef(7, shaA)] {
		t.Fatal("the retry did not save the hold and mark the head")
	}
}

// The release phrase lifts the hold, and the next review starts on the head;
// the saved release stands afterwards.
func TestRoundHoldNewerHead_ReleaseLetsReviewStart(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	nhPush(hh)
	if err := nhReview(s, r, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("not held: %v", err)
	}
	hh.issueComments = append(hh.issueComments, github.IssueComment{User: github.PullUser{Login: "operator", Type: "User"}, Body: "Mergewake-Round-Hold: release", CreatedAt: time.Now().Add(time.Minute)})
	if err := nhReview(s, r, Options{}); errors.Is(err, ErrReviewHeld) {
		t.Fatalf("review stayed held after the release: %v", err)
	}
	hh.issueComments = nil
	if v := nhGate(t, s, hh, r); nhHeld(v) {
		t.Fatalf("gate held after the release: %v", v.Reasons)
	}
}

// The daemon's review-first path ends at ErrReviewHeld without settling the
// head: NotifyRoundHold sends the one decision there, from the saved hold, and
// retries after a failed read.
func TestRoundHoldNewerHead_HeldReviewPathSendsTheDecisionOnce(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	nhPush(hh)
	if err := nhReview(s, r, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("not held: %v", err)
	}
	hh.historyErr = errors.New("history unavailable")
	if err := s.NotifyRoundHold(context.Background(), r, hh.pull, Options{}); err == nil {
		t.Fatal("outage not returned")
	}
	if nhDecisions(s) != 0 {
		t.Fatal("decision sent during the outage")
	}
	hh.historyErr = nil
	for i := 0; i < 3; i++ {
		if err := s.NotifyRoundHold(context.Background(), r, hh.pull, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := nhDecisions(s); got != 1 {
		t.Fatalf("decisions = %d, want 1", got)
	}
}

func TestRoundHoldNewerHead_NoHoldNoDecision(t *testing.T) {
	s, hh, r := nhSetup(t, 5, nhHigh)
	if err := s.NotifyRoundHold(context.Background(), r, hh.pull, Options{}); err != nil {
		t.Fatal(err)
	}
	if nhDecisions(s) != 0 {
		t.Fatal("a decision with no hold")
	}
}

// The decision asks for a release: it is not sent while the hold is unsaved.
func TestRoundHoldNewerHead_NoDecisionUntilTheHoldIsSaved(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	hh.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 2)}
	if err := s.sendRoundHoldDecision(context.Background(), r, hh.pull, hh.feedback, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if nhDecisions(s) != 0 {
		t.Fatal("a decision was sent with no saved hold")
	}
}

// Five completed rounds, one abandoned two-pass patch that posted only its
// first pass, and a new patch that completes with a high: six rounds finished,
// so no hold.
func TestRoundHoldNewerHead_AbandonedPartialPatchCountsNoRound(t *testing.T) {
	s, hh, r := nhSetup(t, 5, nhHigh)
	hh.passes[nhHead(6)] = []github.PassReceipt{{Pass: 1, Patch: "patch-6", Round: 6, Needed: 2}}
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatal(err)
	}
	if nhSaved(s, r).held() {
		t.Fatal("an abandoned partial patch counted a round: hold saved at six completed rounds")
	}
}

// Pass 1 posted a high; a reasoned dismissal resolved it before the round ended.
// Its receipt still names a posted high, but its thread is visible and
// disposed: nothing is open at the round's end, so no hold.
func TestRoundHoldNewerHead_PostedHighAnsweredBeforeTheEndIsNotOpen(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	for i := 1; i <= 6; i++ {
		hh.refs[doneRef(7, nhHead(i))] = true
	}
	t1 := ownFinding("T1", "high", 2)
	t1.Comments[0].CommitSHA, t1.Comments[0].OriginalCommitSHA = shaA, shaA
	t1.IsResolved = true
	hh.feedback.Threads = []github.ReviewThread{t1}
	rc := github.PassReceipt{Pass: 1, Needed: 1, Patch: "patch-7", Round: 7, Blocking: 1}
	if err := s.holdAtRoundEnd(context.Background(), r, hh.pull, shaA, nhHistory(hh), []github.PassReceipt{rc}, Options{}); err != nil {
		t.Fatal(err)
	}
	if nhSaved(s, r).held() {
		t.Fatal("a posted high already resolved at the round's end saved a hold")
	}
}

// nhHistory is the PR's recorded passes as the history read returns them.
func nhHistory(hh *nhGH) []github.ReviewedPass {
	h, _ := hh.ReviewHistory(context.Background(), "", 7, "")
	return h
}

// A fix claim the lookups cannot settle is returned before any hold is saved:
// the review stops before the marker and is retried.
func TestRoundHoldNewerHead_UnsettledFixLookupSavesNoHold(t *testing.T) {
	s, hh, r := nhSetup(t, 6, `{"findings":[]}`)
	carried := ownFinding("T0", "high", 5, github.ReviewComment{Author: "pr-author", Body: "Fixed in " + shaC[:7]})
	hh.feedback.Threads = []github.ReviewThread{carried}
	hh.commitErr = map[string]error{shaC[:7]: errors.New("github down")}
	if err := nhReview(s, r, Options{}); err == nil {
		t.Fatal("an unsettled fix lookup did not stop the review")
	}
	if nhSaved(s, r).held() || hh.refs[doneRef(7, shaA)] {
		t.Fatal("a hold or a marker was saved from a failed lookup")
	}
	// GitHub recovers and verifies the fix: the retry saves no hold.
	hh.commitErr = nil
	if err := nhReview(s, r, Options{}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if nhSaved(s, r).held() {
		t.Fatal("a verified fix left a hold")
	}
}

// Round 7 posts its last pass with a high, the hold write fails, and the author
// pushes before the retry: the newer head's review recovers the owed hold
// before it starts.
func TestRoundHoldNewerHead_PushBeforeTheRetryStillMeetsTheHold(t *testing.T) {
	s, hh, r := nhSetup(t, 6, nhHigh)
	file := s.roundRuleFile(r.Name, 7)
	if err := os.MkdirAll(file, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := nhReview(s, r, Options{}); err == nil {
		t.Fatal("a failed hold write did not stop the review")
	}
	if err := os.RemoveAll(file); err != nil {
		t.Fatal(err)
	}
	nhPush(hh)
	posts := hh.postCount
	if err := nhReview(s, r, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("review of the newer head: %v, want ErrReviewHeld", err)
	}
	if hh.postCount != posts || !nhSaved(s, r).held() {
		t.Fatalf("recovery did not save the hold before the newer review: posts %d->%d held %v", posts, hh.postCount, nhSaved(s, r).held())
	}
}

// nhFailedRoundEnd runs round 7 with a high posted and the hold write failing:
// every pass is posted, no marker, no hold.
func nhFailedRoundEnd(t *testing.T) (*Service, *nhGH, config.Repo) {
	t.Helper()
	s, hh, r := nhSetup(t, 6, nhHigh)
	file := s.roundRuleFile(r.Name, 7)
	if err := os.MkdirAll(file, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := nhReview(s, r, Options{}); err == nil {
		t.Fatal("a failed hold write did not stop the review")
	}
	if err := os.RemoveAll(file); err != nil {
		t.Fatal(err)
	}
	return s, hh, r
}

// A fix that only the newer head carries is no fix for the older head's
// finding: recovery verifies fix replies against the head being finalized.
func TestRoundHoldNewerHead_RecoveryDoesNotTakeAFixFromTheNewerHead(t *testing.T) {
	s, hh, r := nhFailedRoundEnd(t)
	nhPush(hh)
	// The thread is visible, unresolved, answered with a fix that exists on the
	// newer head only (shaC is not an ancestor of the round's head, shaA).
	hh.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 2, github.ReviewComment{Author: "pr-author", Body: "Fixed in " + shaC[:7]})}
	hh.notAncestor = map[[2]string]bool{{shaC, shaA}: true}
	if err := nhReview(s, r, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("review of the newer head: %v, want ErrReviewHeld", err)
	}
	if !nhSaved(s, r).held() {
		t.Fatal("a fix on the newer head disposed round 7's high during recovery")
	}
}

// A same-patch rebase inherits the unfinished head's receipts: the gate
// finishes that round end first, so no ready verdict precedes the hold, even
// while the thread is not yet visible.
func TestRoundHoldNewerHead_InheritedHeadFinishesTheUnfinishedRoundEnd(t *testing.T) {
	s, hh, r := nhFailedRoundEnd(t)
	nhPush(hh)
	hh.diffs[shaB] = hh.diffs[shaA]
	v := nhGate(t, s, hh, r)
	if v.Review.Inherited == "" {
		t.Fatalf("head did not inherit: %+v", v.Review)
	}
	if !nhHeld(v) || v.State != "blocked" {
		t.Fatalf("an inherited head passed the gate before the hold: %q %v", v.State, v.Reasons)
	}
	if !nhSaved(s, r).held() {
		t.Fatal("the gate did not save the owed hold")
	}
}
