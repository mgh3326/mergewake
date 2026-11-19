package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/reap"
)

// Round steps act on the first qualifying round at or after their number, not
// at one exact round (hk 5535): a round that was not done before the author
// pushed, or that had nothing open, must not use the step up.

// pushHead records the author's push to sha: its review covers patch at round,
// and the head's done marker is set only when done.
func pushHead(g *roundRuleGH, sha, patch string, round int, done bool) {
	g.pull.Head.SHA = sha
	g.passes[sha] = []github.PassReceipt{{Pass: 1, Patch: patch, Round: round}}
	if done {
		g.refs[doneRef(7, sha)] = true
	} else {
		delete(g.refs, doneRef(7, sha))
	}
}

func TestRoundSteps_ConsultOnTheFirstQualifyingRound(t *testing.T) {
	for _, tt := range []struct {
		name string
		// first is the round the PR sits at before the author's push; its head
		// is done (or not) and carries the threads.
		firstRound int
		firstDone  bool
		firstOpen  []github.ReviewThread
		// push is the round and open threads after the push.
		pushRound int
		pushOpen  []github.ReviewThread
	}{
		{
			name:       "round 3 not done, round 4 is",
			firstRound: 3, firstDone: false, firstOpen: []github.ReviewThread{ownFinding("T1", "high", 10)},
			pushRound: 4, pushOpen: []github.ReviewThread{ownFinding("T1", "high", 10)},
		},
		{
			name:       "round 3 had nothing open, round 5 has",
			firstRound: 3, firstDone: true, firstOpen: nil,
			pushRound: 5, pushOpen: []github.ReviewThread{ownFinding("T2", "medium", 20)},
		},
		{
			name:       "round 3 had only low, round 4 has medium",
			firstRound: 3, firstDone: true, firstOpen: []github.ReviewThread{ownFinding("T1", "low", 10)},
			pushRound: 4, pushOpen: []github.ReviewThread{ownFinding("T1", "low", 10), ownFinding("T2", "medium", 20)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, _, run, hub, r, _ := hubConsultSetup(t, tt.firstRound)
			if !tt.firstDone {
				delete(g.refs, doneRef(7, shaA))
			}
			g.feedback.Threads = tt.firstOpen
			deliver(t, s, r)
			if run.requestCount() != 0 {
				t.Fatalf("consult requested on round %d: %v", tt.firstRound, run.requests)
			}
			before := n.count

			pushHead(g, shaC, "patch-push", tt.pushRound, true)
			g.feedback.Threads = tt.pushOpen
			// Rounds count distinct patches: add the rounds between.
			for i := tt.firstRound + 1; i < tt.pushRound; i++ {
				sha := strings.Repeat(string(rune('0'+i)), 40)
				g.passes[sha] = []github.PassReceipt{{Pass: 1, Patch: "patch-" + string(rune('0'+i))}}
			}
			hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}
			deliver(t, s, r)

			if run.requestCount() != 1 {
				t.Fatalf("want one consult at the first qualifying round, got %d: %v", run.requestCount(), run.requests)
			}
			if got := flagValue(run.lastRequest(), "--head"); got != shaC {
				t.Fatalf("consult requested for %q, want the pushed head", got)
			}
			if n.count != before {
				t.Fatalf("findings delivered while the consult is queued: %+v", n.events[before:])
			}
		})
	}
}

func TestRoundSteps_LadderOnTheFirstQualifyingRound(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	// Round 5 is done with nothing open: the ladder is not used up.
	g.feedback.Threads = nil
	deliver(t, s, r)
	if run.escalateCalls != 0 {
		t.Fatalf("ladder ran on an empty round 5: %d calls", run.escalateCalls)
	}

	// Round 6 is done with a high finding open: the ladder runs, once.
	pushHead(g, shaC, "patch-6", 6, true)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	deliver(t, s, r)
	if run.escalateCalls != 1 {
		t.Fatalf("want one escalate-author call at round 6, got %d", run.escalateCalls)
	}
	if !argvHas(run.lastArgs, "--round", "5") {
		t.Fatalf("the call names the ladder round: %v", run.lastArgs)
	}

	// Later rounds and passes do not call it again (once per PR).
	pushHead(g, shaD, "patch-7", 7, true)
	g.feedback.Threads = append(g.feedback.Threads, ownFinding("T2", "high", 20))
	deliver(t, s, r)
	deliver(t, s, r)
	if run.escalateCalls != 1 {
		t.Fatalf("ladder ran again: %d calls", run.escalateCalls)
	}
}

func TestRoundSteps_HoldFollowsThePRToItsNextHead(t *testing.T) {
	for _, tt := range []struct {
		name     string
		severity string
		reply    bool
		wantHold bool
	}{
		{"round 7 ended with high, author fixed and pushed", "high", true, true},
		{"round 7 ended with high, author pushed first", "high", false, true},
		{"round 7 ended with medium only", "medium", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, _, r := roundRuleSetup(t, 7)
			var replies []github.ReviewComment
			if tt.reply {
				replies = append(replies, github.ReviewComment{Author: "dev", AuthorType: "User", Body: "Fixed in abc1234."})
			}
			g.feedback.Threads = []github.ReviewThread{ownFinding("T1", tt.severity, 10, replies...)}
			p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}

			// The author pushed before the gate looked at round 7's head: the
			// new head is pending, with no pass and no done marker.
			g.reviewed = false
			g.pull.Head.SHA = shaC
			v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
			if err != nil {
				t.Fatal(err)
			}
			held := false
			for _, reason := range v.Reasons {
				held = held || reason == "round_hold"
			}
			if held != tt.wantHold {
				t.Fatalf("round_hold=%v, want %v (state %s, reasons %v)", held, tt.wantHold, v.State, v.Reasons)
			}
			if tt.wantHold && v.State != "blocked" {
				t.Fatalf("held head state %q, want blocked", v.State)
			}
		})
	}
}

// threadPostedOn is an own finding whose first comment was posted on head.
func threadPostedOn(id, severity, head string, replies ...github.ReviewComment) github.ReviewThread {
	th := ownFinding(id, severity, 10, replies...)
	th.Comments[0].CommitSHA, th.Comments[0].OriginalCommitSHA = head, head
	return th
}

func gateHoldVerdict(t *testing.T, s *Service, g *roundRuleGH, r config.Repo) gateVerdict {
	t.Helper()
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func roundHeld(v gateVerdict) bool { return slices.Contains(v.Reasons, "round_hold") }

// The reviewer does not repeat a finding that is still open, so round 7 can
// end with a high finding first posted on round 6. The hold follows it to the
// pending head; a finding the author answered before round 7 finished does not.
func TestRoundSteps_HoldCountsFindingsCarriedIntoTheRound(t *testing.T) {
	done := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)
	round6 := strings.Repeat("6", 40)
	fixed := func(at time.Time) github.ReviewComment {
		return github.ReviewComment{Author: "dev", AuthorType: "User", Body: "Fixed in abc1234.", CreatedAt: at}
	}
	for _, tt := range []struct {
		name     string
		thread   github.ReviewThread
		wantHold bool
	}{
		{"carried from round 6, open through round 7", threadPostedOn("T1", "high", round6), true},
		{"carried from round 6, fixed after round 7 finished", threadPostedOn("T1", "high", round6, fixed(done.Add(time.Hour))), true},
		{"carried from round 6, fixed before round 7 finished", threadPostedOn("T1", "high", round6, fixed(done.Add(-time.Hour))), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, _, r := roundRuleSetup(t, 7)
			g.submitted = map[string]time.Time{shaA: done}
			g.feedback.Threads = []github.ReviewThread{tt.thread}
			g.reviewed = false
			g.pull.Head.SHA = shaC
			if got := roundHeld(gateHoldVerdict(t, s, g, r)); got != tt.wantHold {
				t.Fatalf("round_hold=%v, want %v", got, tt.wantHold)
			}
		})
	}
}

// The hold threshold is the selected head's own round. Round 6 completed with
// a high finding open and round 7's first receipt is partial: the PR has seven
// reviewed patches but no round 7 has finished, so nothing is held yet.
func TestRoundSteps_HoldThresholdIsTheCompletedHeadsRound(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 6)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	g.reviewed = false
	pushHead(g, shaC, "patch-7", 7, false)
	if v := gateHoldVerdict(t, s, g, r); roundHeld(v) {
		t.Fatalf("held before any round 7 finished: %v", v.Reasons)
	}

	// Round 7 finishes with the finding still open: now the PR is held.
	g.refs[doneRef(7, shaC)] = true
	g.passes[shaC] = []github.PassReceipt{{Pass: 1, Patch: "patch-7", Round: 7}, {Pass: 2, Patch: "patch-7", Round: 7}}
	g.reviewed = true
	if v := gateHoldVerdict(t, s, g, r); !roundHeld(v) {
		t.Fatalf("round 7 ended with a high open and no hold: %v", v.Reasons)
	}
}
