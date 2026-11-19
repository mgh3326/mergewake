package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"

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

// threadPostedOn is an own finding whose first comment was posted on head.
func threadPostedOn(id, severity, head string) github.ReviewThread {
	th := ownFinding(id, severity, 10)
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

// The hold is per PR: a round at or after the hold round that ended with a
// critical/high finding open holds the PR's next head too, even when the author
// fixed the finding and pushed before the gate had looked at the done head.
// The findings delivery records what the round ended with, so the open set is
// the one the round left — not a reconstruction after the push.
func TestRoundSteps_HoldFollowsThePRToItsNextHead(t *testing.T) {
	fixed := github.ReviewComment{Author: "dev", AuthorType: "User", Body: "Fixed in abc1234."}
	resolved := func(th github.ReviewThread) github.ReviewThread { th.IsResolved = true; return th }
	withReply := func(th github.ReviewThread) github.ReviewThread {
		th.Comments = append(th.Comments, fixed)
		return th
	}
	round6 := strings.Repeat("6", 40)
	for _, tt := range []struct {
		name     string
		rounds   int
		before   []github.ReviewThread // open on the done head
		after    []github.ReviewThread // what the push left
		wantHold bool
	}{
		{"high, author fixed and pushed", 7,
			[]github.ReviewThread{ownFinding("T1", "high", 10)},
			[]github.ReviewThread{withReply(ownFinding("T1", "high", 10))}, true},
		{"high carried from round 6, resolved after round 7", 7,
			[]github.ReviewThread{threadPostedOn("T1", "high", round6)},
			[]github.ReviewThread{resolved(threadPostedOn("T1", "high", round6))}, true},
		{"high resolved before round 7 finished", 7,
			[]github.ReviewThread{resolved(threadPostedOn("T1", "high", round6))},
			[]github.ReviewThread{resolved(threadPostedOn("T1", "high", round6))}, false},
		{"medium only", 7,
			[]github.ReviewThread{ownFinding("T1", "medium", 10)},
			[]github.ReviewThread{ownFinding("T1", "medium", 10)}, false},
		{"round 6 ended with high, round 7 only just started", 6,
			[]github.ReviewThread{ownFinding("T1", "high", 10)},
			[]github.ReviewThread{ownFinding("T1", "high", 10)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, _, _, r := roundRuleSetup(t, tt.rounds)
			g.feedback.Threads = tt.before
			deliver(t, s, r)

			// The author pushed before the gate looked at the done head: the new
			// head is a partial round with no done marker.
			g.reviewed = false
			g.feedback.Threads = tt.after
			if tt.rounds == 6 {
				pushHead(g, shaC, "patch-7", 7, false)
			} else {
				g.pull.Head.SHA = shaC
			}
			v := gateHoldVerdict(t, s, g, r)
			if roundHeld(v) != tt.wantHold {
				t.Fatalf("round_hold=%v, want %v (state %s, reasons %v)", roundHeld(v), tt.wantHold, v.State, v.Reasons)
			}
			if !tt.wantHold {
				return
			}
			if v.State != "blocked" {
				t.Fatalf("held head state %q, want blocked", v.State)
			}
			// The decision names the round and the finding it ended with, though
			// the push left nothing open.
			if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
				t.Fatal(err)
			}
			var text string
			for _, e := range n.events {
				if e.Type == "decision" {
					text = e.Text
				}
			}
			if !strings.Contains(text, "round 7 ended without a pass (1 open finding(s))") || !strings.Contains(text, "T1") {
				t.Fatalf("decision event lost the round's findings: %q", text)
			}
		})
	}
}

// A done head at or after the hold round that ends clean clears what an earlier
// done head recorded, so a later unreviewed head is not held for a round that
// no longer stands.
func TestRoundSteps_HoldDueClearedByACleanRound(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	deliver(t, s, r)
	if st := s.loadRoundRuleState(r.Name, 7); !st.HoldDue || st.HoldDueRound != 7 {
		t.Fatalf("hold not recorded: %+v", st)
	}
	g.feedback.Threads = nil
	deliver(t, s, r)
	if st := s.loadRoundRuleState(r.Name, 7); st.HoldDue {
		t.Fatalf("hold still recorded after a clean round: %+v", st)
	}
}
