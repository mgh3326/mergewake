package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
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

// One consult runs per PR across rounds: a consult requested at the first done
// round at or after the consult round is not asked again at a later round, even
// when a push superseded it before it answered.
func TestRoundSteps_ConsultIsNotAskedAgainAtALaterRound(t *testing.T) {
	s, g, n, _, run, hub, r, _ := hubConsultSetup(t, 4)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}
	deliver(t, s, r)
	if run.requestCount() != 1 || n.count != 0 {
		t.Fatalf("first pass: requests=%d events=%d", run.requestCount(), n.count)
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.HubConsultRound != 4 {
		t.Fatalf("request round not recorded: %+v", st)
	}

	pushHead(g, shaC, "patch-5", 5, true)
	g.feedback.Threads = append(g.feedback.Threads, ownFinding("T2", "high", 20))
	deliver(t, s, r)
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("a second consult was requested at a later round: %v", run.requests)
	}
	if n.count != 1 {
		t.Fatalf("the later round's findings were not delivered once: %+v", n.events)
	}
}

// A consult whose request round is unknown is never guessed from today's
// configuration: after the threshold was raised, a push first done at the new
// threshold does not POST a second paid consult for the PR.
func TestRoundSteps_ConsultWithAnUnknownRoundIsNotAskedAgain(t *testing.T) {
	s, g, _, _, run, hub, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	hub.rows["c-1"] = reap.ConsultRow{ID: "c-1", State: "queued"}
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("first pass: %v", run.requests)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	st.HubConsultRound = 0
	if err := s.saveRoundRuleState(r.Name, 7, st); err != nil {
		t.Fatal(err)
	}

	// The operator raised the consult round, and the author pushed before any
	// pass saw the requested head: the request round cannot be learned.
	s.Config.Review.RoundRule.Consult = 5
	pushHead(g, shaD, "patch-4", 4, true)
	pushHead(g, shaC, "patch-5", 5, true)
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("a second paid consult was requested: %v", run.requests)
	}
}

// An attempt that failed before the POST asked nothing: after hk recovers, a
// qualifying later head still gets the consult.
func TestRoundSteps_ConsultFailedBeforeThePostIsNotConsumed(t *testing.T) {
	s, g, _, sink, run, _, r, _ := hubConsultSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	sink.recordErr = errors.New("hk unavailable")
	deliver(t, s, r)
	if run.requestCount() != 0 {
		t.Fatalf("requested without a question: %v", run.requests)
	}

	sink.recordErr = nil
	pushHead(g, shaC, "patch-4", 4, true)
	deliver(t, s, r)
	if run.requestCount() != 1 {
		t.Fatalf("the consult was consumed by an attempt that never reached the hub: %v", run.requests)
	}
	if got := flagValue(run.lastRequest(), "--head"); got != shaC {
		t.Fatalf("request names %q, want the new head", got)
	}
}
