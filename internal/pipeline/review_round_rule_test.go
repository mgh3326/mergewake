package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

type roundRuleRunner struct {
	*fakeRunner
	callsMu        sync.Mutex
	escalateCalls  int
	lastArgs       []string
	findingsJSON   string
	escalateRC     int
	escalateErr    error
	escalateOutput string
}

func (r *roundRuleRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "escalate-author" {
		r.callsMu.Lock()
		r.escalateCalls++
		r.lastArgs = append([]string(nil), args...)
		for i, a := range args {
			if a == "--findings-file" && i+1 < len(args) {
				if b, err := os.ReadFile(args[i+1]); err == nil {
					r.findingsJSON = string(b)
				}
			}
		}
		rc := r.escalateRC
		err := r.escalateErr
		out := r.escalateOutput
		r.callsMu.Unlock()
		if err != nil {
			return nil, err
		}
		if rc != 0 {
			return []byte(out), &integrations.CommandError{
				Command: "panewire",
				Err:     fmt.Errorf("exit status %d", rc),
				Stderr:  out,
			}
		}
		if out == "" {
			out = "job=w-7 rung=claude-opus@high\n"
		}
		return []byte(out), nil
	}
	return r.fakeRunner.Run(ctx, command, args, input, dir)
}

type roundRuleGH struct {
	*fakeGH
	commentsMu    sync.Mutex
	issueComments []github.IssueComment
}

func (g *roundRuleGH) IssueComments(_ context.Context, _ string, _ int) ([]github.IssueComment, error) {
	g.commentsMu.Lock()
	defer g.commentsMu.Unlock()
	return append([]github.IssueComment(nil), g.issueComments...), nil
}

func (g *roundRuleGH) PostIssueComment(ctx context.Context, repo string, n int, body string) error {
	g.commentsMu.Lock()
	g.issueComments = append(g.issueComments, github.IssueComment{
		Body: body,
		User: github.PullUser{Login: "operator", Type: "User"},
	})
	g.commentsMu.Unlock()
	return g.fakeGH.PostIssueComment(ctx, repo, n, body)
}

func roundRuleSetup(t *testing.T, rounds int) (*Service, *roundRuleGH, *ackNotifier, *jobSink, *roundRuleRunner, config.Repo) {
	t.Helper()
	s, g, n, sink, r, _ := deliverSetup(t)
	run := &roundRuleRunner{fakeRunner: s.Runner.(*fakeRunner)}
	rgh := &roundRuleGH{fakeGH: g}
	s.GH = rgh
	s.Runner = run

	// Set task lane to a dedicated design lane
	sink.lanes["2481"] = "design-lane"

	passes := map[string][]github.PassReceipt{}
	for i := 1; i <= rounds; i++ {
		sha := shaA
		if i < rounds {
			sha = strings.Repeat(string(rune('0'+i)), 40)
		}
		passes[sha] = []github.PassReceipt{{Pass: 1, Patch: "patch-" + string(rune('0'+i)), Round: i}}
		if i < rounds {
			rgh.fakeGH.refs["mergewake/reviews/7/"+sha+"-done"] = true
		}
	}
	rgh.fakeGH.passes = passes
	markReviewed(rgh.fakeGH)

	return s, rgh, n, sink, run, r
}

// (a) round 3 (consult) without a pass gets the directive only;
// round 5 (ladder) without a pass gets one escalate-author call with the right flags and findings file.
func TestRoundRule_a_Round3DirectiveAndEscalateAuthor(t *testing.T) {
	// 1. Round 3 (consult): directive to worker, no panewire escalate-author call
	t.Run("round_3_consult_directive_only", func(t *testing.T) {
		s, g, n, _, run, r := roundRuleSetup(t, 3)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
			ownFinding("T2", "medium", 20),
		}

		deliver(t, s, r)

		if n.count != 1 {
			t.Fatalf("want 1 findings delivery event, got %d: %+v", n.count, n.events)
		}
		ev := n.events[0]
		const wantDirective = "stop patching; ask a consult one grade above your seat in another family, give it every finding so far, ask for the root cause"
		if !strings.Contains(ev.Text, wantDirective) {
			t.Fatalf("findings message missing directive: %s", ev.Text)
		}
		if ev.Lane != "w-7" {
			t.Fatalf("findings sent to lane %q, want worker lane w-7", ev.Lane)
		}

		run.callsMu.Lock()
		calls := run.escalateCalls
		run.callsMu.Unlock()

		if calls != 0 {
			t.Fatalf("round 3 must not call escalate-author, got %d calls", calls)
		}

		// Verify nothing was sent to design lane at round 3
		for _, e := range n.events {
			if e.Lane == "design-lane" || e.Lane == "operator-desk" {
				t.Errorf("unexpected event sent to design lane at round 3: %+v", e)
			}
		}
	})

	// 2. Round 5 (ladder): executes panewire escalate-author with --round 5 and findings file
	t.Run("round_5_ladder_escalate_author", func(t *testing.T) {
		s, g, n, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
			ownFinding("T2", "medium", 20),
		}

		deliver(t, s, r)

		run.callsMu.Lock()
		calls := run.escalateCalls
		args := append([]string(nil), run.lastArgs...)
		findingsJSON := run.findingsJSON
		run.callsMu.Unlock()

		if calls != 1 {
			t.Fatalf("want 1 escalate-author call at round 5, got %d", calls)
		}

		wantArgs := map[string]string{
			"--task":  "2481",
			"--job":   "w-7",
			"--pr":    r.Name + "#7",
			"--round": "5",
		}
		for flag, want := range wantArgs {
			found := false
			for i := 0; i < len(args)-1; i++ {
				if args[i] == flag && args[i+1] == want {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("escalate-author missing flag %s %s, got args: %v", flag, want, args)
			}
		}

		var fs []struct {
			Ref      string `json:"ref"`
			Severity string `json:"severity"`
			Path     string `json:"path"`
			Line     int    `json:"line"`
			Title    string `json:"title"`
			Body     string `json:"body"`
		}
		if err := json.Unmarshal([]byte(findingsJSON), &fs); err != nil {
			t.Fatalf("unmarshal findings file JSON: %v, raw: %s", err, findingsJSON)
		}
		if len(fs) < 2 {
			t.Fatalf("want at least 2 findings in findings file, got %d: %+v", len(fs), fs)
		}
		hasT1 := false
		for _, f := range fs {
			if f.Ref == "T1" {
				hasT1 = true
				if f.Severity != "high" || f.Line != 10 || f.Path != "tool" {
					t.Errorf("T1 finding mismatch: %+v", f)
				}
			}
		}
		if !hasT1 {
			t.Errorf("findings file missing T1: %+v", fs)
		}

		for _, e := range n.events {
			if e.Lane == "design-lane" || e.Lane == "operator-desk" {
				t.Errorf("unexpected event sent to design lane at round 5: %+v", e)
			}
		}
	})
}

// (b) a restart before or after the call does not repeat it.
func TestRoundRule_b_Round3RestartNoDuplicate(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}

	// First pass
	deliver(t, s, r)

	run.callsMu.Lock()
	calls1 := run.escalateCalls
	run.callsMu.Unlock()
	if calls1 != 1 {
		t.Fatalf("first run calls = %d, want 1", calls1)
	}

	// Simulate restart on same state directory
	restarted := *s
	deliver(t, &restarted, r)

	run.callsMu.Lock()
	calls2 := run.escalateCalls
	run.callsMu.Unlock()
	if calls2 != 1 {
		t.Fatalf("after restart calls = %d, want 1 (must not repeat)", calls2)
	}
}

// (c) rc 2 and a missing command are logged and the review goes on.
func TestRoundRule_c_Round3RC2AndMissingCommandLogged(t *testing.T) {
	// Sub-case 1: rc 2 with no_rung or quota_refused
	t.Run("rc_2", func(t *testing.T) {
		s, g, _, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}
		run.escalateRC = 2
		run.escalateOutput = "no_rung\n"

		if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err != nil {
			t.Fatalf("DeliverFindings should succeed and review go on, got err: %v", err)
		}
	})

	// Sub-case 2: missing command
	t.Run("missing_command", func(t *testing.T) {
		s, g, _, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}
		run.escalateErr = exec.ErrNotFound

		if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err != nil {
			t.Fatalf("DeliverFindings should continue when command missing, got err: %v", err)
		}
	})

	// Sub-case 3: timeout
	t.Run("timeout", func(t *testing.T) {
		s, g, _, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}
		run.escalateErr = context.DeadlineExceeded

		if err := s.DeliverFindings(context.Background(), r, 7, Options{}); err != nil {
			t.Fatalf("DeliverFindings should continue on timeout, got err: %v", err)
		}
	})
}

// (d) a base-only merge head does not count as a round.
func TestRoundRule_d_BaseOnlyMergeHeadNoRound(t *testing.T) {
	history := []github.ReviewedPass{
		{Head: "sha-1", PassReceipt: github.PassReceipt{Pass: 1, Patch: "patch-1", Round: 1}},
		{Head: "sha-2", PassReceipt: github.PassReceipt{Pass: 1, Patch: "patch-2", Round: 2}},
		// sha-3 is a base-only merge of sha-2: patch is identical to patch-2
		{Head: "sha-3", PassReceipt: github.PassReceipt{Pass: 1, Patch: "patch-2", Round: 2}},
	}
	rounds, patches := reviewRounds(history)
	if rounds != 2 {
		t.Fatalf("want 2 rounds for base-only merge head, got %d (patches: %v)", rounds, patches)
	}
}

// (e) round 7 (hold) without a pass holds the PR and sends exactly one decision event
// to the task lane (desk when the task lane is the desk or unknown); a later pass does not send another.
func TestRoundRule_e_Round5HoldAndDecisionEvent(t *testing.T) {
	s, g, n, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}

	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	baseSHA := shaB

	// Check merge gate: round 7 without a pass must hold PR
	v, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	if v.State != "blocked" {
		t.Fatalf("gate state = %q, want blocked", v.State)
	}
	hasRoundHold := false
	for _, reason := range v.Reasons {
		if strings.Contains(reason, "round_hold") {
			hasRoundHold = true
			break
		}
	}
	if !hasRoundHold {
		t.Fatalf("gate reasons missing round_hold: %v", v.Reasons)
	}

	// Review on held PR must report ErrReviewHeld
	if err := s.Review(context.Background(), r, 7, "google", Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("Review on held PR want ErrReviewHeld, got: %v", err)
	}

	// NotifyPR / Settle must send exactly one decision event to task lane
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatalf("NotifyPR err: %v", err)
	}

	decisionEvents := 0
	var lastDecision integrations.Event
	for _, e := range n.events {
		if e.Type == "decision" && (e.Lane == "design-lane" || e.Lane == "operator-desk") {
			decisionEvents++
			lastDecision = e
		}
	}
	if decisionEvents != 1 {
		t.Fatalf("want exactly 1 decision event on design lane, got %d: %+v", decisionEvents, n.events)
	}
	if lastDecision.Lane != "design-lane" {
		t.Errorf("decision event went to %q, want design-lane", lastDecision.Lane)
	}
	// Verify content: PR, round count, open findings, choices
	text := lastDecision.Text
	if !strings.Contains(text, "7") || !strings.Contains(text, "round 7") {
		t.Errorf("decision text missing PR / round 7: %s", text)
	}
	if !strings.Contains(text, "cut the scope") || !strings.Contains(text, "SS consult") {
		t.Errorf("decision text missing choices: %s", text)
	}

	// Later pass does not send another decision event
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatalf("second NotifyPR err: %v", err)
	}
	decisionEvents2 := 0
	for _, e := range n.events {
		if e.Type == "decision" && (e.Lane == "design-lane" || e.Lane == "operator-desk") {
			decisionEvents2++
		}
	}
	if decisionEvents2 != 1 {
		t.Fatalf("later pass sent second decision event: got %d, want 1", decisionEvents2)
	}
}

// (f) the release reply ends the hold.
func TestRoundRule_f_Round5ReleaseEndsHold(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	baseSHA := shaB

	// Initially held
	v1, _ := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if v1.State != "blocked" {
		t.Fatalf("initial gate state = %q, want blocked", v1.State)
	}
	hasRoundHold := false
	for _, reason := range v1.Reasons {
		if strings.Contains(reason, "round_hold") {
			hasRoundHold = true
			break
		}
	}
	if !hasRoundHold {
		t.Fatalf("initial gate missing round_hold: %v", v1.Reasons)
	}

	// Post release comment from authorized user
	g.issueComments = []github.IssueComment{
		{
			User: github.PullUser{Login: "operator", Type: "User"},
			Body: "Mergewake-Round-Hold: release",
		},
	}

	// Now hold should be lifted
	v2, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	for _, reason := range v2.Reasons {
		if strings.Contains(reason, "round_hold") {
			t.Fatalf("gate still has round_hold after release: %v", v2.Reasons)
		}
	}
}

// (g) a PR that passes in round 3, 5 or 7 gets neither directive/escalate nor hold.
func TestRoundRule_g_PassRound3Or5GetsNeither(t *testing.T) {
	// Passing round 3 (no open blocking findings)
	t.Run("round_3_pass", func(t *testing.T) {
		s, g, n, _, run, r := roundRuleSetup(t, 3)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "low", 10),
		}

		deliver(t, s, r)

		run.callsMu.Lock()
		calls := run.escalateCalls
		run.callsMu.Unlock()
		if calls != 0 {
			t.Fatalf("passing round 3 called escalate-author %d times, want 0", calls)
		}
		for _, e := range n.events {
			if strings.Contains(e.Text, "stop patching; ask a consult") {
				t.Fatalf("passing round 3 carried directive: %s", e.Text)
			}
		}
	})

	// Passing round 5
	t.Run("round_5_pass", func(t *testing.T) {
		s, g, _, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "low", 10),
		}

		deliver(t, s, r)

		run.callsMu.Lock()
		calls := run.escalateCalls
		run.callsMu.Unlock()
		if calls != 0 {
			t.Fatalf("passing round 5 called escalate-author %d times, want 0", calls)
		}
	})

	// Passing round 7
	t.Run("round_7_pass", func(t *testing.T) {
		s, g, n, _, _, r := roundRuleSetup(t, 7)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "low", 10),
		}
		p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
		v, _ := s.mergeGate(context.Background(), r, g.pull, p, shaB)
		for _, reason := range v.Reasons {
			if strings.Contains(reason, "round_hold") {
				t.Fatalf("passing round 7 held: %v", v.Reasons)
			}
		}
		_ = s.NotifyPR(context.Background(), r, 7, Options{})
		for _, e := range n.events {
			if e.Type == "decision" && (e.Lane == "design-lane" || e.Lane == "operator-desk") {
				t.Fatalf("passing round 7 sent decision event: %+v", e)
			}
		}
	})
}

// (h) quiet mode after round 3 is unchanged.
func TestRoundRule_h_QuietModeAfterRound3Unchanged(t *testing.T) {
	s, _, _, _, r := reviewSetup(t)
	s.Config.Review.BudgetRounds = 3

	heads := []github.ReviewedHead{
		{SHA: "sha-1", Round: 1},
		{SHA: "sha-2", Round: 2},
		{SHA: "sha-3", Round: 3},
	}
	rg := &roundGH{
		fakeGH: s.GH.(*fakeGH),
		heads:  heads,
		old:    patchOne,
		full:   patchTwo,
	}
	s.GH = rg

	// Round 4 (after budget 3) must have quiet: true
	rp := s.planRound(context.Background(), r, rg.pull, patchTwo)
	if rp.round != 4 {
		t.Fatalf("expected round 4, got %d", rp.round)
	}
	if !rp.quiet {
		t.Fatalf("round 4 quiet = false, want true")
	}
}

func TestRoundRule_StaleReleaseCommentDoesNotReleaseNewHold(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	baseSHA := shaB

	// Stale release comment posted prior to round hold creation
	g.issueComments = []github.IssueComment{
		{
			User: github.PullUser{Login: "operator", Type: "User"},
			Body: "Mergewake-Round-Hold: release",
		},
	}

	// First evaluation: hold is entered at round 7. The stale comment must NOT release it.
	v1, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	if v1.State != "blocked" {
		t.Fatalf("gate state = %q, want blocked (stale comment should not release)", v1.State)
	}
	hasRoundHold := false
	for _, reason := range v1.Reasons {
		if strings.Contains(reason, "round_hold") {
			hasRoundHold = true
			break
		}
	}
	if !hasRoundHold {
		t.Fatalf("gate reasons missing round_hold: %v", v1.Reasons)
	}

	// Verify isRoundHoldActive also considers it active
	if !s.isRoundHoldActive(context.Background(), r, g.pull) {
		t.Fatalf("isRoundHoldActive returned false with stale comment, want true")
	}

	// Post a fresh release comment after the hold was created
	g.issueComments = append(g.issueComments, github.IssueComment{
		User: github.PullUser{Login: "operator", Type: "User"},
		Body: "Mergewake-Round-Hold: release",
	})

	// Now hold should be lifted
	v2, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	for _, reason := range v2.Reasons {
		if strings.Contains(reason, "round_hold") {
			t.Fatalf("gate still has round_hold after fresh release: %v", v2.Reasons)
		}
	}
	if s.isRoundHoldActive(context.Background(), r, g.pull) {
		t.Fatalf("isRoundHoldActive returned true after fresh release, want false")
	}
}

func TestRoundRule_DirectiveEventIDDistinctFromPriorFindings(t *testing.T) {
	s, _, _, _, _, r := roundRuleSetup(t, 3)
	fs := []pendingFinding{
		{thread: "T1", severity: "high", path: "a.go", line: 10},
	}
	route := findingsRoute{lane: "author-lane", via: "author"}

	d1, err := s.sendFindings(context.Background(), r, github.Pull{Number: 7, Head: github.Ref{SHA: shaA}}, route, "author", fs, Options{})
	if err != nil {
		t.Fatalf("sendFindings without directive: %v", err)
	}

	d2, err := s.sendFindings(context.Background(), r, github.Pull{Number: 7, Head: github.Ref{SHA: shaA}}, route, "author", fs, Options{}, Round3Directive)
	if err != nil {
		t.Fatalf("sendFindings with directive: %v", err)
	}

	if d1.ID == d2.ID {
		t.Fatalf("expected directive delivery to have distinct event ID from earlier findings event, got both = %s", d1.ID)
	}
}

func TestRoundRule_DryRunDoesNotPersistSentFlags(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 3)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}

	// Deliver with DryRun: true at round 3
	if err := s.DeliverFindings(context.Background(), r, 7, Options{DryRun: true}); err != nil {
		t.Fatalf("DeliverFindings dry run err: %v", err)
	}

	st := s.loadRoundRuleState(r.Name, 7)
	if st.directiveSent() {
		t.Fatalf("dry run persisted directiveSent = true")
	}

	// Ladder dry run at round 5
	s5, g5, _, _, run5, r5 := roundRuleSetup(t, 5)
	g5.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	if err := s5.DeliverFindings(context.Background(), r5, 7, Options{DryRun: true}); err != nil {
		t.Fatalf("DeliverFindings dry run round 5 err: %v", err)
	}
	st5 := s5.loadRoundRuleState(r5.Name, 7)
	if st5.escalated() {
		t.Fatalf("dry run persisted escalated = true")
	}
	run5.callsMu.Lock()
	calls := run5.escalateCalls
	run5.callsMu.Unlock()
	if calls != 0 {
		t.Fatalf("dry run called escalate-author %d times, want 0", calls)
	}

	// Round 7 decision dry run
	s7, g7, _, _, _, r7 := roundRuleSetup(t, 7)
	g7.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	if err := s7.NotifyPR(context.Background(), r7, 7, Options{DryRun: true}); err != nil {
		t.Fatalf("NotifyPR dry run err: %v", err)
	}
	st7 := s7.loadRoundRuleState(r7.Name, 7)
	if st7.eventSent() {
		t.Fatalf("dry run persisted eventSent = true")
	}
}

func TestRoundRule_LegacyHoldBackfillAndSnapshotFailure(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	baseSHA := shaB

	// Simulate legacy hold state persisted without baseline
	_ = s.saveRoundRuleState(r.Name, 7, roundRuleState{
		Round5Held: true,
	})

	// Pre-existing release comment in PR history
	g.issueComments = []github.IssueComment{
		{
			User: github.PullUser{Login: "operator", Type: "User"},
			Body: "Mergewake-Round-Hold: release",
		},
	}

	// Legacy hold evaluation: must backfill baseline and remain held
	v, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	if v.State != "blocked" {
		t.Fatalf("legacy hold unexpectedly unblocked by pre-existing release comment")
	}

	st := s.loadRoundRuleState(r.Name, 7)
	if !st.baselineCaptured() || st.commentCount() != 1 {
		t.Fatalf("expected baseline backfilled with count=1, captured=true, got %+v", st)
	}

	// Post fresh release comment after backfill
	g.issueComments = append(g.issueComments, github.IssueComment{
		User: github.PullUser{Login: "operator", Type: "User"},
		Body: "Mergewake-Round-Hold: release",
	})
	v2, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	if slices.Contains(v2.Reasons, "round_hold") {
		t.Fatalf("gate still held after fresh post-backfill release comment")
	}
}

// Config override test: setting review.round_rule.consult=2, ladder=4, hold=6 moves all three.
func TestRoundRule_ConfigOverrideMovesAllThree(t *testing.T) {
	override := config.RoundRuleConfig{
		Consult: 2,
		Ladder:  4,
		Hold:    6,
	}

	// 1. Consult at round 2
	t.Run("consult_round_2", func(t *testing.T) {
		s, g, n, _, run, r := roundRuleSetup(t, 2)
		s.Config.Review.RoundRule = override
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}

		deliver(t, s, r)

		if n.count != 1 {
			t.Fatalf("want 1 findings delivery event at round 2, got %d", n.count)
		}
		if !strings.Contains(n.events[0].Text, "stop patching; ask a consult") {
			t.Fatalf("round 2 findings missing consult directive: %s", n.events[0].Text)
		}
		run.callsMu.Lock()
		calls := run.escalateCalls
		run.callsMu.Unlock()
		if calls != 0 {
			t.Fatalf("consult round 2 must not call escalate-author, got %d calls", calls)
		}
	})

	// 2. Ladder at round 4
	t.Run("ladder_round_4", func(t *testing.T) {
		s, g, _, _, run, r := roundRuleSetup(t, 4)
		s.Config.Review.RoundRule = override
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}

		deliver(t, s, r)

		run.callsMu.Lock()
		calls := run.escalateCalls
		args := append([]string(nil), run.lastArgs...)
		run.callsMu.Unlock()
		if calls != 1 {
			t.Fatalf("ladder round 4 want 1 escalate-author call, got %d", calls)
		}
		foundRound := false
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--round" && args[i+1] == "4" {
				foundRound = true
				break
			}
		}
		if !foundRound {
			t.Errorf("escalate-author at ladder round 4 missing --round 4 flag: %v", args)
		}
	})

	// 3. Hold at round 6
	t.Run("hold_round_6", func(t *testing.T) {
		s, g, n, _, _, r := roundRuleSetup(t, 6)
		s.Config.Review.RoundRule = override
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}
		p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
		baseSHA := shaB

		v, err := s.mergeGate(context.Background(), r, g.pull, p, baseSHA)
		if err != nil {
			t.Fatalf("mergeGate err: %v", err)
		}
		if !slices.Contains(v.Reasons, "round_hold") {
			t.Fatalf("round 6 without pass must be held: %v", v.Reasons)
		}

		if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
			t.Fatalf("NotifyPR err: %v", err)
		}
		foundDecision := false
		for _, e := range n.events {
			if e.Type == "decision" && (e.Lane == "design-lane" || e.Lane == "operator-desk") {
				foundDecision = true
				if !strings.Contains(e.Text, "round 6") {
					t.Errorf("decision event text missing round 6: %s", e.Text)
				}
			}
		}
		if !foundDecision {
			t.Fatalf("round 6 decision event not sent")
		}
	})
}

// auditLines returns the raw decisions.jsonl rows of repo.
func auditLines(t *testing.T, s *Service, repo string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(s.state(repo), "decisions.jsonl"))
	if err != nil {
		t.Fatalf("read decisions.jsonl: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func auditHas(t *testing.T, s *Service, repo, command string) bool {
	t.Helper()
	for _, line := range auditLines(t, s, repo) {
		var row struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal([]byte(line), &row); err == nil && row.Command == command {
			return true
		}
	}
	return false
}

// escalateArgsEqual compares two escalate-author argument vectors ignoring the
// --findings-file value, whose temp path differs per call.
func escalateArgsEqual(a, b []string) bool {
	strip := func(args []string) []string {
		out := make([]string, 0, len(args))
		for i := 0; i < len(args); i++ {
			if args[i] == "--findings-file" {
				i++
				continue
			}
			out = append(out, args[i])
		}
		return out
	}
	return slices.Equal(strip(a), strip(b))
}

func escalateSnapshot(run *roundRuleRunner) (calls int, args []string, findings string) {
	run.callsMu.Lock()
	defer run.callsMu.Unlock()
	return run.escalateCalls, append([]string(nil), run.lastArgs...), run.findingsJSON
}

// (a) rc 124 is pending: the ladder stays un-escalated and the next
// evaluation calls again with the same arguments.
func TestRoundRule_LadderPendingRetries(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	run.escalateRC = 124
	run.escalateOutput = "pending\n"

	deliver(t, s, r)

	if st := s.loadRoundRuleState(r.Name, 7); st.escalated() {
		t.Fatalf("rc 124 marked the ladder escalated")
	}
	calls, args1, findings1 := escalateSnapshot(run)
	if calls != 1 {
		t.Fatalf("want 1 escalate-author call, got %d", calls)
	}
	if !auditHas(t, s, r.Name, "ladder-pending") {
		t.Fatalf("rc 124 missing ladder-pending audit: %v", auditLines(t, s, r.Name))
	}

	deliver(t, s, r)

	calls, args2, findings2 := escalateSnapshot(run)
	if calls != 2 {
		t.Fatalf("pending rc 124 must be retried on the next evaluation, calls = %d", calls)
	}
	if !escalateArgsEqual(args1, args2) {
		t.Fatalf("retry args differ: %v vs %v", args1, args2)
	}
	if findings1 != findings2 {
		t.Fatalf("retry findings file content differs:\n%s\nvs\n%s", findings1, findings2)
	}
}

// (b) rc 1, and rc 124 past the retry bound, end the ladder with no confirmed
// successor: one decision event to the owner lane, none on later passes.
func TestRoundRule_LadderEndedDecisionOnce(t *testing.T) {
	decisionEvents := func(n *ackNotifier) []integrations.Event {
		var out []integrations.Event
		for _, e := range n.events {
			if e.Type == "decision" && strings.HasPrefix(e.ID, "mergewake-ladder-ended-") {
				out = append(out, e)
			}
		}
		return out
	}
	checkEnded := func(t *testing.T, s *Service, n *ackNotifier, run *roundRuleRunner, r config.Repo, wantCalls int) {
		t.Helper()
		if st := s.loadRoundRuleState(r.Name, 7); !st.escalated() {
			t.Fatalf("ended outcome must mark the ladder escalated")
		}
		if !auditHas(t, s, r.Name, "ladder-ended-no-successor") {
			t.Fatalf("ended outcome missing ladder-ended-no-successor audit: %v", auditLines(t, s, r.Name))
		}
		evs := decisionEvents(n)
		if len(evs) != 1 {
			t.Fatalf("want exactly 1 decision event, got %d: %+v", len(evs), n.events)
		}
		if evs[0].Lane != "design-lane" {
			t.Fatalf("decision event lane = %q, want design-lane", evs[0].Lane)
		}
		for _, want := range []string{"author ended without successor", "example/project#7"} {
			if !strings.Contains(evs[0].Text, want) {
				t.Fatalf("decision text missing %q: %s", want, evs[0].Text)
			}
		}
		calls, _, _ := escalateSnapshot(run)
		if calls != wantCalls {
			t.Fatalf("escalate-author calls = %d, want %d", calls, wantCalls)
		}
	}

	t.Run("rc_1", func(t *testing.T) {
		s, g, n, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}
		run.escalateRC = 1
		run.escalateOutput = "rebind_unconfirmed\n"

		deliver(t, s, r)
		deliver(t, s, r)
		checkEnded(t, s, n, run, r, 1)
	})

	t.Run("pending_past_bound", func(t *testing.T) {
		s, g, n, _, run, r := roundRuleSetup(t, 5)
		g.feedback.Threads = []github.ReviewThread{
			ownFinding("T1", "high", 10),
		}
		run.escalateRC = 124
		run.escalateOutput = "pending\n"

		deliver(t, s, r)
		if st := s.loadRoundRuleState(r.Name, 7); st.escalated() {
			t.Fatalf("first rc 124 must not mark the ladder escalated")
		}

		// Move the clock past the pending retry bound; the next rc 124 is
		// treated as ended.
		s.Now = func() time.Time { return time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC) }
		deliver(t, s, r)
		deliver(t, s, r)
		checkEnded(t, s, n, run, r, 2)
	})
}

// (c) rc 0 and rc 2 mark the ladder escalated with their audit and are not
// retried.
func TestRoundRule_LadderTerminalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name      string
		rc        int
		output    string
		wantAudit string
	}{
		{"rc_0", 0, "", "ladder-escalated"},
		{"rc_2", 2, "quota_refused\n", "ladder-intact"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, run, r := roundRuleSetup(t, 5)
			g.feedback.Threads = []github.ReviewThread{
				ownFinding("T1", "high", 10),
			}
			run.escalateRC = tt.rc
			run.escalateOutput = tt.output

			deliver(t, s, r)
			deliver(t, s, r)

			if st := s.loadRoundRuleState(r.Name, 7); !st.escalated() {
				t.Fatalf("%s must mark the ladder escalated", tt.name)
			}
			if !auditHas(t, s, r.Name, tt.wantAudit) {
				t.Fatalf("%s missing %s audit: %v", tt.name, tt.wantAudit, auditLines(t, s, r.Name))
			}
			calls, _, _ := escalateSnapshot(run)
			if calls != 1 {
				t.Fatalf("%s must not be retried, calls = %d", tt.name, calls)
			}
		})
	}
}

// (d) two evaluations of the same PR at once make at most one
// escalate-author call.
func TestRoundRule_LadderConcurrentEvaluationsSingleCall(t *testing.T) {
	s, g, _, _, run, r := roundRuleSetup(t, 5)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			st := s.loadRoundRuleState(r.Name, 7)
			s.escalateAuthorLadder(context.Background(), r, g.pull, g.feedback, Options{}, 5, &st)
		}()
	}
	close(start)
	wg.Wait()

	calls, _, _ := escalateSnapshot(run)
	if calls > 1 {
		t.Fatalf("two concurrent evaluations made %d escalate-author calls, want at most 1", calls)
	}
	if calls != 1 {
		t.Fatalf("want 1 escalate-author call, got %d", calls)
	}
	if st := s.loadRoundRuleState(r.Name, 7); !st.escalated() {
		t.Fatalf("the evaluation that ran did not record the outcome")
	}
}
