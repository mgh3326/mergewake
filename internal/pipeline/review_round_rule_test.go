package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"

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
		if rc == 2 {
			return []byte(out), &integrations.CommandError{
				Command: "panewire",
				Err:     errors.New("exit status 2"),
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
