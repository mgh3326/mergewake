package pipeline

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// emitRecorder is panewire under a round hold. It records every `panewire
// emit` of the round-hold decision and answers them with the scripted stderr
// of rejects, one per call (empty: accepted). Every other call goes to the
// round-rule runner it wraps.
type emitRecorder struct {
	*roundRuleRunner
	mu       sync.Mutex
	holdArgs [][]string
	rejects  []string
}

func (e *emitRecorder) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "emit" && strings.HasPrefix(flagValue(args, "--event-id"), "mergewake-round-hold-") {
		e.mu.Lock()
		e.holdArgs = append(e.holdArgs, append([]string(nil), args...))
		var stderr string
		if len(e.rejects) > 0 {
			stderr, e.rejects = e.rejects[0], e.rejects[1:]
		}
		e.mu.Unlock()
		if stderr != "" {
			return nil, &integrations.CommandError{Command: command, Err: errors.New("exit status 2"), Stderr: stderr}
		}
		return nil, nil
	}
	return e.roundRuleRunner.Run(ctx, command, args, input, dir)
}

// roundHoldAtSeven is a PR held at round 7 with one open finding. The notifier
// is left as the test fixture's, so a test chooses what carries the events.
func roundHoldAtSeven(t *testing.T) (*Service, *roundRuleGH, *emitRecorder, config.Repo) {
	t.Helper()
	s, g, _, _, run, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	rec := &emitRecorder{roundRuleRunner: run}
	s.Runner = rec
	return s, g, rec, r
}

// wirePanewire sends notifications through the real panewire notifier, so the
// argv the decision builds is the argv panewire receives.
func wirePanewire(s *Service, rec *emitRecorder) {
	s.Notifier = integrations.Panewire{Runner: rec, Command: "panewire", Lane: "operator-desk"}
}

func flagValue(args []string, flag string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

// panewireLanePattern is panewire's hubAgentLabelPattern, the --lane rule
// (hub_jobs.go:15 at panewire 1b4d7b7).
var panewireLanePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.:-]{0,127}$`)

// emitTextOK is panewire's validLaneEventText and validLaneEventID rule
// (emit.go:469-491 at 1b4d7b7): valid UTF-8 and no byte or rune in the
// control ranges 0x00-0x1f and 0x7f-0x9f.
func emitTextOK(v string) bool {
	if v == "" || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}

// TestRoundHoldDecisionEmitPassesPanewireEmitRules: the round-hold decision is
// emitted with argv panewire's `emit --kind lane.event` accepts (emit.go:
// 161-196 at 1b4d7b7): a lane matching hubAgentLabelPattern, an event id and
// a text free of control bytes, a text within laneEventTextLimit (2048,
// emit.go:52) so panewire never cuts it, and a declared --type.
func TestRoundHoldDecisionEmitPassesPanewireEmitRules(t *testing.T) {
	s, _, rec, r := roundHoldAtSeven(t)
	wirePanewire(s, rec)
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatalf("NotifyPR err: %v", err)
	}
	if len(rec.holdArgs) != 1 {
		t.Fatalf("want one round-hold emit, got %d", len(rec.holdArgs))
	}
	args := rec.holdArgs[0]
	if got := flagValue(args, "--kind"); got != "lane.event" {
		t.Errorf("--kind = %q, want lane.event", got)
	}
	if lane := flagValue(args, "--lane"); !panewireLanePattern.MatchString(lane) {
		t.Errorf("--lane %q fails panewire's lane rule", lane)
	}
	if id := flagValue(args, "--event-id"); len(id) > 512 || !emitTextOK(id) {
		t.Errorf("--event-id %q fails panewire's event-id rule", id)
	}
	text := flagValue(args, "--text")
	if !emitTextOK(text) {
		t.Errorf("--text carries a control byte or is not UTF-8 (panewire rejects it with exit 2): %q", text)
	}
	if len(text) > 2048 {
		t.Errorf("--text is %d bytes, over panewire's laneEventTextLimit 2048: %q", len(text), text)
	}
	if got := flagValue(args, "--type"); got != "decision" {
		t.Errorf("--type = %q, want decision", got)
	}
	for _, want := range []string{"#7", "round 7", "finding 1:", "cut the scope", "SS consult", "Mergewake-Round-Hold: release", "<!-- mergewake"} {
		if !strings.Contains(text, want) {
			t.Errorf("decision text missing %q: %q", want, text)
		}
	}
}

func TestRoundHoldInvalidDesignLaneUsesConfiguredDeskLane(t *testing.T) {
	s, g, _, sink, run, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 10)}
	sink.lanes["2481"] = "design_a"
	s.Config.Notifier.Lane = "desk-a"
	s.Config.Notifier.FallbackLane = "desk-a"
	rec := &emitRecorder{roundRuleRunner: run}
	s.Runner = rec
	wirePanewire(s, rec)

	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatalf("NotifyPR err: %v", err)
	}
	if len(rec.holdArgs) != 1 {
		t.Fatalf("want one round-hold emit, got %d", len(rec.holdArgs))
	}
	if got := flagValue(rec.holdArgs[0], "--lane"); got != "desk-a" {
		t.Fatalf("round-hold --lane = %q, want desk-a", got)
	}
}

// TestRoundHoldDecisionNotifyFailureAuditedAndRetried: a rejected emit keeps
// panewire's redacted, bounded stderr in the audit and is sent again on the
// next pass; once accepted it is never sent again for that hold.
func TestRoundHoldDecisionNotifyFailureAuditedAndRetried(t *testing.T) {
	s, _, rec, r := roundHoldAtSeven(t)
	wirePanewire(s, rec)
	rec.rejects = []string{"emit: invalid --text\ncontact operator@example.com\n"}
	ctx := context.Background()

	if err := s.NotifyPR(ctx, r, 7, Options{}); err != nil {
		t.Fatalf("pass 1 NotifyPR err: %v", err)
	}
	if len(rec.holdArgs) != 1 {
		t.Fatalf("pass 1: want one emit, got %d", len(rec.holdArgs))
	}
	if st := s.loadRoundRuleState(r.Name, 7); st.eventSent() {
		t.Fatalf("pass 1: a rejected event was recorded as sent")
	}
	rows := auditRows(t, s, r, "round-hold-notify-failed")
	if len(rows) != 1 {
		t.Fatalf("pass 1: want one round-hold-notify-failed audit row, got %v", rows)
	}
	msg, _ := rows[0]["error"].(string)
	if !strings.Contains(msg, "emit: invalid --text") {
		t.Errorf("audit error lacks panewire's stderr: %q", msg)
	}
	if strings.Contains(msg, "operator@example.com") {
		t.Errorf("audit error is not redacted: %q", msg)
	}
	if len(msg) > 1024 {
		t.Errorf("audit error is %d bytes, not bounded: %q", len(msg), msg)
	}

	if err := s.NotifyPR(ctx, r, 7, Options{}); err != nil {
		t.Fatalf("pass 2 NotifyPR err: %v", err)
	}
	if len(rec.holdArgs) != 2 {
		t.Fatalf("pass 2: want the event retried (2 emits), got %d", len(rec.holdArgs))
	}
	if a, b := flagValue(rec.holdArgs[0], "--event-id"), flagValue(rec.holdArgs[1], "--event-id"); a != b {
		t.Errorf("event id changed between retries: %q then %q", a, b)
	}
	if st := s.loadRoundRuleState(r.Name, 7); !st.eventSent() {
		t.Fatalf("pass 2: accepted event not recorded as sent")
	}

	if err := s.NotifyPR(ctx, r, 7, Options{}); err != nil {
		t.Fatalf("pass 3 NotifyPR err: %v", err)
	}
	if len(rec.holdArgs) != 2 {
		t.Fatalf("pass 3: an accepted event was sent again (%d emits)", len(rec.holdArgs))
	}
}

// TestRoundHoldIsNotReviewFailure: review-retry does not count a round hold as
// a failure, backs off nothing and sends no "review failed" event, however
// many passes see the hold.
func TestRoundHoldIsNotReviewFailure(t *testing.T) {
	s, g, _, r := roundHoldAtSeven(t)
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 1}
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	if v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB); err != nil || v.State != "blocked" {
		t.Fatalf("mergeGate: state=%v err=%v, want blocked", v.State, err)
	}
	for i := 1; i <= 3; i++ {
		if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewHeld) {
			t.Fatalf("run %d: err=%v, want ErrReviewHeld", i, err)
		}
		st := retryStateOf(t, s, r)
		if st.Failures != 0 || st.Backoffs != 0 || st.Escalated {
			t.Fatalf("run %d: state=%+v, want no failure, backoff or escalation", i, st)
		}
	}
	for _, e := range s.Notifier.(*ackNotifier).events {
		if strings.Contains(e.Text, "review failed") {
			t.Fatalf("a round hold sent a review-failed event: %+v", e)
		}
	}
	if rows := auditRows(t, s, r, "review-failed"); len(rows) != 0 {
		t.Fatalf("a round hold wrote review-failed audit rows: %v", rows)
	}
	var held reviewFailureClass
	held.add(ErrReviewHeld)
	if held.real || len(held.failures) != 0 {
		t.Fatalf("a round hold was classified as a failure: %+v", held)
	}
}

// TestReviewFailureStillCountsAsFailure: a real review failure is still a
// counted failure, also when it comes joined with a hold.
func TestReviewFailureStillCountsAsFailure(t *testing.T) {
	var real reviewFailureClass
	real.add(errors.New("review: reviewer exited 1"))
	if !real.real || len(real.failures) != 1 {
		t.Fatalf("a real failure was not counted: %+v", real)
	}
	var joined reviewFailureClass
	joined.add(errors.Join(ErrReviewHeld, errors.New("review: reviewer exited 1")))
	if !joined.real {
		t.Fatalf("a real failure joined with a hold was not counted: %+v", joined)
	}
}
