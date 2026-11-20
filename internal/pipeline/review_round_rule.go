package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// roundHoldNotifyDetailLimit bounds the detail of a failed decision notify in
// the log and the audit.
const roundHoldNotifyDetailLimit = 512

// ladderPendingBound bounds how long a pending (rc 124) escalate-author may
// be retried before the outcome is treated as ended without a successor.
const ladderPendingBound = 2 * time.Hour

// ladderPaneBusyMaxCalls bounds retries after panewire cannot reach the author pane.
const ladderPaneBusyMaxCalls = 3

var ladderHubRefusalCodes = map[string]bool{
	"no_rung": true, "successor_invalid": true, "job_unreadable": true,
	"job_changed": true, "job_not_live": true, "pane_busy": true,
	"quota_refused": true, "pane_unavailable": true, "worktree_unreadable": true,
}

// escalateExitCode is the exit code of a failed panewire call, or -1 when there
// is none. integrations.Exec wraps the process error with a prefix ("panewire
// failed: exit status 2"), so the code is found anywhere in the message, not
// only at its start.
func escalateExitCode(err error) int {
	var ee interface{ ExitCode() int }
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	msg := err.Error()
	if i := strings.LastIndex(msg, "exit status "); i >= 0 {
		var code int
		if _, scanErr := fmt.Sscanf(msg[i:], "exit status %d", &code); scanErr == nil {
			return code
		}
	}
	return -1
}

func ladderRefusal(output string) (code string, hub bool) {
	text := strings.TrimSpace(output)
	text = strings.TrimPrefix(text, "panewire: ")
	if i := strings.IndexByte(text, ':'); i > 0 {
		candidate := strings.TrimSpace(text[:i])
		if ladderHubRefusalCodes[candidate] {
			return candidate, true
		}
	}
	return "", false
}

// roundHoldNotifyDetail is a failed notify's error with panewire's stderr,
// redacted, as one line of at most roundHoldNotifyDetailLimit bytes. The
// stderr is the reason panewire refused the event; CommandError keeps it out
// of its own message.
func roundHoldNotifyDetail(err error) string {
	detail := err.Error()
	var ce *integrations.CommandError
	if errors.As(err, &ce) && strings.TrimSpace(ce.Stderr) != "" {
		detail += ": " + fleetevent.Redact(strings.TrimSpace(ce.Stderr))
	}
	return clip(strings.Join(strings.Fields(detail), " "), roundHoldNotifyDetailLimit)
}

// roundHoldNotifyFailed records a decision notify that panewire refused: the
// log and one audit row. The event stays unsent, so a later pass sends it again.
func (s *Service) roundHoldNotifyFailed(r config.Repo, pr github.Pull, rounds int, err error, o Options) {
	detail := roundHoldNotifyDetail(err)
	if s.Log != nil {
		s.Log.Warn("round-hold decision notify failed", "pr", pr.Number, "error", detail)
	}
	if aerr := s.audit(r.Name, "round-hold-notify-failed", map[string]any{"pr": pr.Number, "round": rounds, "error": detail}, o.DryRun); aerr != nil && s.Log != nil {
		s.Log.Warn("round-hold notify audit failed", "pr", pr.Number, "error", aerr)
	}
}

// Round3Directive is sent to the author when round 3 (or configured consult round) ends without a pass.
const Round3Directive = "stop patching; ask your design lane for a consult one grade above your seat in another family: ESC --type decision to your owner lane with the packet as an hk doc key (every finding so far, ask for the root cause); do not run panewire consult request and do not call any model API yourself"

// RoundDirective is an alias for Round3Directive.
const RoundDirective = Round3Directive

var roundHoldReleaseRegex = regexp.MustCompile(`(?i)\bMergewake-Round-Hold:\s*release\b`)

// roundRuleState persists whether consult directive and ladder escalation have run,
// and whether hold and decision event are active or released.
type roundRuleState struct {
	// Consult
	ConsultDirectiveSent bool      `json:"consult_directive_sent,omitempty"`
	Round3DirectiveSent  bool      `json:"round3_directive_sent,omitempty"` // legacy alias
	ConsultAt            time.Time `json:"consult_at,omitzero"`

	// Hub consult (review.round_rule.hub_consult, hk 4551): one hub consult
	// per PR and head. The stored row fields let a later pass or a restart
	// poll instead of requesting again.
	//
	// Owed marks an outstanding consult and is independent of the current
	// round: it is set when the consult is requested and cleared once its
	// outcome went out with a delivery. Head is the commit the row was requested
	// for — the hub keys consults by (task, repo#pr, head); after a push the
	// row is still followed and its outcome names this head.
	// Posted records that the request may have reached the hub, so an id lost
	// to a failed state write is reconciled from the audit journal instead of
	// a re-POST. Done marks the outcome settled and audited.
	HubConsultOwed bool   `json:"hub_consult_owed,omitempty"`
	HubConsultHead string `json:"hub_consult_head,omitempty"`
	// HubConsultRound is the round the consult was requested at.
	HubConsultRound     int       `json:"hub_consult_round,omitempty"`
	HubConsultPosted    bool      `json:"hub_consult_posted,omitempty"`
	HubConsultID        string    `json:"hub_consult_id,omitempty"`
	HubConsultState     string    `json:"hub_consult_state,omitempty"`
	HubConsultQuestion  string    `json:"hub_consult_question,omitempty"`
	HubConsultAnswerDoc string    `json:"hub_consult_answer_doc,omitempty"`
	HubConsultVerdict   string    `json:"hub_consult_verdict,omitempty"`
	HubConsultSeat      string    `json:"hub_consult_seat,omitempty"`
	HubConsultJob       string    `json:"hub_consult_job,omitempty"`
	HubConsultReason    string    `json:"hub_consult_reason,omitempty"`
	HubConsultAt        time.Time `json:"hub_consult_at,omitzero"`
	HubConsultDone      bool      `json:"hub_consult_done,omitempty"`
	HubConsultCommented bool      `json:"hub_consult_commented,omitempty"`
	// HubConsultCarried records that a push moved the head while the row was
	// outstanding: from then on its POST is never replayed, whatever head the
	// PR is on, since the row may have settled at the hub.
	HubConsultCarried bool `json:"hub_consult_carried,omitempty"`

	// Ladder
	LadderEscalated bool      `json:"ladder_escalated,omitempty"`
	Round3Escalated bool      `json:"round3_escalated,omitempty"` // legacy alias
	LadderOutput    string    `json:"ladder_output,omitempty"`
	LadderAt        time.Time `json:"ladder_at,omitzero"`
	// LadderPendingSince is the first pending (rc 124) outcome; retries stop
	// at ladderPendingBound and the step is treated as ended.
	LadderPendingSince time.Time `json:"ladder_pending_since,omitzero"`
	// LadderEndedNoSuccessor records an ended or unconfirmed outcome;
	// LadderEndedEventSent marks its one owner-lane decision event delivered.
	LadderEndedNoSuccessor bool   `json:"ladder_ended_no_successor,omitempty"`
	LadderEndedEventSent   bool   `json:"ladder_ended_event_sent,omitempty"`
	LadderPaneBusyCalls    int    `json:"ladder_pane_busy_calls,omitempty"`
	LadderRefusalCode      string `json:"ladder_refusal_code,omitempty"`
	LadderRefusalText      string `json:"ladder_refusal_text,omitempty"`
	LadderRefusalRound     int    `json:"ladder_refusal_round,omitempty"`
	LadderRefusalEventSent bool   `json:"ladder_refusal_event_sent,omitempty"`

	// Hold
	HoldHeld               bool      `json:"hold_held,omitempty"`
	Round5Held             bool      `json:"round5_held,omitempty"` // legacy alias
	HoldEventSent          bool      `json:"hold_event_sent,omitempty"`
	Round5EventSent        bool      `json:"round5_event_sent,omitempty"` // legacy alias
	HoldReleased           bool      `json:"hold_released,omitempty"`
	Round5Released         bool      `json:"round5_released,omitempty"` // legacy alias
	HoldReleasedBy         string    `json:"hold_released_by,omitempty"`
	Round5ReleasedBy       string    `json:"round5_released_by,omitempty"` // legacy alias
	HoldAt                 time.Time `json:"hold_at,omitzero"`
	Round5At               time.Time `json:"round5_at,omitzero"` // legacy alias
	HoldCommentCount       int       `json:"hold_comment_count,omitempty"`
	Round5CommentCount     int       `json:"round5_comment_count,omitempty"` // legacy alias
	HoldBaselineCaptured   bool      `json:"hold_baseline_captured,omitempty"`
	Round5BaselineCaptured bool      `json:"round5_baseline_captured,omitempty"` // legacy alias
}

func (st *roundRuleState) directiveSent() bool {
	return st.ConsultDirectiveSent || st.Round3DirectiveSent
}

func (st *roundRuleState) setDirectiveSent(v bool, now time.Time) {
	st.ConsultDirectiveSent = v
	st.Round3DirectiveSent = v
	st.ConsultAt = now
}

func (st *roundRuleState) escalated() bool {
	return st.LadderEscalated || st.Round3Escalated
}

func (st *roundRuleState) setEscalated(v bool, now time.Time, output string) {
	st.LadderEscalated = v
	st.Round3Escalated = v
	st.LadderAt = now
	st.LadderOutput = output
	st.LadderPendingSince = time.Time{}
}

func (st *roundRuleState) held() bool {
	return st.HoldHeld || st.Round5Held
}

func (st *roundRuleState) setHeld(v bool, now time.Time, commentCount int, baselineCaptured bool) {
	st.HoldHeld = v
	st.Round5Held = v
	st.HoldAt = now
	st.Round5At = now
	st.HoldCommentCount = commentCount
	st.Round5CommentCount = commentCount
	st.HoldBaselineCaptured = baselineCaptured
	st.Round5BaselineCaptured = baselineCaptured
}

func (st *roundRuleState) released() bool {
	return st.HoldReleased || st.Round5Released
}

func (st *roundRuleState) setReleased(v bool, by string) {
	st.HoldReleased = v
	st.Round5Released = v
	st.HoldReleasedBy = by
	st.Round5ReleasedBy = by
}

func (st *roundRuleState) eventSent() bool {
	return st.HoldEventSent || st.Round5EventSent
}

func (st *roundRuleState) setEventSent(v bool) {
	st.HoldEventSent = v
	st.Round5EventSent = v
}

func (st *roundRuleState) commentCount() int {
	if st.HoldCommentCount > 0 {
		return st.HoldCommentCount
	}
	return st.Round5CommentCount
}

func (st *roundRuleState) baselineCaptured() bool {
	return st.HoldBaselineCaptured || st.Round5BaselineCaptured
}

func (s *Service) roundRuleFile(repo string, pr int) string {
	return filepath.Join(s.state(repo), "round-rules", strconv.Itoa(pr)+".json")
}

func (s *Service) loadRoundRuleState(repo string, pr int) roundRuleState {
	var st roundRuleState
	_, _ = fsutil.ReadJSON(s.roundRuleFile(repo, pr), &st)
	return st
}

func (s *Service) saveRoundRuleState(repo string, pr int, st roundRuleState) error {
	return fsutil.WriteJSON(s.roundRuleFile(repo, pr), st)
}

// roundEndsWithoutPass reports whether the PR has at least one critical or high
// finding still open after the verdict.
func (s *Service) roundEndsWithoutPass(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback) (bool, []github.ReviewThread) {
	blocking, _ := s.openFindings(ctx, r, pr, fb)
	return len(blocking) > 0, blocking
}

// aboveLow reports whether a finding's severity ranks above low for the hub
// consult: medium and up, with the legacy aliases this tool ranks there —
// blocking (high) and major (CodeRabbit's rank above minor). An unrated or
// unknown severity does not count: the tool cannot tell it above low, and a
// blocking path already holds it for triage.
func aboveLow(severity string) bool {
	switch severity {
	case "critical", "high", "blocking", "medium", "major":
		return true
	}
	return false
}

// openFindings lists the threads still open after the verdict — unresolved and
// not disposed by the gate's rule — at the two severity thresholds the round
// rule reads: blocking (critical/high, the must-fix set) and aboveLow (medium
// and up, the hub consult's trigger, operator decision 250: one consult per PR
// and round, none for an all-low round). A thread can be in both.
func (s *Service) openFindings(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback) (blocking, above []github.ReviewThread) {
	verify := s.fixVerifier(ctx, r.Name, pr.Head.SHA).verified
	own := policy.NormalizeAuthor(s.Config.Review.Login)
	for _, t := range fb.Threads {
		if t.IsResolved || len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		sev := github.ThreadSeverity(first.Author, first.Body)
		must := mustFix(sev)
		if !must && !aboveLow(sev) {
			continue
		}
		if disposed(t, own, verify, s.Config.Review.FleetAuthors) {
			continue
		}
		if must {
			blocking = append(blocking, t)
		}
		above = append(above, t)
	}
	return blocking, above
}

type escalateFinding struct {
	Ref      string `json:"ref"`
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Title    string `json:"title"`
	Body     string `json:"body"`
}

// findingsFit counts the findings in an escalate-author file: sent ones and
// those left out to keep the file within escalateAuthorFindingsMax.
type findingsFit struct {
	sent, leftOut int
}

// buildFindingsFile encodes every unresolved thread as an escalateFinding, within
// escalateAuthorFindingsMax. Resolved threads are left out: they no longer need
// the author, and they must not take the bound from open findings. Bodies are
// cut before a finding is left out (see keepFindings); a file that fits
// unchanged is the same bytes as before.
func buildFindingsFile(fb github.ReviewFeedback) ([]byte, findingsFit, error) {
	list := make([]escalateFinding, 0, len(fb.Threads))
	sevs := make([]string, 0, len(fb.Threads))
	for _, t := range fb.Threads {
		if t.IsResolved || len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		ref := t.ID
		if ref == "" {
			ref = first.URL
		}
		sev := github.ThreadSeverity(first.Author, first.Body)
		if sev == "" {
			sev = "unrated"
		}
		title := firstLine(first.Body)
		if title == "" {
			title = t.Path + ":" + strconv.Itoa(t.Line)
		}
		list = append(list, escalateFinding{
			Ref:      ref,
			Severity: sev,
			Path:     t.Path,
			Line:     t.Line,
			Title:    title,
			Body:     first.Body,
		})
		sevs = append(sevs, sev)
	}
	keep, cut := keepFindings(sevs, escalateAuthorFindingsMax, func(keep []int, cut int) int {
		data, err := findingsJSON(list, keep, cut)
		if err != nil {
			return escalateAuthorFindingsMax + 1 // strings and ints cannot fail to encode
		}
		return len(data)
	})
	data, err := findingsJSON(list, keep, cut)
	return data, findingsFit{sent: len(keep), leftOut: len(list) - len(keep)}, err
}

// findingsJSON encodes the findings at keep (ascending indices) with each text
// field cut to cut bytes.
func findingsJSON(list []escalateFinding, keep []int, cut int) ([]byte, error) {
	out := make([]escalateFinding, 0, len(keep))
	for _, i := range keep {
		f := list[i]
		f.Title = cutText(f.Title, cut)
		f.Body = cutText(f.Body, cut)
		out = append(out, f)
	}
	return json.Marshal(out)
}

// escalateAuthorRound3 executes `panewire escalate-author` for compatibility.
func (s *Service) escalateAuthorRound3(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState) {
	s.escalateAuthorLadder(ctx, r, pr, fb, o, 3, st)
}

// escalateAuthorLadder executes `panewire escalate-author` once per (task, PR, round ladder)
// and acts on its exit code (panewire docs/escalate-author.md): 0 escalated,
// 2 intact, 124 pending — retried by a later evaluation, the call is
// idempotent — and 1 ended or unconfirmed. Any other code or a run error keeps
// the old behaviour: marked escalated and audited as an error. The whole step
// runs under the round-rule state lock, so two evaluations of the same PR
// cannot call panewire twice.
func (s *Service) escalateAuthorLadder(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, ladderRound int, st *roundRuleState) {
	if o.DryRun {
		return
	}
	file := s.roundRuleFile(r.Name, pr.Number)
	defer lockPath(file)()
	unlock, err := fsutil.Lock(file+".lock", true)
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("round-rule state lock failed", "pr", pr.Number, "error", err)
		}
		return
	}
	defer unlock()

	// The caller's copy may be stale: another evaluation of this PR can
	// settle between the caller's load and this lock.
	*st = s.loadRoundRuleState(r.Name, pr.Number)
	if st.escalated() {
		s.sendLadderEndedDecision(ctx, r, pr, o, st)
		s.sendLadderRefusalDecision(ctx, r, pr, ladderRound, o, st)
		return
	}

	task, _ := s.prTask(ctx, r, pr)
	job, _, _, _ := s.boundJob(ctx, task)

	data, fit, err := buildFindingsFile(fb)
	if err != nil {
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{"pr": pr.Number, "error": err.Error()}, o.DryRun)
		return
	}

	cleanRepo := strings.ReplaceAll(r.Name, "/", "-")
	tmp, err := os.CreateTemp("", fmt.Sprintf("mw-findings-%s-%d-*.json", cleanRepo, pr.Number))
	if err != nil {
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{"pr": pr.Number, "error": err.Error()}, o.DryRun)
		return
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{"pr": pr.Number, "error": err.Error()}, o.DryRun)
		return
	}
	tmp.Close()

	args := []string{
		"escalate-author",
		"--task", task,
		"--job", job,
		"--pr", fmt.Sprintf("%s#%d", r.Name, pr.Number),
		"--round", strconv.Itoa(ladderRound),
		"--findings-file", tmp.Name(),
	}

	cmdCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Run drops stdout on a failed exit; RunWithOutput keeps it, so a refusal
	// that panewire prints there reaches ladderRefusal.
	var out []byte
	if withOutput, ok := s.Runner.(interface {
		RunWithOutput(context.Context, string, []string, string, string) ([]byte, error)
	}); ok {
		out, err = withOutput.RunWithOutput(cmdCtx, "panewire", args, "", "")
	} else {
		out, err = s.Runner.Run(cmdCtx, "panewire", args, "", "")
	}
	outputStr := strings.TrimSpace(string(out))
	now := s.now().UTC()

	if err == nil {
		st.setEscalated(true, now, outputStr)
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "ladder-escalated", map[string]any{
			"pr":                pr.Number,
			"round":             ladderRound,
			"rc":                0,
			"output":            outputStr,
			"findings_sent":     fit.sent,
			"findings_left_out": fit.leftOut,
		}, o.DryRun)
		if s.Log != nil {
			s.Log.Info("escalate-author escalated", "pr", pr.Number, "output", outputStr)
		}
		return
	}

	exitCode := escalateExitCode(err)
	var ce *integrations.CommandError
	if errors.As(err, &ce) && outputStr == "" && ce.Stderr != "" {
		outputStr = strings.TrimSpace(ce.Stderr)
	}

	switch exitCode {
	case 2:
		code, hub := ladderRefusal(outputStr)
		st.LadderRefusalCode = code
		if hub && code == "pane_busy" {
			st.LadderPaneBusyCalls++
			if st.LadderPaneBusyCalls < ladderPaneBusyMaxCalls {
				st.LadderOutput = outputStr
				_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
				_ = s.audit(r.Name, "ladder-pane-busy-retry", map[string]any{
					"pr": pr.Number, "round": ladderRound, "rc": 2,
					"code": code, "calls": st.LadderPaneBusyCalls, "output": outputStr,
				}, o.DryRun)
				return
			}
		}
		st.setEscalated(true, now, outputStr)
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "ladder-intact", map[string]any{
			"pr":                pr.Number,
			"round":             ladderRound,
			"rc":                2,
			"code":              code,
			"output":            outputStr,
			"findings_sent":     fit.sent,
			"findings_left_out": fit.leftOut,
		}, o.DryRun)
		if !hub {
			// Empty output must still owe the owner a notice: an empty text
			// would skip the send and leave the refusal owed with no retry.
			refusal := outputStr
			if refusal == "" {
				refusal = "panewire exited 2 with no output"
			}
			st.LadderRefusalText = refusal
			st.LadderRefusalRound = ladderRound
			st.LadderRefusalEventSent = false
			_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
			s.sendLadderRefusalDecision(ctx, r, pr, ladderRound, o, st)
		}
		if s.Log != nil {
			s.Log.Info("escalate-author intact", "pr", pr.Number, "output", outputStr)
		}
	case 124:
		if st.LadderPendingSince.IsZero() {
			st.LadderPendingSince = now
		}
		if now.Sub(st.LadderPendingSince) < ladderPendingBound {
			_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
			_ = s.audit(r.Name, "ladder-pending", map[string]any{
				"pr":                pr.Number,
				"round":             ladderRound,
				"rc":                exitCode,
				"output":            outputStr,
				"findings_sent":     fit.sent,
				"findings_left_out": fit.leftOut,
			}, o.DryRun)
			if s.Log != nil {
				s.Log.Info("escalate-author pending", "pr", pr.Number, "output", outputStr)
			}
			return
		}
		s.ladderEnded(ctx, r, pr, o, st, now, outputStr, ladderRound, exitCode, fit)
	case 1:
		s.ladderEnded(ctx, r, pr, o, st, now, outputStr, ladderRound, exitCode, fit)
	default:
		st.setEscalated(true, now, outputStr)
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{
			"pr":                pr.Number,
			"round":             ladderRound,
			"rc":                exitCode,
			"error":             err.Error(),
			"output":            outputStr,
			"findings_sent":     fit.sent,
			"findings_left_out": fit.leftOut,
		}, o.DryRun)
		if s.Log != nil {
			s.Log.Warn("escalate-author error", "pr", pr.Number, "error", err, "output", outputStr)
		}
	}
}

// sendLadderRefusalDecision tells the owner when CLI preflight prevented the
// ladder step from replacing the author. The event identity is stable across retries.
func (s *Service) sendLadderRefusalDecision(ctx context.Context, r config.Repo, pr github.Pull, ladderRound int, o Options, st *roundRuleState) {
	if st.LadderRefusalText == "" || st.LadderRefusalEventSent || !o.Reporting() {
		return
	}
	lane := s.designLane(ctx, r, pr)
	if !config.PanewireLane(lane) {
		lane = s.deskLane()
	}
	round := st.LadderRefusalRound
	if round == 0 {
		round = ladderRound
	}
	eventID := fmt.Sprintf("mergewake-ladder-refusal-%s-%d-%d", r.Name, pr.Number, round)
	text := emitSafe(clip(fmt.Sprintf("PR %s#%d round %d: escalate-author was refused before replacing the author: %s", r.Name, pr.Number, round, strings.TrimPrefix(strings.TrimSpace(st.LadderRefusalText), "panewire: ")), findingsLaneText))
	ctx, cancel := reportContext(ctx)
	defer cancel()
	if err := s.Notifier.Notify(ctx, integrations.Event{ID: eventID, Lane: lane, Type: "decision", Text: text}); err != nil {
		_ = s.audit(r.Name, "ladder-refusal-notify-failed", map[string]any{"pr": pr.Number, "round": round, "error": roundHoldNotifyDetail(err)}, o.DryRun)
		return
	}
	st.LadderRefusalEventSent = true
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
}

func (st roundRuleState) ladderRefusalOwed() bool {
	return st.LadderRefusalText != "" && !st.LadderRefusalEventSent
}

// ladderEnded records the ended-without-successor outcome: the step is done
// (marked escalated so it is not retried), audited, and the owner lane gets
// one decision event. The caller holds the round-rule state lock.
func (s *Service) ladderEnded(ctx context.Context, r config.Repo, pr github.Pull, o Options, st *roundRuleState, now time.Time, output string, ladderRound, exitCode int, fit findingsFit) {
	st.setEscalated(true, now, output)
	st.LadderEndedNoSuccessor = true
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
	_ = s.audit(r.Name, "ladder-ended-no-successor", map[string]any{
		"pr":                pr.Number,
		"round":             ladderRound,
		"rc":                exitCode,
		"output":            output,
		"findings_sent":     fit.sent,
		"findings_left_out": fit.leftOut,
	}, o.DryRun)
	if s.Log != nil {
		s.Log.Warn("escalate-author ended without successor", "pr", pr.Number, "rc", exitCode, "output", output)
	}
	s.sendLadderEndedDecision(ctx, r, pr, o, st)
}

// sendLadderEndedDecision emits the one decision event owed when the ladder
// ended with no confirmed successor, to the same lane the round-hold decision
// uses. The caller holds the round-rule state lock, so the sent flag and the
// event cannot race a second evaluation; a failed notify stays unsent and a
// later pass retries it.
func (s *Service) sendLadderEndedDecision(ctx context.Context, r config.Repo, pr github.Pull, o Options, st *roundRuleState) {
	if !st.LadderEndedNoSuccessor || st.LadderEndedEventSent || !o.Reporting() {
		return
	}
	lane := s.designLane(ctx, r, pr)
	if !config.PanewireLane(lane) {
		lane = s.deskLane()
	}
	eventID := fmt.Sprintf("mergewake-ladder-ended-%s-%d", r.Name, pr.Number)
	text := emitSafe(fmt.Sprintf("author ended without successor; PR %s#%d needs a new author", r.Name, pr.Number))

	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if err := s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   eventID,
		Lane: lane,
		Type: "decision",
		Text: text,
	}); err != nil {
		detail := roundHoldNotifyDetail(err)
		if s.Log != nil {
			s.Log.Warn("ladder-ended decision notify failed", "pr", pr.Number, "error", detail)
		}
		if aerr := s.audit(r.Name, "ladder-ended-notify-failed", map[string]any{"pr": pr.Number, "error": detail}, o.DryRun); aerr != nil && s.Log != nil {
			s.Log.Warn("ladder-ended notify audit failed", "pr", pr.Number, "error", aerr)
		}
		return
	}
	st.LadderEndedEventSent = true
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
}

// checkRoundHoldRelease finds the first comment that releases the round hold:
// it matches the release phrase, is not from a bot, and is not a comment
// mergewake generated — those carry a `<!-- mergewake` marker. The review
// login is not skipped: it is also the operator's account, and only its
// mergewake-marked comments are excluded.
func checkRoundHoldRelease(comments []github.IssueComment) (bool, string) {
	for _, c := range comments {
		if c.User.Type == "Bot" || strings.HasSuffix(c.User.Login, "[bot]") || strings.Contains(c.Body, "<!-- mergewake") {
			continue
		}
		if roundHoldReleaseRegex.MatchString(c.Body) {
			return true, c.User.Login
		}
	}
	return false, ""
}

// designLane returns the PR's design lane for holds that need a person: the
// lane the PR names outright (its Mergewake-Notify-Lane trailer) or its
// task's lane, then the repo's lane. The desk takes it only when no lane
// resolves, i.e. a repo with no design session.
func (s *Service) designLane(ctx context.Context, r config.Repo, pr github.Pull) string {
	desk := s.deskLane()
	if task, err := s.prTask(ctx, r, pr); err == nil {
		if lane, _, err := s.explicitLane(ctx, r, pr, task, desk); err == nil && lane != "" {
			return lane
		}
	}
	return s.ownerLane(r)
}

// emitSafe replaces every control byte and rune (0x00-0x1f, 0x7f-0x9f) with a
// space: panewire's `emit --text` refuses any of them with exit 2 (emit.go:
// 481-491 at panewire 1b4d7b7), so a lane event's text is one line of printable
// runes.
func emitSafe(s string) string {
	return strings.Map(func(r rune) rune {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return ' '
		}
		return r
	}, s)
}

func round5DecisionText(r config.Repo, pr github.Pull, rounds int, open []github.ReviewThread) string {
	head := "<!-- mergewake-round-hold --> " + fmt.Sprintf("[mergewake] ESC %s#%d | round %d ended without a pass (%d open finding(s))", r.Name, pr.Number, rounds, len(open))
	choices := " | choices: 1) cut the scope (the part that keeps drawing findings goes to a follow-up task, the rest merges) or 2) one SS consult on the design | reply with PR comment: Mergewake-Round-Hold: release"

	var details strings.Builder
	for i, t := range open {
		if len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		sev := github.ThreadSeverity(first.Author, first.Body)
		item := fmt.Sprintf("; finding %d: %s %s:%d %s", i+1, sev, t.Path, t.Line, t.ID)
		if len(head)+details.Len()+len(item)+len(choices) > findingsLaneText {
			break
		}
		details.WriteString(item)
	}
	text := emitSafe(head + details.String() + choices)
	return clip(text, findingsLaneText)
}

func (s *Service) issueComments(ctx context.Context, repo string, n int) (comments []github.IssueComment, err error) {
	if s.GH == nil {
		return nil, nil
	}
	defer func() {
		if r := recover(); r != nil {
			comments, err = nil, fmt.Errorf("panic fetching issue comments: %v", r)
		}
	}()
	return s.GH.IssueComments(ctx, repo, n)
}

// isRoundHoldActive checks whether PR is held under round hold and not released.
func (s *Service) isRoundHoldActive(ctx context.Context, r config.Repo, pr github.Pull) bool {
	st := s.loadRoundRuleState(r.Name, pr.Number)
	if !st.held() || st.released() {
		return false
	}
	comments, err := s.issueComments(ctx, r.Name, pr.Number)
	if err != nil {
		return true
	}
	if !st.baselineCaptured() {
		st.setHeld(true, st.HoldAt, len(comments), true)
		_ = s.saveRoundRuleState(r.Name, pr.Number, st)
		return true
	}
	if len(comments) > st.commentCount() {
		if released, by := checkRoundHoldRelease(comments[st.commentCount():]); released {
			st.setReleased(true, by)
			_ = s.saveRoundRuleState(r.Name, pr.Number, st)
			_ = s.audit(r.Name, "round-hold-release", map[string]any{"pr": pr.Number, "by": by}, false)
			return false
		}
	}
	return true
}

// checkRound5Gate evaluates the round hold condition during gate evaluation.
func (s *Service) checkRound5Gate(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, rounds int, reviewed bool, v *gateVerdict) bool {
	return s.checkRoundHoldGate(ctx, r, pr, fb, rounds, reviewed, v)
}

// checkRoundHoldGate evaluates the round hold condition during gate evaluation.
func (s *Service) checkRoundHoldGate(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, rounds int, reviewed bool, v *gateVerdict) bool {
	st := s.loadRoundRuleState(r.Name, pr.Number)
	open, _ := s.roundEndsWithoutPass(ctx, r, pr, fb)
	holdRound := s.Config.Review.RoundRuleHold()

	if st.held() {
		if st.released() {
			return false
		}
		comments, err := s.issueComments(ctx, r.Name, pr.Number)
		if err == nil {
			if !st.baselineCaptured() {
				st.setHeld(true, st.HoldAt, len(comments), true)
				_ = s.saveRoundRuleState(r.Name, pr.Number, st)
			} else if len(comments) > st.commentCount() {
				if released, by := checkRoundHoldRelease(comments[st.commentCount():]); released {
					st.setReleased(true, by)
					_ = s.saveRoundRuleState(r.Name, pr.Number, st)
					_ = s.audit(r.Name, "round-hold-release", map[string]any{"pr": pr.Number, "by": by}, false)
					return false
				}
			}
		}
		v.Reasons = append(v.Reasons, "round_hold")
		v.Review.Block = "round_hold"
		v.Review.Hold = "round_hold"
		v.State = "blocked"
		return true
	}

	if reviewed && rounds >= holdRound && open {
		comments, err := s.issueComments(ctx, r.Name, pr.Number)
		count := 0
		captured := false
		if err == nil {
			count = len(comments)
			captured = true
		}
		st.setHeld(true, s.now().UTC(), count, captured)
		_ = s.saveRoundRuleState(r.Name, pr.Number, st)

		v.Reasons = append(v.Reasons, "round_hold")
		v.Review.Block = "round_hold"
		v.Review.Hold = "round_hold"
		v.State = "blocked"
		return true
	}

	return false
}

// sendRound5Decision sends exactly one decision event for compatibility.
func (s *Service) sendRound5Decision(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, rounds int, o Options) error {
	return s.sendRoundHoldDecision(ctx, r, pr, fb, rounds, o)
}

// sendRoundHoldDecision sends exactly one decision event to the design lane if round hold ends without a pass.
func (s *Service) sendRoundHoldDecision(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, rounds int, o Options) error {
	st := s.loadRoundRuleState(r.Name, pr.Number)
	if st.eventSent() || st.released() {
		return nil
	}
	open, openThreads := s.roundEndsWithoutPass(ctx, r, pr, fb)
	holdRound := s.Config.Review.RoundRuleHold()
	if !st.held() && !(rounds >= holdRound && open) {
		return nil
	}

	lane := s.designLane(ctx, r, pr)
	if !config.PanewireLane(lane) {
		// panewire refuses a lane outside its rule; the desk takes the event.
		lane = s.deskLane()
	}
	text := round5DecisionText(r, pr, rounds, openThreads)
	eventID := fmt.Sprintf("mergewake-round-hold-%s-%d-%d", r.Name, pr.Number, holdRound)

	if o.Reporting() {
		reportCtx, cancel := reportContext(ctx)
		defer cancel()
		if err := s.Notifier.Notify(reportCtx, integrations.Event{
			ID:   eventID,
			Lane: lane,
			Type: "decision",
			Text: text,
		}); err != nil {
			return err
		}
		st.setEventSent(true)
		return s.saveRoundRuleState(r.Name, pr.Number, st)
	}
	return nil
}
