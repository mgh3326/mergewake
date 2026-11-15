package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The hub consult of operator decision 250 (hk 4551, panewire hk 4552,
// docs/consult-on-request.md): at the consult round mergewake, not the
// author, asks the hub for a consult and delivers its answer with the
// findings. One consult runs per PR and head; the row lives in the
// round-rule state file so a later pass or a restart polls it instead of
// asking again. The switch is review.round_rule.hub_consult, default off.

// consultTerminal lists the consult row states after which nothing is
// polled or re-requested: re-POSTing a refused or failed row retries it
// (contract §Idempotency), so a terminal row is only ever read.
func consultTerminal(state string) bool {
	switch state {
	case "answered", "refused", "failed", "cancelled":
		return true
	}
	return false
}

// hubConsultDirective renders the directive a consult-round delivery
// carries once the consult is settled: the answer document's pointer on
// answered, else today's directive plus why no consult could be had.
func (st *roundRuleState) hubConsultDirective() string {
	if st.HubConsultState == "answered" {
		d := "the hub consult answered: " + st.HubConsultAnswerDoc + " — read it before patching"
		if st.HubConsultVerdict != "" {
			d += " (verdict: " + st.HubConsultVerdict + ")"
		}
		return d
	}
	reason := strings.TrimSpace(st.HubConsultReason)
	if reason == "" {
		reason = st.HubConsultState
	}
	return "the hub consult could not be had: " + clip(reason, 200) + " | directive stands: " + RoundDirective
}

// consultCmd is the panewire binary hub calls go through.
func (s *Service) consultCmd() string {
	if c := s.Config.Review.Panewire.Command; c != "" {
		return c
	}
	return "panewire"
}

// hubConsultOutcome runs one pass of the consult step for a PR at its
// consult round and answers the directive the round's findings delivery
// carries. pending is true while the consult is queued or running inside
// the wait — that round's findings stay undelivered; the stored row lets
// the next pass (or the next pass after a restart) resume the poll.
// Everything runs under the round-rule state lock, so concurrent
// evaluations of one PR cannot request or settle twice.
func (s *Service) hubConsultOutcome(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState) (directive string, pending bool) {
	if o.DryRun {
		return "", true
	}
	file := s.roundRuleFile(r.Name, pr.Number)
	defer lockPath(file)()
	unlock, err := fsutil.Lock(file+".lock", true)
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("round-rule state lock failed", "pr", pr.Number, "error", err)
		}
		return "", true
	}
	defer unlock()

	// The caller's copy may be stale: another evaluation of this PR can
	// settle between the caller's load and this lock.
	*st = s.loadRoundRuleState(r.Name, pr.Number)
	if st.HubConsultDone {
		return st.hubConsultDirective(), false
	}
	now := s.now().UTC()
	if st.HubConsultID == "" && st.HubConsultState == "" {
		return s.hubConsultRequest(ctx, r, pr, fb, o, st, now)
	}
	return s.hubConsultPoll(ctx, r, pr, o, st, now)
}

// hubConsultRequest writes the question document to hk and POSTs one
// `panewire consult request` through the hub credential. The caller holds
// the round-rule state lock. Any failure the contract counts as no
// consult (no task trailer, rc 1, rc 2, a run error or an unreadable
// answer) settles as fallback; a failed document write or task read is
// transient and the pass stays pending.
func (s *Service) hubConsultRequest(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState, now time.Time) (string, bool) {
	task, err := s.prTask(ctx, r, pr)
	if err != nil {
		_ = s.audit(r.Name, "consult-error", map[string]any{"pr": pr.Number, "error": integrations.Detail(err)}, o.DryRun)
		return "", true
	}
	if task == "" {
		st.HubConsultDone = true
		st.HubConsultReason = "PR carries no Mergewake-Task trailer"
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "consult-skipped", map[string]any{"pr": pr.Number, "reason": st.HubConsultReason}, o.DryRun)
		return st.hubConsultDirective(), false
	}

	key := fmt.Sprintf("mergewake/consult/%s-%d-%s", strings.ReplaceAll(r.Name, "/", "-"), pr.Number, short7(pr.Head.SHA))
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if e := s.Sink.Record(reportCtx, key, buildConsultQuestion(r, pr, task, fb)); e != nil {
		_ = s.audit(r.Name, "consult-question-error", map[string]any{"pr": pr.Number, "key": key, "error": integrations.Detail(e)}, o.DryRun)
		return "", true
	}
	st.HubConsultQuestion = "hk:doc/" + key

	args := []string{
		"consult", "request",
		"--task", task,
		"--question", st.HubConsultQuestion,
	}
	if job, _, ok, e := s.boundJob(ctx, task); e == nil && ok && job != "" {
		args = append(args, "--author-job", job)
	} else if family, fe := s.prTrailer(ctx, r, pr, "Mergewake-Author-Family", familyValue.MatchString); fe == nil && family != "" {
		args = append(args, "--author-family", family)
	}
	args = append(args,
		"--pr", fmt.Sprintf("%s#%d", r.Name, pr.Number),
		"--head", pr.Head.SHA,
		"--requester", fmt.Sprintf("mergewake/%s#%d", r.Name, pr.Number),
		"--json")

	cmdCtx, cmdCancel := context.WithTimeout(ctx, 60*time.Second)
	defer cmdCancel()
	out, err := s.Runner.Run(cmdCtx, s.consultCmd(), integrations.HubArgv(s.Config.Reap.Hub, args...), "", "")
	if err != nil {
		rc := commandExitCode(err)
		detail := strings.TrimSpace(string(out))
		var ce *integrations.CommandError
		if errors.As(err, &ce) && strings.TrimSpace(ce.Stderr) != "" {
			detail = strings.TrimSpace(ce.Stderr)
		}
		st.HubConsultDone = true
		st.HubConsultReason = fmt.Sprintf("consult request failed (rc %d): %s", rc, clip(detail, 200))
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "consult-request-failed", map[string]any{"pr": pr.Number, "rc": rc, "error": integrations.Detail(err), "output": detail}, o.DryRun)
		return st.hubConsultDirective(), false
	}

	var row reap.ConsultRow
	if e := json.Unmarshal(out, &row); e != nil || row.ID == "" {
		reason := fmt.Sprintf("unreadable consult row: %s", clip(string(out), 200))
		if e != nil {
			reason = "unreadable consult row: " + e.Error()
		}
		st.HubConsultDone = true
		st.HubConsultReason = reason
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "consult-request-failed", map[string]any{"pr": pr.Number, "rc": 0, "error": reason}, o.DryRun)
		return st.hubConsultDirective(), false
	}

	st.HubConsultID = row.ID
	st.HubConsultAt = now
	s.hubConsultApply(st, row)
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
	_ = s.audit(r.Name, "consult-request", map[string]any{
		"pr": pr.Number, "id": row.ID, "state": row.State, "question": st.HubConsultQuestion,
	}, o.DryRun)
	if s.Log != nil {
		s.Log.Info("hub consult requested", "pr", pr.Number, "id", row.ID, "state", row.State)
	}
	return s.hubConsultSettle(ctx, r, pr, o, st)
}

// hubConsultPoll reads the stored consult row through GET /v1/consults/{id}
// and settles it when terminal. The CLI's only consult verb is request and
// re-POSTing a terminal row retries it, so a non-terminal row is read over
// HTTP, never re-requested. The wait bound is measured from the first
// request; past it the findings go out without the consult.
func (s *Service) hubConsultPoll(ctx context.Context, r config.Repo, pr github.Pull, o Options, st *roundRuleState, now time.Time) (string, bool) {
	if consultTerminal(st.HubConsultState) {
		return s.hubConsultSettle(ctx, r, pr, o, st)
	}
	wait := s.Config.Review.RoundRuleHubConsultWait()
	if now.Sub(st.HubConsultAt) >= wait {
		st.HubConsultDone = true
		st.HubConsultReason = fmt.Sprintf("no terminal state within %s (last state %s)", wait, st.HubConsultState)
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "consult-outcome", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "state": "wait-expired", "reason": st.HubConsultReason}, o.DryRun)
		return st.hubConsultDirective(), false
	}
	hub, err := s.reapHub()
	if err != nil || hub == nil {
		if err != nil {
			_ = s.audit(r.Name, "consult-poll-error", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "error": integrations.Detail(err)}, o.DryRun)
		}
		return "", true
	}
	row, err := hub.Consult(ctx, st.HubConsultID)
	if err != nil {
		_ = s.audit(r.Name, "consult-poll-error", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "error": integrations.Detail(err)}, o.DryRun)
		return "", true
	}
	if row.State != st.HubConsultState {
		_ = s.audit(r.Name, "consult-state", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "from": st.HubConsultState, "to": row.State}, o.DryRun)
	}
	s.hubConsultApply(st, row)
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
	return s.hubConsultSettle(ctx, r, pr, o, st)
}

// hubConsultApply copies the row fields the state file keeps.
func (s *Service) hubConsultApply(st *roundRuleState, row reap.ConsultRow) {
	if row.State != "" {
		st.HubConsultState = row.State
	}
	st.HubConsultAnswerDoc = row.AnswerDoc
	st.HubConsultVerdict = row.Verdict
	st.HubConsultJob = row.Job
	if row.Reason != "" {
		st.HubConsultReason = row.Reason
	}
	if row.Seat.Profile != "" || row.Seat.Family != "" || row.Seat.Grade != "" {
		st.HubConsultSeat = strings.TrimSpace(strings.Join([]string{row.Seat.Profile, row.Seat.Family, row.Seat.Grade}, "/"))
	}
}

// hubConsultSettle ends the step once the stored row is terminal: the
// outcome is marked applied, audited once and recorded on the hk task once.
// A non-terminal row stays pending; the caller holds the state lock.
func (s *Service) hubConsultSettle(ctx context.Context, r config.Repo, pr github.Pull, o Options, st *roundRuleState) (string, bool) {
	if !consultTerminal(st.HubConsultState) {
		return "", true
	}
	st.HubConsultDone = true
	if st.HubConsultState != "answered" && st.HubConsultReason == "" {
		st.HubConsultReason = "hub consult " + st.HubConsultState
	}
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
	_ = s.audit(r.Name, "consult-outcome", map[string]any{
		"pr": pr.Number, "id": st.HubConsultID, "state": st.HubConsultState,
		"answer_doc": st.HubConsultAnswerDoc, "reason": st.HubConsultReason,
	}, o.DryRun)
	s.hubConsultComment(ctx, r, pr, o, st)
	return st.hubConsultDirective(), false
}

// hubConsultComment records the finished consult on the hk task once: id,
// state, seat and answer_doc. A comment failure stays uncommented so a
// later pass retries it. The caller holds the state lock.
func (s *Service) hubConsultComment(ctx context.Context, r config.Repo, pr github.Pull, o Options, st *roundRuleState) {
	if st.HubConsultCommented || st.HubConsultID == "" {
		return
	}
	c, ok := s.Sink.(interface {
		Comment(context.Context, string, string) error
	})
	if !ok {
		return
	}
	task, err := s.prTask(ctx, r, pr)
	if err != nil || task == "" {
		return
	}
	body := fmt.Sprintf("hub consult %s: state=%s seat=%s job=%s answer_doc=%s reason=%s",
		st.HubConsultID, st.HubConsultState, st.HubConsultSeat, st.HubConsultJob, st.HubConsultAnswerDoc, st.HubConsultReason)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if err := c.Comment(reportCtx, task, body); err != nil {
		_ = s.audit(r.Name, "consult-comment-failed", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "error": integrations.Detail(err)}, o.DryRun)
		return
	}
	st.HubConsultCommented = true
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
}

// commandExitCode extracts the exit status of a failed Runner call the way
// escalateAuthorLadder reads panewire's rc; -1 when there is none.
func commandExitCode(err error) int {
	var ce *integrations.CommandError
	if errors.As(err, &ce) {
		if ee, ok := ce.Err.(interface{ ExitCode() int }); ok {
			return ee.ExitCode()
		}
		if strings.Contains(ce.Err.Error(), "exit status ") {
			code := -1
			fmt.Sscanf(ce.Err.Error(), "exit status %d", &code)
			return code
		}
	}
	if ee, ok := err.(interface{ ExitCode() int }); ok {
		return ee.ExitCode()
	}
	code := -1
	if strings.Contains(err.Error(), "exit status ") {
		fmt.Sscanf(err.Error(), "exit status %d", &code)
	}
	return code
}

// buildConsultQuestion writes the question document the hub consult runs
// on: the PR, the reviewed head, the task's contract pointer and every
// open finding with its id, severity, path:line and text.
func buildConsultQuestion(r config.Repo, pr github.Pull, task string, fb github.ReviewFeedback) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# Consult question — %s#%d\n\n", r.Name, pr.Number)
	fmt.Fprintf(&b, "- PR: %s#%d\n- Reviewed head: %s\n- Task: hk %s (the task record is the contract pointer)\n\n", r.Name, pr.Number, pr.Head.SHA, task)
	b.WriteString("The PR's review round reached the consult round without a pass. Find the root cause of the open findings and answer with the consult report shape (Verdict / Root cause / Design / Findings).\n\n## Open findings\n\n")
	for _, t := range fb.Threads {
		if t.IsResolved || len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		sev := github.ThreadSeverity(first.Author, first.Body)
		if sev == "" {
			sev = "unrated"
		}
		id := t.ID
		if id == "" {
			id = first.URL
		}
		fmt.Fprintf(&b, "### %s — %s %s:%d\n\n%s\n\n", id, sev, t.Path, t.Line, first.Body)
	}
	return []byte(b.String())
}
