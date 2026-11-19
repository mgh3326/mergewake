package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
// findings. It is owed when the round ended without a pass and at least one
// finding is still open above low — medium, high or critical — so an all-low
// round asks nothing (decision 250). One consult runs per PR; the
// row lives in the round-rule state file so a later pass or a restart polls
// it instead of asking again. The switch is review.round_rule.hub_consult,
// default off.
//
// The consult is owed on the first done round at or after the consult round
// that has a finding open above low, and at most once per PR (decision 250):
// a round that was not done before the author pushed, or one with nothing
// open, does not use the consult up. Once requested it is advanced and
// settled on every pass until it is terminal, whatever the current head,
// round or done marker says: a push does not drop it, and its outcome goes out
// with the next author delivery.

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
		// Answer fields come from the hub row: control bytes would fail
		// every lane-event delivery, so they are flattened here as well as
		// on apply (an older state file may hold raw values).
		d := "the hub consult answered: " + emitSafe(st.HubConsultAnswerDoc) + " — read it before patching"
		if st.HubConsultVerdict != "" {
			d += " (verdict: " + emitSafe(st.HubConsultVerdict) + ")"
		}
		return d
	}
	reason := strings.TrimSpace(st.HubConsultReason)
	if reason == "" {
		reason = st.HubConsultState
	}
	return "the hub consult could not be had: " + clip(emitSafe(reason), 200) + " | directive stands: " + RoundDirective
}

// consultCmd is the panewire binary hub calls go through.
func (s *Service) consultCmd() string {
	if c := s.Config.Review.Panewire.Command; c != "" {
		return c
	}
	return "panewire"
}

// consultWaitExpired reports whether a consult attempt started at has been
// outstanding past review.round_rule.hub_consult_wait.
func (s *Service) consultWaitExpired(at, now time.Time) bool {
	if at.IsZero() {
		return false
	}
	return now.Sub(at) >= s.Config.Review.RoundRuleHubConsultWait()
}

// hubConsultStep runs the consult step when the hub consult switch is on and
// answers the directive the author's delivery carries. openAboveLow is the
// consult trigger: the round ended and at least one finding is still open
// above low (medium and up; an all-low round owes no consult, operator
// decision 250). hold is true while the consult requested for the current head
// is queued or running inside the wait — that delivery waits; the stored row
// lets the next pass (or the next pass after a restart) resume it.
//
// Everything runs under the round-rule state lock, so concurrent evaluations
// of one PR cannot request or settle twice.
func (s *Service) hubConsultStep(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState, done bool, rounds int, openAboveLow bool) (directive string, hold bool) {
	if o.DryRun {
		if s.consultDue(st, done, rounds, openAboveLow) {
			// A dry run decides only: it neither requests nor delivers the
			// held round's findings.
			return "", true
		}
		return "", false
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

	// A file written before the head key existed adopts the current head for
	// an outstanding row, so an upgrade resumes it instead of orphaning it.
	if !st.HubConsultOwed && st.HubConsultID != "" && !st.HubConsultDone && !st.directiveSent() {
		st.HubConsultOwed = true
		if st.HubConsultHead == "" {
			st.HubConsultHead = pr.Head.SHA
		}
	}

	// An owed consult is advanced on every pass, whatever the current head,
	// round or done marker says: a push or a new review round must not orphan
	// the row, its wait bound or its outcome. Its question was made from the
	// head it was requested for, so once the PR has moved on a queued or
	// running row does not hold the new head's findings; the outcome joins
	// the next author delivery once it settles.
	if st.HubConsultOwed {
		directive, hold = s.hubConsultAdvance(ctx, r, pr, fb, o, st, s.now().UTC())
		if hold && st.HubConsultHead != "" && st.HubConsultHead != pr.Head.SHA {
			hold = false
		}
		return directive, hold
	}

	if s.consultDue(st, done, rounds, openAboveLow) {
		// Persist the attempt before the slow hk and hub work: the wait bound
		// is measured from here, so an hk outage retries only inside the wait.
		st.HubConsultOwed = true
		st.HubConsultHead = pr.Head.SHA
		st.HubConsultAt = s.now().UTC()
		if e := s.saveRoundRuleState(r.Name, pr.Number, *st); e != nil {
			_ = s.audit(r.Name, "consult-state-error", map[string]any{"pr": pr.Number, "error": integrations.Detail(e)}, o.DryRun)
			return "", true
		}
		return s.hubConsultAdvance(ctx, r, pr, fb, o, st, s.now().UTC())
	}
	return "", false
}

// consultAsked reports whether a consult was ever requested for the PR: one
// consult runs per PR (operator decision 250), whatever head it was asked on.
func (st *roundRuleState) consultAsked() bool {
	return st.HubConsultOwed || st.HubConsultPosted || st.HubConsultID != "" || st.HubConsultDone || !st.HubConsultAt.IsZero()
}

// consultDue reports whether the consult (or, with the hub consult off, the
// directive) is owed now: the first done round at or after the consult round
// with a finding open above the bar, if none was requested or sent yet.
func (s *Service) consultDue(st *roundRuleState, done bool, rounds int, open bool) bool {
	return done && rounds >= s.Config.Review.RoundRuleConsult() && open && !st.directiveSent() && !st.consultAsked()
}

// hubConsultAdvance moves an owed consult one step: request it when no row
// exists yet, poll it otherwise. A row whose id was lost to a failed state
// write is reconciled from the audit journal, never re-requested.
func (s *Service) hubConsultAdvance(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState, now time.Time) (string, bool) {
	if st.HubConsultDone {
		return st.hubConsultDirective(), false
	}
	if st.HubConsultID == "" {
		if st.HubConsultPosted {
			if id := s.recoverConsultID(r.Name, pr.Number, st.HubConsultHead); id != "" {
				st.HubConsultID = id
				if st.HubConsultState == "" {
					st.HubConsultState = "queued"
				}
				_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
				_ = s.audit(r.Name, "consult-recovered", map[string]any{"pr": pr.Number, "id": id, "head": st.HubConsultHead}, o.DryRun)
				return s.hubConsultPoll(ctx, r, pr, o, st, now)
			}
		}
		if s.consultWaitExpired(st.HubConsultAt, now) {
			st.HubConsultDone = true
			if st.HubConsultReason == "" {
				st.HubConsultReason = "no consult row within " + s.Config.Review.RoundRuleHubConsultWait().String()
			}
			_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
			_ = s.audit(r.Name, "consult-outcome", map[string]any{"pr": pr.Number, "state": "wait-expired", "reason": st.HubConsultReason}, o.DryRun)
			return st.hubConsultDirective(), false
		}
		return s.hubConsultRequest(ctx, r, pr, fb, o, st, now)
	}
	return s.hubConsultPoll(ctx, r, pr, o, st, now)
}

// consultAuthor is the author a consult request names. The hub needs the bound
// job for repository evidence, and it reads the job's recorded family unless
// the request names one, so the family is sent beside the job. The family is
// bounded by panewireAuthorFamily, as the review spawn bounds its author
// family: the bound job's record when that is readable, else the
// Mergewake-Author-Family trailer. source is "job", "trailer", or empty when
// neither names a family. A read error leaves that source unnamed, as before.
type consultAuthor struct {
	job, family, source string
}

func (s *Service) consultAuthorFor(ctx context.Context, r config.Repo, pr github.Pull, task string) consultAuthor {
	var a consultAuthor
	if job, rec, ok, e := s.boundJob(ctx, task); e == nil && ok && job != "" {
		a.job = job
		if rec.Family != "" {
			a.family, a.source = rec.Family, "job"
		}
	}
	if a.family == "" {
		if f, fe := s.prTrailer(ctx, r, pr, "Mergewake-Author-Family", familyValue.MatchString); fe == nil && f != "" {
			a.family, a.source = f, "trailer"
		}
	}
	if a.family != "" {
		a.family = panewireAuthorFamily(a.family)
	}
	return a
}

// hubConsultRequest writes the question document to hk and POSTs one
// `panewire consult request` through the hub credential. The caller holds
// the round-rule state lock and has persisted the attempt time. A failure
// the contract counts as no consult (no task trailer, rc >= 0, or an
// unreadable answer) settles as fallback but stays owed until the outcome is
// delivered, so a failed delivery never re-POSTs; a failed document write or
// task read is transient and the pass stays held — the attempt time already
// persisted bounds those retries. A run without an exit status (timeout,
// cancel, killed child) may have reached the hub and created a paid row, so
// it holds for reconcile instead of settling.
func (s *Service) hubConsultRequest(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState, now time.Time) (string, bool) {
	// The request names the head it was first made for, whatever head the PR
	// has now: a POST that reached the hub but lost its row to a timeout is
	// retried with the same key and --head, so the hub answers the same row
	// instead of opening a second paid consult for the new head.
	if st.HubConsultHead != "" {
		pr.Head.SHA = st.HubConsultHead
	}
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
	author := s.consultAuthorFor(ctx, r, pr, task)
	if author.job != "" {
		args = append(args, "--author-job", author.job)
	}
	if author.family != "" {
		args = append(args, "--author-family", author.family)
	}
	args = append(args,
		"--pr", fmt.Sprintf("%s#%d", r.Name, pr.Number),
		"--head", pr.Head.SHA,
		"--requester", fmt.Sprintf("mergewake/%s#%d", r.Name, pr.Number),
		"--json")

	// Record that the POST may reach the hub before it runs: if the id is
	// lost afterwards, the pass reconciles it from the journal instead of
	// re-requesting.
	st.HubConsultPosted = true
	if e := s.saveRoundRuleState(r.Name, pr.Number, *st); e != nil {
		_ = s.audit(r.Name, "consult-state-error", map[string]any{"pr": pr.Number, "error": integrations.Detail(e)}, o.DryRun)
		return "", true
	}

	cmdCtx, cmdCancel := context.WithTimeout(ctx, 60*time.Second)
	defer cmdCancel()
	out, err := s.Runner.Run(cmdCtx, s.consultCmd(), integrations.HubArgv(s.Config.Reap.Hub, args...), "", "")
	if err != nil {
		rc := commandExitCode(err)
		if rc < 0 {
			// No exit status (timeout, cancel, killed child): the POST may
			// already have created a paid row, so hold and reconcile on
			// the next pass instead of abandoning it as failed.
			_ = s.audit(r.Name, "consult-request-error", map[string]any{"pr": pr.Number, "rc": rc, "error": integrations.Detail(err)}, o.DryRun)
			return "", true
		}
		detail := strings.TrimSpace(string(out))
		var ce *integrations.CommandError
		if errors.As(err, &ce) && strings.TrimSpace(ce.Stderr) != "" {
			detail = strings.TrimSpace(ce.Stderr)
		}
		st.HubConsultDone = true
		// One printable line: the reason ends up in a panewire lane event,
		// which refuses control bytes (emitSafe, as the round hold does).
		st.HubConsultReason = fmt.Sprintf("consult request failed (rc %d): %s", rc, clip(emitSafe(detail), 200))
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "consult-request-failed", map[string]any{"pr": pr.Number, "rc": rc, "error": integrations.Detail(err), "output": detail}, o.DryRun)
		return st.hubConsultDirective(), false
	}

	var row reap.ConsultRow
	if e := json.Unmarshal(out, &row); e != nil || row.ID == "" {
		reason := fmt.Sprintf("unreadable consult row: %s", emitSafe(clip(string(out), 200)))
		if e != nil {
			reason = "unreadable consult row: " + emitSafe(e.Error())
		}
		st.HubConsultDone = true
		st.HubConsultReason = reason
		_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
		_ = s.audit(r.Name, "consult-request-failed", map[string]any{"pr": pr.Number, "rc": 0, "error": reason}, o.DryRun)
		return st.hubConsultDirective(), false
	}

	st.HubConsultID = row.ID
	s.hubConsultApply(st, row)
	// Journal the row with its head before the state file: the request is
	// durable (and possibly paid), so a lost state write is reconciled from
	// this record, never by re-POSTing a row that may have gone terminal.
	if e := s.audit(r.Name, "consult-request", map[string]any{
		"pr": pr.Number, "head": pr.Head.SHA, "id": row.ID, "state": row.State, "question": st.HubConsultQuestion,
		"author_job": author.job, "author_family": author.family, "family_source": author.source,
	}, o.DryRun); e != nil && s.Log != nil {
		s.Log.Warn("consult request audit failed", "pr", pr.Number, "error", e)
	}
	if e := s.saveRoundRuleState(r.Name, pr.Number, *st); e != nil {
		if e2 := s.saveRoundRuleState(r.Name, pr.Number, *st); e2 != nil {
			_ = s.audit(r.Name, "consult-state-error", map[string]any{"pr": pr.Number, "id": row.ID, "error": integrations.Detail(e2)}, o.DryRun)
			return "", true
		}
	}
	if s.Log != nil {
		s.Log.Info("hub consult requested", "pr", pr.Number, "id", row.ID, "state", row.State)
	}
	return s.hubConsultSettle(ctx, r, pr, o, st)
}

// hubConsultPoll reads the stored consult row through GET /v1/consults/{id}
// and settles it when terminal. The CLI's only consult verb is request and
// re-POSTing a terminal row retries it, so a non-terminal row is read over
// HTTP, never re-requested. The wait bound is measured from the first
// attempt; past it the findings go out without the consult.
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
	// The poll reads reap.hub directly, whatever reap.transport is: the
	// request goes through the panewire CLI with the same credential, so a
	// host whose reap transport is ssh must still be able to read the row.
	hub, err := s.hubClient(s.Config.Reap.Hub)
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
	// The answer pointer and verdict travel to a lane event through the
	// directive, which refuses control bytes: flatten them on the way in.
	st.HubConsultAnswerDoc = emitSafe(row.AnswerDoc)
	st.HubConsultVerdict = emitSafe(row.Verdict)
	st.HubConsultJob = row.Job
	if row.Reason != "" {
		st.HubConsultReason = emitSafe(row.Reason)
	}
	if row.Seat.Profile != "" || row.Seat.Family != "" || row.Seat.Grade != "" {
		st.HubConsultSeat = strings.TrimSpace(strings.Join([]string{row.Seat.Profile, row.Seat.Family, row.Seat.Grade}, "/"))
	}
}

// hubConsultSettle ends the step once the stored row is terminal: the
// outcome is marked settled, audited once and recorded on the hk task. The
// task comment stays retryable on its own until it succeeds (see
// hubConsultCommentRetry). A non-terminal row stays pending; the caller
// holds the state lock.
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

// hubConsultCommentRetry finishes a settled consult's task comment that a
// transient hk failure left unrecorded. It runs on every pass, independently
// of the findings delivery and of directiveSent, so the promised task record
// is never lost.
func (s *Service) hubConsultCommentRetry(ctx context.Context, r config.Repo, pr github.Pull, o Options) {
	if o.DryRun {
		return
	}
	file := s.roundRuleFile(r.Name, pr.Number)
	defer lockPath(file)()
	unlock, err := fsutil.Lock(file+".lock", true)
	if err != nil {
		return
	}
	defer unlock()
	st := s.loadRoundRuleState(r.Name, pr.Number)
	if !st.HubConsultDone || st.HubConsultCommented || st.HubConsultID == "" {
		return
	}
	s.hubConsultComment(ctx, r, pr, o, &st)
}

// hubConsultComment records the finished consult on the hk task once: id,
// state, seat and answer_doc. A comment failure stays uncommented so a
// later pass retries it. The caller holds the state lock.
//
// retry is true only when a later pass can still record it: a failed write or
// a failed task read. A sink without Comment (integrations.Noop) and a PR with
// no task can never record it, so the caller must not wait on them.
func (s *Service) hubConsultComment(ctx context.Context, r config.Repo, pr github.Pull, o Options, st *roundRuleState) (retry bool) {
	if st.HubConsultCommented || st.HubConsultID == "" {
		return false
	}
	c, ok := s.Sink.(interface {
		Comment(context.Context, string, string) error
	})
	if !ok {
		return false
	}
	task, err := s.prTask(ctx, r, pr)
	if err != nil {
		// The task read failed, so a later pass may still find the task and
		// record the comment: an error is never read as "this PR has no task".
		_ = s.audit(r.Name, "consult-comment-failed", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "error": integrations.Detail(err)}, o.DryRun)
		return true
	}
	if task == "" {
		return false
	}
	body := fmt.Sprintf("hub consult %s: state=%s seat=%s job=%s answer_doc=%s reason=%s",
		st.HubConsultID, st.HubConsultState, st.HubConsultSeat, st.HubConsultJob, st.HubConsultAnswerDoc, emitSafe(st.HubConsultReason))
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if err := c.Comment(reportCtx, task, body); err != nil {
		_ = s.audit(r.Name, "consult-comment-failed", map[string]any{"pr": pr.Number, "id": st.HubConsultID, "error": integrations.Detail(err)}, o.DryRun)
		return true
	}
	st.HubConsultCommented = true
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)
	return false
}

// recoverConsultID reads the id of a consult request this PR and head already
// made from the repo's read-only audit journal. A failed state write (or a
// crash between the POST and the state file) is reconciled from it instead of
// a re-POST, which would retry a row that may have gone terminal.
func (s *Service) recoverConsultID(repo string, pr int, head string) string {
	const maxScan = 8 << 20
	f, err := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return ""
	}
	off := info.Size() - maxScan
	if off < 0 {
		off = 0
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return ""
	}
	buf, err := io.ReadAll(f)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(buf), "\n")
	if off > 0 && len(lines) > 0 {
		lines = lines[1:] // the first line was cut mid-record
	}
	var rec struct {
		Command string `json:"command"`
		Value   struct {
			PR   int    `json:"pr"`
			Head string `json:"head"`
			ID   string `json:"id"`
		} `json:"value"`
	}
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" || !strings.Contains(line, `"consult-request"`) {
			continue
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec.Command == "consult-request" && rec.Value.PR == pr && rec.Value.Head == head && rec.Value.ID != "" {
			return rec.Value.ID
		}
	}
	return ""
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
