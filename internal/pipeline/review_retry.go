package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type reviewRetryState struct {
	Head      string    `json:"head"`
	Failures  int       `json:"failures"`
	Next      time.Time `json:"next"`
	Escalated bool      `json:"escalated"`
	// Backoffs counts the failures that lengthen the backoff: every counted
	// failure plus the collection failures, which never count toward
	// escalation. A file without it reads as Failures.
	Backoffs int `json:"backoffs,omitempty"`
	// Held is set while the last evaluation ended on a round hold: nothing
	// failed, but no review completed, so the daemon must not settle the PR as
	// unchanged.
	Held bool `json:"held,omitempty"`
	// QuotaStopped is set while the last evaluation ended on a quota stop with
	// nothing else wrong: nothing failed, but no review completed, so the
	// daemon must not settle the PR as unchanged.
	QuotaStopped bool `json:"quota_stopped,omitempty"`
	// WaitText names the wait Held or QuotaStopped records, with the reason its
	// audit row carried ("held (round-hold)", "quota-stopped (pool seat)"),
	// so a pass deferred inside the wait and the review-stall alert name it.
	WaitText string `json:"wait_text,omitempty"`
	// LastError is the redacted, clipped text of the last counted failure, for
	// the escalation notice.
	LastError string `json:"last_error,omitempty"`
	// WaitConflict and WaitCIRed record the wait reasons this head already
	// audited, so the review-retry-waiting row is written once per reason,
	// not once per poll.
	WaitConflict bool `json:"wait_conflict,omitempty"`
	WaitCIRed    bool `json:"wait_ci_red,omitempty"`
	// DeferredNext is the Next value whose review-retry-deferred row is already
	// in the journal, so a deferral is audited once per next, not once per poll.
	DeferredNext time.Time `json:"deferred_next,omitempty"`
}

// reviewErrorLimit bounds the error text a failure row and notice carry.
const reviewErrorLimit = 300

// reviewErrorText is err as one redacted line of at most reviewErrorLimit bytes.
func reviewErrorText(err error) string {
	text := strings.Join(strings.Fields(fleetevent.Redact(err.Error())), " ")
	if len(text) > reviewErrorLimit {
		text = clip(text, reviewErrorLimit) + "…"
	}
	return text
}

// reviewFailureClass sorts what Review returned by how RetryReview accounts
// for it: a quota stop waits for its reset and is no failure, a round hold
// waits for its release and is no failure, a pending pass whose reviewer runs
// on or whose claim another host holds is no failure, a collection failure
// backs off without counting toward escalation, and anything else is a real
// review failure.
type reviewFailureClass struct {
	quotas     []*reviewQuotaStopped
	roundHold  bool
	collection bool
	pending    bool
	real       bool
	// failures are the counted failures with the pass and label they came from
	// (zero and empty when the failure was not one pass's).
	failures []countedFailure
}

type countedFailure struct {
	pass  int
	label string
	err   error
}

// waitText is the reason a review wait's audit row recorded ("held
// (round-hold)", "quota-stopped (pool seat)"), carried in the wait's sentinel
// error so the caller names it too.
type waitText string

func (w waitText) Error() string { return string(w) }

// reviewWaitErr is the sentinel of a review wait with the wait's reason text
// when the pass recorded one.
func reviewWaitErr(sentinel error, text string) error {
	if text == "" {
		return sentinel
	}
	return errors.Join(sentinel, waitText(text))
}

// waitText names this pass's wait with the reason its audit row recorded: the
// round hold, or the pool a reviewer stopped on.
func (c *reviewFailureClass) waitText() string {
	switch {
	case c.roundHold:
		return "held (round-hold)"
	case len(c.quotas) > 0 && c.quotas[0].pool != "":
		return "quota-stopped (pool " + c.quotas[0].pool + ")"
	}
	return ""
}

func (c *reviewFailureClass) add(err error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range joined.Unwrap() {
			c.add(e)
		}
		return
	}
	var quota *reviewQuotaStopped
	var collect *collectionFailure
	var lost *reviewResultLost
	switch {
	case errors.As(err, &quota):
		c.quotas = append(c.quotas, quota)
	case errors.Is(err, ErrReviewHeld):
		c.roundHold = true
	case errors.Is(err, errPassPending), errors.Is(err, ErrReviewPending):
		c.pending = true
	case errors.As(err, &collect), errors.As(err, &lost):
		c.collection = true
	default:
		c.real = true
		f := countedFailure{err: err}
		var pass *passFailure
		if errors.As(err, &pass) {
			f.pass, f.label = pass.pass, pass.label
		}
		c.failures = append(c.failures, f)
	}
}

// retryDelay is the backoff after n backed-off failures: initial, doubling up
// to maximum.
func retryDelay(n int, initial, maximum time.Duration) time.Duration {
	delay := initial
	for i := 1; i < n && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
		} else {
			delay *= 2
		}
	}
	return delay
}

// RetryReview checks review evidence even if a completion marker exists. A
// marker without receipts is not evidence that a review ran.
// A skipped poll returns an error wrapping github.ErrPollSkipped, which the
// caller counts as no review.
func (s *Service) RetryReview(ctx context.Context, r config.Repo, pr github.Pull, o Options) error {
	if !o.Acting() {
		return s.Review(ctx, r, pr.Number, "", o)
	}
	file := filepath.Join(s.state(r.Name), fmt.Sprintf("review-retry-%d.json", pr.Number))
	var state reviewRetryState
	if _, err := fsutil.ReadJSON(file, &state); err != nil {
		return err
	}
	if state.Head != pr.Head.SHA {
		state = reviewRetryState{Head: pr.Head.SHA}
	}
	state.Backoffs = max(state.Backoffs, state.Failures)
	initial, maximum, threshold := s.Config.Daemon.PR.ReviewRetry.Settings()
	if state.Failures >= threshold && !state.Escalated {
		// The escalation waits with the retries while the failed head
		// conflicts or its required checks are red.
		fresh, err := s.GH.Pull(github.FreshEvidence(ctx), r.Name, pr.Number)
		if err != nil {
			return err
		}
		if fresh.State == "open" && !fresh.Merged && !fresh.Draft && fresh.Head.SHA == pr.Head.SHA {
			proceed, err := s.reviewRetryHold(ctx, r, file, &state, fresh)
			if err != nil {
				return err
			}
			if !proceed {
				return nil
			}
		}
		sent, err := s.escalateReviewFailure(ctx, r, pr, state.Failures, state.LastError)
		if err != nil {
			return err
		}
		if !sent {
			return nil
		}
		state.Escalated = true
		if err := fsutil.WriteJSON(file, state); err != nil {
			return err
		}
	}
	if s.now().Before(state.Next) {
		// The deferral leaves one row per next value, so a PR in its backoff
		// is visible in the journal. The flag is set only once the row is in:
		// a failed append leaves the next evaluation to write it again.
		if !state.DeferredNext.Equal(state.Next) {
			row := map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "next": state.Next.UTC().Format(time.RFC3339), "failures": state.Failures, "last_error": state.LastError}
			if s.audit(r.Name, "review-retry-deferred", row, false) == nil {
				state.DeferredNext = state.Next
				if err := fsutil.WriteJSON(file, state); err != nil {
					return err
				}
			}
		}
		switch {
		case state.Held:
			return reviewWaitErr(ErrReviewHeld, state.WaitText)
		case state.QuotaStopped:
			return reviewWaitErr(ErrReviewQuotaStopped, state.WaitText)
		}
		return nil
	}
	fresh, err := s.GH.Pull(github.FreshEvidence(ctx), r.Name, pr.Number)
	if err != nil {
		return err
	}
	if fresh.State != "open" || fresh.Merged || fresh.Draft || fresh.Head.SHA != pr.Head.SHA {
		return nil
	}
	proceed, err := s.reviewRetryHold(ctx, r, file, &state, fresh)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}
	ctx, attempt := withAttempt(ctx)
	err = s.Review(ctx, r, pr.Number, "", o)
	if attempt.placed.Load() {
		// A reviewer that really landed starts a new attempt: the count of the
		// earlier one must not hold it. An ACCEPTED receipt is no landing, so a
		// request the hub refused after accepting it keeps counting, and
		// repeated accepted-then-refused spawns reach the escalation threshold
		// (hk 5005). Backoffs stay, so a spawn-and-lose loop still slows down.
		state.Failures, state.Escalated, state.Held, state.QuotaStopped = 0, false, false, false
		if saveErr := fsutil.WriteJSON(file, state); saveErr != nil {
			return errors.Join(err, saveErr)
		}
	}
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	if errors.Is(err, ErrReviewBlocked) {
		// The author acts next, not a reviewer: no failure, backoff or escalation.
		return fsutil.WriteJSON(file, reviewRetryState{Head: pr.Head.SHA})
	}
	if err == nil {
		var complete, pending bool
		complete, pending, err = s.retryReviewStatus(ctx, r, pr)
		if err == nil && complete {
			return fsutil.WriteJSON(file, reviewRetryState{Head: pr.Head.SHA})
		}
		if err == nil && pending {
			// A reviewer still runs, or another host holds the pass: there is
			// no result yet, so nothing failed and nothing may be settled.
			err = ErrReviewPending
		}
		if err == nil {
			err = errors.New("review produced no completed pass")
		}
	}
	// A skipped poll is a wait, not a review: the error wraps
	// github.ErrPollSkipped and the caller counts the head as unreviewed.
	if errors.Is(err, github.ErrPollSkipped) {
		return err
	}
	var class reviewFailureClass
	class.add(err)
	if class.real || class.collection {
		state.Backoffs++
		state.Next = s.now().Add(retryDelay(state.Backoffs, initial, maximum))
	}
	if class.real {
		state.Failures++
		// Every counted failure leaves a row: the retry state keeps only counts,
		// and a failure with no text in the journal cannot be diagnosed.
		for _, f := range class.failures {
			row := map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "failures": state.Failures, "error": reviewErrorText(f.err)}
			if f.pass > 0 {
				row["pass"] = f.pass
			}
			if f.label != "" {
				row["label"] = f.label
			}
			if d := errorDetailOf(f.err); d != "" {
				row["detail"] = d
			}
			_ = s.audit(r.Name, "review-failed", row, false)
		}
		state.LastError = reviewErrorText(class.failures[len(class.failures)-1].err)
	}
	for _, q := range class.quotas {
		// A reset not yet reached is waited for, but never longer than the
		// maximum backoff: panewire's auto seat skips a pool its quota gate
		// measures exhausted, so the retry need not wait out a far reset. An
		// unknown or past one waits the initial backoff, so a stale reset
		// cannot respawn at every poll.
		at := s.now().Add(initial)
		if q.resetAt.After(s.now()) {
			at = q.resetAt
			if limit := s.now().Add(maximum); at.After(limit) {
				at = limit
			}
		}
		if at.After(state.Next) {
			state.Next = at
		}
	}
	// Only a round hold or a quota stop with nothing else wrong is marked so: a
	// real failure supersedes it, and the daemon must not settle this
	// evaluation. A round hold is the stronger marker, so a run with both waits
	// for the hold.
	state.Held = class.roundHold && !class.real && !class.collection
	state.QuotaStopped = len(class.quotas) > 0 && !class.real && !class.collection && !state.Held
	state.WaitText = ""
	if state.Held || state.QuotaStopped {
		state.WaitText = class.waitText()
	}
	if saveErr := fsutil.WriteJSON(file, state); saveErr != nil {
		return errors.Join(err, saveErr)
	}
	if !class.real && !class.collection {
		// Only quota stops, holds and pending passes: the audit row says it,
		// and nothing failed.
		if state.Held {
			return reviewWaitErr(ErrReviewHeld, state.WaitText)
		}
		if state.QuotaStopped {
			return reviewWaitErr(ErrReviewQuotaStopped, state.WaitText)
		}
		if class.pending {
			return ErrReviewPending
		}
		return nil
	}
	// Escalate after persisting the failure, including the first poll at K.
	if state.Failures >= threshold && !state.Escalated {
		sent, notifyErr := s.escalateReviewFailure(ctx, r, pr, state.Failures, state.LastError)
		if notifyErr != nil {
			return errors.Join(err, notifyErr)
		}
		if sent {
			state.Escalated = true
			if saveErr := fsutil.WriteJSON(file, state); saveErr != nil {
				return errors.Join(err, saveErr)
			}
		}
	}
	return err
}

// reviewRetryHold decides whether a head with counted failures waits before
// its next attempt or escalation. pr is the fresh pull of the open head. A
// conflicting head, or one whose required checks under the trusted base
// policy are red, waits: the first wait per head and reason writes a
// review-retry-waiting row, and the failure count, backoff and last error
// stay as they were. A head whose passes are already complete is done
// instead, and a pending reviewer is reported. proceed is false whenever the
// caller must not attempt.
// A skipped status poll is returned as its error, which wraps github.ErrPollSkipped.
func (s *Service) reviewRetryHold(ctx context.Context, r config.Repo, file string, state *reviewRetryState, pr github.Pull) (bool, error) {
	if state.Failures < 1 {
		return true, nil
	}
	var reason string
	switch {
	case conflicting(pr):
		reason = "conflict"
	case s.reviewRetryRed(ctx, r, pr):
		reason = "ci_red"
	}
	if reason == "" {
		return true, nil
	}
	complete, pending, err := s.retryReviewStatus(ctx, r, pr)
	if err != nil {
		return false, err
	}
	if complete {
		return false, fsutil.WriteJSON(file, reviewRetryState{Head: pr.Head.SHA})
	}
	if pending {
		return false, ErrReviewPending
	}
	audited := &state.WaitConflict
	if reason == "ci_red" {
		audited = &state.WaitCIRed
	}
	if *audited {
		return false, nil
	}
	// The flag is set, and the state written, only once the row is in the
	// journal: a failed append leaves the next evaluation to write it again.
	if err := s.audit(r.Name, "review-retry-waiting", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "reason": reason, "failures": state.Failures}, false); err != nil {
		return false, nil
	}
	*audited = true
	return false, fsutil.WriteJSON(file, *state)
}

// reviewRetryRed reports whether a required check under the base branch's
// trusted policy is red on the head. Pending and unreadable states are not
// red: the wait fails open, so a retry runs exactly as it would without it.
func (s *Service) reviewRetryRed(ctx context.Context, r config.Repo, pr github.Pull) bool {
	head, err := s.GH.Head(ctx, r.Name, r.Branch)
	if err != nil {
		return false
	}
	p, err := s.Policy(ctx, r, head)
	if err != nil {
		return false
	}
	ci, err := s.GH.Check(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	return err == nil && ci == github.Red
}

func (s *Service) escalateReviewFailure(ctx context.Context, r config.Repo, pr github.Pull, failures int, lastError string) (bool, error) {
	fresh, err := s.GH.Pull(github.FreshEvidence(ctx), r.Name, pr.Number)
	if err != nil {
		return false, err
	}
	if fresh.State != "open" || fresh.Merged || fresh.Draft || fresh.Head.SHA != pr.Head.SHA {
		return false, nil
	}
	nctx, cancel := reportContext(ctx)
	defer cancel()
	id := fmt.Sprintf("mergewake-review-failed-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA)
	text := fmt.Sprintf("[mergewake] review failed %d times for %s#%d head=%s%s; decision needed", failures, r.Name, pr.Number, short7(pr.Head.SHA), lastErrorNote(lastError))
	owner := s.ownerLane(r)
	// Mergewake's own automation gets its own copy on the tool lane, so the
	// desk hears it only when no tool lane and no design lane resolve to
	// anything else. The suffixed id keeps both copies deduped per lane and
	// stable across retries. Until tool_lane is set the owner notice is the
	// only copy, exactly as before. The owner notice goes first and is
	// always attempted: a failing tool-lane emit must neither suppress the
	// design-lane escalation nor stall the retry. A tool copy lost this way
	// is audited and dropped; the next head escalates again on its own ids.
	ownerErr := s.Notifier.Notify(nctx, integrations.Event{ID: id, Lane: owner, Type: "automation-failed", Text: text})
	// The give-up is always sent (rule 1 of notice_cause.go); its row says so.
	// A row that cannot be written is logged: it must not change the escalation.
	if aerr := s.noteGaveUp(r, pr, 0, ownerErr == nil, fmt.Sprintf("review failed %d times", failures)); aerr != nil && s.Log != nil {
		s.Log.Warn("review give-up audit failed", "pr", pr.Number, "error", aerr)
	}
	var toolErr error
	tool := ""
	if s.Config.Notifier.ToolLane != "" {
		if tool = s.toolLane(); tool != owner {
			toolErr = s.Notifier.Notify(nctx, integrations.Event{ID: id + "-tool", Lane: tool, Type: "automation-failed", Text: text})
		}
	}
	if ownerErr != nil {
		return false, errors.Join(ownerErr, toolErr)
	}
	if toolErr != nil {
		row := map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "lane": tool, "error": reviewErrorText(toolErr)}
		if aerr := s.audit(r.Name, "review-retry-tool-notice-failed", row, false); aerr != nil && s.Log != nil {
			s.Log.Warn("review retry tool notice audit failed", "pr", pr.Number, "error", aerr)
		}
	}
	return true, nil
}

// retryReviewStatus distinguishes a live lease from an unsuccessful attempt.
func (s *Service) retryReviewStatus(ctx context.Context, r config.Repo, pr github.Pull) (bool, bool, error) {
	needed, settled, err := s.reviewPassesNeeded(ctx, r, pr)
	if err != nil {
		return false, false, err
	}
	if !settled {
		// The file list is still settling: skip this pass without a failure,
		// as for a live lease.
		return false, true, nil
	}
	history, err := s.GH.ReviewHistory(ctx, r.Name, pr.Number, s.Config.Review.Login)
	if err != nil {
		return false, false, err
	}
	receipts := passesFor(history, pr.Head.SHA)
	if _, ok := distinctPasses(receipts, needed); ok {
		return true, false, nil
	}
	if src, diffErr := s.reviewDiff(ctx, r, pr); diffErr == nil && src.text != "" {
		if _, _, covered := inheritedPasses(history, reviewPatchID(src.text), pr.Head.SHA, needed); covered {
			return true, false, nil
		}
	}
	if _, _, covered := s.cleanBaseMergePasses(ctx, r, pr, history, needed); covered {
		return true, false, nil
	}
	ttl, err := s.reviewLeaseTTL()
	if err != nil {
		return false, false, err
	}
	posted := map[int]bool{}
	for _, receipt := range receipts {
		posted[receipt.Pass] = true
	}
	for i := 1; i <= needed; i++ {
		if !posted[i] {
			_, live, err := s.GH.ReviewLease(ctx, r.Name, claimRef(pr.Number, pr.Head.SHA, i), ttl)
			if err != nil {
				return false, false, err
			}
			if live {
				return false, true, nil
			}
		}
	}
	return false, false, nil
}

// reviewPassesNeeded is how many independent passes a PR head needs under the
// base branch's trusted policy: two for a guarded blocking change, else one.
// settled is false while GitHub's changed_files and file list still disagree
// just after a push; callers treat that round as having no usable data.
func (s *Service) reviewPassesNeeded(ctx context.Context, r config.Repo, pr github.Pull) (needed int, settled bool, err error) {
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return 0, false, e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return 0, false, e
	}
	files, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return 0, false, e
	}
	if len(files) != pr.ChangedFiles {
		return 0, false, nil
	}
	if p.ClassifyGuarded(files, r.Policy).Class == "blocking" {
		return 2, true, nil
	}
	return 1, true, nil
}

// lastErrorNote is the notice clause naming the last failure, or nothing when
// the state predates the field.
func lastErrorNote(text string) string {
	if text == "" {
		return ""
	}
	return "; last error: " + text
}
