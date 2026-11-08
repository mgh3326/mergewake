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
	// Held is set while the last evaluation ended held: nothing failed, but no
	// review completed, so the daemon must not settle the PR as unchanged.
	Held bool `json:"held,omitempty"`
	// QuotaStopped is set while the last evaluation ended on a quota stop with
	// nothing else wrong: nothing failed, but no review completed, so the
	// daemon must not settle the PR as unchanged.
	QuotaStopped bool `json:"quota_stopped,omitempty"`
	// LastError is the redacted, clipped text of the last counted failure, for
	// the escalation notice.
	LastError string `json:"last_error,omitempty"`
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
// for it: a quota stop waits for its reset and is no failure, a held reviewer
// waits for the time panewire named, if any, and is no failure, a pending pass
// whose reviewer runs on or whose claim another host holds is no failure, a
// collection failure backs off without counting toward escalation, and
// anything else is a real review failure.
type reviewFailureClass struct {
	quotas     []*reviewQuotaStopped
	held       []*reviewerHeld
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

func (c *reviewFailureClass) add(err error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range joined.Unwrap() {
			c.add(e)
		}
		return
	}
	var quota *reviewQuotaStopped
	var held *reviewerHeld
	var collect *collectionFailure
	var lost *reviewResultLost
	switch {
	case errors.As(err, &quota):
		c.quotas = append(c.quotas, quota)
	case errors.As(err, &held):
		c.held = append(c.held, held)
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
		switch {
		case state.Held:
			return ErrReviewHeld
		case state.QuotaStopped:
			return ErrReviewQuotaStopped
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
	ctx, attempt := withAttempt(ctx)
	err = s.Review(ctx, r, pr.Number, "", o)
	if attempt.placed.Load() {
		// A reviewer that no earlier run had placed starts a new attempt: the
		// count of the earlier one must not hold it. Backoffs stay, so a
		// spawn-and-lose loop still slows down.
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
	if errors.Is(err, github.ErrPollSkipped) {
		return nil
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
	for _, h := range class.held {
		// The hold frees by itself. Wait for the time panewire named, however
		// often it repeats; a refusal that named none retries at the next
		// evaluation. No failure, backoff or escalation comes from a hold.
		if at := s.now().Add(h.retryAfter); h.retryAfter > 0 && at.After(state.Next) {
			state.Next = at
		}
	}
	// Only a held or quota-stopped evaluation with nothing else wrong is marked
	// so: a real failure supersedes it, and the daemon must not settle this
	// evaluation. A hold is the stronger marker, so a run with both waits for
	// the hold.
	state.Held = len(class.held) > 0 && !class.real && !class.collection
	state.QuotaStopped = len(class.quotas) > 0 && !class.real && !class.collection && !state.Held
	if saveErr := fsutil.WriteJSON(file, state); saveErr != nil {
		return errors.Join(err, saveErr)
	}
	if !class.real && !class.collection {
		// Only quota stops, holds and pending passes: the audit row says it,
		// and nothing failed.
		if state.Held {
			return ErrReviewHeld
		}
		if state.QuotaStopped {
			return ErrReviewQuotaStopped
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
	err = s.Notifier.Notify(nctx, integrations.Event{ID: fmt.Sprintf("mergewake-review-failed-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA), Lane: s.ownerLane(r), Type: "automation-failed", Text: fmt.Sprintf("[mergewake] review failed %d times for %s#%d head=%s%s; decision needed", failures, r.Name, pr.Number, short7(pr.Head.SHA), lastErrorNote(lastError))})
	return err == nil, err
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
