package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type reviewRetryState struct {
	Head      string    `json:"head"`
	Failures  int       `json:"failures"`
	Next      time.Time `json:"next"`
	Escalated bool      `json:"escalated"`
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
	initial, maximum, threshold := s.Config.Daemon.PR.ReviewRetry.Settings()
	if state.Failures >= threshold && !state.Escalated {
		sent, err := s.escalateReviewFailure(ctx, r, pr, state.Failures)
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
		return nil
	}
	fresh, err := s.GH.Pull(github.FreshEvidence(ctx), r.Name, pr.Number)
	if err != nil {
		return err
	}
	if fresh.State != "open" || fresh.Merged || fresh.Draft || fresh.Head.SHA != pr.Head.SHA {
		return nil
	}
	err = s.Review(ctx, r, pr.Number, "", o)
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
			return nil
		}
		if err == nil {
			err = errors.New("review produced no completed pass")
		}
	}
	if errors.Is(err, github.ErrPollSkipped) {
		return nil
	}
	state.Failures++
	delay := initial
	for i := 1; i < state.Failures && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
		} else {
			delay *= 2
		}
	}
	state.Next = s.now().Add(delay)
	if saveErr := fsutil.WriteJSON(file, state); saveErr != nil {
		return errors.Join(err, saveErr)
	}
	// Escalate after persisting the failure, including the first poll at K.
	if state.Failures >= threshold && !state.Escalated {
		sent, notifyErr := s.escalateReviewFailure(ctx, r, pr, state.Failures)
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

func (s *Service) escalateReviewFailure(ctx context.Context, r config.Repo, pr github.Pull, failures int) (bool, error) {
	fresh, err := s.GH.Pull(github.FreshEvidence(ctx), r.Name, pr.Number)
	if err != nil {
		return false, err
	}
	if fresh.State != "open" || fresh.Merged || fresh.Draft || fresh.Head.SHA != pr.Head.SHA {
		return false, nil
	}
	nctx, cancel := reportContext(ctx)
	defer cancel()
	err = s.Notifier.Notify(nctx, integrations.Event{ID: fmt.Sprintf("mergewake-review-failed-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA), Lane: s.Config.Notifier.Lane, Type: "automation-failed", Text: fmt.Sprintf("[mergewake] review failed %d times for %s#%d head=%s; decision needed", failures, r.Name, pr.Number, short7(pr.Head.SHA))})
	return err == nil, err
}

// retryReviewStatus distinguishes a live lease from an unsuccessful attempt.
func (s *Service) retryReviewStatus(ctx context.Context, r config.Repo, pr github.Pull) (bool, bool, error) {
	head, err := s.GH.Head(ctx, r.Name, r.Branch)
	if err != nil {
		return false, false, err
	}
	p, err := s.Policy(ctx, r, head)
	if err != nil {
		return false, false, err
	}
	files, err := s.GH.Files(ctx, r.Name, pr.Number)
	if err != nil {
		return false, false, err
	}
	if len(files) != pr.ChangedFiles {
		return false, false, errors.New("incomplete PR file list")
	}
	needed := 1
	if p.ClassifyGuarded(files, r.Policy).Class == "blocking" {
		needed = 2
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
