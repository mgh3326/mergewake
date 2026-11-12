package pipeline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/outcomes"
	"github.com/mgh3326/mergewake/internal/policy"
)

// ErrReviewBlocked means the review cannot start until the PR's author adds
// what the PR comment names. It is no reviewer failure: RetryReview neither
// backs off nor escalates it, so the fix is picked up on the next poll.
var ErrReviewBlocked = errors.New("review blocked")

// A review block is a closed set of outcomes recorded on the PR's outcome row.
const (
	blockMissingTrailer = "blocked: missing-task-trailer"
	blockMissingMarker  = "blocked: task-missing-T-marker"
	blockUnknownFamily  = "blocked: author-family-unknown"
	blockSecretLike     = "blocked: secret-like-content"
)

var secretLikeBlock = reviewBlock{blockSecretLike, "The diff contains a string that looks like a credential. Remove it, or if it is a test fixture make it obviously fake (for example, use a placeholder), and push a new head."}

// reviewBlock is what stops one head's review, and the fix for the comment.
type reviewBlock struct {
	reason string
	fix    string
}

// reviewBlockFor finds a fleet PR the panewire executor could not place. Only
// a positive answer blocks: an unreadable PR commit or task is not a missing
// trailer or marker, so it stays with the executor's skip and retry. So do
// non-fleet authors and repos without a root, where panewire fails the pass.
func (s *Service) reviewBlockFor(ctx context.Context, r config.Repo, pr github.Pull) (reviewBlock, bool) {
	if config.Expand(r.Root) == "" || !s.fleetAuthor(pr.User.Login) {
		return reviewBlock{}, false
	}
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		return reviewBlock{}, false
	}
	if task == "" {
		if pr.Head.Ref == github.RecoveryBranch && strings.EqualFold(pr.Head.Repo.FullName, r.Name) && trailer(pr.Body, "Mergewake-Recovery") != "" {
			state, err := s.ledger().Read()
			if err != nil {
				_ = s.audit(r.Name, "recovery-ledger-read-error", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "error": err.Error()}, false)
				return reviewBlock{}, false
			}
			for _, outcome := range state.Latest {
				if outcome.Repo == r.Name && outcome.RevertPR == pr.Number && pr.Number > 0 {
					return reviewBlock{}, false
				}
			}
		}
		return reviewBlock{blockMissingTrailer, "Add one line to the PR body, with the hk task ID:\n\n    Mergewake-Task: <id>\n\nThe task's title must also carry `(grade X, Tn)`, for example `(grade B, T1)`."}, true
	}
	jobs, ok := s.Sink.(interface {
		TaskJob(context.Context, string) (string, string, error)
	})
	if !ok {
		return reviewBlock{}, false
	}
	_, title, e := jobs.TaskJob(ctx, task)
	if e != nil {
		return reviewBlock{}, false
	}
	if _, ok := taskScale(title); ok {
		return reviewBlock{}, false
	}
	return reviewBlock{blockMissingMarker, fmt.Sprintf("Add `(grade X, Tn)` to the title of hk task %s, for example `(grade B, T1)`.", task)}, true
}

func (s *Service) fleetAuthor(login string) bool {
	author := policy.NormalizeAuthor(login)
	if author == "" {
		return false
	}
	return author == policy.NormalizeAuthor(s.Config.Review.Login) || slices.ContainsFunc(s.Config.Review.FleetAuthors, func(a string) bool {
		return author == policy.NormalizeAuthor(a)
	})
}

// blockReview makes the block visible and writes no pass, claim or done
// marker, so the merge gate keeps reading the head as review_missing. Reruns
// converge: the ledger row, the comment and the event each land once. A dry
// run decides only, and shadow records locally without telling anyone, since
// it reviews nothing either.
func (s *Service) blockReview(ctx context.Context, r config.Repo, pr github.Pull, b reviewBlock, o Options) error {
	report := map[string]any{"action": "review-blocked", "pr": pr.Number, "head": pr.Head.SHA, "outcome": b.reason}
	if e := s.recordReviewBlock(r.Name, pr.Number, pr.Head.SHA, b.reason, o.DryRun); e != nil {
		return e
	}
	if !o.Acting() {
		report["dry_run"] = true
		return s.Print(report)
	}
	// The desk still hears of a block whose comment could not be posted.
	commentErr := s.commentReviewBlock(ctx, r, pr, b)
	if e := errors.Join(commentErr, s.notifyReviewBlock(ctx, r, pr, b)); e != nil {
		return e
	}
	return errors.Join(fmt.Errorf("%w: %s", ErrReviewBlocked, b.reason), s.Print(report))
}

func (s *Service) recordReviewBlock(repo string, pr int, head, reason string, dry bool) error {
	if dry {
		return nil
	}
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		if reason != "" {
			if v.ReviewBlocked == nil {
				v.ReviewBlocked = map[string]string{}
			}
			v.ReviewBlocked[head] = reason
			return
		}
		delete(v.ReviewBlocked, head)
	})
}

// reviewBlockKey is the dedupe key hidden in the comment: GitHub holds it, so
// a restart or another host finds it. A later reason on the same head reuses
// the comment, which names both requirements for that reason.
func reviewBlockKey(head string) string {
	return "<!-- mergewake-review-blocked:" + head + " -->"
}

// commentReviewBlock posts one comment per head. Only the review identity's
// comments count, so nobody else's text can suppress the notice.
func (s *Service) commentReviewBlock(ctx context.Context, r config.Repo, pr github.Pull, b reviewBlock) error {
	key := reviewBlockKey(pr.Head.SHA)
	comments, e := s.GH.IssueComments(github.FreshEvidence(ctx), r.Name, pr.Number)
	if e != nil {
		return e
	}
	own := policy.NormalizeAuthor(s.Config.Review.Login)
	for _, c := range comments {
		if policy.NormalizeAuthor(c.User.Login) == own && strings.Contains(c.Body, key) {
			return nil
		}
	}
	body := fmt.Sprintf("%s\n**Review blocked** (`%s`): mergewake will not review head `%s`, and the merge gate holds it as not reviewed.\n\n%s\n\nFor review, the PR must have `Mergewake-Task: <id>` and `Mergewake-Author-Family: <family>` in the PR body (head commit trailers also work), and the task title must have `(grade X, Tn)`. If these are already present, no changes to them are needed.\n\nThe review starts on the next poll after the fix.", key, b.reason, short7(pr.Head.SHA), b.fix)
	return s.GH.PostIssueComment(ctx, r.Name, pr.Number, body)
}

// notifyReviewBlock tells one lane once per (PR, head, reason). The event ID
// lets the notifier dedupe a send from another host.
func (s *Service) notifyReviewBlock(ctx context.Context, r config.Repo, pr github.Pull, b reviewBlock) error {
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "review-blocked-sent"), 255)
	if e != nil {
		return e
	}
	key := fmt.Sprintf("%d %s %s", pr.Number, pr.Head.SHA, b.reason)
	eventKey := pr.Head.SHA
	if b.reason == blockMissingTrailer || b.reason == blockUnknownFamily {
		bodyDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(pr.Body)))
		key = fmt.Sprintf("%d body:%s %s", pr.Number, bodyDigest, b.reason)
		eventKey = "body-" + bodyDigest
	}
	if sent.has(key) {
		audited, e := loadSentKeys(filepath.Join(s.state(r.Name), "review-blocked-suppressed-sent"), 1023)
		if e != nil {
			return e
		}
		if audited.has(key) {
			return nil
		}
		if e = s.audit(r.Name, "review-blocked-suppressed", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "outcome": b.reason, "reason": "same PR body already notified"}, false); e != nil {
			return e
		}
		return audited.add(key)
	}
	lane, via, e := s.blockLane(ctx, r, pr, b)
	if e != nil {
		return e
	}
	code := strings.TrimPrefix(b.reason, "blocked: ")
	nctx, cancel := reportContext(ctx)
	defer cancel()
	if e = s.Notifier.Notify(nctx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-review-blocked-%s-%d-%s-%s", r.Name, pr.Number, eventKey, code),
		Text: clip(fmt.Sprintf("[mergewake] REVIEW BLOCKED %s#%d head=%s | %s | the PR comment names the fix | lane=%s via %s", r.Name, pr.Number, short7(pr.Head.SHA), b.reason, lane, via), findingsLaneText),
		Lane: lane,
		Type: "decision",
	}); e != nil {
		return fmt.Errorf("review block event to %s: %w", lane, e)
	}
	if e = sent.add(key); e != nil {
		return e
	}
	return s.audit(r.Name, "review-blocked", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "outcome": b.reason, "lane": lane, "via": via}, false)
}

// blockLane is the lane a review block goes to. A missing trailer and an
// unknown family are fixed by the PR's author, so the author's lane is found
// first (findingsRoute); with none found, and for the other reasons, which a
// human judges, it is the repo's owner lane. A route read error is returned,
// so nothing is sent and the next pass retries.
func (s *Service) blockLane(ctx context.Context, r config.Repo, pr github.Pull, b reviewBlock) (lane, via string, err error) {
	if b.reason == blockMissingTrailer || b.reason == blockUnknownFamily {
		route, e := s.findingsRoute(ctx, r, pr)
		if e != nil {
			return "", "", fmt.Errorf("review block route: %w", e)
		}
		if route.via != "unknown author; desk" {
			return route.lane, route.via, nil
		}
	}
	return s.ownerLane(r), "owner lane", nil
}
