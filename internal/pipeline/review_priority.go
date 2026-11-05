package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// ReviewPriority is the hk priority that orders a PR's review start. Value 1
// is the most urgent; 0 means none and sorts last.
type ReviewPriority struct {
	Value int
	// Source is where Value came from: "change" (hk's change for the PR URL),
	// "task" (the Mergewake-Task task), "none", or "error" for a failed
	// lookup. A failed lookup is worth the same as none.
	Source string
}

// Urgent reports whether the review starts at once, past review_parallel.
func (p ReviewPriority) Urgent() bool { return p.Value == 1 }

// Rank orders reviews ascending; no priority ranks after every priority.
func (p ReviewPriority) Rank() int {
	if p.Value < 1 {
		return int(^uint(0) >> 1)
	}
	return p.Value
}

type priorityReader interface {
	TaskPriority(context.Context, string) (int, error)
	ChangePriority(context.Context, string) (int, error)
}

// reviewPriorityTimeout bounds one PR's lookups: ordering is a hint, so a slow
// hk costs that PR its place, not the pass its time. A var so tests can
// shorten it.
var reviewPriorityTimeout = 5 * time.Second

// ReviewPriority resolves a PR's effective priority: the change priority hk
// holds for the PR URL, else the priority of its Mergewake-Task task, else
// none. It reads each at most once and never fails: a sink without priorities
// or any failed read gives the PR no priority, which keeps today's order.
func (s *Service) ReviewPriority(ctx context.Context, r config.Repo, pr github.Pull) ReviewPriority {
	reader, ok := s.Sink.(priorityReader)
	if !ok {
		return ReviewPriority{Source: "none"}
	}
	ctx, cancel := context.WithTimeout(ctx, reviewPriorityTimeout)
	defer cancel()
	url := pr.HTMLURL
	if url == "" {
		url = fmt.Sprintf("https://github.com/%s/pull/%d", r.Name, pr.Number)
	}
	failed := ReviewPriority{Source: "error"}
	v, err := reader.ChangePriority(ctx, url)
	if err != nil {
		return failed
	}
	if v > 0 {
		return ReviewPriority{v, "change"}
	}
	task, err := s.prTask(ctx, r, pr)
	if err != nil {
		return failed
	}
	if task == "" || !integrations.TaskID.MatchString(task) {
		return ReviewPriority{Source: "none"}
	}
	if v, err = reader.TaskPriority(ctx, task); err != nil {
		return failed
	}
	if v > 0 {
		return ReviewPriority{v, "task"}
	}
	return ReviewPriority{Source: "none"}
}

// AuditReviewStart records the priority a review started under, and whether
// it started past the review_parallel bound.
func (s *Service) AuditReviewStart(r config.Repo, pr github.Pull, p ReviewPriority, o Options, overBound bool) error {
	return s.audit(r.Name, "review-start", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "priority": p.Value, "priority_source": p.Source, "over_bound": overBound}, o.DryRun)
}
