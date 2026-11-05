package pipeline

import (
	"context"
	"path/filepath"
	"strconv"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

// prURL is the PR's canonical URL, the form hk takes as source_ref. It is the
// outcomes ledger key too, so a task event and its outcome name the same PR.
func prURL(repo string, n int) string { return outcomes.Key(repo, n) }

// taskEventRecord is the audit row of one task-event post.
type taskEventRecord struct {
	Source    string                        `json:"source"`
	Task      string                        `json:"task"`
	SourceRef string                        `json:"source_ref"`
	Result    *integrations.TaskEventResult `json:"result,omitempty"`
	Error     string                        `json:"error,omitempty"`
}

// fieldsRefusedRecord is the audit row of an hk that answered 400 to the
// optional head/base/merge/reviews fields: the event was resent without them.
// It names the hk error code only, never the payload.
type fieldsRefusedRecord struct {
	Source    string `json:"source"`
	Task      string `json:"task"`
	SourceRef string `json:"source_ref"`
	Code      string `json:"code"`
}

// postOutcome is what postTaskEvent did with one event.
type postOutcome int

const (
	// postGated: the repo or sink does not post task events.
	postGated postOutcome = iota
	// postUnreadable: the task trailer could not be read; a later try may.
	postUnreadable
	// postNoTask: the PR names no task.
	postNoTask
	// postInvalid: the trailer names a task ID hk cannot hold (over int64).
	postInvalid
	// postSent: the event went to hk, whatever hk answered.
	postSent
)

// postTaskEvent tells hk that a mergewake action on pr has happened: it runs
// after the action and never fails it. It posts only when the repo keeps
// hk_task_events on, the sink can post task events, and pr carries a
// Mergewake-Task trailer. revert is the merged revert PR of pr for
// mergewake.revert and nil otherwise. Every post, failed or not, is one
// hk-task-event audit row.
func (s *Service) postTaskEvent(ctx context.Context, r config.Repo, pr github.Pull, source string, revert *github.Pull, at time.Time) postOutcome {
	return s.postTaskEventWith(ctx, r, pr, source, revert, at, nil)
}

// postTaskEventWith is postTaskEvent for a mergewake.review event that also
// carries reviews, one record per posted pass.
func (s *Service) postTaskEventWith(ctx context.Context, r config.Repo, pr github.Pull, source string, revert *github.Pull, at time.Time, reviews []integrations.ReviewRecord) postOutcome {
	hk, ok := s.Sink.(integrations.TaskEventPoster)
	if !ok || !r.TaskEventsOn() {
		return postGated
	}
	ctx, cancel := reportContext(ctx)
	defer cancel()
	row := taskEventRecord{Source: source, SourceRef: prURL(r.Name, pr.Number)}
	if revert != nil {
		row.SourceRef = prURL(r.Name, revert.Number)
	}
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		row.Error = "task trailer unreadable: " + e.Error()
		_ = s.audit(r.Name, "hk-task-event", row, false)
		return postUnreadable
	}
	if task == "" {
		return postNoTask
	}
	row.Task = task
	id, e := strconv.ParseInt(task, 10, 64)
	if e != nil {
		row.Error = "task ID out of range; not posted"
		_ = s.audit(r.Name, "hk-task-event", row, false)
		return postInvalid
	}
	ev := integrations.TaskEvent{Source: source, SourceRef: row.SourceRef, TaskID: id, PR: prURL(r.Name, pr.Number), At: &at}
	if revert != nil {
		ev.RevertPR, ev.RevertedPR = row.SourceRef, ev.PR
	}
	ev.HeadSHA, ev.BaseSHA = integrations.SHA(pr.Head.SHA), integrations.SHA(pr.Base.SHA)
	if source == integrations.SourceMerge {
		ev.MergeSHA = s.mergeCommit(ctx, r, pr)
	}
	if source == integrations.SourceReview && len(reviews) > 0 {
		ev.Reviews = reviews[:min(len(reviews), integrations.MaxReviewRecords)]
	}
	res, e := hk.PostTaskEvent(ctx, ev)
	if e != nil {
		row.Error = e.Error()
	} else {
		row.Result = &res
	}
	_ = s.audit(r.Name, "hk-task-event", row, false)
	if e == nil && res.FieldsRefused != "" {
		_ = s.audit(r.Name, "task-event-fields-refused", fieldsRefusedRecord{Source: source, Task: task, SourceRef: row.SourceRef, Code: res.FieldsRefused}, false)
	}
	return postSent
}

// mergeCommitBudget bounds the merge commit read, so a stalled GitHub cannot
// spend the deadline the event post itself needs.
const mergeCommitBudget = 5 * time.Second

// mergeCommit reads the merge commit of the just-merged pr, or "" when GitHub
// does not show the PR merged yet, the read fails or it exceeds
// mergeCommitBudget: the event then goes without merge_sha rather than not at
// all.
func (s *Service) mergeCommit(ctx context.Context, r config.Repo, pr github.Pull) string {
	ctx, cancel := context.WithTimeout(ctx, mergeCommitBudget)
	defer cancel()
	merged, e := s.GH.Pull(ctx, r.Name, pr.Number)
	if e != nil || !merged.Merged {
		return ""
	}
	return integrations.SHA(merged.MergeCommitSHA)
}

// passRecord is the hk reviews[] record of one posted pass: what the pass
// reviewed (head, base, patch), who produced it and how many findings of each
// severity it posted. Fields mergewake does not know here stay empty.
func passRecord(pr github.Pull, receipt github.PassReceipt, findings []github.Finding, contractSource string, started, finished time.Time) integrations.ReviewRecord {
	rec := integrations.ReviewRecord{
		ReviewedHead:   pr.Head.SHA,
		ReviewedBase:   integrations.SHA(pr.Base.SHA),
		PatchID:        integrations.Token(receipt.Patch),
		Round:          receipt.Round,
		Pass:           receipt.Pass,
		Lens:           integrations.Token(receipt.Lens),
		Family:         integrations.Token(receipt.Family),
		Executor:       integrations.Token(receipt.Executor),
		Fallback:       receipt.Fallback != "",
		ContractSource: integrations.Token(contractSource),
		StartedAt:      &started,
		FinishedAt:     &finished,
	}
	for _, f := range findings {
		switch f.Severity {
		case "critical":
			rec.Critical++
		case "high", "blocking":
			rec.High++
		case "medium", "major":
			rec.Medium++
		case "low", "minor":
			rec.Low++
		}
	}
	return rec
}

// taskEventKeys is the per-repo record of events already posted, so a poll
// does not post them again. A lost or concurrent write costs one more post,
// which hk records as redelivered.
func (s *Service) taskEventKeys(repo string) (*sentKeys, error) {
	return loadSentKeys(filepath.Join(s.state(repo), "hk-task-events"), 511)
}

// taskEventsActive reports whether an acting observation of r posts events.
func (s *Service) taskEventsActive(r config.Repo, o Options) bool {
	_, ok := s.Sink.(integrations.TaskEventPoster)
	return ok && o.Acting() && r.TaskEventsOn()
}

// observePRTaskEvent posts mergewake.pr the first time an open, ready PR is
// observed while acting. A posted PR is remembered and not posted again, even
// when the post failed: the client already retried it, and a later poll
// retrying an unreachable hk for every open PR would stall the loop. Like the
// post, its bookkeeping never fails the observation.
func (s *Service) observePRTaskEvent(ctx context.Context, r config.Repo, pr github.Pull, o Options) {
	if !s.taskEventsActive(r, o) || pr.State != "open" || pr.Draft {
		return
	}
	key := integrations.SourcePR + " " + prURL(r.Name, pr.Number)
	at := s.now()
	if opened := parsedTime(pr.CreatedAt); opened != nil {
		at = *opened
	}
	s.postOnce(ctx, r, key, func() bool {
		switch s.postTaskEvent(ctx, r, pr, integrations.SourcePR, nil, at) {
		case postSent, postInvalid:
			return true
		}
		// No trailer yet: an open PR may still gain one.
		return false
	})
}

// revertEventWindow bounds how old a merged revert PR may be and still be
// posted, so the first acting pass over an old ledger does not replay reverts
// hk has long since handled.
const revertEventWindow = 7 * 24 * time.Hour

// revertEventKey names the mergewake.revert post of a revert PR.
func revertEventKey(repo string, revertPR int) string {
	return integrations.SourceRevert + " " + prURL(repo, revertPR)
}

// revertEventPending reports whether the merged PR v has a revert PR whose
// event an acting pass has not settled yet, so reconciliation keeps visiting
// the row after its outcome is final. Unreadable keys (sent is nil) leave the
// row pending, so observeRevertTaskEvent audits the read failure and a later
// poll retries it.
func (s *Service) revertEventPending(r config.Repo, v outcomes.Outcome, o Options, sent *sentKeys) bool {
	return v.MergedAt != nil && v.RevertPR > 0 && s.taskEventsActive(r, o) && (sent == nil || !sent.has(revertEventKey(r.Name, v.RevertPR)))
}

// observeRevertTaskEvent posts mergewake.revert once the revert PR of the
// merged PR p is merged, independently of the outcome's seven-day reverted
// verdict: an earlier shadow pass or a revert merged after the window still
// gets its event. An open revert PR posts nothing and is checked again; a
// revert closed unmerged, or merged before revertEventWindow, is settled
// without a post.
func (s *Service) observeRevertTaskEvent(ctx context.Context, r config.Repo, p github.Pull, merged *time.Time, revertPR int, o Options) {
	if merged == nil || revertPR == 0 || !s.taskEventsActive(r, o) {
		return
	}
	s.postOnce(ctx, r, revertEventKey(r.Name, revertPR), func() bool {
		recovery, e := s.GH.Pull(ctx, r.Name, revertPR)
		if e != nil {
			_ = s.audit(r.Name, "hk-task-event", taskEventRecord{Source: integrations.SourceRevert, SourceRef: prURL(r.Name, revertPR), Error: "revert PR unreadable: " + e.Error()}, false)
			return false
		}
		if recovery.MergedAt == nil {
			return recovery.State == "closed"
		}
		at := s.now()
		if t := parsedTime(*recovery.MergedAt); t != nil {
			at = *t
		}
		if s.now().Sub(at) > revertEventWindow {
			return true
		}
		recovery.Number = revertPR
		return s.postTaskEvent(ctx, r, p, integrations.SourceRevert, &recovery, at) != postUnreadable
	})
}

// postOnce runs post unless key is recorded, and records key when post
// reports the event settled. Bookkeeping failures are audited, never returned.
func (s *Service) postOnce(ctx context.Context, r config.Repo, key string, post func() bool) {
	sent, e := s.taskEventKeys(r.Name)
	if e != nil {
		_ = s.audit(r.Name, "hk-task-event", taskEventRecord{SourceRef: key, Error: "sent keys unreadable: " + e.Error()}, false)
		return
	}
	if sent.has(key) || !post() {
		return
	}
	if e = sent.add(key); e != nil {
		_ = s.audit(r.Name, "hk-task-event", taskEventRecord{SourceRef: key, Error: "sent key not saved: " + e.Error()}, false)
	}
}
