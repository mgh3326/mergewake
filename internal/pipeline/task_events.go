package pipeline

import (
	"context"
	"path/filepath"
	"strconv"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// prURL is the PR's canonical URL, the form hk takes as source_ref.
func prURL(repo string, n int) string {
	return "https://github.com/" + repo + "/pull/" + strconv.Itoa(n)
}

// taskEventRecord is the audit row of one task-event post.
type taskEventRecord struct {
	Source    string                        `json:"source"`
	Task      string                        `json:"task"`
	SourceRef string                        `json:"source_ref"`
	Result    *integrations.TaskEventResult `json:"result,omitempty"`
	Error     string                        `json:"error,omitempty"`
}

// postTaskEvent tells hk that a mergewake action on pr has happened: it runs
// after the action and never fails it. It posts only when the repo keeps
// hk_task_events on, the sink can post task events, and pr carries a
// Mergewake-Task trailer. revert is the merged revert PR of pr for
// mergewake.revert and nil otherwise. Every post, failed or not, is one
// hk-task-event audit row. It reports whether it posted.
func (s *Service) postTaskEvent(ctx context.Context, r config.Repo, pr github.Pull, source string, revert *github.Pull, at time.Time) bool {
	hk, ok := s.Sink.(integrations.TaskEventPoster)
	if !ok || !r.TaskEventsOn() {
		return false
	}
	ctx, cancel := reportContext(ctx)
	defer cancel()
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		_ = s.audit(r.Name, "hk-task-event", taskEventRecord{Source: source, SourceRef: prURL(r.Name, pr.Number), Error: "task trailer unreadable: " + e.Error()}, false)
		return false
	}
	if task == "" {
		return false
	}
	id, _ := strconv.ParseInt(task, 10, 64)
	ev := integrations.TaskEvent{Source: source, SourceRef: prURL(r.Name, pr.Number), TaskID: id, PR: prURL(r.Name, pr.Number), At: &at}
	if revert != nil {
		ev.SourceRef, ev.RevertPR, ev.RevertedPR = prURL(r.Name, revert.Number), prURL(r.Name, revert.Number), ev.PR
	}
	res, e := hk.PostTaskEvent(ctx, ev)
	row := taskEventRecord{Source: source, Task: task, SourceRef: ev.SourceRef}
	if e != nil {
		row.Error = e.Error()
	} else {
		row.Result = &res
	}
	_ = s.audit(r.Name, "hk-task-event", row, false)
	return true
}

// observePRTaskEvent posts mergewake.pr the first time an open, ready PR is
// observed while acting. A posted PR is remembered and not posted again, even
// when the post failed: the client already retried it, and a later poll
// retrying an unreachable hk for every open PR would stall the loop. Like the
// post, its bookkeeping never fails the observation.
func (s *Service) observePRTaskEvent(ctx context.Context, r config.Repo, pr github.Pull, o Options) {
	if !o.Acting() || pr.State != "open" || pr.Draft || !r.TaskEventsOn() {
		return
	}
	if _, ok := s.Sink.(integrations.TaskEventPoster); !ok {
		return
	}
	key := integrations.SourcePR + " " + prURL(r.Name, pr.Number)
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "hk-task-events"), 511)
	if e != nil {
		_ = s.audit(r.Name, "hk-task-event", taskEventRecord{Source: integrations.SourcePR, SourceRef: prURL(r.Name, pr.Number), Error: "sent keys unreadable: " + e.Error()}, false)
		return
	}
	if sent.has(key) {
		return
	}
	at := s.now()
	if opened := parsedTime(pr.CreatedAt); opened != nil {
		at = *opened
	}
	if s.postTaskEvent(ctx, r, pr, integrations.SourcePR, nil, at) {
		if e = sent.add(key); e != nil {
			_ = s.audit(r.Name, "hk-task-event", taskEventRecord{Source: integrations.SourcePR, SourceRef: prURL(r.Name, pr.Number), Error: "sent key not saved: " + e.Error()}, false)
		}
	}
}
