package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// Notice causes (hk 5361; operator decision 433). Every reviewer problem the
// daemon would tell someone about is first counted by cause: one `notice-cause`
// audit row per event, in decisions.jsonl. A notice goes out (to the tool lane
// for rules 2 and 3, to the owner lane for the give-up of rule 1) only when
//
//  1. the daemon gives up on a pass or head (causeGaveUp; always sent, from
//     the callers that give up, see noteGaveUp),
//  2. the cause is one someone must fix (fixNeededCauses): once per cause per
//     repo per fixNeededQuiet, or
//  3. one cause repeats past notifier.cause_threshold events in one repo within
//     notifier.cause_window: one notice with the count, then quiet for the rest
//     of that window.
//
// Everything else, a single lost review or a replayed timeout for instance, is
// recorded with sent=false and emits nothing.

// causeReapCloseScopeForbidden is the cause of a reap close the hub refused
// as scope_forbidden for a job owned by mergewake's own lane. The credential
// may have lost its close grant, so it notifies at most once a day (hk 6010).
const causeReapCloseScopeForbidden = "reap:close-scope-forbidden"

// reapCloseScopeForbiddenQuiet is how long the own-lane close-scope-forbidden
// cause stays quiet after its notice: one notice a day. Every other
// fix-needed cause renotifies after fixNeededQuiet.
const reapCloseScopeForbiddenQuiet = 24 * time.Hour

// causeGaveUp is the cause of a notice that the daemon has stopped retrying.
const causeGaveUp = "gave-up"

// Lost-result causes, named by the lost reason the pass recorded.
const (
	causeLostOverdue     = "lost:overdue"
	causeLostPaneFinish  = "lost:pane-finished"
	causeLostNeverPlaced = "lost:never-placed"
	causeLostOther       = "lost:other"
)

// fixNeededCauses are the causes a person must fix: the notice is not left to
// the threshold alone.
var fixNeededCauses = map[string]bool{
	"spawn:auth":                 true,
	"spawn:workspace":            true,
	"spawn:node-upgrade":         true,
	"spawn:host-unconfigured":    true,
	"spawn:repo-root":            true,
	"spawn:brief-upload":         true,
	causeReapCloseScopeForbidden: true,
}

// fixNeededQuiet is how long a fix-needed cause stays quiet after its notice.
const fixNeededQuiet = time.Hour

// lostCause names the cause of a lost reviewer result from the reason its pass
// recorded.
func lostCause(why string) string {
	switch why {
	case lostOverdue:
		return causeLostOverdue
	case lostPaneFinished:
		return causeLostPaneFinish
	case lostNeverPlaced:
		return causeLostNeverPlaced
	}
	return causeLostOther
}

// spawnCause names the cause of a reviewer spawn failure from the text the
// daemon kept for it: the skip's reason, its cause and its stderr tail. Text
// that names no known cause is spawn:other.
func spawnCause(text string) string {
	t := strings.ToLower(text)
	has := func(words ...string) bool {
		for _, w := range words {
			if strings.Contains(t, w) {
				return true
			}
		}
		return false
	}
	switch {
	case has("repo root unset"):
		return "spawn:repo-root"
	case has("job-host-unconfigured", "in the environment (unset or empty)"):
		return "spawn:host-unconfigured"
	case has("workspace_unresolved"):
		return "spawn:workspace"
	case has("auth_unavailable"):
		return "spawn:auth"
	case has("node_upgrade_required"):
		return "spawn:node-upgrade"
	case has("brief upload", "brief_doc_unavailable", "attachment"):
		return "spawn:brief-upload"
	case has("quota", "free limit"):
		return "spawn:quota"
	case has("admission"):
		return "spawn:admission-refused"
	case has("expired"):
		return "spawn:expired"
	case has("timeout", "timed out", "deadline", "no time left"):
		return "spawn:timeout"
	}
	return "spawn:other"
}

// causeEvent is one reviewer problem for the cause gate.
type causeEvent struct {
	Cause string
	// Lost marks a lost reviewer result; otherwise the event is a spawn failure.
	// It picks the notice's wording and the audit row's kind.
	Lost   bool
	PR     int
	Head   string
	Pass   int
	Detail string
	// Within, when set, bounds the send by the caller's context and this
	// duration instead of the detached report context: a notice sent from a
	// serial sweep must not outlast the sweep's deadline (hk 6014).
	Within time.Duration
}

// causeRecord is what the gate keeps per cause in one repo: the event times in
// the window, and when a notice last went out for the cause.
type causeRecord struct {
	Times    []time.Time `json:"times,omitempty"`
	Notified time.Time   `json:"notified,omitempty"`
}

// noteCause counts one reviewer problem and sends at most one notice, under
// the three rules in the comment above. The notice goes to the tool lane. The
// audit row records the decision either way.
func (s *Service) noteCause(ctx context.Context, r config.Repo, ev causeEvent) error {
	file := filepath.Join(s.state(r.Name), "notice-cause.json")
	defer lockPath(file)()
	unlock, err := fsutil.Lock(file+".lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	ledger := map[string]causeRecord{}
	if _, err = fsutil.ReadJSON(file, &ledger); err != nil {
		return err
	}
	now := s.now().UTC()
	window := s.Config.Notifier.CauseWindowOrDefault()
	threshold := s.Config.Notifier.CauseThresholdOrDefault()
	rec := ledger[ev.Cause]
	kept := rec.Times[:0]
	for _, at := range rec.Times {
		if now.Sub(at) < window {
			kept = append(kept, at)
		}
	}
	rec.Times = append(kept, now)
	count := len(rec.Times)
	fix := fixNeededCauses[ev.Cause]
	fixQuiet := fixNeededQuiet
	thresholdQuiet := window
	if ev.Cause == causeReapCloseScopeForbidden {
		fixQuiet = reapCloseScopeForbiddenQuiet
		if thresholdQuiet < reapCloseScopeForbiddenQuiet {
			thresholdQuiet = reapCloseScopeForbiddenQuiet
		}
	}
	fixDue := fix && (rec.Notified.IsZero() || now.Sub(rec.Notified) >= fixQuiet)
	thresholdDue := count >= threshold && (rec.Notified.IsZero() || now.Sub(rec.Notified) >= thresholdQuiet)
	send := fixDue || thresholdDue
	reason := "below threshold"
	switch {
	case fixDue && thresholdDue:
		reason = "fix-needed and threshold"
	case fixDue:
		reason = "fix-needed"
	case thresholdDue:
		reason = "threshold"
	case fix:
		reason = "fix-needed, noticed within the hour"
		if ev.Cause == causeReapCloseScopeForbidden {
			reason = "fix-needed, noticed within the day"
		}
	case count >= threshold:
		reason = "threshold, noticed within the window"
		if ev.Cause == causeReapCloseScopeForbidden {
			reason = "threshold, noticed within the day"
		}
	}
	var sendErr error
	if send {
		var nctx context.Context
		var cancel context.CancelFunc
		if ev.Within > 0 {
			nctx, cancel = context.WithTimeout(ctx, ev.Within)
		} else {
			nctx, cancel = reportContext(ctx)
		}
		sendErr = s.Notifier.Notify(nctx, integrations.Event{
			ID:   fmt.Sprintf("mergewake-cause-%s-%s-%d", r.Name, strings.ReplaceAll(ev.Cause, ":", "-"), now.UnixNano()),
			Lane: s.toolLane(), Type: "automation-failed",
			Text: causeNoticeText(r, ev, count, window, fix),
		})
		cancel()
		if sendErr == nil {
			rec.Notified = now
		}
	}
	ledger[ev.Cause] = rec
	auditErr := s.audit(r.Name, "notice-cause", map[string]any{
		"repo": r.Name, "pr": ev.PR, "head": ev.Head, "pass": ev.Pass, "cause": ev.Cause, "kind": causeKind(ev),
		"count": count, "threshold": threshold, "window": window.String(),
		"sent": send && sendErr == nil, "reason": reason, "detail": ev.Detail,
	}, false)
	return errors.Join(sendErr, auditErr, fsutil.WriteJSON(file, ledger))
}

// noteGaveUp records the daemon giving up on a pass or head. Those notices are
// always sent (rule 1), so the row says whether this one was.
func (s *Service) noteGaveUp(r config.Repo, pr github.Pull, pass int, sent bool, reason string) error {
	return s.audit(r.Name, "notice-cause", map[string]any{
		"repo": r.Name, "pr": pr.Number, "head": pr.Head.SHA, "pass": pass, "cause": causeGaveUp, "kind": "gave-up",
		"sent": sent, "reason": reason,
	}, false)
}

// noteSpawnCauses counts the reviewer spawn failures a pass skipped. A failure
// that cannot be counted is logged, not returned: it must not change the pass.
func (s *Service) noteSpawnCauses(ctx context.Context, r config.Repo, pr github.Pull, pass int, skips []reviewSkip) {
	for _, sk := range skips {
		ev := causeEvent{
			Cause: spawnCause(sk.Reason + " " + sk.Cause + " " + sk.StderrTail),
			PR:    pr.Number, Head: pr.Head.SHA, Pass: pass,
			Detail: reviewErrorText(errors.New(sk.Reason)),
		}
		if err := s.noteCause(ctx, r, ev); err != nil && s.Log != nil {
			s.Log.Warn("reviewer cause notice failed", "repo", r.Name, "pr", pr.Number, "cause", ev.Cause, "error", err)
		}
	}
}

func causeKind(ev causeEvent) string {
	if ev.Lost {
		return "lost"
	}
	if strings.HasPrefix(ev.Cause, "reap:") {
		return "reap"
	}
	return "spawn"
}

// causeNoticeText is the tool-lane notice for one cause: the subject, the cause,
// the count in the window, and the latest event.
func causeNoticeText(r config.Repo, ev causeEvent, count int, window time.Duration, fix bool) string {
	what := "reviewer spawn failed"
	if ev.Lost {
		what = "reviewer finished but its result was lost"
	}
	note := ""
	if fix {
		note = "; the cause needs a fix"
	}
	return fmt.Sprintf("[mergewake] %s for %s: cause=%s, %d in the last %s%s; latest #%d head=%s pass=%d: %s",
		what, r.Name, ev.Cause, count, window, note, ev.PR, short7(ev.Head), ev.Pass, ev.Detail)
}
