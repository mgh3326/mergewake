package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// The merge_overdue thresholds are the starting values of hk doc
// policy/merge-sla-lead-escalation (rule 7: tune them from the counts this
// notice leaves). They are constants, not config keys: internal/config/** is a
// deploy_approval path.
const (
	// overdueReviewStartAfter: no review pass started this long after the
	// head's required checks completed.
	overdueReviewStartAfter = 20 * time.Minute
	// overdueLandAfter: the head's review is done and the PR is still open
	// this long after.
	overdueLandAfter = 10 * time.Minute
	// overdueReadyAfter: the PR is still open this long after mergewake first
	// saw the head.
	overdueReadyAfter = 45 * time.Minute
	// overdueLeadLane is the one shared lane merge_overdue goes to first.
	overdueLeadLane = "lead"
)

// The merge_overdue conditions, in the order they are checked.
const (
	overdueReviewNotStarted = "review_not_started"
	overdueNotLanded        = "not_landed"
	overdueReadyTooLong     = "ready_too_long"
)

// overdueClock holds the three clocks of one (PR, head). A zero time means
// the event has not been seen yet. Touched only bounds how long an idle entry
// is kept.
type overdueClock struct {
	HeadSeen   time.Time `json:"head_seen,omitzero"`
	ChecksDone time.Time `json:"checks_done,omitzero"`
	ReviewDone time.Time `json:"review_done,omitzero"`
	Touched    time.Time `json:"touched,omitzero"`
}

func (s *Service) overdueClocksFile(repo string) string {
	return filepath.Join(s.state(repo), "pr-overdue-clocks.json")
}

// overdueCounts is how many merge_overdue notices were delivered per repo
// since the pr pass line last took them.
var overdueCounts = struct {
	sync.Mutex
	n map[string]int
}{n: map[string]int{}}

// TakeMergeOverdue returns the merge_overdue notices delivered for repo since
// the last call and starts the next window. The pr pass log line carries it.
func TakeMergeOverdue(repo string) int {
	overdueCounts.Lock()
	defer overdueCounts.Unlock()
	n := overdueCounts.n[repo]
	delete(overdueCounts.n, repo)
	return n
}

// checksCompleted reports whether every required check of the head has
// completed. A cancelled check has not: it waits for a rerun, and the notice
// is excluded while it is expired anyway. An absent check has not either.
func checksCompleted(v gateVerdict) bool {
	for _, c := range v.Status.Checks {
		switch github.CI(c.State) {
		case github.Green, github.Red, github.Skipped:
		default:
			return false
		}
	}
	return true
}

// reviewCompleted reports whether the head carries every review pass it
// needs: the done marker with all passes posted, or an earlier head's review of
// the same patch.
func reviewCompleted(v gateVerdict) bool {
	return v.Review.Inherited != "" || (v.Status.Done && v.PassesNeeded > 0 && v.HeadPasses >= v.PassesNeeded)
}

// tickOverdueClocks starts each clock the first time its event is seen for
// this (PR, head) and returns the clocks as they stand. They live in
// pr-overdue-clocks.json, so a restart keeps them; a new head has its own
// entry. Entries untouched for a week are dropped.
func (s *Service) tickOverdueClocks(r config.Repo, pr github.Pull, v gateVerdict) (overdueClock, error) {
	file := s.overdueClocksFile(r.Name)
	defer lockPath(file)()
	clocks := map[string]overdueClock{}
	if found, e := fsutil.ReadJSON(file, &clocks); found && e != nil {
		return overdueClock{}, fmt.Errorf("%s: %w", file, e)
	} else if e != nil {
		return overdueClock{}, e
	}
	now := s.now().UTC()
	key := fmt.Sprintf("%d %s", pr.Number, pr.Head.SHA)
	c, existed := clocks[key]
	changed := !existed
	start := func(at *time.Time, due bool) {
		if due && at.IsZero() {
			*at, changed = now, true
		}
	}
	start(&c.HeadSeen, true)
	start(&c.ChecksDone, checksCompleted(v))
	start(&c.ReviewDone, reviewCompleted(v))
	if now.Sub(c.Touched) >= time.Hour {
		c.Touched, changed = now, true
	}
	if !changed {
		return c, nil
	}
	clocks[key] = c
	for k, o := range clocks {
		if o.Touched.Before(now.Add(-7 * 24 * time.Hour)) {
			delete(clocks, k)
		}
	}
	return c, fsutil.WriteJSON(file, clocks)
}

// overdueHeld reports whether the verdict holds the head for a reason that is
// not a plain wait: an open critical/high finding, a failed, expired or absent
// check, a conflict or a stale base, and the other reasons that wait on the
// author (open threads, unanswered triage, a review blocked on a detail) or
// on the evidence to settle. Only a head with no reason, or with mergewake's
// own review_missing queue state, can be overdue. It is evaluated again on
// every pass.
func overdueHeld(v gateVerdict) bool {
	for _, reason := range v.Reasons {
		if reason == "review_missing" && v.Review.Block == "" {
			continue
		}
		return true
	}
	return false
}

// overdueDue lists the conditions whose time has passed, with the age each is
// reported at.
func overdueDue(c overdueClock, started bool, now time.Time) (conditions []string, ages map[string]time.Duration) {
	ages = map[string]time.Duration{}
	add := func(name string, since time.Time, after time.Duration) {
		if !since.IsZero() && now.Sub(since) >= after {
			conditions = append(conditions, name)
			ages[name] = now.Sub(since)
		}
	}
	if !started {
		add(overdueReviewNotStarted, c.ChecksDone, overdueReviewStartAfter)
	}
	add(overdueNotLanded, c.ReviewDone, overdueLandAfter)
	add(overdueReadyTooLong, c.HeadSeen, overdueReadyAfter)
	return conditions, ages
}

// reviewStarted reports whether a review pass was started for the head: a
// reviewer job is recorded, a pass is posted, or the head's review is done or
// inherited. A spawn that was refused or held leaves no record.
func (s *Service) reviewStarted(r config.Repo, pr github.Pull, v gateVerdict) bool {
	return v.HeadPasses > 0 || v.Status.Done || v.Review.Inherited != "" || len(s.reviewerJobLabels(r, pr)) > 0
}

// overdueExcluded reports the exclusions that cost a read: a T3 change (the
// Mergewake-Tier trailer, else the hk task title's marker) and a change to a
// deploy_approval path. The hk hold exclusion is not here: no hk read exposes
// a hold to mergewake, so it is skipped.
func (s *Service) overdueExcluded(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy) (bool, error) {
	tier, e := s.prTrailer(ctx, r, pr, "Mergewake-Tier", validTier)
	if e != nil {
		return false, e
	}
	if tier == "" {
		_, t, _, e := s.taskTitleScale(ctx, r, pr)
		if e != nil {
			return false, e
		}
		tier = t
	}
	if strings.EqualFold(tier, "T3") {
		return true, nil
	}
	if len(p.DeployApproval) == 0 {
		return false, nil
	}
	files, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return false, e
	}
	for _, f := range files {
		for _, name := range []string{f.Path, f.Previous} {
			if name == "" {
				continue
			}
			for _, rule := range p.DeployApproval {
				match, e := policy.MatchAny(rule.Paths, name)
				if e != nil {
					return false, e
				}
				if match {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// mergeOverdue starts the head's clocks and announces each overdue condition
// once. It never holds or changes a verdict: an error is audited and the next
// pass tries again.
func (s *Service) mergeOverdue(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, v gateVerdict) {
	if e := s.announceOverdue(ctx, r, pr, p, v); e != nil {
		_ = s.audit(r.Name, "merge-overdue-error", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "error": integrations.Detail(e)}, false)
	}
}

func (s *Service) announceOverdue(ctx context.Context, r config.Repo, pr github.Pull, p policy.Policy, v gateVerdict) error {
	c, e := s.tickOverdueClocks(r, pr, v)
	if e != nil {
		return e
	}
	if overdueHeld(v) {
		return nil
	}
	now := s.now()
	conditions, ages := overdueDue(c, s.reviewStarted(r, pr, v), now)
	if len(conditions) == 0 {
		return nil
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		return e
	}
	keyOf := func(condition string) string {
		return fmt.Sprintf("merge_overdue %d %s %s", pr.Number, pr.Head.SHA, condition)
	}
	var fresh []string
	for _, condition := range conditions {
		if k := keyOf(condition); !sent.has(k) && !sent.has("decided "+k) {
			fresh = append(fresh, condition)
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	if excluded, e := s.overdueExcluded(ctx, r, pr, p); e != nil || excluded {
		return e
	}
	var errs []error
	for _, condition := range fresh {
		errs = append(errs, s.sendOverdue(ctx, r, pr, v, sent, keyOf(condition), condition, ages[condition]))
	}
	return errors.Join(errs...)
}

// overdueText is the notice: the condition and age, then what the gate, the
// required checks and the review say about the head.
func (s *Service) overdueText(r config.Repo, pr github.Pull, v gateVerdict, condition string, age time.Duration) string {
	gate := v.State
	if len(v.Reasons) > 0 {
		gate += " " + strings.Join(v.Reasons, "; ")
	}
	checks := "-"
	if len(v.Status.Checks) > 0 {
		parts := make([]string, 0, len(v.Status.Checks))
		for _, c := range v.Status.Checks {
			parts = append(parts, c.Name+"="+c.State)
		}
		checks = strings.Join(parts, ", ")
	}
	needed := "?"
	if v.PassesNeeded > 0 {
		needed = fmt.Sprint(v.PassesNeeded)
	}
	reviewer := "none"
	if labels := s.reviewerJobLabels(r, pr); len(labels) > 0 {
		reviewer = strings.Join(labels, ", ")
	}
	return fmt.Sprintf("[mergewake] MERGE_OVERDUE %s#%d head=%s | %s after %s | gate: %s | checks: %s | review: passes %d/%s, reviewer %s",
		r.Name, pr.Number, short7(pr.Head.SHA), condition, age.Round(time.Minute), gate, checks, v.HeadPasses, needed, reviewer)
}

// sendOverdue delivers one notice. The lead lane gets it first. A lane the
// notifier refuses (it is not registered on this host) sends it to the PR's
// task lane instead, and the desk hears a short FYI. A notice neither lane
// takes is parked under a decided key, so a failing delivery is not retried
// every pass. The notice is counted and audited once it is delivered.
func (s *Service) sendOverdue(ctx context.Context, r config.Repo, pr github.Pull, v gateVerdict, sent *sentKeys, key, condition string, age time.Duration) error {
	text := s.overdueText(r, pr, v, condition, age)
	id := fmt.Sprintf("mergewake-merge-overdue-%s-%d-%s-%s", r.Name, pr.Number, pr.Head.SHA, condition)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	route, lane := overdueLeadLane, overdueLeadLane
	leadErr := s.Notifier.Notify(reportCtx, integrations.Event{ID: id, Text: text, Lane: overdueLeadLane, Type: "decision"})
	var fyi string
	if leadErr != nil {
		taskLane, via, e := s.notifyLane(ctx, r, pr)
		if e != nil {
			return e
		}
		lane, route = taskLane, taskLane+" via "+via
		if e := s.Notifier.Notify(reportCtx, integrations.Event{ID: id + "-fallback", Text: text + " | lead lane unavailable; sent to " + route, Lane: taskLane, Type: "decision"}); e != nil {
			if ae := sent.add("decided " + key); ae != nil {
				return errors.Join(e, ae)
			}
			return s.audit(r.Name, "merge-overdue-delivery-failed", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "condition": condition, "error": integrations.Detail(errors.Join(leadErr, e))}, false)
		}
		if !s.isDeskLane(taskLane) {
			fyi = fmt.Sprintf("[mergewake] FYI merge_overdue %s#%d head=%s | %s after %s | lead lane unavailable; sent to %s",
				r.Name, pr.Number, short7(pr.Head.SHA), condition, age.Round(time.Minute), route)
		}
	}
	if e := sent.add(key); e != nil {
		return e
	}
	overdueCounts.Lock()
	overdueCounts.n[r.Name]++
	overdueCounts.Unlock()
	audit := s.audit(r.Name, "merge_overdue", map[string]any{
		"repo": r.Name, "pr": pr.Number, "head": pr.Head.SHA, "condition": condition,
		"age": age.Round(time.Minute).String(), "route": route, "lane": lane,
	}, false)
	if fyi == "" {
		return audit
	}
	desk := s.Config.Notifier.FallbackLane
	if desk == "" {
		desk = s.Config.Notifier.Lane
	}
	// The notice is delivered and counted; a failed FYI changes neither.
	if e := s.Notifier.Notify(reportCtx, integrations.Event{ID: id + "-fyi", Text: fyi, Lane: desk, Type: "info"}); e != nil {
		return errors.Join(audit, fmt.Errorf("desk FYI: %w", e))
	}
	return audit
}
