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
	// overdueReadyAfter: the PR is still open this long after mergewake
	// recorded its READY verdict for the head.
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

// overdueHoldSpans bounds the holds an entry keeps. Past it the two oldest are
// merged into one, which over-counts the open time between them as held; the
// entry is pruned a week after its last touch either way.
const overdueHoldSpans = 32

// holdSpan is one closed draft hold: the PR was a draft from From to To. It is
// data, not a correction: no clock is moved for it, and an age takes off the
// part of the span it overlaps (openAge).
type holdSpan struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// overdueClock holds the clocks of one (PR, head). A zero time means the event
// has not been seen yet. Each is the raw time its event was recorded and no
// hold ever moves it: the draft holds are kept beside them, and an age is
// computed when it is read. Touched only bounds how long an idle entry is kept.
type overdueClock struct {
	HeadSeen   time.Time `json:"head_seen,omitzero"`
	ChecksDone time.Time `json:"checks_done,omitzero"`
	ReviewDone time.Time `json:"review_done,omitzero"`
	// Ready is when mergewake recorded the READY verdict for the head; no READY,
	// no ready_too_long.
	Ready time.Time `json:"ready,omitzero"`
	// DraftSince is when the draft hold that is going on started. While it is
	// set the clocks do not count: the per-pass PR path returns before the
	// gate for a draft, so noteDraftHold records it, and the pass that sees
	// the PR leave draft closes it into Holds (closeHold). The hold starts at
	// LastOpen, not at the pass that first saw it (see below).
	DraftSince time.Time `json:"draft_since,omitzero"`
	// Holds are the draft holds this head has been through, oldest first. A
	// file an earlier release wrote has none: its holds were taken off the
	// clocks it stored, which is the same age as holding them would give.
	Holds []holdSpan `json:"holds,omitempty"`
	// LastOpen is the last pass that saw the PR not a draft. The time between
	// it and the pass that first sees a draft may already have been a draft,
	// so a hold that starts now is anchored here. Zero on a file an earlier
	// release wrote, and for a head first seen as a draft: the hold then
	// starts at the pass that saw it, as it did before.
	LastOpen time.Time `json:"last_open,omitzero"`
	Touched  time.Time `json:"touched,omitzero"`
}

// overdueState is pr-overdue-clocks.json: the clocks by (PR, head). A file an
// earlier release wrote may carry an "updates" list; it is not read.
type overdueState struct {
	Clocks map[string]overdueClock `json:"clocks"`
}

func (s *Service) overdueClocksFile(repo string) string {
	return filepath.Join(s.state(repo), "pr-overdue-clocks.json")
}

// readOverdueState reads the clocks file. A file written before the clocks had
// their own key is a bare map of clocks: it has no "clocks" key, and still loads.
func readOverdueState(file string) (overdueState, error) {
	st := overdueState{}
	found, e := fsutil.ReadJSON(file, &st)
	if e != nil {
		return overdueState{}, fmt.Errorf("%s: %w", file, e)
	}
	if found && st.Clocks == nil {
		clocks := map[string]overdueClock{}
		if _, e := fsutil.ReadJSON(file, &clocks); e != nil {
			return overdueState{}, fmt.Errorf("%s: %w", file, e)
		}
		st.Clocks = clocks
	}
	if st.Clocks == nil {
		st.Clocks = map[string]overdueClock{}
	}
	return st, nil
}

func writeOverdueState(file string, st overdueState) error {
	return fsutil.WriteJSON(file, st)
}

// pruneClocks drops the entries untouched for a week.
func pruneClocks(clocks map[string]overdueClock, now time.Time) {
	for k, o := range clocks {
		if o.Touched.Before(now.Add(-7 * 24 * time.Hour)) {
			delete(clocks, k)
		}
	}
}

// overdueKey is the clocks key of one (PR, head).
func overdueKey(pr github.Pull) string {
	return fmt.Sprintf("%d %s", pr.Number, pr.Head.SHA)
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
// entry. The READY time comes from the
// READY verdict mergewake recorded for the head (notify.go recordOnlyEvent). A
// draft hold does not count: while the PR is a draft no clock starts its
// countdown, and the pass that sees it leave draft closes the hold into Holds,
// where the ages are computed against it (openAge). The span is measured from
// the last pass that saw the PR open (LastOpen, noteOpenPass), so the draft time
// before the pass that first saw the draft is not counted as open. Entries
// untouched for a week are dropped.
func (s *Service) tickOverdueClocks(ctx context.Context, r config.Repo, pr github.Pull, v gateVerdict) (overdueClock, error) {
	file := s.overdueClocksFile(r.Name)
	defer lockPath(file)()
	st, e := readOverdueState(file)
	if e != nil {
		return overdueClock{}, e
	}
	now := s.now().UTC()
	key := overdueKey(pr)
	c, existed := st.Clocks[key]
	changed := !existed
	start := func(at *time.Time, due bool) {
		if due && at.IsZero() {
			*at, changed = now, true
		}
	}
	start(&c.HeadSeen, true)
	start(&c.ChecksDone, checksCompleted(v))
	start(&c.ReviewDone, reviewCompleted(v))
	if c.Ready.IsZero() {
		ready, ok, e := s.readySent(r.Name, pr)
		if e != nil {
			return overdueClock{}, e
		}
		if ok {
			c.Ready, changed = ready, true
		}
	}
	if pr.Draft {
		if c.DraftSince.IsZero() {
			// The hold may have started any time after the last pass that saw
			// the PR open: count it from there, not from this pass.
			c.DraftSince, changed = draftAnchor(c.LastOpen, now), true
		}
	} else {
		changed = closeHold(&c, now) || changed
	}
	if now.Sub(c.Touched) >= time.Hour {
		c.Touched, changed = now, true
	}
	if !changed {
		return c, nil
	}
	st.Clocks[key] = c
	pruneClocks(st.Clocks, now)
	return c, writeOverdueState(file, st)
}

// closeHold ends the draft hold the entry still carries at now: the span is
// recorded in Holds and DraftSince cleared, and nothing else moves. Both places
// that see the PR open call it, tickOverdueClocks and noteOpenPass, so a pass
// that errors later still ends the hold. It reports whether the entry changed.
func closeHold(c *overdueClock, now time.Time) bool {
	if c.DraftSince.IsZero() {
		return false
	}
	c.Holds = append(c.Holds, holdSpan{From: c.DraftSince, To: now})
	c.DraftSince = time.Time{}
	// Keep the list bounded: merge the two oldest spans. That counts the open
	// time between them as held, the conservative direction, and the entry is
	// pruned a week after its last touch anyway.
	for len(c.Holds) > overdueHoldSpans {
		c.Holds[1] = holdSpan{From: c.Holds[0].From, To: c.Holds[1].To}
		c.Holds = c.Holds[1:]
	}
	return true
}

// draftAnchor is where a draft hold that starts now is anchored: the last pass
// that saw the PR open, so draft time before the pass that first saw it is not
// counted as open. No last open (a file an earlier release wrote, or a head
// first seen as a draft): today's anchor, this pass.
func draftAnchor(lastOpen, now time.Time) time.Time {
	if lastOpen.IsZero() {
		return now
	}
	return lastOpen
}

// noteOpenPass records that this pass saw the open PR not a draft. notify.go
// calls it as soon as it has read the PR, before the reads that can fail: a
// pass that errors before the gate saw the PR open all the same, so a hold that
// starts after it is anchored here, not at an older pass, and a hold it ends is
// closed into Holds then. It does no fallible read of its own, so nothing can
// keep it from writing the sighting: the READY time is filled by
// tickOverdueClocks, raw, whenever a pass gets to read it.
func (s *Service) noteOpenPass(r config.Repo, pr github.Pull) error {
	file := s.overdueClocksFile(r.Name)
	defer lockPath(file)()
	st, e := readOverdueState(file)
	if e != nil {
		return e
	}
	now := s.now().UTC()
	key := overdueKey(pr)
	c := st.Clocks[key]
	c.LastOpen, c.Touched = now, now
	closeHold(&c, now)
	st.Clocks[key] = c
	pruneClocks(st.Clocks, now)
	return writeOverdueState(file, st)
}

// noteDraftHold records that the open PR is a draft now. The per-pass PR path
// returns before the gate for a draft, so this is the only place the hold is
// seen; the pass that sees the PR leave draft closes its span into Holds. The
// entry is written when the hold starts and then hourly, so an idle draft does
// not rewrite the file on every pass.
func (s *Service) noteDraftHold(r config.Repo, pr github.Pull) error {
	file := s.overdueClocksFile(r.Name)
	defer lockPath(file)()
	st, e := readOverdueState(file)
	if e != nil {
		return e
	}
	now := s.now().UTC()
	key := overdueKey(pr)
	c, existed := st.Clocks[key]
	if existed && !c.DraftSince.IsZero() && now.Sub(c.Touched) < time.Hour {
		return nil
	}
	if c.DraftSince.IsZero() {
		c.DraftSince = draftAnchor(c.LastOpen, now)
	}
	c.Touched = now
	st.Clocks[key] = c
	pruneClocks(st.Clocks, now)
	return writeOverdueState(file, st)
}

// readySent is when the READY verdict for this head was recorded, if it was.
func (s *Service) readySent(repo string, pr github.Pull) (time.Time, bool, error) {
	clocks, e := s.loadReadyClocks(repo)
	if e != nil {
		return time.Time{}, false, e
	}
	c, ok := clocks[verdictKey("ready", pr.Number, pr.Head.SHA)]
	if !ok || c.First.IsZero() {
		return time.Time{}, false, nil
	}
	return c.First, true, nil
}

// overdueHeld reports whether the verdict holds the head for a reason that is
// not a plain wait: an open critical/high finding, a failed, expired or absent
// check, a conflict or a stale base, and the other reasons that wait on the
// author (open threads, unanswered triage, a review blocked on a detail) or
// on the evidence to settle. Only a head with no reason, or with mergewake's
// own review_missing queue state, can be overdue. A pending verdict whose
// required checks are all done waits on GitHub's mergeability computation
// alone, which holds this evaluation; a pending verdict that waits on CI is
// not held, so ready_too_long still fires for a head whose checks were rerun
// after its READY. It is evaluated again on every pass.
func overdueHeld(v gateVerdict) bool {
	for _, reason := range v.Reasons {
		if reason == "review_missing" && v.Review.Block == "" {
			continue
		}
		return true
	}
	return v.State == "pending" && checksCompleted(v)
}

// openAge is how long the clock that started at start has counted at now: the
// time since start, less every draft hold it overlaps, the hold in progress
// included up to now. A clock that starts inside a hold counts from the hold's
// end; a hold that ends before the clock starts takes nothing off. Nothing here
// depends on the order the clocks were filled in or on which pass read what:
// the same raw times and hold spans always give the same age.
func openAge(c overdueClock, start, now time.Time) time.Duration {
	if start.IsZero() || !now.After(start) {
		return 0
	}
	held := time.Duration(0)
	take := func(from, to time.Time) {
		if from.Before(start) {
			from = start
		}
		if to.After(now) {
			to = now
		}
		if to.After(from) {
			held += to.Sub(from)
		}
	}
	for _, h := range c.Holds {
		take(h.From, h.To)
	}
	if !c.DraftSince.IsZero() {
		take(c.DraftSince, now)
	}
	if age := now.Sub(start) - held; age > 0 {
		return age
	}
	return 0
}

// overdueDue lists the conditions whose time has passed, with the age each is
// reported at. The age is open time (openAge), never the wall time since the
// clock started: a draft hold is not the author's wait.
func overdueDue(c overdueClock, started bool, now time.Time) (conditions []string, ages map[string]time.Duration) {
	ages = map[string]time.Duration{}
	add := func(name string, since time.Time, after time.Duration) {
		if since.IsZero() {
			return
		}
		if age := openAge(c, since, now); age >= after {
			conditions = append(conditions, name)
			ages[name] = age
		}
	}
	if !started {
		add(overdueReviewNotStarted, c.ChecksDone, overdueReviewStartAfter)
	}
	add(overdueNotLanded, c.ReviewDone, overdueLandAfter)
	add(overdueReadyTooLong, c.Ready, overdueReadyAfter)
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
	c, e := s.tickOverdueClocks(ctx, r, pr, v)
	if e != nil {
		return e
	}
	// A draft is a deliberate hold: no condition fires while it lasts.
	if pr.Draft {
		return nil
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
		errs = append(errs, s.sendOverdue(ctx, r, pr, v, c, sent, keyOf(condition), condition, ages[condition]))
	}
	return errors.Join(errs...)
}

// overdueText is the notice: the condition and age, then what the gate, the
// required checks and the review say about the head.
func (s *Service) overdueText(r config.Repo, pr github.Pull, v gateVerdict, c overdueClock, condition string, age time.Duration) string {
	gate := v.State
	if len(v.Reasons) > 0 {
		gate += " " + strings.Join(v.Reasons, "; ")
	}
	checks := "-"
	if len(v.Status.Checks) > 0 {
		parts := make([]string, 0, len(v.Status.Checks))
		for _, chk := range v.Status.Checks {
			parts = append(parts, chk.Name+"="+chk.State)
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
func (s *Service) sendOverdue(ctx context.Context, r config.Repo, pr github.Pull, v gateVerdict, c overdueClock, sent *sentKeys, key, condition string, age time.Duration) error {
	text := s.overdueText(r, pr, v, c, condition, age)
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
