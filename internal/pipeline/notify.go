package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

var laneName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

// NotifyPR acts on the merge-gate verdict for a PR head once it becomes ready
// or blocked: READY once every required check is green, no review thread is
// unresolved, the review -done marker exists, the PR is mergeable and, when
// GitHub requires an up-to-date branch, the base is an ancestor of the head;
// BLOCKED with the closed-vocabulary reasons that hold it. Pending checks do
// nothing. BLOCKED is sent as one lane event; READY is only recorded (its sent
// key, the READY clock ready_too_long counts from, and a pr-notify decision
// marked "record only"), because mergewake merges on its own gate and no lane
// has anything to do with it. Each (PR, head, verdict) is handled once across
// restarts. A READY is never repeated: there are no READY reminders. A merged
// or closed PR is never notified here.
//
// A conflict is told by its ESC (EscalateConflict), sent first, drafts
// included; the BLOCKED event carries only the verdict's other reasons and is
// not sent when the conflict is the whole verdict.
//
// The pr role notifies under shadow too — telling a lane is not merging — while
// dry-run only prints the decision and writes no keys or events.
func (s *Service) NotifyPR(ctx context.Context, r config.Repo, n int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "pr-notify-error", map[string]any{"pr": n, "error": integrations.Detail(retErr)}, o.DryRun))
		}
	}()
	if !s.Config.Daemon.PR.Notify.On() {
		return nil
	}
	ctx = WithAuthorScan(ctx)
	started := s.now()
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	// The ESC never holds up the verdict: its error is returned with the rest.
	escErr := s.EscalateConflict(ctx, r, pr, o)
	defer func() { retErr = errors.Join(retErr, escErr) }()
	if pr.State != "open" || pr.Draft || pr.Base.Ref != r.Branch {
		return nil
	}
	baseSHA, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, baseSHA)
	if e != nil {
		return e
	}
	v, e := s.mergeGate(ctx, r, pr, p, baseSHA)
	if e != nil {
		return e
	}
	if slices.Contains(v.Reasons, "round_hold") {
		if fb, fbErr := s.GH.ReviewFeedback(ctx, r.Name, n); fbErr == nil {
			if err := s.sendRound5Decision(ctx, r, pr, fb, v.Review.Rounds, o); err != nil && s.Log != nil {
				s.Log.Warn("round-5 decision notify failed", "pr", n, "error", err)
			}
		}
	}
	if !o.DryRun {
		s.recordStatus(r, pr, v, started)
	}
	record := struct {
		PR              int      `json:"pr"`
		Head            string   `json:"head"`
		State           string   `json:"state"`
		Reasons         []string `json:"reasons,omitempty"`
		ChecksCancelled []string `json:"checks_cancelled,omitempty"`
		ChecksSkipped   []string `json:"checks_skipped,omitempty"`
		ReviewBlock     string   `json:"review_block,omitempty"`
		BaseBehind      bool     `json:"base_behind,omitempty"`
		Lane            string   `json:"lane,omitempty"`
		Via             string   `json:"via,omitempty"`
		FailedHosts     []string `json:"failed_hosts,omitempty"`
		Sent            bool     `json:"sent"`
		Deduped         bool     `json:"deduped,omitempty"`
		Suppressed      string   `json:"suppressed,omitempty"`
		Error           string   `json:"error,omitempty"`
	}{PR: n, Head: pr.Head.SHA, State: v.State, Reasons: v.Reasons, ChecksCancelled: v.Review.ChecksCancelled, ChecksSkipped: v.Review.ChecksSkipped, ReviewBlock: v.Review.Block, BaseBehind: v.Review.BaseBehind}
	if !o.Reporting() {
		return s.audit(r.Name, "pr-notify", record, o.DryRun)
	}
	s.mergeOverdue(ctx, r, pr, p, v)
	if e := s.ageCheckFailures(ctx, r, pr, v.Reasons); e != nil {
		return e
	}
	if v.State == "pending" {
		return s.audit(r.Name, "pr-notify", record, false)
	}
	if o.Acting() && v.State == "blocked" && len(v.Reasons) == 1 && v.Reasons[0] == "base_stale" && p.AutoUpdateBranchEnabled() {
		status, attempted, err := s.updateStaleBranch(ctx, r, pr, baseSHA)
		if err != nil {
			return err
		}
		if status == "recheck" {
			return nil
		}
		if status == "ok" {
			if attempted {
				return s.audit(r.Name, "base_updated", map[string]any{"pr": n, "from": pr.Head.SHA}, false)
			}
			return nil
		}
		v.Reasons = append(v.Reasons, "update_failed:"+status)
		record.Reasons = v.Reasons
	}
	summary := pr.Title
	// identity is the summary as it was before the conflict ESC took the
	// conflict out of the BLOCKED text: the sent key and event id still carry
	// the conflict, so a verdict that gains or loses it is a new verdict.
	identity := ""
	conflicted := false
	if v.State == "blocked" {
		reasons, waiting := splitInternalWaits(v.Reasons, v.Review.Block)
		all := reasons
		// The conflict went out as its ESC; the BLOCKED carries the rest.
		conflicted = slices.Contains(reasons, "conflict")
		reasons = slices.DeleteFunc(slices.Clone(reasons), func(reason string) bool { return reason == "conflict" })
		if waiting {
			since, e := s.reviewWaitSince(r, pr)
			if e != nil {
				return e
			}
			if len(reasons) == 0 && !conflicted {
				// Only mergewake's own queue holds the head: nothing for any
				// session to do until the wait outlasts reviewStallAfter.
				lane, via, sent, e := s.reviewStalled(ctx, r, pr, v, since)
				if e != nil {
					if !errors.Is(e, errNotifyDelivery) {
						return e
					}
					record.Via, record.Error = "delivery-failed", e.Error()
					return s.audit(r.Name, "pr-notify", record, false)
				}
				if !sent {
					return nil
				}
				record.Reasons, record.Lane, record.Via, record.Sent = []string{"review_stalled"}, lane, via, true
				return s.audit(r.Name, "pr-notify", record, false)
			}
		}
		if conflicted && len(reasons) == 0 {
			record.Suppressed = "conflict ESC"
			return s.audit(r.Name, "pr-notify", record, false)
		}
		cancelled := make(map[string]bool, len(v.Review.ChecksCancelled))
		for _, name := range v.Review.ChecksCancelled {
			cancelled["check_cancelled:"+name] = true
		}
		blockedSummary := func(reasons []string) string {
			annotated := make([]string, 0, len(reasons))
			for _, reason := range reasons {
				if cancelled[reason] {
					reason += " (rerun it)"
				}
				annotated = append(annotated, reason)
			}
			out := strings.Join(annotated, "; ")
			if v.Review.Block != "" {
				out = strings.Replace(out, "review_missing", "review_missing ("+v.Review.Block+")", 1)
			}
			return out
		}
		summary, identity = blockedSummary(reasons), blockedSummary(all)
	}
	if n := len(v.Review.ChecksSkipped); v.State == "ready" && n > 0 {
		summary += fmt.Sprintf(" | %d required check(s) skipped: %s", n, strings.Join(v.Review.ChecksSkipped, ", "))
	}
	if n := v.Review.Findings.DismissedBlocking; v.State == "ready" && n > 0 {
		// A dismissed critical/high no longer holds the gate; the desk still
		// checks the dismissal's evidence before merging.
		summary += fmt.Sprintf(" | %d critical/high dismissed: check the evidence", n)
	}
	// A mixed verdict keeps the route its conflict gave it: the author's.
	author := v.State == "blocked" && (conflicted || authorAction(summary))
	if identity == "" {
		identity = summary
	}
	lane, via, sent, e := s.verdictEvent(ctx, r, pr, v.State, summary, identity, v.Review.BaseBehind, author)
	if e != nil {
		if !errors.Is(e, errNotifyDelivery) {
			return e
		}
		// The decision to stop retrying a failing delivery is logged once per
		// (PR, head, verdict): laneEvent dedupes further attempts.
		record.Via, record.Error = "delivery-failed", e.Error()
		return s.audit(r.Name, "pr-notify", record, false)
	}
	if lane == "" && via == "" {
		// A skip or failure decision for this verdict was already logged.
		return nil
	}
	record.Lane, record.Via, record.Sent, record.Deduped = lane, via, sent, !sent
	if len(authorScanFailures(ctx)) > 0 {
		record.FailedHosts = authorScanFailures(ctx)
	}
	return s.audit(r.Name, "pr-notify", record, false)
}

// updateStaleBranch reserves each PR/base pair before the network call. A
// unknown outcome is retried only after fresh PR, base and gate checks.
func (s *Service) updateStaleBranch(ctx context.Context, r config.Repo, pr github.Pull, baseSHA string) (string, bool, error) {
	dir := filepath.Join(s.state(r.Name), "base-updates")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", false, err
	}
	key := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", pr.Number, baseSHA)))
	path := filepath.Join(dir, hex.EncodeToString(key[:]))
	unlock, err := fsutil.Lock(path+".lock", true)
	if err != nil {
		return "", false, err
	}
	defer unlock()
	data, err := os.ReadFile(path)
	if err == nil && string(data) != "unknown" {
		return string(data), false, nil
	}
	if err == nil {
		fresh := github.FreshEvidence(ctx)
		current, e := s.GH.Pull(fresh, r.Name, pr.Number)
		if e != nil {
			return "", false, e
		}
		base, e := s.GH.Head(fresh, r.Name, r.Branch)
		if e != nil {
			return "", false, e
		}
		if current.State != "open" || current.Draft || current.Base.Ref != r.Branch || current.Head.SHA != pr.Head.SHA || base != baseSHA {
			return "recheck", false, nil
		}
		p, e := s.Policy(fresh, r, base)
		if e != nil {
			return "", false, e
		}
		if !p.AutoUpdateBranchEnabled() {
			return "recheck", false, nil
		}
		v, e := s.mergeGate(fresh, r, current, p, base)
		if e != nil {
			return "", false, e
		}
		if v.State != "blocked" || len(v.Reasons) != 1 || v.Reasons[0] != "base_stale" {
			return "recheck", false, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	if err = fsutil.Atomic(path, []byte("unknown"), 0600); err != nil {
		return "", false, err
	}
	status := "ok"
	if err = s.GH.UpdateBranch(ctx, r.Name, pr.Number, pr.Head.SHA); err != nil {
		status = "unknown"
		var api *github.APIError
		if errors.As(err, &api) {
			status = fmt.Sprint(api.Code)
		}
	} else if e := s.recordBranchUpdate(r.Name, pr.Number, pr.Head.SHA); e != nil {
		// The update happened; a lost record only restarts the overdue clocks
		// of the head it produces.
		_ = s.audit(r.Name, "base-update-record-error", map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "error": integrations.Detail(e)}, false)
	}
	if err = fsutil.Atomic(path, []byte(status), 0600); err != nil {
		_ = s.audit(r.Name, "base-update-marker-error", map[string]any{"pr": pr.Number, "error": integrations.Detail(err)}, false)
	}
	return status, true, nil
}

// errNotifyDelivery marks a lane event the notifier refused. The first
// failure is logged with the captured stderr and recorded so the loop does
// not retry — and re-log — the same verdict every pass.
var errNotifyDelivery = errors.New("notify delivery failed")

// laneEvent resolves the owning lane and emits one deduped lane event for the
// verdict: BLOCKED is sent, MERGED goes once to the PR's design lane (see
// mergedEvent), and READY is only recorded (below). The sent key covers the state, PR, head
// and summary so a changed reason set re-notifies while an identical verdict
// stays quiet. A PR with no owning lane routes to the configured fallback
// lane; a delivery the notifier fails is retried once on that lane, then
// parked with one logged decision per (PR, head, verdict) instead of failing
// every loop. An empty via means a decision was already logged and the
// caller stays silent.
//
// READY is never sent: mergewake merges on its own gate, so no lane has
// anything to do with it. It is recorded instead (see recordOnlyEvent), which
// keeps the sent key and the READY clock.
func (s *Service) laneEvent(ctx context.Context, r config.Repo, pr github.Pull, state, summary string) (string, string, bool, error) {
	return s.verdictEvent(ctx, r, pr, state, summary, summary, false, state == "blocked" && authorAction(summary))
}

// verdictEvent is laneEvent for a verdict whose gate result is known: a
// recorded READY carries baseBehind into its decision row. author routes a
// BLOCKED the author acts on through findingsRoute; NotifyPR decides it on
// the whole reason set, which may hold a conflict the summary leaves to the
// ESC. identity stands for summary in a BLOCKED's sent key and event id:
// the reason set with the conflict the text leaves out.
func (s *Service) verdictEvent(ctx context.Context, r config.Repo, pr github.Pull, state, summary, identity string, baseBehind, author bool) (string, string, bool, error) {
	var lane, via string
	var e error
	switch {
	case state == "ready" || state == "merged":
		// Recorded or sent below, once the sent key is known; each resolves its own lane.
	case state == "blocked" && author:
		route, err := s.findingsRoute(ctx, r, pr)
		if err != nil {
			return "", "", false, err
		}
		lane, via = route.lane, route.via
	default:
		lane, via, e = s.notifyLane(ctx, r, pr)
	}
	if e != nil {
		return "", "", false, e
	}
	textFor := func(l, v string) string {
		shown := l
		if shown == "" {
			shown = "-"
		}
		return fmt.Sprintf("[mergewake] %s %s#%d head=%s | %s | lane=%s via %s",
			strings.ToUpper(state), r.Name, pr.Number, short7(pr.Head.SHA), summary, shown, v)
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		return "", "", false, e
	}
	// The verdict key dedupes per (PR, head, verdict): a BLOCKED reason set is
	// part of the verdict, while a retitled READY is the same verdict.
	// readyClocksFile holds each READY under this key; the overdue notice
	// counts ready_too_long from it.
	key := verdictKey(state, pr.Number, pr.Head.SHA)
	if state == "blocked" {
		key += " " + identity
	}
	if sent.has(key) {
		return lane, via, false, nil
	}
	if state == "ready" {
		return s.recordOnlyEvent(ctx, r, pr, state, summary, key, baseBehind, sent)
	}
	if state == "merged" {
		return s.mergedEvent(ctx, r, pr, summary, key, sent)
	}
	if sent.has("decided " + key) {
		return "", "", false, nil
	}
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	// A changed BLOCKED reason set is a new verdict under the same head: fold
	// the summary into the event ID so a notifier deduping on ID cannot drop it
	// while the sent key records delivery.
	id := fmt.Sprintf("mergewake-%s-%s-%d-%s", state, r.Name, pr.Number, pr.Head.SHA)
	if state == "blocked" {
		sum := sha256.Sum256([]byte(identity))
		id += "-" + hex.EncodeToString(sum[:4])
	}
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   id,
		Text: textFor(lane, via),
		Lane: lane,
		Type: verdictType(state, via),
	}); e != nil {
		fallback := s.Config.Notifier.FallbackLane
		// An author-action BLOCKED never falls back to the desk: it is parked
		// with the decided key.
		if fallback != "" && fallback != lane && !(state == "blocked" && author) {
			// The resolved lane may not exist on this host; the fallback lane
			// gets one try under a distinct ID before the verdict is parked.
			if fb := s.Notifier.Notify(reportCtx, integrations.Event{
				ID:   id + "-fallback",
				Text: textFor(fallback, via+"+fallback"),
				Lane: fallback,
				Type: verdictType(state, via),
			}); fb == nil {
				if ae := sent.add(key); ae != nil {
					return "", "", false, ae
				}
				return fallback, via + "+fallback", true, nil
			}
		}
		if ae := sent.add("decided " + key); ae != nil {
			return "", "", false, errors.Join(e, ae)
		}
		return "", "", false, fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(e))
	}
	if e = sent.add(key); e != nil {
		return "", "", false, e
	}
	return lane, via, true, nil
}

// verdictKey is the sent and READY-clock key of one (PR, head, verdict).
func verdictKey(state string, n int, head string) string {
	return fmt.Sprintf("%s %d %s", state, n, head)
}

// workerLane resolves the PR author's worker lane: the job hk bound to its
// task, taken over by a live author pane when there is one, else the author
// pane's job, else for a finished author the lane the PR names explicitly
// (see findingsRoute and explicitLane). Empty when none gives a lane. A read
// error retries rather than reroutes.
func (s *Service) workerLane(ctx context.Context, r config.Repo, pr github.Pull) (string, string, error) {
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		return "", "", e
	}
	al, e := s.boundAuthorLane(ctx, r, pr, task)
	if e != nil {
		return "", "", e
	}
	if al.lane != "" {
		return al.lane, al.via, nil
	}
	lane, via, ok, e := s.authorPane(ctx, r, pr)
	if e != nil {
		return "", "", e
	}
	if ok {
		return lane, via, nil
	}
	if al.finished != "" {
		return s.explicitLane(ctx, r, pr, task, s.deskLane())
	}
	return "", "", nil
}

// recordOnlyEvent records a READY without sending it. The decision
// is recorded as if it had been sent — the sent key so it is not retried every
// pass, and the READY clock so ready_too_long still counts from the verdict —
// with one logged pr-notify row naming the author worker lane the event would
// have gone to, if any. The empty lane tells the caller the decision was
// already logged.
func (s *Service) recordOnlyEvent(ctx context.Context, r config.Repo, pr github.Pull, state, summary, key string, baseBehind bool, sent *sentKeys) (string, string, bool, error) {
	workerLane, workerVia, e := s.workerLane(ctx, r, pr)
	if e != nil {
		return "", "", false, e
	}
	if e = sent.add(key); e != nil {
		return "", "", false, e
	}
	if state == "ready" {
		if e = s.startReadyClock(r, key, workerLane); e != nil {
			return "", "", false, e
		}
	}
	record := struct {
		PR          int      `json:"pr"`
		Head        string   `json:"head"`
		State       string   `json:"state"`
		Summary     string   `json:"summary,omitempty"`
		BaseBehind  bool     `json:"base_behind,omitempty"`
		Lane        string   `json:"lane,omitempty"`
		Via         string   `json:"via,omitempty"`
		FailedHosts []string `json:"failed_hosts,omitempty"`
		Sent        bool     `json:"sent"`
		Suppressed  string   `json:"suppressed"`
	}{PR: pr.Number, Head: pr.Head.SHA, State: state, Summary: summary, BaseBehind: baseBehind, Lane: workerLane, Via: workerVia, FailedHosts: authorScanFailures(ctx), Suppressed: "record only"}
	if e = s.audit(r.Name, "pr-notify", record, false); e != nil {
		return "", "", false, e
	}
	return "", "", false, nil
}

// mergedRecord is the pr-notify row of one MERGED decision.
type mergedRecord struct {
	PR         int    `json:"pr"`
	Head       string `json:"head"`
	State      string `json:"state"`
	Summary    string `json:"summary,omitempty"`
	Lane       string `json:"lane,omitempty"`
	Via        string `json:"via,omitempty"`
	Sent       bool   `json:"sent"`
	Suppressed string `json:"suppressed,omitempty"`
}

// mergedEvent sends one MERGED per (PR, head) to the PR's design lane: a valid
// Mergewake-Notify-Lane trailer, else the repo lane. With neither, nothing is
// sent and the row says "no design lane"; the desk fallback never gets it. The
// sent key is added only after a successful send, so a failed send is returned
// (as errNotifyDelivery) and the next pass retries; the merge itself is never
// blocked by it.
func (s *Service) mergedEvent(ctx context.Context, r config.Repo, pr github.Pull, summary, key string, sent *sentKeys) (string, string, bool, error) {
	lane, via, e := s.mergedLane(ctx, r, pr)
	if e != nil {
		return "", "", false, e
	}
	record := mergedRecord{PR: pr.Number, Head: pr.Head.SHA, State: "merged", Summary: summary}
	if lane == "" {
		record.Suppressed = "no design lane"
		if e = sent.add(key); e != nil {
			return "", "", false, e
		}
		return "", "", false, s.audit(r.Name, "pr-notify", record, false)
	}
	merge := ""
	if pr.MergeCommitSHA != "" {
		merge = " merge=" + short7(pr.MergeCommitSHA)
	}
	text := fmt.Sprintf("[mergewake] MERGED %s#%d head=%s%s | %s | lane=%s via %s",
		r.Name, pr.Number, short7(pr.Head.SHA), merge, summary, lane, via)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-merged-%s-%d-%s", r.Name, pr.Number, pr.Head.SHA),
		Text: clip(text, findingsLaneText),
		Lane: lane,
		Type: verdictType("merged", via),
	}); e != nil {
		return "", "", false, fmt.Errorf("%w: %s", errNotifyDelivery, integrations.Detail(e))
	}
	if e = sent.add(key); e != nil {
		return "", "", false, e
	}
	record.Lane, record.Via, record.Sent = lane, via, true
	return lane, via, true, s.audit(r.Name, "pr-notify", record, false)
}

// mergedLane is the design lane a MERGED goes to: a valid Mergewake-Notify-Lane
// trailer (the PR body, then the head commit), else the repo's valid lane. Empty
// when neither names one.
func (s *Service) mergedLane(ctx context.Context, r config.Repo, pr github.Pull) (string, string, error) {
	notify, e := s.prTrailer(ctx, r, pr, "Mergewake-Notify-Lane", func(v string) bool { return laneName.MatchString(v) })
	if e != nil {
		return "", "", e
	}
	if laneName.MatchString(notify) {
		return notify, "notify-lane trailer", nil
	}
	if laneName.MatchString(r.Lane) {
		return r.Lane, "repo lane", nil
	}
	return "", "", nil
}

// eventType is the panewire notification type a verdict is declared with, so
// a BLOCKED stays unread on the receiving lane until it is acked. READY is
// recorded rather than sent, so its type is only named here.
func eventType(state string) string {
	switch state {
	case "ready", "blocked", "merged":
		return state
	}
	return ""
}

// readyClock is when a READY verdict was recorded, the worker lane it would
// have gone to, and a reminder count that no longer advances: no READY
// reminder is sent.
type readyClock struct {
	Lane    string    `json:"lane"`
	First   time.Time `json:"first"`
	Last    time.Time `json:"last"`
	Repeats int       `json:"repeats"`
}

func (s *Service) readyClocksFile(repo string) string {
	return filepath.Join(s.state(repo), "pr-ready-renotify.json")
}

func (s *Service) loadReadyClocks(repo string) (map[string]readyClock, error) {
	clocks := map[string]readyClock{}
	if found, e := fsutil.ReadJSON(s.readyClocksFile(repo), &clocks); found && e != nil {
		return nil, fmt.Errorf("%s: %w", s.readyClocksFile(repo), e)
	} else if e != nil {
		return nil, e
	}
	return clocks, nil
}

// saveReadyClocks keeps the clocks touched in the last week.
func (s *Service) saveReadyClocks(repo string, clocks map[string]readyClock) error {
	cutoff := s.now().Add(-7 * 24 * time.Hour)
	for k, c := range clocks {
		if c.Last.Before(cutoff) {
			delete(clocks, k)
		}
	}
	return fsutil.WriteJSON(s.readyClocksFile(repo), clocks)
}

// startReadyClock records the first READY for key; nothing is sent.
func (s *Service) startReadyClock(r config.Repo, key, lane string) error {
	clocks, e := s.loadReadyClocks(r.Name)
	if e != nil {
		return e
	}
	now := s.now().UTC()
	clocks[key] = readyClock{Lane: lane, First: now, Last: now}
	return s.saveReadyClocks(r.Name, clocks)
}

// notifyLane resolves the destination lane without guessing: the PR's author
// pane when its task names no bound job (see authorPane), the handoffkeep
// task's lane via a Mergewake-Task trailer (PR body, then head commit message),
// an explicit Mergewake-Notify-Lane trailer in the same order, then the
// configured notifier lane. The returned rule names the choosing source and is
// recorded in the event text; a sink read error retries rather than reroutes.
func (s *Service) notifyLane(ctx context.Context, r config.Repo, pr github.Pull) (string, string, error) {
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		return "", "", e
	}
	// A task with a bound job keeps the rules below; the author pane stands in
	// only where the task names no job, as for FINDINGS.
	if len(s.Config.Hosts) > 0 {
		bound, _, e := s.boundJobLane(ctx, task)
		if e != nil {
			return "", "", e
		}
		if bound == "" {
			lane, via, ok, e := s.authorPane(ctx, r, pr)
			if e != nil {
				return "", "", e
			}
			if ok {
				return lane, via, nil
			}
		}
	}
	if task != "" {
		if l, ok := s.Sink.(interface {
			Lane(context.Context, string) (string, error)
		}); ok {
			lane, e := l.Lane(ctx, task)
			if e != nil {
				return "", "", e
			}
			if laneName.MatchString(lane) {
				return lane, "task " + task, nil
			}
		}
	}
	notify, e := s.prTrailer(ctx, r, pr, "Mergewake-Notify-Lane", nil)
	if e != nil {
		return "", "", e
	}
	if laneName.MatchString(notify) {
		return notify, "notify-lane trailer", nil
	}
	if laneName.MatchString(r.Lane) {
		return r.Lane, "repo lane", nil
	}
	fallback := s.Config.Notifier.FallbackLane
	if fallback == "" {
		fallback = s.Config.Notifier.Lane
	}
	if fallback != "" {
		return fallback, "fallback", nil
	}
	return "", "no-lane", nil
}

// authorAction reports whether summary holds a reason the PR's author fixes:
// a conflict, findings, a failed, cancelled or absent check, or a review blocked on a
// detail only the author can add (NotifyPR renders it "review_missing (detail)").
func authorAction(summary string) bool {
	for _, reason := range strings.Split(summary, "; ") {
		if reason == "conflict" || strings.HasPrefix(reason, "review_missing (") {
			return true
		}
		for _, prefix := range []string{"blocking_findings:", "finding_outside_diff:", "triage_unanswered:", "check_failed:", "check_cancelled:", "check_absent:"} {
			if strings.HasPrefix(reason, prefix) {
				return true
			}
		}
	}
	return false
}

// ownerLane is the repo's design-session (owner) lane: this repo's
// human-judgment notices and holds go here. An empty repo lane falls back to
// the desk (notifier.fallback_lane, else notifier.lane).
func (s *Service) ownerLane(r config.Repo) string {
	if laneName.MatchString(r.Lane) {
		return r.Lane
	}
	desk := s.Config.Notifier.FallbackLane
	if desk == "" {
		desk = s.Config.Notifier.Lane
	}
	return desk
}

// isDeskLane reports whether lane is the desk lane (notifier.fallback_lane, else
// notifier.lane). An empty lane goes to the notifier's own default, the desk.
func (s *Service) isDeskLane(lane string) bool {
	desk := s.Config.Notifier.FallbackLane
	if desk == "" {
		desk = s.Config.Notifier.Lane
	}
	return lane == "" || lane == desk
}

// verdictType is the panewire type a lane event is declared with. BLOCKED
// interrupts and must be acked. A BLOCKED never reaches here for mergewake's
// own review wait: NotifyPR sends that only as the review-stalled alert.
func verdictType(state, via string) string {
	if state == botNoticeState {
		return "decision"
	}
	if state == "blocked" && via == "unknown author; desk" {
		return "decision"
	}
	return eventType(state)
}
