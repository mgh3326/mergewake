package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

type watchResult struct {
	Head       string `json:"head"`
	Stage      string `json:"stage,omitempty"`
	Action     string `json:"action"`
	Shadow     bool   `json:"shadow"`
	Culprit    string `json:"culprit,omitempty"`
	PR         int    `json:"pr,omitempty"`
	URL        string `json:"url,omitempty"`
	Candidates []int  `json:"candidates,omitempty"`
	Reason     string `json:"reason,omitempty"`
	// AnyRefDispatch is the commit whose any-ref CI this pass dispatched, or
	// would dispatch in shadow and dry runs.
	AnyRefDispatch string `json:"any_ref_dispatch,omitempty"`
}

type watchHoldEpisode struct {
	ID         string          `json:"id"`
	Notified   bool            `json:"notified"`
	Suppressed map[string]bool `json:"suppressed,omitempty"`
}

// UnknownEvidenceError names the bracket commit whose CI evidence Bisect
// needed but could not read as green or red.
type UnknownEvidenceError struct {
	Index int
	SHA   string
}

func (e *UnknownEvidenceError) Error() string {
	return "bisect has missing or pending CI; operator or any-ref CI required"
}

// Bisect returns the first red candidate following a known green baseline.
// Missing/pending evidence stops the search rather than assigning blame; the
// error is then an *UnknownEvidenceError naming the commit.
func Bisect(shas []string, check func(string) (github.CI, error)) (int, error) {
	if len(shas) < 2 {
		return 0, errors.New("bisect needs a green baseline and red head")
	}
	lo, hi := 0, len(shas)-1
	state, e := check(shas[lo])
	if e != nil {
		return 0, e
	}
	if state != github.Green {
		return 0, errors.New("bisect baseline is not green")
	}
	state, e = check(shas[hi])
	if e != nil {
		return 0, e
	}
	if state != github.Red {
		return 0, errors.New("bisect head is not red")
	}
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		state, e = check(shas[mid])
		if e != nil {
			return 0, e
		}
		switch state {
		case github.Green:
			lo = mid
		case github.Red:
			hi = mid
		default:
			return 0, &UnknownEvidenceError{Index: mid, SHA: shas[mid]}
		}
	}
	return hi, nil
}
func (s *Service) Watch(ctx context.Context, r config.Repo, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "watch-error", map[string]any{"error": retErr.Error()}, o.DryRun))
		}
	}()
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return e
	}
	main := watchStream{Service: s, suites: p.DeployStage(), mainHead: head}
	if p.DeployStage() {
		deploy := watchStream{Service: s, deploy: true}
		if p.Enabled && !o.Shadow && !o.DryRun {
			if err := deploy.finishMerged(ctx, r, head); err != nil {
				return err
			}
			st, found, err := deploy.loadRecovery(r.Name)
			if err != nil {
				return err
			}
			if found && st.Yielding {
				if _, err := deploy.yieldLatch(ctx, r); err != nil {
					return err
				}
			}
		}
		owned, err := deploy.ownsLatch(ctx, r)
		if err != nil {
			return err
		}
		main.otherLatch = owned
		main.blocked = owned
		if owned {
			ci, err := s.GH.Check(ctx, r.Name, head, p.WatchSet())
			// An ambiguous main read is not known red: it neither yields the
			// deploy latch nor fails the pass, and the incident decides it.
			if err != nil && !errors.Is(err, github.ErrAmbiguousCheck) {
				return err
			}
			if ci == github.Red && p.Enabled && !o.Shadow && !o.DryRun {
				yielded, err := deploy.yieldLatch(ctx, r)
				if err != nil {
					return err
				}
				if yielded {
					main.otherLatch = false
					main.blocked = false
				}
			}
		}
	}
	if e = main.watch(ctx, r, p, head, o); e != nil {
		return e
	}
	if p.DeployStage() {
		_, recorded, err := s.loadRecovery(r.Name)
		if err != nil {
			return err
		}
		ci, err := s.GH.Check(ctx, r.Name, head, p.WatchSet())
		// An ambiguous main read is not known green: deploy waits for main
		// instead of failing the pass on it.
		if err != nil && !errors.Is(err, github.ErrAmbiguousCheck) {
			return err
		}
		probe := watchStream{Service: s, deploy: true}
		owned, err := probe.ownsLatch(ctx, r)
		if err != nil {
			return err
		}
		active, err := s.GH.RecoveryActive(ctx, r.Name)
		if err != nil {
			return err
		}
		otherLatch := active && !owned
		deploy := watchStream{Service: s, deploy: true, suites: true, mainHead: head, mainChecks: p.WatchSet(), otherLatch: otherLatch, blocked: recorded || ci != github.Green || otherLatch}
		return deploy.watchDeploy(ctx, r, p, head, o)
	}
	return nil
}

func (s *watchStream) watch(ctx context.Context, r config.Repo, p policy.Policy, head string, o Options) error {

	checks := p.WatchSet()
	bisectChecks := p.BisectChecks()
	// With any_ref_deploy_checks the deploy stream's candidates are judged by
	// the any-ref deploy run, which reports the deploy stage as a whole under
	// names the real deploy run never posts. The head is judged by the deploy
	// set alone, so the head-versus-candidate subset guard below cannot apply.
	deployAnyRef := s.deploy && len(p.AnyRefChecks) > 0
	if !s.deploy {
		s.ciPolicy(ctx, r, head, o)
	}
	result := watchResult{Head: head, Action: "waiting-for-ci", Shadow: !p.Enabled || o.Shadow}
	if s.deploy {
		result.Stage = "deploy"
	}
	inc, recorded, e := s.loadRecovery(r.Name)
	if e != nil {
		return e
	}
	// When the head is the incident's culprit, a passed re-run posts a status
	// beside the head's own failed run. The incident still gathering evidence
	// owns that verdict: resolving it records the mixed result instead of
	// failing on the ambiguous read or clearing the incident as healthy. A
	// confirmed incident owns it too, but its verdict is settled: the mixed
	// result is decided once and the automatic revert stops.
	mixedHead := recorded && !inc.Confirmed && inc.Baseline != "" && inc.Culprit == head
	confirmedHead := recorded && inc.Confirmed && inc.Baseline != "" && inc.Culprit == head
	ci, e := s.GH.Check(ctx, r.Name, head, checks)
	if e != nil {
		if errors.Is(e, github.ErrAmbiguousCheck) {
			if confirmedHead {
				return s.settleConfirmedHead(ctx, r, result, o, inc)
			}
			if mixedHead {
				return s.recoverHeadCulprit(ctx, r, p, head, checks, result, o, inc)
			}
		}
		return e
	}
	checkpoint := s.checkpoint(r.Name)
	if ci == github.Green {
		if !s.deploy && !o.DryRun {
			if e = s.clearWatchHoldEpisode(r.Name); e != nil {
				return e
			}
		}
		// A same-sha re-run can pass beside the head's own failed run. Record
		// the mixed result once, then close the incident as healthy: a green
		// main needs no revert, and a stale record would block a later red.
		if mixedHead {
			settled, e := s.settleMixedHead(ctx, r, p, result, o, inc)
			if e != nil {
				return e
			}
			if !settled {
				return nil
			}
		}
		result.Action = "healthy"
		if !o.DryRun {
			if e = fsutil.Atomic(checkpoint, []byte(head), 0600); e != nil {
				return e
			}
			if e = s.endAnyRef(r.Name); e != nil {
				return e
			}
			if !result.Shadow {
				if !s.otherLatch {
					if e = s.fixWon(ctx, r, head); e != nil {
						return e
					}
					if e = s.clearRecoveryLatch(ctx, r, head); e != nil {
						return e
					}
				}
				if e = s.clearRecoveryState(r.Name); e != nil {
					return e
				}
			}
		}
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if ci != github.Red {
		// A status-only re-run can leave the aggregate pending through a sibling
		// check; the record settles the incident without waiting on that check.
		if mixedHead {
			return s.recoverHeadCulprit(ctx, r, p, head, checks, result, o, inc)
		}
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	s.feedBranchCI(ctx, r, head, checks, o)
	active, e := s.GH.RecoveryActive(ctx, r.Name)
	if e != nil {
		return e
	}
	if active && !s.blocked {
		return s.recovery(ctx, r, p, head, checks, result, o)
	}
	// A persisted incident outranks a fresh bisect: its confirmation evidence
	// is applied first, so a re-run's status cannot move its culprit or green
	// boundary and a mixed result is recorded instead of blamed on a new commit.
	// A green commit after the recorded head means main was fixed and broke
	// again, so that red is diagnosed afresh instead of reverting the old one.
	if recorded && inc.Culprit != "" && inc.Baseline != "" {
		current, e := s.incidentCurrent(ctx, r, p, head, inc)
		if e != nil {
			return e
		}
		if current {
			return s.recoverIncident(ctx, r, p, head, checks, result, o, inc)
		}
		if !result.Shadow && !o.DryRun {
			if e = s.clearRecoveryState(r.Name); e != nil {
				return e
			}
		}
	}
	baselineBytes, e := os.ReadFile(checkpoint)
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	baseline := string(baselineBytes)
	chain := []string{head}
	green := ""
	// holdAs ends the pass with a hold. A hold that needs the desk is reported
	// once per head and reason; a quiet hold (waiting for any-ref CI) is only
	// audited and fed.
	holdAs := func(reason string, notify bool) error {
		candidates, e := s.bracketCandidates(ctx, r, chain, green)
		if e != nil {
			return e
		}
		result.Candidates = candidates
		result.Action = "hold"
		result.Reason = reason
		if notify {
			return s.reportWatch(ctx, r, result, head+":"+reason, o)
		}
		if e = s.audit(r.Name, "watch", result, o.DryRun); e != nil {
			return e
		}
		if !result.Shadow && !o.DryRun {
			s.feedWatch(r, result, o)
		}
		return nil
	}
	hold := func(reason string) error { return holdAs(reason, true) }
	// With any_ref_workflow set, unknown evidence is resolved by dispatching
	// the any-ref CI for the commit bisect needs, not by holding for the
	// operator.
	anyRef := p.AnyRefWorkflow != ""
	cache := map[string]github.CI{head: ci}
	current := head
	found := false
	check := func(sha string) (github.CI, error) {
		if v, ok := cache[sha]; ok {
			return v, nil
		}
		v, e := s.candidateCI(ctx, r.Name, sha, p)
		if e == nil {
			cache[sha] = v
		}
		return v, e
	}
	// A head that is red only on checks outside any_ref_checks cannot be
	// bisected: no commit can be shown green or red on them. Never blame a
	// commit on partial evidence.
	if len(bisectChecks) < len(checks) && !deployAnyRef {
		v, e := s.GH.Check(ctx, r.Name, head, bisectChecks)
		if e != nil {
			return e
		}
		if v != github.Red {
			outside, e := s.redOutside(ctx, r.Name, head, checks, bisectChecks)
			if e != nil {
				return e
			}
			return hold("red only on checks any-ref CI does not report: " + strings.Join(outside, ", "))
		}
	}
	// Only first parents walk mainline: a merge commit's second parent is the
	// merged branch, and the merge commit itself is the integration unit.
	for len(chain) < s.Config.WatchLimit {
		c, e := s.GH.Commit(ctx, r.Name, current)
		if e != nil {
			return e
		}
		if len(c.Parents) == 0 {
			return hold("reached history root without a green ancestor")
		}
		current = c.Parents[0].SHA
		chain = append(chain, current)
		if baseline != "" {
			if current == baseline {
				found = true
				break
			}
		} else {
			v, e := check(current)
			if e != nil {
				return e
			}
			if v == github.Green {
				green = current
				found = true
				break
			}
		}
	}
	if !found {
		return hold("no green ancestor within watch_limit")
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	// A later green commit supersedes an older checkpoint. Inspect the interval
	// before bisecting so an independent later regression cannot blame an older
	// PR. Unknown evidence (missing, pending, cancelled or skipped runs) holds
	// the search rather than assigning blame.
	for i := len(chain) - 2; i >= 0; i-- {
		v, e := check(chain[i])
		if e != nil {
			return e
		}
		if v == github.Green {
			green = chain[i]
			chain = chain[i:]
			break
		}
		if v != github.Red {
			if anyRef {
				// Bisect asks for the evidence it needs; the rest stays unknown.
				continue
			}
			return hold("incomplete CI evidence in recovery interval")
		}
	}
	index, e := Bisect(chain, check)
	var unknown *UnknownEvidenceError
	if anyRef && errors.As(e, &unknown) {
		reason, notify, e := s.anyRef(ctx, r, p, head, unknown.SHA, len(chain)-1, !result.Shadow && !o.DryRun, &result)
		if e != nil {
			return e
		}
		return holdAs(reason, notify)
	}
	if e != nil {
		return hold("CI evidence inside the bracket is ambiguous: " + e.Error())
	}
	culprit := chain[index]
	// The confirmed boundary must be the first parent of the first red commit,
	// even when that commit is a merge commit.
	parent, e := s.GH.Commit(ctx, r.Name, culprit)
	if e != nil {
		return e
	}
	if len(parent.Parents) == 0 || parent.Parents[0].SHA != chain[index-1] {
		return hold("culprit parent mismatch")
	}
	pulls, e := s.GH.PRsForCommit(ctx, r.Name, culprit)
	if e != nil {
		return e
	}
	// A candidate is a merged PR whose recorded landing SHA sits inside the
	// red bracket: a squash commit maps directly, a merge commit through its
	// merge SHA, and a rebased chain through its final landing commit.
	inBracket := map[string]bool{}
	for _, sha := range chain[index:] {
		inBracket[sha] = true
	}
	var origin *github.Pull
	seen := map[int]bool{}
	for i := range pulls {
		v := &pulls[i]
		if !(v.Merged || v.MergedAt != nil) || v.Base.Ref != r.Branch || !inBracket[v.MergeCommitSHA] || seen[v.Number] {
			continue
		}
		seen[v.Number] = true
		result.Candidates = append(result.Candidates, v.Number)
		if origin == nil {
			origin = v
		}
	}
	if len(result.Candidates) != 1 || origin == nil {
		result.Culprit = culprit
		return hold("culprit is not a single identifiable merged PR")
	}
	if e = s.ObservePR(ctx, r, *origin, o); e != nil {
		return e
	}
	result.Culprit = culprit
	result.PR = origin.Number
	// A culprit that touched a blocking path is reported, never auto-reverted.
	files, e := s.GH.Files(ctx, r.Name, origin.Number)
	if e != nil {
		return e
	}
	if len(files) != origin.ChangedFiles {
		return hold("culprit file list is incomplete")
	}
	if p.Classify(files).Class == "blocking" {
		result.Action = "report-culprit"
		return s.reportWatch(ctx, r, result, head+":"+culprit, o)
	}
	if len(parent.Parents) != 1 {
		return hold("automatic revert requires a single-parent commit; merge-commit culprit needs a first-parent recovery change")
	}
	task := r.Task
	originTask, e := s.prTask(ctx, r, *origin)
	if e != nil {
		return e
	}
	if originTask != "" {
		task = originTask
	}
	if s.Config.Sink.Type == "handoffkeep" && !integrations.TaskID.MatchString(task) {
		return errors.New("origin task missing; add Mergewake-Task trailer or repo task")
	}
	culpritCI, culpritKnown := github.CI(""), false
	if v, ce := check(culprit); ce == nil && (v == github.Green || v == github.Red) {
		culpritCI, culpritKnown = v, true
	}
	persist := !result.Shadow && !o.DryRun
	st, e := s.beginRecovery(r, culprit, origin.Number, head, green, task, strings.Join(bisectChecks, ","), culpritCI, culpritKnown, persist)
	if e != nil {
		return e
	}
	if st.RevertPR != 0 {
		result.Action = "recovery-hold"
		result.Reason = fmt.Sprintf("revert PR #%d exists without a recovery latch", st.RevertPR)
		return s.reportWatch(ctx, r, result, "recovery:"+strconv.Itoa(st.RevertPR)+":unlatched", o)
	}
	st, confirmed, reason, needsDesk, e := s.confirmRecovery(ctx, r, p, st, persist, persist)
	if e != nil {
		return e
	}
	if !confirmed {
		result.Action = "recovery-confirming"
		result.Reason = reason
		if needsDesk {
			return s.reportWatch(ctx, r, result, "recovery:"+culprit+":confirm:"+reason, o)
		}
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if s.blocked {
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	return s.openRevert(ctx, r, head, checks, culprit, *origin, task, result, o)
}

// openRevert opens the exact-revert PR for a confirmed incident. It re-reads
// the head and its CI first, so a pass that lost a race does not open a PR for
// a main that already moved or went green.
func (s *watchStream) openRevert(ctx context.Context, r config.Repo, head string, checks []string, culprit string, origin github.Pull, task string, result watchResult, o Options) error {
	result.Action = "open-revert-pr"
	if e := s.audit(r.Name, "watch", result, o.DryRun); e != nil {
		return e
	}
	if result.Shadow || o.DryRun {
		return nil
	}
	fresh, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	if fresh != s.targetHead(head) {
		return errors.New("main changed during diagnosis; rerun watch")
	}
	if s.deploy {
		mainCI, err := s.GH.Check(github.FreshEvidence(ctx), r.Name, fresh, s.mainChecks)
		// An ambiguous main read is not known green, so deploy recovery waits
		// rather than failing the pass on it.
		if err != nil && !errors.Is(err, github.ErrAmbiguousCheck) {
			return err
		}
		if mainCI != github.Green {
			return nil
		}
	}
	ci, e := s.GH.Check(github.FreshEvidence(ctx), r.Name, head, checks)
	if e != nil {
		// A same-sha re-run can land between the diagnosis and this read. When
		// it did, the culprit head carries a pass beside its failed run, which
		// a confirmed incident decides once instead of failing the pass on it.
		inc, found, le := s.loadRecovery(r.Name)
		if le != nil {
			return le
		}
		if found && inc.Confirmed && inc.Culprit == head && errors.Is(e, github.ErrAmbiguousCheck) {
			return s.settleConfirmedHead(ctx, r, result, o, inc)
		}
		return e
	}
	if ci != github.Red {
		return errors.New("main CI changed during diagnosis")
	}
	trailers, e := s.recoveryTrailers(ctx, r, origin)
	if e != nil {
		return e
	}
	reverter, ok := s.GH.(interface {
		RevertWithTrailers(context.Context, string, string, string, string, int, string) (string, error)
	})
	if !ok {
		return errors.New("GitHub client does not support recovery trailers")
	}
	result.URL, e = reverter.RevertWithTrailers(ctx, r.Name, r.Branch, fresh, culprit, origin.Number, trailers)
	if e != nil {
		return e
	}
	if e = s.recordRevert(r.Name, origin.Number, result.URL); e != nil {
		return e
	}
	if e = s.recordRecoveryRevert(r.Name, result.URL, pullNumber(result.URL)); e != nil {
		return e
	}
	result.Action = "opened-revert-pr"
	s.feedWatch(r, result, o)
	data, _ := json.Marshal(result)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	stage := "main"
	if s.deploy {
		stage = "deploy"
	}
	note := fmt.Sprintf("mergewake: %s CI failed after PR #%d; revert PR %s", stage, origin.Number, result.URL)
	var reopen error
	outcome := "none"
	if task != "" {
		outcome = "reopened"
		if reopen = s.Sink.Reopen(reportCtx, task, note); reopen != nil {
			outcome = "failed"
		}
	}
	if reopen != nil {
		note += "; task reopen failed (recovery comment retained): " + reopen.Error()
	}
	// The outcome is stored before anything announces it: the merged notice
	// reads it later, on a pass that never saw the reopen.
	recorded := s.recordTaskReopen(r.Name, outcome)
	return errors.Join(s.audit(r.Name, "watch", result, false), reopen, recorded, s.Sink.Record(reportCtx, "mergewake/recovery/"+culprit, data), s.Notifier.Notify(reportCtx, integrations.Event{ID: "mergewake-revert-" + culprit, Type: "culprit-hold", Text: note}))
}

// redOutside names the red checks of sha that are in checks but not in
// inside, in checks order.
func (s *Service) redOutside(ctx context.Context, repo, sha string, checks, inside []string) ([]string, error) {
	in := map[string]bool{}
	for _, n := range inside {
		in[n] = true
	}
	var red []string
	for _, n := range checks {
		if in[n] {
			continue
		}
		v, e := s.GH.Check(ctx, repo, sha, []string{n})
		if e != nil {
			return nil, e
		}
		if v == github.Red {
			red = append(red, n)
		}
	}
	return red, nil
}

// reportWatch audits every watch outcome, sends one notice per main-hold
// episode, and records each held head once. Repeat polls keep the episode
// quiet while retaining an audit trail.
func (s *Service) reportWatch(ctx context.Context, r config.Repo, result watchResult, key string, o Options) error {
	if e := s.audit(r.Name, "watch", result, o.DryRun); e != nil {
		return e
	}
	if result.Shadow || o.DryRun {
		return nil
	}
	s.feedWatch(r, result, o)
	isMainHold := result.Action == "hold" && result.Stage == ""
	var episode watchHoldEpisode
	if isMainHold {
		var err error
		episode, err = s.readWatchHoldEpisode(r.Name)
		if err != nil {
			return err
		}
		if episode.ID != "" && episode.Notified {
			key := result.Head + " " + result.Reason
			if err = s.audit(r.Name, "watch-hold-suppressed", map[string]any{"head": result.Head, "reason": result.Reason, "episode": episode.ID}, false); err != nil {
				return err
			}
			if episode.Suppressed[key] {
				return nil
			}
			data, _ := json.Marshal(result)
			reportCtx, cancel := reportContext(ctx)
			defer cancel()
			if err = s.Sink.Record(reportCtx, "mergewake/"+result.Action+"/"+result.Head, data); err != nil {
				return err
			}
			episode.Suppressed[key] = true
			return s.writeWatchHoldEpisode(r.Name, episode)
		}
		if episode.ID == "" {
			episode.ID = fmt.Sprintf("%d-%s", s.now().UTC().UnixNano(), short7(result.Head))
			episode.Suppressed = map[string]bool{}
			if err = s.writeWatchHoldEpisode(r.Name, episode); err != nil {
				return err
			}
		}
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "watch-reported"), 127)
	if e != nil {
		return e
	}
	if sent.has(key) && !isMainHold {
		return nil
	}
	data, _ := json.Marshal(result)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	stage := ""
	if result.Stage != "" {
		stage = result.Stage + " "
	}
	note := "mergewake: " + stage + "watch " + result.Action + " on " + r.Name + " " + result.Head
	if result.Reason != "" {
		note += ": " + result.Reason
	}
	eventID := "mergewake-" + result.Action + "-" + result.Head
	if isMainHold {
		eventID = "mergewake-watch-hold-" + episode.ID
	}
	e = errors.Join(s.Sink.Record(reportCtx, "mergewake/"+result.Action+"/"+result.Head, data),
		s.Notifier.Notify(reportCtx, integrations.Event{ID: eventID, Type: "watch-hold", Text: note, Lane: s.ownerLane(r)}))
	if e != nil {
		return e
	}
	if e = sent.add(key); e != nil {
		return e
	}
	if isMainHold {
		episode.Notified = true
		episode.Suppressed[result.Head+" "+result.Reason] = true
		return s.writeWatchHoldEpisode(r.Name, episode)
	}
	return nil
}

func (s *Service) readWatchHoldEpisode(repo string) (watchHoldEpisode, error) {
	var episode watchHoldEpisode
	file := filepath.Join(s.state(repo), "watch-hold-active.json")
	if found, err := fsutil.ReadJSON(file, &episode); found && err != nil {
		return episode, err
	} else if err != nil {
		return episode, err
	}
	if episode.Suppressed == nil {
		episode.Suppressed = map[string]bool{}
	}
	return episode, nil
}

func (s *Service) writeWatchHoldEpisode(repo string, episode watchHoldEpisode) error {
	return fsutil.WriteJSON(filepath.Join(s.state(repo), "watch-hold-active.json"), episode)
}

func (s *Service) clearWatchHoldEpisode(repo string) error {
	file := filepath.Join(s.state(repo), "watch-hold-active.json")
	if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// bracketCandidates lists merged PRs in the inspected interval, excluding a
// confirmed-green boundary. It works before CI identifies one culprit.
func (s *Service) bracketCandidates(ctx context.Context, r config.Repo, chain []string, green string) ([]int, error) {
	inBracket := map[string]bool{}
	for _, sha := range chain {
		inBracket[sha] = true
	}
	delete(inBracket, green)

	seen := map[int]bool{}
	for _, sha := range chain {
		if !inBracket[sha] {
			continue
		}
		pulls, err := s.GH.PRsForCommit(ctx, r.Name, sha)
		if err != nil {
			return nil, err
		}
		for _, pr := range pulls {
			if (pr.Merged || pr.MergedAt != nil) && pr.Base.Ref == r.Branch && inBracket[pr.MergeCommitSHA] {
				seen[pr.Number] = true
			}
		}
	}
	out := []int{}
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out, nil
}

// recoveryTrailers copies the culprit's PR-body values, falling back to its head commit.
func (s *Service) recoveryTrailers(ctx context.Context, r config.Repo, pr github.Pull) (string, error) {
	var lines []string
	for _, t := range []struct {
		key   string
		valid func(string) bool
	}{
		{"Mergewake-Task", integrations.TaskID.MatchString},
		{"Mergewake-Grade", config.ValidGrade},
		{"Mergewake-Tier", validTier},
		{"Mergewake-Notify-Lane", nil},
	} {
		value, err := s.prTrailer(ctx, r, pr, t.key, t.valid)
		if err != nil {
			return "", err
		}
		if value != "" {
			lines = append(lines, t.key+": "+value)
		}
	}
	return strings.Join(lines, "\n"), nil
}
