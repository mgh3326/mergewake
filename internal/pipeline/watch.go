package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type watchResult struct {
	Head       string `json:"head"`
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
	checks := p.RequiredChecks
	if len(p.WatchChecks) > 0 {
		checks = p.WatchChecks
	}
	s.ciPolicy(ctx, r, head, o)
	ci, e := s.GH.Check(ctx, r.Name, head, checks)
	if e != nil {
		return e
	}
	result := watchResult{Head: head, Action: "waiting-for-ci", Shadow: !p.Enabled || o.Shadow}
	checkpoint := filepath.Join(s.state(r.Name), "last-green")
	if ci == github.Green {
		result.Action = "healthy"
		if !o.DryRun {
			if e = fsutil.Atomic(checkpoint, []byte(head), 0600); e != nil {
				return e
			}
			if e = s.endAnyRef(r.Name); e != nil {
				return e
			}
			if !result.Shadow {
				if e = s.clearRecoveryLatch(ctx, r, head); e != nil {
					return e
				}
			}
		}
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if ci != github.Red {
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	s.feedBranchCI(ctx, r, head, checks, o)
	active, e := s.GH.RecoveryActive(ctx, r.Name)
	if e != nil {
		return e
	}
	if active {
		result.Action = "operator-required-recovery-latch"
		return s.audit(r.Name, "watch", result, o.DryRun)
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
		v, e := s.GH.Check(ctx, r.Name, sha, checks)
		if e == nil {
			cache[sha] = v
		}
		return v, e
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
	result.Action = "open-revert-pr"
	if e = s.audit(r.Name, "watch", result, o.DryRun); e != nil {
		return e
	}
	if result.Shadow || o.DryRun {
		return nil
	}
	fresh, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	if fresh != head {
		return errors.New("main changed during diagnosis; rerun watch")
	}
	ci, e = s.GH.Check(ctx, r.Name, head, checks)
	if e != nil {
		return e
	}
	if ci != github.Red {
		return errors.New("main CI changed during diagnosis")
	}
	trailers, e := s.recoveryTrailers(ctx, r, *origin)
	if e != nil {
		return e
	}
	reverter, ok := s.GH.(interface {
		RevertWithTrailers(context.Context, string, string, string, string, int, string) (string, error)
	})
	if !ok {
		return errors.New("GitHub client does not support recovery trailers")
	}
	result.URL, e = reverter.RevertWithTrailers(ctx, r.Name, r.Branch, head, culprit, origin.Number, trailers)
	if e != nil {
		return e
	}
	if e = s.recordRevert(r.Name, origin.Number, result.URL); e != nil {
		return e
	}
	result.Action = "opened-revert-pr"
	s.feedWatch(r, result, o)
	data, _ := json.Marshal(result)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	note := fmt.Sprintf("mergewake: main CI failed after PR #%d; revert PR %s", origin.Number, result.URL)
	var reopen error
	if task != "" {
		reopen = s.Sink.Reopen(reportCtx, task, note)
	}
	if reopen != nil {
		note += "; task reopen failed (recovery comment retained): " + reopen.Error()
	}
	return errors.Join(s.audit(r.Name, "watch", result, false), reopen, s.Sink.Record(reportCtx, "mergewake/recovery/"+culprit, data), s.Notifier.Notify(reportCtx, integrations.Event{ID: "mergewake-revert-" + culprit, Type: "culprit-hold", Text: note}))
}

// reportWatch audits a held or reported watch outcome once per head and key,
// then notifies and records when the host is acting. Repeat polls on the same
// head with the same key stay quiet.
func (s *Service) reportWatch(ctx context.Context, r config.Repo, result watchResult, key string, o Options) error {
	if e := s.audit(r.Name, "watch", result, o.DryRun); e != nil {
		return e
	}
	if result.Shadow || o.DryRun {
		return nil
	}
	s.feedWatch(r, result, o)
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "watch-reported"), 127)
	if e != nil {
		return e
	}
	if sent.has(key) {
		return nil
	}
	data, _ := json.Marshal(result)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	note := "mergewake: watch " + result.Action + " on " + r.Name + " " + result.Head
	if result.Reason != "" {
		note += ": " + result.Reason
	}
	e = errors.Join(s.Sink.Record(reportCtx, "mergewake/"+result.Action+"/"+result.Head, data),
		s.Notifier.Notify(reportCtx, integrations.Event{ID: "mergewake-" + result.Action + "-" + result.Head, Type: "watch-hold", Text: note}))
	if e != nil {
		return e
	}
	return sent.add(key)
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
