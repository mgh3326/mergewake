package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// fixWindow is how long a red main has to go green again by a fix before watch
// may merge the exact revert of the confirmed culprit.
const fixWindow = 30 * time.Minute

// confirmRuns is how many independent any-ref results a commit needs before it
// counts as 2/2: the recorded result, when one exists, plus re-runs. A window
// alone never reverts; a flaky result never blames a commit.
const confirmRuns = 2

// recoveryIncident is one repository's red-main incident: when main first
// turned red with an identified culprit, the confirmation evidence, and the
// revert PR watch opened for it. It is persisted per repo, so a restart keeps
// the same window and evidence.
type recoveryIncident struct {
	RedSince  time.Time `json:"red_since"`
	Culprit   string    `json:"culprit"`
	CulpritPR int       `json:"culprit_pr"`
	Head      string    `json:"head"`
	Baseline  string    `json:"baseline,omitempty"`
	Task      string    `json:"task,omitempty"`
	// Runs is the confirmation evidence: per commit, each observed outcome
	// (true pass, false fail). The recorded first result is included.
	Runs map[string][]bool `json:"runs,omitempty"`
	// Pending maps a commit watch dispatched any-ref CI for to the dispatch
	// time; only statuses posted after it count as that run's result.
	Pending map[string]time.Time `json:"pending,omitempty"`
	// Dispatches counts the re-runs per commit, bounding them at confirmRuns.
	Dispatches map[string]int `json:"dispatches,omitempty"`
	Confirmed  bool           `json:"confirmed,omitempty"`
	// Flaky records a mixed result: the incident then holds and blames nobody.
	Flaky     string `json:"flaky,omitempty"`
	RevertPR  int    `json:"revert_pr,omitempty"`
	RevertURL string `json:"revert_url,omitempty"`
	FixPR     int    `json:"fix_pr,omitempty"`
}

// revertCloser closes an open pull request without merging it.
type revertCloser interface {
	ClosePull(context.Context, string, int) error
}

// treeReader reads one path's entry from a commit tree. Revert verification
// compares the culprit's pre- and post-images with what the revert restores.
type treeReader interface {
	Entry(context.Context, string, string, string) (github.TreeEntry, error)
}

// statusReader reads the newest commit status a check name has on a sha.
type statusReader interface {
	LatestStatus(context.Context, string, string, string) (github.CI, time.Time, bool, error)
}

// openLister lists the open pull requests of a repository.
type openLister interface {
	OpenPulls(context.Context, string, string) ([]github.Pull, error)
}

var (
	_ recoveryLatchGH = (*github.Client)(nil)
	_ revertCloser    = (*github.Client)(nil)
	_ treeReader      = (*github.Client)(nil)
	_ statusReader    = (*github.Client)(nil)
	_ openLister      = (*github.Client)(nil)
)

// designLane is the repository's design-session (owner) lane: repos[].lane
// when set, else the desk (notifier.fallback_lane, else notifier.lane).
func (s *Service) designLane(r config.Repo) string {
	if laneName.MatchString(r.Lane) {
		return r.Lane
	}
	desk := s.Config.Notifier.FallbackLane
	if desk == "" {
		desk = s.Config.Notifier.Lane
	}
	return desk
}

func (s *Service) recoveryFile(repo string) string {
	return filepath.Join(s.state(repo), "recovery.json")
}

func (s *Service) loadRecovery(repo string) (recoveryIncident, bool, error) {
	var st recoveryIncident
	found, e := fsutil.ReadJSON(s.recoveryFile(repo), &st)
	return st, found, e
}

func (s *Service) saveRecovery(repo string, st recoveryIncident) error {
	return fsutil.WriteJSON(s.recoveryFile(repo), st)
}

func (s *Service) clearRecoveryState(repo string) error {
	var errs []error
	for _, file := range []string{s.recoveryFile(repo), s.urgentFile(repo)} {
		if e := os.Remove(file); e != nil && !errors.Is(e, os.ErrNotExist) {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}

// urgentFile names the PRs watch named as the fix for a red main. The review
// queue reads it through ReviewPriority, so the fix is reviewed first through
// the existing priority mechanism.
func (s *Service) urgentFile(repo string) string {
	return filepath.Join(s.state(repo), "recovery-urgent.json")
}

func (s *Service) markUrgent(repo string, pr int) error {
	urgent := map[int]bool{}
	if found, e := fsutil.ReadJSON(s.urgentFile(repo), &urgent); e != nil {
		return e
	} else if !found {
		urgent = map[int]bool{}
	}
	urgent[pr] = true
	return fsutil.WriteJSON(s.urgentFile(repo), urgent)
}

func (s *Service) urgentFix(repo string, pr int) bool {
	urgent := map[int]bool{}
	found, e := fsutil.ReadJSON(s.urgentFile(repo), &urgent)
	return found && e == nil && urgent[pr]
}

// beginRecovery records the incident's first red observation and its recorded
// evidence. The same culprit keeps its red_since and evidence; a different
// culprit starts a new incident. culpritCI is the recorded state of the
// culprit on the bisect checks; known is false when that read was unknown.
func (s *Service) beginRecovery(r config.Repo, culprit string, culpritPR int, head, baseline, task string, culpritCI github.CI, known, persist bool) (recoveryIncident, error) {
	st, found, e := s.loadRecovery(r.Name)
	if e != nil {
		return st, e
	}
	save := func(st recoveryIncident) (recoveryIncident, error) {
		if !persist {
			return st, nil
		}
		return st, s.saveRecovery(r.Name, st)
	}
	if found && st.Culprit == culprit {
		st.Head = head
		if st.Baseline == "" {
			st.Baseline = baseline
		}
		if st.Task == "" {
			st.Task = task
		}
		if st.CulpritPR == 0 {
			st.CulpritPR = culpritPR
		}
		return save(st)
	}
	st = recoveryIncident{
		RedSince: s.now().UTC(), Culprit: culprit, CulpritPR: culpritPR,
		Head: head, Baseline: baseline, Task: task,
		Runs: map[string][]bool{}, Pending: map[string]time.Time{}, Dispatches: map[string]int{},
	}
	if baseline != "" {
		st.Runs[baseline] = []bool{true}
	}
	switch {
	case known && culpritCI == github.Red:
		st.Runs[culprit] = []bool{false}
	case known && culpritCI == github.Green:
		st.Runs[culprit] = []bool{true}
	}
	return save(st)
}

// recordRecoveryRevert binds the revert PR watch opened to the incident.
func (s *Service) recordRecoveryRevert(repo, url string, pr int) error {
	st, found, e := s.loadRecovery(repo)
	if e != nil || !found {
		return e
	}
	st.RevertPR, st.RevertURL = pr, url
	return s.saveRecovery(repo, st)
}

// pullNumber reads the PR number out of a pull request URL.
func pullNumber(url string) int {
	n, _ := strconv.Atoi(url[strings.LastIndex(url, "/")+1:])
	return n
}

// culpritFromRevertBody reads the culprit commit and PR an automatic revert's
// body names. It is the fallback when the incident state was lost: the body is
// written by RevertWithTrailers as "Reverts <sha> from PR #<n>.".
func culpritFromRevertBody(body string) (string, int) {
	fields := strings.Fields(body)
	if len(fields) < 2 || fields[0] != "Reverts" {
		return "", 0
	}
	culprit := fields[1]
	for i, f := range fields {
		if f == "PR" && i+1 < len(fields) {
			n, _ := strconv.Atoi(strings.TrimPrefix(strings.TrimRight(fields[i+1], "."), "#"))
			return culprit, n
		}
	}
	return culprit, 0
}

// confirmRecovery advances the failure evidence: each needed commit is re-run
// through the repo's any-ref CI until it has confirmRuns results. It reports
// whether the incident is confirmed and, when it is not, a reason and whether
// that reason needs the desk (a flaky result, a timed-out re-run or a reached
// bound) rather than a quiet wait for the next poll.
func (s *Service) confirmRecovery(ctx context.Context, r config.Repo, p policy.Policy, st recoveryIncident, acting, persist bool) (recoveryIncident, bool, string, bool, error) {
	save := func(st recoveryIncident) error {
		if !persist {
			return nil
		}
		return s.saveRecovery(r.Name, st)
	}
	if st.Flaky != "" {
		return st, false, st.Flaky, true, nil
	}
	if st.Confirmed {
		return st, true, "", false, nil
	}
	if st.Baseline == "" {
		return st, false, "confirmation needs a green baseline", true, nil
	}
	reader, ok := s.GH.(statusReader)
	if !ok {
		return st, false, "GitHub client cannot read any-ref statuses", true, nil
	}
	names := p.BisectChecks()
	hold := func(st recoveryIncident, reason string) (recoveryIncident, bool, string, bool, error) {
		return st, false, reason, true, save(st)
	}
	for sha, at := range st.Pending {
		state, ready, e := s.rerunOutcome(ctx, reader, r, sha, names, at)
		if e != nil {
			return st, false, "", false, e
		}
		if !ready {
			if s.now().Sub(at) >= p.AnyRefWait() {
				delete(st.Pending, sha)
				return hold(st, "any-ref re-run timed out on "+short7(sha))
			}
			continue
		}
		delete(st.Pending, sha)
		st.Runs[sha] = append(st.Runs[sha], state == github.Green)
	}
	for _, v := range st.Runs[st.Baseline] {
		if !v {
			st.Flaky = "the green baseline failed a re-run"
			return hold(st, st.Flaky)
		}
	}
	for _, v := range st.Runs[st.Culprit] {
		if v {
			st.Flaky = "the culprit passed a re-run"
			return hold(st, st.Flaky)
		}
	}
	if len(st.Runs[st.Baseline]) >= confirmRuns && len(st.Runs[st.Culprit]) >= confirmRuns {
		st.Confirmed = true
		return st, true, "", false, save(st)
	}
	for _, sha := range []string{st.Baseline, st.Culprit} {
		if len(st.Runs[sha]) >= confirmRuns {
			continue
		}
		if _, pending := st.Pending[sha]; pending {
			continue
		}
		if st.Dispatches[sha] >= confirmRuns {
			return hold(st, "confirmation bound reached on "+short7(sha))
		}
		if !acting {
			return st, false, "would re-run any-ref CI on " + short7(sha), false, nil
		}
		dispatcher, ok := s.GH.(workflowDispatcher)
		if !ok {
			return hold(st, "GitHub client cannot dispatch workflows")
		}
		if p.AnyRefWorkflow == "" {
			return hold(st, "no any_ref_workflow to confirm the failure")
		}
		if st.Pending == nil {
			st.Pending = map[string]time.Time{}
		}
		if st.Dispatches == nil {
			st.Dispatches = map[string]int{}
		}
		st.Pending[sha] = s.now().UTC()
		st.Dispatches[sha]++
		if e := save(st); e != nil {
			return st, false, "", false, e
		}
		if e := dispatcher.DispatchWorkflow(ctx, r.Name, p.AnyRefWorkflow, r.Branch, s.confirmInputs(ctx, r, p, st, sha)); e != nil {
			if github.DispatchRefused(e) {
				delete(st.Pending, sha)
				return hold(st, "any-ref CI dispatch refused for "+short7(sha)+": "+e.Error())
			}
			return st, false, "", false, e
		}
	}
	if e := save(st); e != nil {
		return st, false, "", false, e
	}
	return st, false, fmt.Sprintf("confirming the failure: %d/%d at %s, %d/%d at %s",
		len(st.Runs[st.Baseline]), confirmRuns, short7(st.Baseline),
		len(st.Runs[st.Culprit]), confirmRuns, short7(st.Culprit)), false, nil
}

// confirmInputs is the dispatch payload for a confirmation re-run: the commit
// always, and the culprit's changed Go packages when the repo's any-ref
// workflow accepts a `packages` input, so the re-run isolates the failing
// packages instead of repeating the whole suite.
func (s *Service) confirmInputs(ctx context.Context, r config.Repo, p policy.Policy, st recoveryIncident, sha string) map[string]string {
	inputs := map[string]string{"ref": sha}
	if st.CulpritPR == 0 {
		return inputs
	}
	workflow, e := s.GH.ReadFile(ctx, r.Name, sha, ".github/workflows/"+p.AnyRefWorkflow)
	if e != nil || !strings.Contains(string(workflow), "packages:") {
		return inputs
	}
	files, e := s.GH.Files(ctx, r.Name, st.CulpritPR)
	if e != nil {
		return inputs
	}
	dirs := map[string]bool{}
	for _, f := range files {
		if !strings.HasSuffix(f.Path, ".go") || f.Kind == "removed" {
			continue
		}
		dir := strings.TrimSuffix(filepath.Dir(f.Path), "/")
		if dir == "." || dir == "" {
			dirs["./"] = true
			continue
		}
		dirs["./"+dir+"/"] = true
	}
	if len(dirs) == 0 {
		return inputs
	}
	patterns := make([]string, 0, len(dirs))
	for dir := range dirs {
		patterns = append(patterns, dir)
	}
	sort.Strings(patterns)
	inputs["packages"] = strings.Join(patterns, " ")
	return inputs
}

// rerunOutcome reads the any-ref statuses a dispatch posted after at. ready is
// false until every check has a status newer than the dispatch.
func (s *Service) rerunOutcome(ctx context.Context, reader statusReader, r config.Repo, sha string, names []string, at time.Time) (github.CI, bool, error) {
	outcome := github.Green
	for _, name := range names {
		state, seenAt, ok, e := reader.LatestStatus(ctx, r.Name, sha, name)
		if e != nil {
			return "", false, e
		}
		if !ok || !seenAt.After(at) {
			return "", false, nil
		}
		switch state {
		case github.Green:
		case github.Red:
			outcome = github.Red
		default:
			return "", false, nil
		}
	}
	return outcome, true, nil
}

// fixPR finds the open PR that names this red as its fix through a
// `Fixes-Red-Main: <sha>` line in its body or head commit message.
func (s *Service) fixPR(ctx context.Context, r config.Repo, st recoveryIncident) (*github.Pull, error) {
	lister, ok := s.GH.(openLister)
	if !ok {
		return nil, nil
	}
	prs, e := lister.OpenPulls(ctx, r.Name, r.Branch)
	if e != nil {
		return nil, e
	}
	for i := range prs {
		pr := &prs[i]
		if pr.Number == st.RevertPR {
			continue
		}
		if namesFix(pr.Body, st) {
			return pr, nil
		}
		c, e := s.GH.Commit(ctx, r.Name, pr.Head.SHA)
		if e != nil {
			continue
		}
		if namesFix(c.Commit.Message, st) {
			return pr, nil
		}
	}
	return nil, nil
}

// namesFix reports whether text names the red through a Fixes-Red-Main line.
func namesFix(text string, st recoveryIncident) bool {
	for _, line := range strings.Split(text, "\n") {
		v, ok := strings.CutPrefix(strings.TrimSpace(line), "Fixes-Red-Main:")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		for _, sha := range []string{st.Culprit, st.Head} {
			if sha != "" && (v == sha || len(v) >= 7 && strings.HasPrefix(sha, v)) {
				return true
			}
		}
	}
	return false
}

// recovery handles a red main while a recovery latch is active. It resolves the
// revert PR the latch belongs to, waits out the fix window and an open fix PR,
// and merges the exact revert only when every condition holds. A condition that
// forbids an automatic merge holds and tells the design lane once. A latch that
// is not an open revert watch can reason about keeps its old operator hold.
func (s *Service) recovery(ctx context.Context, r config.Repo, p policy.Policy, head string, checks []string, result watchResult, o Options) error {
	latch, ok := s.GH.(recoveryLatchGH)
	if !ok {
		result.Action = "operator-required-recovery-latch"
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	pr, e := latch.RecoveryRevert(ctx, r.Name)
	if e != nil {
		return e
	}
	if pr == nil || pr.MergedAt != nil || pr.State != "open" {
		result.Action = "operator-required-recovery-latch"
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	acting := !result.Shadow && !o.DryRun
	st, found, e := s.loadRecovery(r.Name)
	if e != nil {
		return e
	}
	if !found || st.RevertPR != pr.Number {
		// The revert exists but watch has no incident for it: a latch a person
		// created, or a lost state file. Start the window now rather than merge
		// a revert whose culprit watch cannot attest.
		culprit, culpritPR := culpritFromRevertBody(pr.Body)
		st = recoveryIncident{
			RedSince: s.now().UTC(), Culprit: culprit, CulpritPR: culpritPR, Head: head,
			RevertPR: pr.Number, RevertURL: pr.HTMLURL,
			Runs: map[string][]bool{}, Pending: map[string]time.Time{}, Dispatches: map[string]int{},
		}
		st.Confirmed = false
		if acting {
			if e = s.saveRecovery(r.Name, st); e != nil {
				return e
			}
		}
	}
	if !st.Confirmed {
		var confirmed, needsDesk bool
		var reason string
		var ce error
		st, confirmed, reason, needsDesk, ce = s.confirmRecovery(ctx, r, p, st, acting, acting)
		if ce != nil {
			return ce
		}
		if !confirmed {
			result.Action = "recovery-confirming"
			result.Reason = reason
			if needsDesk {
				return s.reportWatch(ctx, r, result, "recovery:"+strconv.Itoa(pr.Number)+":confirm:"+reason, o)
			}
			return s.audit(r.Name, "watch", result, o.DryRun)
		}
	}
	if remaining := fixWindow - s.now().Sub(st.RedSince); remaining > 0 {
		result.Action = "recovery-fix-window"
		result.Reason = "fix window: " + remaining.Round(time.Minute).String() + " remaining"
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if fix, e := s.fixPR(ctx, r, st); e != nil {
		return e
	} else if fix != nil && s.now().Sub(st.RedSince) < 2*fixWindow {
		if st.FixPR != fix.Number {
			st.FixPR = fix.Number
			if acting {
				if e = s.saveRecovery(r.Name, st); e != nil {
					return e
				}
				if e = s.markUrgent(r.Name, fix.Number); e != nil {
					return e
				}
			}
			priority := s.ReviewPriority(ctx, r, *fix)
			row := struct {
				PR       int    `json:"pr"`
				Head     string `json:"head"`
				Priority int    `json:"priority"`
				Source   string `json:"priority_source"`
			}{fix.Number, fix.Head.SHA, priority.Value, priority.Source}
			if e = s.audit(r.Name, "recovery-fix-pr", row, o.DryRun); e != nil {
				return e
			}
		}
		result.Action = "recovery-fix-pr"
		result.Reason = fmt.Sprintf("fix PR #%d is open; the revert waits", fix.Number)
		return s.reportWatch(ctx, r, result, "recovery-fix:"+strconv.Itoa(pr.Number)+":"+strconv.Itoa(fix.Number), o)
	}
	if result.Shadow || o.DryRun {
		result.Action = "would-merge-revert"
		result.Reason = "fix window elapsed and the failure is confirmed"
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if reason := s.revertMergeBlocker(ctx, r, checks, head, *pr, st); reason != "" {
		result.Action = "recovery-hold"
		result.Reason = reason
		return s.reportWatch(ctx, r, result, "recovery:"+strconv.Itoa(pr.Number)+":"+reason, o)
	}
	return s.mergeRevert(ctx, r, head, *pr, st, result, o)
}

// revertMergeBlocker names the first condition that forbids merging the revert,
// or "" when the merge may proceed. It re-reads the evidence a merge depends on.
func (s *Service) revertMergeBlocker(ctx context.Context, r config.Repo, checks []string, head string, pr github.Pull, st recoveryIncident) string {
	if st.Culprit == "" || st.CulpritPR == 0 || st.Baseline == "" {
		return "recovery state cannot attest the culprit"
	}
	if !st.Confirmed {
		return "the failure is not confirmed 2/2"
	}
	descends, e := s.GH.Ancestor(ctx, r.Name, st.Culprit, head)
	if e != nil {
		return "ancestry check failed: " + integrations.Detail(e)
	}
	if !descends {
		return "main head is not a descendant of the culprit"
	}
	if pr.Mergeable != nil && !*pr.Mergeable || pr.MergeableState == "dirty" {
		return "revert PR has conflicts"
	}
	ci, e := s.GH.Check(ctx, r.Name, pr.Head.SHA, checks)
	if e != nil {
		return "revert CI check failed: " + integrations.Detail(e)
	}
	if ci != github.Green {
		return "revert PR CI is " + string(ci)
	}
	culprit, e := s.GH.Commit(ctx, r.Name, st.Culprit)
	if e != nil {
		return "culprit commit unreadable: " + integrations.Detail(e)
	}
	if len(culprit.Parents) != 1 {
		return "automatic revert requires a single-parent commit"
	}
	parent, e := s.GH.Commit(ctx, r.Name, culprit.Parents[0].SHA)
	if e != nil {
		return "culprit parent unreadable: " + integrations.Detail(e)
	}
	files, e := s.GH.Files(ctx, r.Name, st.CulpritPR)
	if e != nil {
		return "culprit files unreadable: " + integrations.Detail(e)
	}
	revertFiles, e := s.GH.Files(ctx, r.Name, pr.Number)
	if e != nil {
		return "revert files unreadable: " + integrations.Detail(e)
	}
	if e = s.exactRevert(ctx, r, culprit, parent, pr.Head.SHA, head, files, revertFiles); e != nil {
		return e.Error()
	}
	return ""
}

// exactRevert proves the revert's diff is the reverse of the culprit's: both
// touch exactly the same paths, main still holds the culprit's post-image on
// each, and the revert head holds the culprit parent's pre-image.
func (s *Service) exactRevert(ctx context.Context, r config.Repo, culprit, parent github.Commit, revertHead, head string, culpritFiles, revertFiles []policy.File) error {
	trees, ok := s.GH.(treeReader)
	if !ok {
		return errors.New("GitHub client cannot verify the revert's tree")
	}
	paths := diffPaths(culpritFiles)
	if !samePaths(paths, diffPaths(revertFiles)) {
		return errors.New("revert does not touch exactly the culprit's paths")
	}
	red, e := s.GH.Commit(ctx, r.Name, head)
	if e != nil {
		return errors.New("main head unreadable: " + integrations.Detail(e))
	}
	revert, e := s.GH.Commit(ctx, r.Name, revertHead)
	if e != nil {
		return errors.New("revert head unreadable: " + integrations.Detail(e))
	}
	for _, path := range sortedKeys(paths) {
		pre, e := trees.Entry(ctx, r.Name, parent.Commit.Tree.SHA, path)
		if e != nil {
			return errors.New("culprit parent tree unreadable: " + integrations.Detail(e))
		}
		post, e := trees.Entry(ctx, r.Name, culprit.Commit.Tree.SHA, path)
		if e != nil {
			return errors.New("culprit tree unreadable: " + integrations.Detail(e))
		}
		now, e := trees.Entry(ctx, r.Name, red.Commit.Tree.SHA, path)
		if e != nil {
			return errors.New("main tree unreadable: " + integrations.Detail(e))
		}
		back, e := trees.Entry(ctx, r.Name, revert.Commit.Tree.SHA, path)
		if e != nil {
			return errors.New("revert tree unreadable: " + integrations.Detail(e))
		}
		if !sameTreeEntry(now, post) {
			return fmt.Errorf("main changed %s after the culprit; not an exact revert", path)
		}
		if !sameTreeEntry(back, pre) {
			return fmt.Errorf("revert does not restore %s to its pre-culprit state", path)
		}
	}
	return nil
}

func diffPaths(files []policy.File) map[string]bool {
	paths := map[string]bool{}
	for _, f := range files {
		for _, p := range []string{f.Path, f.Previous} {
			if p != "" {
				paths[p] = true
			}
		}
	}
	return paths
}

func samePaths(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for p := range a {
		if !b[p] {
			return false
		}
	}
	return true
}

func sameTreeEntry(a, b github.TreeEntry) bool {
	return a.Mode == b.Mode && a.Type == b.Type && a.SHA == b.SHA
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// mergeRevert merges the revert PR at its current head once the evidence has
// not changed, records the outcome and tells the design lane once.
func (s *Service) mergeRevert(ctx context.Context, r config.Repo, head string, pr github.Pull, st recoveryIncident, result watchResult, o Options) error {
	fresh, e := s.GH.Pull(ctx, r.Name, pr.Number)
	if e != nil {
		return e
	}
	if fresh.State != "open" || fresh.MergedAt != nil || fresh.Head.SHA != pr.Head.SHA {
		result.Action = "recovery-hold"
		result.Reason = "revert PR changed during recovery"
		return s.reportWatch(ctx, r, result, "recovery:"+strconv.Itoa(pr.Number)+":changed", o)
	}
	main, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	if main != head {
		result.Action = "recovery-hold"
		result.Reason = "main changed during recovery"
		return s.reportWatch(ctx, r, result, "recovery:"+strconv.Itoa(pr.Number)+":main-moved", o)
	}
	if e = s.GH.Merge(ctx, r.Name, pr.Number, pr.Head.SHA); e != nil {
		return e
	}
	result.Action = "recovery-merged"
	row := struct {
		Culprit   string    `json:"culprit"`
		CulpritPR int       `json:"culprit_pr"`
		RevertPR  int       `json:"revert_pr"`
		RedSince  time.Time `json:"red_since"`
		Head      string    `json:"head"`
	}{Culprit: st.Culprit, CulpritPR: st.CulpritPR, RevertPR: pr.Number, RedSince: st.RedSince, Head: head}
	if e = s.audit(r.Name, "watch", result, o.DryRun); e != nil {
		return e
	}
	if e = s.audit(r.Name, "recovery-merged", row, false); e != nil {
		return e
	}
	return s.announceRevert(ctx, r, st, pr.Number)
}

// announceRevert sends the one design-lane line for a merged revert.
func (s *Service) announceRevert(ctx context.Context, r config.Repo, st recoveryIncident, revertPR int) error {
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "watch-reported"), 127)
	if e != nil {
		return e
	}
	key := "recovery-merged:" + st.Culprit + ":" + strconv.Itoa(revertPR)
	if sent.has(key) {
		return nil
	}
	task := st.Task
	if task == "" {
		task = "-"
	}
	window := strconv.Itoa(int(fixWindow/time.Minute)) + "m"
	text := fmt.Sprintf("%s main red since %s by PR #%d; reverted by PR #%d after %s; task %s reopened",
		r.Name, st.RedSince.UTC().Format(time.RFC3339), st.CulpritPR, revertPR, window, task)
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-recovery-merged-%s-%d", r.Name, revertPR),
		Text: text,
		Lane: s.designLane(r),
		Type: "info",
	}); e != nil {
		return e
	}
	return sent.add(key)
}

// fixWon runs on a green main: a revert PR the latch belongs to that never
// merged is no longer needed. It closes the PR with a comment, clears the latch
// and records the outcome once.
func (s *Service) fixWon(ctx context.Context, r config.Repo, head string) error {
	active, e := s.GH.RecoveryActive(ctx, r.Name)
	if e != nil || !active {
		return e
	}
	latch, ok := s.GH.(recoveryLatchGH)
	if !ok {
		return nil
	}
	pr, e := latch.RecoveryRevert(ctx, r.Name)
	if e != nil {
		return e
	}
	if pr == nil {
		st, found, e := s.loadRecovery(r.Name)
		if e != nil || !found || st.RevertPR == 0 {
			return e
		}
		open, e := s.GH.Pull(ctx, r.Name, st.RevertPR)
		if e != nil {
			return e
		}
		pr = &open
	}
	if pr.MergedAt != nil || pr.Merged {
		return nil
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "watch-reported"), 127)
	if e != nil {
		return e
	}
	outcome := "recovery-fix-won"
	if !sent.has(outcome) {
		culprit, culpritPR := culpritFromRevertBody(pr.Body)
		reportCtx, cancel := reportContext(ctx)
		defer cancel()
		if pr.State == "open" {
			comment := "main is green again at " + head + "; revert not needed"
			if e = s.GH.PostIssueComment(reportCtx, r.Name, pr.Number, comment); e != nil {
				return e
			}
			closer, ok := s.GH.(revertCloser)
			if !ok {
				return nil
			}
			if e = closer.ClosePull(reportCtx, r.Name, pr.Number); e != nil {
				return e
			}
		}
		row := struct {
			Culprit   string `json:"culprit,omitempty"`
			CulpritPR int    `json:"culprit_pr,omitempty"`
			RevertPR  int    `json:"revert_pr"`
			Head      string `json:"head"`
		}{Culprit: culprit, CulpritPR: culpritPR, RevertPR: pr.Number, Head: head}
		if e = s.audit(r.Name, outcome, row, false); e != nil {
			return e
		}
		if e = sent.add(outcome); e != nil {
			return e
		}
	}
	return latch.ClearRecovery(ctx, r.Name)
}
