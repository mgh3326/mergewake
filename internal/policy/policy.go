// Package policy classifies changes by whether harm can precede a revert.
package policy

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"time"
)

type File struct {
	Path     string `json:"filename"`
	Previous string `json:"previous_filename"`
	Kind     string `json:"status"`
	// Additions and Deletions are GitHub's per-file line counts; the fast lane
	// checks them against the PR totals before trusting either.
	Additions int `json:"additions"`
	Deletions int `json:"deletions"`
}
type Rule struct {
	Paths  []string `yaml:"paths"`
	Kinds  []string `yaml:"kinds"`
	Reason string   `yaml:"reason"`
}

// ReviewFinding is one review thread folded into the land decision. Severity
// and authorship are normalized by the GitHub adapter; Answered means a non-author human
// reply exists on the thread. Rechecked binds a reviewer recheck to the current head.
type ReviewFinding struct {
	Author           string `json:"author"`
	Severity         string `json:"severity"`
	Path             string `json:"path"`
	Line             int    `json:"line"`
	Resolved         bool   `json:"resolved"`
	Outdated         bool   `json:"outdated"`
	Answered         bool   `json:"answered"`
	ReviewerResolved bool   `json:"reviewer_resolved"`
	Rechecked        bool   `json:"rechecked"`
	OwnReview        bool   `json:"own_review"`
	CommitSHA        string `json:"commit_sha"`
}

// ReviewGateRule grades findings by normalized author and severity; classes
// limits the rule to that PR classification (empty means all classes).
type ReviewGateRule struct {
	Author   string   `yaml:"author"`
	Severity []string `yaml:"severity"`
	Classes  []string `yaml:"classes"`
}

// ReviewGate replaces the raw unresolved-thread count with graded enforcement
// at the land gate when land is "graded".
// An absent section uses the default graded rules and requires no CI bots.
type ReviewGate struct {
	// Land opts the land gate into graded enforcement: "graded". Empty keeps
	// the raw unresolved-thread count, so a policy that already carries a
	// review_gate section for other reasons does not change behaviour.
	Land              string           `yaml:"land"`
	RequiredBots      []string         `yaml:"required_bots"`
	Block             []ReviewGateRule `yaml:"block"`
	Triage            []ReviewGateRule `yaml:"triage"`
	OwnMediumTriage   *bool            `yaml:"own_medium_triage"`
	IgnoreAuthors     []string         `yaml:"ignore_authors"`
	IgnoreOutdated    *bool            `yaml:"ignore_outdated"`
	IgnoreBotResolved *bool            `yaml:"ignore_bot_resolved"`
}

// FastLane relaxes the land gate for small routine PRs: one that touches no
// blocking path and changes at most MaxChangedLines lines (additions plus
// deletions) is held only by block-class findings.
type FastLane struct {
	MaxChangedLines int `yaml:"max_changed_lines"`
}

// BotUpdates names the dependency bots whose patch updates land without a
// review and the files such an update may change. Land is "patch", the only
// mode.
type BotUpdates struct {
	Authors []string `yaml:"authors"`
	Paths   []string `yaml:"paths"`
	Land    string   `yaml:"land"`
}

// Lists reports whether a PR opened by login, a GitHub user of userType, is by
// a listed bot. The login must match exactly (case aside): NormalizeAuthor
// would let a human account named like the bot pass.
func (b *BotUpdates) Lists(login, userType string) bool {
	if b == nil || userType != "Bot" {
		return false
	}
	for _, a := range b.Authors {
		if strings.EqualFold(a, login) {
			return true
		}
	}
	return false
}

// GateVerdict is the graded outcome for one PR classification. Blocking
// findings stop the merge until fixed; triage findings gate it until answered.
type GateVerdict struct {
	Blocking []ReviewFinding `json:"blocking"`
	Triage   []ReviewFinding `json:"triage"`
	Advisory int             `json:"advisory"`
	Ignored  int             `json:"ignored"`
}

// NormalizeAuthor folds GitHub bot logins into their report names.
func NormalizeAuthor(login string) string {
	return strings.ToLower(strings.TrimSuffix(login, "[bot]"))
}

func defaultReviewGate() ReviewGate {
	yes := true
	return ReviewGate{
		// CodeRabbit Critical and Major rank with a mergewake high (operator
		// 2026-11-01): fixed, or dismissed with a reason; never deferred.
		Block: []ReviewGateRule{
			{Author: "coderabbitai", Severity: []string{"critical", "major"}, Classes: []string{"blocking", "routine"}},
			{Author: "chatgpt-codex-connector", Severity: []string{"p1"}},
		},
		Triage: []ReviewGateRule{
			{Author: "chatgpt-codex-connector", Severity: []string{"p2"}},
		},
		IgnoreAuthors:     []string{"qlty", "commitcheck", "wip", "accesslint", "codecov", "gitguardian"},
		IgnoreOutdated:    &yes,
		IgnoreBotResolved: &yes,
	}
}

// WithDefaults fills omitted fields of a present review_gate section. Explicit
// empty lists stay empty. CI bots are optional unless required_bots names them.
func (g ReviewGate) WithDefaults() ReviewGate {
	d := defaultReviewGate()
	if g.Block == nil {
		g.Block = d.Block
	}
	if g.Triage == nil {
		g.Triage = d.Triage
	}
	if g.IgnoreAuthors == nil {
		g.IgnoreAuthors = d.IgnoreAuthors
	}
	if g.IgnoreOutdated == nil {
		g.IgnoreOutdated = d.IgnoreOutdated
	}
	if g.IgnoreBotResolved == nil {
		g.IgnoreBotResolved = d.IgnoreBotResolved
	}
	return g
}

// WithOwnLogin adds default rules for the trusted review identity unless the
// policy names it in the corresponding list. Critical and high findings block;
// medium and major findings require triage on blocking-class PRs unless disabled.
func (g ReviewGate) WithOwnLogin(login string) ReviewGate {
	g = g.WithDefaults()
	own := NormalizeAuthor(login)
	if own == "" {
		return g
	}
	names := func(rules []ReviewGateRule) bool {
		for _, r := range rules {
			if NormalizeAuthor(r.Author) == own {
				return true
			}
		}
		return false
	}
	if !names(g.Block) {
		g.Block = append(append([]ReviewGateRule(nil), g.Block...), ReviewGateRule{Author: own, Severity: []string{"critical", "high", "blocking"}})
	}
	if (g.OwnMediumTriage == nil || *g.OwnMediumTriage) && !names(g.Triage) {
		g.Triage = append(append([]ReviewGateRule(nil), g.Triage...), ReviewGateRule{Author: own, Severity: []string{"medium", "major"}, Classes: []string{"blocking"}})
	}
	return g
}

func (g ReviewGate) ignoresOutdated() bool {
	return g.IgnoreOutdated == nil || *g.IgnoreOutdated
}
func (g ReviewGate) ignoredAuthor(author string) bool {
	for _, a := range g.IgnoreAuthors {
		if NormalizeAuthor(a) == author {
			return true
		}
	}
	return false
}
func (g ReviewGate) gateAuthors() map[string]bool {
	out := map[string]bool{}
	for _, rules := range [][]ReviewGateRule{g.Block, g.Triage} {
		for _, r := range rules {
			out[NormalizeAuthor(r.Author)] = true
		}
	}
	return out
}

func (r ReviewGateRule) applies(f ReviewFinding, class string) bool {
	if NormalizeAuthor(r.Author) != f.Author {
		return false
	}
	sev := false
	for _, s := range r.Severity {
		if strings.ToLower(s) == f.Severity {
			sev = true
			break
		}
	}
	if !sev {
		return false
	}
	if len(r.Classes) == 0 {
		return true
	}
	for _, c := range r.Classes {
		if c == class {
			return true
		}
	}
	return false
}

// Evaluate grades before applying disposition. Replies never clear block rules;
// an outdated block needs a recheck by that reviewer on the current head.
func (g ReviewGate) Evaluate(fs []ReviewFinding, class string) GateVerdict {
	g = g.WithDefaults()
	v := GateVerdict{}
	authors := g.GateAuthors()
	for _, f := range fs {
		f.Author = NormalizeAuthor(f.Author)
		f.Severity = strings.ToLower(f.Severity)
		if g.ignoredAuthor(f.Author) {
			v.Ignored++
			continue
		}
		blocked, triaged := false, false
		for _, r := range g.Block {
			blocked = blocked || r.applies(f, class)
		}
		for _, r := range g.Triage {
			triaged = triaged || r.applies(f, class)
		}
		// Missing authorship or unrecognized severity is unknown evidence on a
		// blocking path, rather than an implicit clean pass.
		if class == "blocking" && (f.Author == "" || (authors[f.Author] && !validSeverity(f.Severity))) {
			triaged = true
		}
		botResolved := f.Resolved && f.ReviewerResolved && !f.OwnReview && *g.IgnoreBotResolved
		switch {
		case blocked:
			if botResolved || (!f.OwnReview && g.ignoresOutdated() && f.Outdated && f.Rechecked) {
				v.Ignored++
			} else {
				v.Blocking = append(v.Blocking, f)
			}
		case botResolved || (!f.OwnReview && f.Outdated && f.Severity == "major" && g.ignoresOutdated()):
			v.Ignored++
		case triaged && !f.Answered:
			v.Triage = append(v.Triage, f)
		case !triaged && !blocked && ((f.Author == "coderabbitai" && (f.Severity == "minor" || f.Severity == "nitpick")) || (f.Author == "chatgpt-codex-connector" && f.Severity == "p3")):
			v.Ignored++
		default:
			v.Advisory++
		}
	}
	return v
}

// Grades a finding can receive from Grade.
const (
	GradeBlock    = "block"
	GradeTriage   = "triage"
	GradeAdvisory = "advisory"
	GradeIgnored  = "ignored"
)

// Grade classifies one finding by the rules alone, before any disposition
// (resolved, answered, outdated). Evidence the rules cannot read — a missing
// author on a blocking path, an unrecognized severity from a graded author,
// or any severity outside the known set on the trusted review identity's own
// finding — needs an answer rather than passing silently.
func (g ReviewGate) Grade(f ReviewFinding, class string) string {
	g = g.WithDefaults()
	f.Author = NormalizeAuthor(f.Author)
	f.Severity = strings.ToLower(f.Severity)
	if g.ignoredAuthor(f.Author) {
		return GradeIgnored
	}
	// A finding outside the PR's Acceptance contract is advisory: it never
	// gates and needs no disposition.
	if f.Severity == "advisory" {
		return GradeAdvisory
	}
	blocked, triaged := false, false
	for _, r := range g.Block {
		blocked = blocked || r.applies(f, class)
	}
	for _, r := range g.Triage {
		triaged = triaged || r.applies(f, class)
	}
	switch {
	case blocked:
		return GradeBlock
	case triaged:
		return GradeTriage
	case class == "blocking" && (f.Author == "" || (g.gateAuthors()[f.Author] && !validSeverity(f.Severity))):
		return GradeTriage
	case f.OwnReview && !validSeverity(f.Severity):
		return GradeTriage
	}
	return GradeAdvisory
}

// OutdatedCleared reports whether an outdated finding stops counting under
// ignore_outdated, with the same limits Evaluate applies: never the trusted
// review identity's own findings, a block finding only once its reviewer has
// rechecked the current head, and a triage finding only when it is a CodeRabbit
// style "major".
func (g ReviewGate) OutdatedCleared(f ReviewFinding, grade string) bool {
	if f.OwnReview || !f.Outdated || !g.WithDefaults().ignoresOutdated() {
		return false
	}
	switch grade {
	case GradeBlock:
		return f.Rechecked
	case GradeTriage:
		return strings.ToLower(f.Severity) == "major"
	}
	return false
}

// GateAuthors lists normalized bot logins the configured rules reference. The
// caller uses it for grading and bot reply detection.
func (g ReviewGate) GateAuthors() map[string]bool {
	return g.WithDefaults().gateAuthors()
}

// MissingRequiredBots checks only the explicit opt-in list. Thread evidence
// must come from this head, and reviews are prefiltered to the same head by the
// GitHub adapter. Optional bots leave no missing-coverage record at all.
func (g ReviewGate) MissingRequiredBots(fs []ReviewFinding, reviews map[string]int, head string) []string {
	seen := map[string]bool{}
	for _, f := range fs {
		if head != "" && f.CommitSHA == head {
			seen[f.Author] = true
		}
	}
	var missing []string
	for _, name := range g.RequiredBots {
		author := NormalizeAuthor(name)
		if !seen[author] && reviews[author] == 0 {
			missing = append(missing, author)
		}
	}
	sort.Strings(missing)
	return missing
}

type Policy struct {
	DeployApproval   []Rule `yaml:"deploy_approval"`
	AutoUpdateBranch *bool  `yaml:"auto_update_branch"`
	// RequireUpToDate opts the repository into holding a head whose base moved
	// ahead (GitHub reports the PR "behind") as base_stale. Absent or false
	// lands such a head and records Review.BaseBehind; post-merge CI checks the
	// composed result.
	RequireUpToDate bool     `yaml:"require_up_to_date"`
	Version         int      `yaml:"version"`
	Enabled         bool     `yaml:"enabled"`
	RequiredChecks  []string `yaml:"required_checks"`
	WatchChecks     []string `yaml:"watch_checks"`
	// MainChecks names the checks that judge a main-branch commit. It defaults
	// to watch_checks, its legacy name; a policy may set both only with the
	// same list.
	MainChecks []string `yaml:"main_checks"`
	// DeployChecks names the checks that judge a release commit on the deploy
	// stage. Empty means the repository has no deploy stage: the main checks
	// judge a commit.
	DeployChecks []string    `yaml:"deploy_checks"`
	Blocking     []Rule      `yaml:"blocking"`
	ReviewGate   *ReviewGate `yaml:"review_gate"`
	FastLane     *FastLane   `yaml:"fast_lane"`
	// BotUpdates opts the repository into landing a dependency bot's patch
	// updates without review. Absent keeps every bot PR out of the land path.
	BotUpdates *BotUpdates `yaml:"bot_updates"`
	// AnyRefWorkflow names a workflow file in .github/workflows that runs the
	// watch checks for any commit (workflow_dispatch with input ref). When it
	// is set, watch dispatches it for a commit bisect needs but has no result
	// for. Empty keeps the hold.
	AnyRefWorkflow string `yaml:"any_ref_workflow"`
	// AnyRefChecks names the checks bisect judges a candidate commit by: the
	// ones the any-ref workflow reports. Unset means the whole watch set. Each
	// name must be in the watch set. The newest main head is still judged by
	// the whole watch set.
	AnyRefChecks []string `yaml:"any_ref_checks"`
	// AnyRefDeployChecks names the checks the any-ref workflow posts when it
	// runs the deploy suite (for example ci-ok-deploy): the deploy stream
	// judges a dispatched bisect candidate by them. Unset keeps the deploy
	// stream judging candidates by deploy_checks. It needs any_ref_workflow and
	// deploy_checks; the names are not required to be in deploy_checks, because
	// the deploy run reports under its own names.
	AnyRefDeployChecks []string `yaml:"any_ref_deploy_checks"`
	// AnyRefTimeout bounds the wait for one dispatched commit's result, as a
	// Go duration. Empty means DefaultAnyRefTimeout.
	AnyRefTimeout string `yaml:"any_ref_timeout"`
}

// DefaultAnyRefTimeout is how long watch waits for one any-ref CI result.
const DefaultAnyRefTimeout = 45 * time.Minute

// WatchSet is the checks that judge the newest main head: main_checks, its
// alias watch_checks, else required_checks.
func (p Policy) WatchSet() []string {
	for _, set := range [][]string{p.MainChecks, p.WatchChecks} {
		if len(set) > 0 {
			return set
		}
	}
	return p.RequiredChecks
}

// DeploySet is the checks that judge a release commit: deploy_checks, else the
// main set. A repository with no deploy stage keeps releasing and rolling out
// on its main checks.
func (p Policy) DeploySet() []string {
	if len(p.DeployChecks) > 0 {
		return p.DeployChecks
	}
	return p.WatchSet()
}

// DeployStage reports whether the repository has a deploy stage.
func (p Policy) DeployStage() bool { return len(p.DeployChecks) > 0 }

// BisectChecks is the checks that judge a bisect candidate: any_ref_checks,
// else the watch set.
func (p Policy) BisectChecks() []string {
	if len(p.AnyRefChecks) > 0 {
		return p.AnyRefChecks
	}
	return p.WatchSet()
}

// AnyRefWait returns the validated any_ref_timeout, or the default.
func (p Policy) AnyRefWait() time.Duration {
	if d, e := time.ParseDuration(p.AnyRefTimeout); e == nil && d > 0 {
		return d
	}
	return DefaultAnyRefTimeout
}

// AutoUpdateBranchEnabled defaults branch updates on when the key is absent.
func (p Policy) AutoUpdateBranchEnabled() bool {
	return p.AutoUpdateBranch == nil || *p.AutoUpdateBranch
}

type Decision struct {
	Class   string   `json:"class"`
	Reasons []string `json:"reasons"`
	Shadow  bool     `json:"shadow"`
	Action  string   `json:"action"`
}

func (p Policy) Validate() error {
	if p.Version != 1 {
		return errors.New("policy version must be 1")
	}
	if len(p.RequiredChecks) == 0 {
		return errors.New("required_checks must name at least one CI check")
	}
	seen := map[string]bool{}
	for _, n := range p.RequiredChecks {
		if strings.TrimSpace(n) == "" || seen[n] {
			return errors.New("required_checks must be nonempty and unique")
		}
		seen[n] = true
	}
	watchSeen := map[string]bool{}
	for _, n := range p.WatchChecks {
		if strings.TrimSpace(n) == "" || watchSeen[n] {
			return errors.New("watch_checks must be nonempty and unique")
		}
		watchSeen[n] = true
	}
	for _, set := range []struct {
		key    string
		checks []string
	}{{"main_checks", p.MainChecks}, {"deploy_checks", p.DeployChecks}} {
		seen := map[string]bool{}
		for _, n := range set.checks {
			if strings.TrimSpace(n) == "" || seen[n] {
				return fmt.Errorf("%s must be nonempty and unique", set.key)
			}
			seen[n] = true
		}
	}
	// watch_checks is the legacy name of main_checks. Keeping both is allowed
	// only while they agree, so no policy reads two different main sets.
	if len(p.MainChecks) > 0 && len(p.WatchChecks) > 0 && !slices.Equal(p.MainChecks, p.WatchChecks) {
		return errors.New("main_checks and watch_checks are the same key; set one or give both the same list")
	}
	// An empty list is an error, not "unset": it would silently mean every
	// candidate is judged by no check.
	if p.AnyRefChecks != nil {
		if len(p.AnyRefChecks) == 0 {
			return errors.New("any_ref_checks must name at least one check")
		}
		inWatch := map[string]bool{}
		for _, n := range p.WatchSet() {
			inWatch[n] = true
		}
		anySeen := map[string]bool{}
		for _, n := range p.AnyRefChecks {
			switch {
			case strings.TrimSpace(n) == "" || anySeen[n]:
				return errors.New("any_ref_checks must be nonempty and unique")
			case !inWatch[n]:
				return fmt.Errorf("any_ref_checks: %q is not in the watch set (watch_checks, else required_checks)", n)
			}
			anySeen[n] = true
		}
	}
	// The deploy stream reads this set only to judge a commit its any-ref run
	// reported on, so it means nothing without that workflow or a deploy stage.
	if p.AnyRefDeployChecks != nil {
		if len(p.AnyRefDeployChecks) == 0 {
			return errors.New("any_ref_deploy_checks must name at least one check")
		}
		if p.AnyRefWorkflow == "" {
			return errors.New("any_ref_deploy_checks requires any_ref_workflow")
		}
		if len(p.DeployChecks) == 0 {
			return errors.New("any_ref_deploy_checks requires deploy_checks")
		}
		deploySeen := map[string]bool{}
		for _, n := range p.AnyRefDeployChecks {
			if strings.TrimSpace(n) == "" || deploySeen[n] {
				return errors.New("any_ref_deploy_checks must be nonempty and unique")
			}
			deploySeen[n] = true
		}
	}
	if w := p.AnyRefWorkflow; w != "" && (strings.TrimSpace(w) != w || strings.Contains(w, "/") || !(strings.HasSuffix(w, ".yml") || strings.HasSuffix(w, ".yaml"))) {
		return fmt.Errorf("any_ref_workflow %q must be a workflow file name ending in .yml or .yaml", w)
	}
	if p.AnyRefTimeout != "" {
		if d, e := time.ParseDuration(p.AnyRefTimeout); e != nil || d <= 0 {
			return fmt.Errorf("any_ref_timeout %q must be a positive duration", p.AnyRefTimeout)
		}
		if p.AnyRefWorkflow == "" {
			return errors.New("any_ref_timeout requires any_ref_workflow")
		}
	}
	for _, r := range append(append([]Rule{}, p.Blocking...), p.DeployApproval...) {
		if len(r.Paths) == 0 || strings.TrimSpace(r.Reason) == "" {
			return errors.New("blocking rules require paths and reason")
		}
		for _, g := range r.Paths {
			if _, err := Match(g, "probe"); err != nil {
				return err
			}
		}
		for _, k := range r.Kinds {
			if !validKind(k) {
				return fmt.Errorf("unknown change kind %q", k)
			}
		}
	}
	if p.ReviewGate != nil {
		if p.ReviewGate.Land != "" && p.ReviewGate.Land != "graded" {
			return fmt.Errorf("unknown review_gate land mode %q", p.ReviewGate.Land)
		}
		bots := map[string]bool{}
		for _, name := range p.ReviewGate.RequiredBots {
			author := NormalizeAuthor(name)
			if strings.TrimSpace(author) == "" || strings.TrimSpace(author) != author || bots[author] {
				return errors.New("review_gate required_bots must be nonempty and unique")
			}
			bots[author] = true
		}
		for _, rules := range [][]ReviewGateRule{p.ReviewGate.Block, p.ReviewGate.Triage} {
			for _, r := range rules {
				if strings.TrimSpace(r.Author) == "" || len(r.Severity) == 0 {
					return errors.New("review_gate rules require author and severity")
				}
				for _, s := range r.Severity {
					if !validSeverity(strings.ToLower(s)) {
						return fmt.Errorf("unknown review_gate severity %q", s)
					}
				}
				for _, c := range r.Classes {
					if c != "blocking" && c != "routine" {
						return fmt.Errorf("unknown review_gate class %q", c)
					}
				}
			}
		}
		for _, a := range p.ReviewGate.IgnoreAuthors {
			if strings.TrimSpace(a) == "" {
				return errors.New("review_gate ignore_authors must be nonempty")
			}
		}
	}
	if b := p.BotUpdates; b != nil {
		if b.Land != "patch" {
			return fmt.Errorf("bot_updates land must be %q", "patch")
		}
		if len(b.Authors) == 0 || len(b.Paths) == 0 {
			return errors.New("bot_updates requires authors and paths")
		}
		for _, a := range b.Authors {
			if strings.TrimSpace(a) == "" || strings.TrimSpace(a) != a {
				return errors.New("bot_updates authors must be nonempty logins")
			}
		}
		for _, g := range b.Paths {
			if _, err := Match(g, "probe"); err != nil {
				return err
			}
		}
	}
	if p.FastLane != nil && p.FastLane.MaxChangedLines <= 0 {
		return errors.New("fast_lane max_changed_lines must be positive")
	}
	return nil
}

// FastLaneEligible reports whether a PR of the given class and size takes the
// fast lane: configured, routine, and no larger than max_changed_lines.
func (p Policy) FastLaneEligible(class string, changedLines int) bool {
	return p.FastLane != nil && class == "routine" && changedLines <= p.FastLane.MaxChangedLines
}

// KnownSeverity reports whether s is a severity label the gate can grade.
func KnownSeverity(s string) bool { return validSeverity(strings.ToLower(s)) }
func validSeverity(s string) bool {
	switch s {
	case "critical", "high", "medium", "low", "blocking", "major", "minor", "nitpick", "p1", "p2", "p3":
		return true
	}
	return false
}
func validKind(k string) bool {
	switch k {
	case "added", "modified", "removed", "renamed", "copied", "changed", "unchanged":
		return true
	}
	return false
}

// MatchAny reports whether name matches any glob. Every glob is checked, so
// an invalid one fails even when an earlier glob matched.
func MatchAny(globs []string, name string) (bool, error) {
	matched := false
	for _, g := range globs {
		ok, e := Match(g, name)
		if e != nil {
			return false, e
		}
		matched = matched || ok
	}
	return matched, nil
}

// Match supports slash-aware glob syntax plus ** as a whole path segment.
func Match(glob, name string) (bool, error) {
	if glob == "" || strings.HasPrefix(glob, "/") || strings.Contains(glob, "\\") {
		return false, fmt.Errorf("invalid path glob %q", glob)
	}
	gs := strings.Split(glob, "/")
	ns := strings.Split(name, "/")
	for _, g := range gs {
		if g == ".." || g == "" {
			return false, fmt.Errorf("invalid path glob %q", glob)
		}
		if g != "**" {
			if _, e := path.Match(g, ""); e != nil {
				return false, e
			}
		}
	}
	var match func(int, int) bool
	match = func(i, j int) bool {
		if i == len(gs) {
			return j == len(ns)
		}
		if gs[i] == "**" {
			return match(i+1, j) || (j < len(ns) && match(i, j+1))
		}
		if j == len(ns) {
			return false
		}
		ok, _ := path.Match(gs[i], ns[j])
		return ok && match(i+1, j+1)
	}
	return match(0, 0), nil
}

// ClassifyGuarded additionally counts a change to the policy file itself as a
// blocking path: a PR may never use its own revision to relax its own rules.
func (p Policy) ClassifyGuarded(files []File, self string) Decision {
	d := p.Classify(files)
	if self == "" {
		return d
	}
	for _, f := range files {
		if f.Path == self || f.Previous == self {
			d.Class = "blocking"
			d.Reasons = append(d.Reasons, "policy file "+self+" changed")
			break
		}
	}
	return d
}
func (p Policy) Classify(files []File) Decision {
	d := Decision{Class: "routine", Shadow: !p.Enabled, Reasons: []string{}}
	if len(files) == 0 {
		d.Class = "blocking"
		d.Reasons = append(d.Reasons, "empty or unavailable change list")
		return d
	}
	for _, f := range files {
		if !validKind(f.Kind) {
			d.Class = "blocking"
			d.Reasons = append(d.Reasons, "unknown change kind: "+f.Kind)
		}
		for _, r := range p.Blocking {
			kind := len(r.Kinds) == 0
			for _, k := range r.Kinds {
				kind = kind || k == f.Kind
			}
			if !kind {
				continue
			}
			for _, g := range r.Paths {
				a, _ := Match(g, f.Path)
				b := false
				if f.Previous != "" {
					b, _ = Match(g, f.Previous)
				}
				if a || b {
					d.Class = "blocking"
					d.Reasons = append(d.Reasons, r.Reason+": "+f.Path)
					break
				}
			}
		}
	}
	return d
}
