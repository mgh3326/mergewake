// Package policy classifies changes by whether harm can precede a revert.
package policy

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
)

type File struct {
	Path     string `json:"filename"`
	Previous string `json:"previous_filename"`
	Kind     string `json:"status"`
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

// ReviewGate replaces the raw unresolved-thread count with graded enforcement.
// An absent section uses the default graded rules and requires no CI bots.
type ReviewGate struct {
	RequiredBots      []string         `yaml:"required_bots"`
	Block             []ReviewGateRule `yaml:"block"`
	Triage            []ReviewGateRule `yaml:"triage"`
	IgnoreAuthors     []string         `yaml:"ignore_authors"`
	IgnoreOutdated    *bool            `yaml:"ignore_outdated"`
	IgnoreBotResolved *bool            `yaml:"ignore_bot_resolved"`
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
		Block: []ReviewGateRule{
			{Author: "coderabbitai", Severity: []string{"critical"}, Classes: []string{"blocking", "routine"}},
			{Author: "chatgpt-codex-connector", Severity: []string{"p1"}},
		},
		Triage: []ReviewGateRule{
			{Author: "coderabbitai", Severity: []string{"major"}},
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
	Version        int         `yaml:"version"`
	Enabled        bool        `yaml:"enabled"`
	RequiredChecks []string    `yaml:"required_checks"`
	WatchChecks    []string    `yaml:"watch_checks"`
	Blocking       []Rule      `yaml:"blocking"`
	ReviewGate     *ReviewGate `yaml:"review_gate"`
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
	for _, r := range p.Blocking {
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
	return nil
}
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
