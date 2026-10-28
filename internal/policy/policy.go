// Package policy classifies changes by whether harm can precede a revert.
package policy

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
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
// and authorship are normalized by the GitHub adapter; Answered means a human
// reply exists on the thread.
type ReviewFinding struct {
	Author   string `json:"author"`
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Resolved bool   `json:"resolved"`
	Outdated bool   `json:"outdated"`
	Answered bool   `json:"answered"`
}

// ReviewGateRule grades findings by normalized author and severity; classes
// limits the rule to that PR classification (empty means all classes).
type ReviewGateRule struct {
	Author   string   `yaml:"author"`
	Severity []string `yaml:"severity"`
	Classes  []string `yaml:"classes"`
}

// ReviewGate replaces the raw unresolved-thread count with graded enforcement.
// An absent section keeps the legacy all-unresolved-must-clear rule.
type ReviewGate struct {
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
			{Author: "coderabbitai", Severity: []string{"major"}, Classes: []string{"blocking"}},
			{Author: "chatgpt-codex-connector", Severity: []string{"p1"}, Classes: []string{"blocking"}},
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
// empty lists stay empty; a nil section is the caller's legacy path.
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

// Evaluate folds findings into block, triage, advisory and ignored buckets.
// Resolved and outdated threads are addressed, not open. An answered thread
// covers its cross-bot duplicate on the same path and line.
func (g ReviewGate) Evaluate(fs []ReviewFinding, class string) GateVerdict {
	g = g.WithDefaults()
	v := GateVerdict{}
	answeredAt := map[string]bool{}
	for _, f := range fs {
		if f.Answered && f.Path != "" {
			answeredAt[f.Path+"\x00"+strconv.Itoa(f.Line)] = true
		}
	}
	for _, f := range fs {
		switch {
		case f.Resolved:
			v.Ignored++
			continue
		case f.Outdated && g.ignoresOutdated():
			v.Ignored++
			continue
		case g.ignoredAuthor(f.Author):
			v.Ignored++
			continue
		}
		if !f.Answered && answeredAt[f.Path+"\x00"+strconv.Itoa(f.Line)] {
			f.Answered = true
		}
		blocked := false
		for _, r := range g.Block {
			if r.applies(f, class) {
				blocked = true
				break
			}
		}
		triaged := false
		for _, r := range g.Triage {
			if r.applies(f, class) {
				triaged = true
				break
			}
		}
		switch {
		case f.Answered:
			v.Advisory++
		case blocked:
			v.Blocking = append(v.Blocking, f)
		case triaged:
			v.Triage = append(v.Triage, f)
		default:
			v.Advisory++
		}
	}
	return v
}

// GateAuthors lists normalized bot logins the configured rules reference. The
// caller uses it for coverage-gap records and bot reply detection.
func (g ReviewGate) GateAuthors() map[string]bool {
	return g.WithDefaults().gateAuthors()
}

// CoverageGap names gate-referenced authors that left no review evidence at
// all: no threads and no submitted reviews. A bot that never ran is a gap,
// never a clean pass and never a block.
func (g ReviewGate) CoverageGap(fs []ReviewFinding, reviews map[string]int) []string {
	seen := map[string]bool{}
	for _, f := range fs {
		seen[f.Author] = true
	}
	var gap []string
	for author := range g.GateAuthors() {
		if !seen[author] && reviews[author] == 0 {
			gap = append(gap, author)
		}
	}
	sort.Strings(gap)
	return gap
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
