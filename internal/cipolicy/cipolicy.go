// Package cipolicy checks that a managed repo follows the fleet main-CI
// default: every workflow that runs on push to the watched branch and produces
// a check mergewake watches cancels in-progress runs per ref, and the repo
// names an any-ref CI workflow in its mergewake policy that exists.
//
// Check is a pure function of the files; Fetch reads them through GitHub.
package cipolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/mgh3326/mergewake/internal/policy"
	"gopkg.in/yaml.v3"
)

// WorkflowDir is where GitHub reads workflow files.
const WorkflowDir = ".github/workflows"

// Verdicts.
const (
	OK   = "ok"
	Warn = "warn"
)

// Cancel-in-progress values a workflow can carry. "true" and "false" are the
// literals; "expression" is a `${{ }}` value the checker cannot evaluate.
const (
	CancelTrue       = "true"
	CancelFalse      = "false"
	CancelMissing    = "missing"
	CancelExpression = "expression"
	CancelOther      = "other"
)

// Whether a workflow produces a check the watch reads.
const (
	ProducesYes     = "yes"
	ProducesNo      = "no"
	ProducesUnknown = "unknown"
)

// File is one file at the checked head: its blob SHA and content.
type File struct {
	Path string
	Blob string
	Data []byte
}

// Input is everything Check reads for one repo at one head.
type Input struct {
	Repo       string
	Branch     string
	Head       string
	PolicyPath string
	// PolicyBlob is the policy file's blob SHA, empty when the file is absent.
	PolicyBlob string
	Policy     policy.Policy
	// PolicyErr is set when the policy file is absent or does not decode or
	// validate; the watched checks are then unknown.
	PolicyErr string
	Workflows []File
}

// Concurrency is a workflow's top-level concurrency block as written.
type Concurrency struct {
	Present bool   `json:"present"`
	Group   string `json:"group,omitempty"`
	// PerRef is set when the group expression names the ref.
	PerRef           bool   `json:"per_ref"`
	CancelInProgress string `json:"cancel_in_progress"`
}

// Workflow reports one workflow that runs on push to the branch.
type Workflow struct {
	File string `json:"file"`
	Name string `json:"name,omitempty"`
	// Produces is yes, no or unknown (a job name the checker cannot resolve).
	Produces string `json:"produces_watched_check"`
	// Checks lists the watched checks a job of this workflow matches.
	Checks      []string    `json:"watched_checks,omitempty"`
	Concurrency Concurrency `json:"concurrency"`
	// Compliant is set when the rule does not apply (Produces is no) or the
	// workflow cancels in progress per ref.
	Compliant bool   `json:"compliant"`
	Reason    string `json:"reason,omitempty"`
}

// AnyRef reports the any_ref_workflow policy key.
type AnyRef struct {
	Workflow string `json:"workflow,omitempty"`
	Set      bool   `json:"set"`
	Exists   bool   `json:"exists"`
}

// Report is the verdict for one repo at one head.
type Report struct {
	Repo          string     `json:"repo"`
	Branch        string     `json:"branch"`
	Head          string     `json:"head"`
	WatchedChecks []string   `json:"watched_checks"`
	Workflows     []Workflow `json:"workflows"`
	AnyRef        AnyRef     `json:"any_ref"`
	Verdict       string     `json:"verdict"`
	Reasons       []string   `json:"reasons,omitempty"`
	// Key changes exactly when the policy blob, a workflow blob or the branch
	// changes.
	Key string `json:"key"`
}

// Check reports whether one repo follows the rule.
func Check(in Input) Report {
	rep := Report{Repo: in.Repo, Branch: in.Branch, Head: in.Head, Verdict: OK, Key: key(in), WatchedChecks: []string{}, Workflows: []Workflow{}}
	known := in.PolicyErr == ""
	if known {
		rep.WatchedChecks = in.Policy.RequiredChecks
		if len(in.Policy.WatchChecks) > 0 {
			rep.WatchedChecks = in.Policy.WatchChecks
		}
	} else {
		rep.Reasons = append(rep.Reasons, in.PolicyPath+": "+in.PolicyErr)
	}
	sort.Slice(in.Workflows, func(i, j int) bool { return in.Workflows[i].Path < in.Workflows[j].Path })
	exists := map[string]bool{}
	for _, f := range in.Workflows {
		exists[strings.TrimPrefix(f.Path, WorkflowDir+"/")] = true
	}
	for _, f := range in.Workflows {
		w, pushes, e := inspect(f, in.Branch, rep.WatchedChecks, known)
		if e != nil {
			rep.Reasons = append(rep.Reasons, fmt.Sprintf("workflow %s: cannot parse: %v", w.File, e))
			continue
		}
		if !pushes {
			continue
		}
		if w.Reason != "" {
			rep.Reasons = append(rep.Reasons, "workflow "+w.File+": "+w.Reason)
		}
		rep.Workflows = append(rep.Workflows, w)
	}
	if known {
		rep.AnyRef = AnyRef{Workflow: in.Policy.AnyRefWorkflow, Set: in.Policy.AnyRefWorkflow != "", Exists: exists[in.Policy.AnyRefWorkflow]}
		switch {
		case !rep.AnyRef.Set:
			rep.Reasons = append(rep.Reasons, "any_ref_workflow is not set in "+in.PolicyPath)
		case !rep.AnyRef.Exists:
			rep.Reasons = append(rep.Reasons, fmt.Sprintf("any_ref_workflow %s does not exist in %s", in.Policy.AnyRefWorkflow, WorkflowDir))
		}
	}
	if len(rep.Reasons) > 0 {
		rep.Verdict = Warn
	}
	return rep
}

func key(in Input) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\n", in.Branch, in.PolicyPath, in.PolicyBlob)
	files := append([]File(nil), in.Workflows...)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	for _, f := range files {
		fmt.Fprintf(h, "%s\x00%s\n", f.Path, f.Blob)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// inspect parses one workflow. pushes is false when it does not run on push to
// the branch; the returned Workflow is then only a name for errors.
func inspect(f File, branch string, watched []string, known bool) (w Workflow, pushes bool, err error) {
	w = Workflow{File: strings.TrimPrefix(f.Path, WorkflowDir+"/")}
	var doc yaml.Node
	if e := yaml.Unmarshal(f.Data, &doc); e != nil {
		return w, false, e
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return w, false, fmt.Errorf("not a YAML mapping")
	}
	root := doc.Content[0]
	if n := field(root, "name"); n != nil && n.Kind == yaml.ScalarNode {
		w.Name = n.Value
	}
	// YAML 1.1 reads a bare `on` key as a boolean; yaml.v3 keeps the text, but
	// accept "true" so a differently resolved key is still the trigger block.
	on := field(root, "on")
	if on == nil {
		on = field(root, "true")
	}
	if !pushesTo(on, branch) {
		return w, false, nil
	}
	w.Concurrency = concurrencyOf(field(root, "concurrency"))
	w.Produces, w.Checks = produces(field(root, "jobs"), watched, known)
	w.Compliant, w.Reason = judge(w)
	return w, true, nil
}

func judge(w Workflow) (bool, string) {
	if w.Produces == ProducesNo {
		return true, ""
	}
	c := w.Concurrency
	var problems []string
	switch {
	case !c.Present:
		problems = append(problems, "no concurrency block")
	default:
		switch {
		case c.Group == "":
			problems = append(problems, "concurrency has no group")
		case !c.PerRef:
			problems = append(problems, fmt.Sprintf("concurrency group %q is not per ref", c.Group))
		}
		switch c.CancelInProgress {
		case CancelMissing:
			problems = append(problems, "cancel-in-progress is missing (defaults to false)")
		case CancelFalse:
			problems = append(problems, "cancel-in-progress is false")
		case CancelExpression:
			problems = append(problems, "cancel-in-progress is an expression; cannot verify it is true")
		case CancelOther:
			problems = append(problems, "cancel-in-progress is not a boolean")
		}
	}
	if len(problems) == 0 {
		return true, ""
	}
	reason := strings.Join(problems, "; ")
	if w.Produces == ProducesUnknown {
		reason += " (cannot tell whether it produces a watched check)"
	}
	return false, reason
}

var perRef = regexp.MustCompile(`\bgithub\.(ref|ref_name|head_ref)\b`)

func concurrencyOf(n *yaml.Node) Concurrency {
	c := Concurrency{CancelInProgress: CancelMissing}
	if n == nil {
		return c
	}
	c.Present = true
	switch n.Kind {
	case yaml.ScalarNode:
		c.Group = n.Value
	case yaml.MappingNode:
		if g := field(n, "group"); g != nil && g.Kind == yaml.ScalarNode {
			c.Group = g.Value
		}
		if v := field(n, "cancel-in-progress"); v != nil {
			c.CancelInProgress = cancelValue(v)
		}
	}
	c.PerRef = perRef.MatchString(c.Group)
	return c
}

func cancelValue(v *yaml.Node) string {
	if v.Kind != yaml.ScalarNode {
		return CancelOther
	}
	switch {
	case strings.Contains(v.Value, "${{"):
		return CancelExpression
	case v.Tag == "!!bool" && v.Value == "true":
		return CancelTrue
	case v.Tag == "!!bool" && v.Value == "false":
		return CancelFalse
	case v.Tag == "!!str" && v.Value == "true":
		// GitHub reads the string "true" as true.
		return CancelTrue
	case v.Tag == "!!str" && v.Value == "false":
		return CancelFalse
	}
	return CancelOther
}

// field returns the value of a mapping key, or nil.
func field(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func scalars(n *yaml.Node) []string {
	switch {
	case n == nil:
		return nil
	case n.Kind == yaml.ScalarNode:
		return []string{n.Value}
	case n.Kind == yaml.SequenceNode:
		var out []string
		for _, c := range n.Content {
			if c.Kind == yaml.ScalarNode {
				out = append(out, c.Value)
			}
		}
		return out
	}
	return nil
}

// pushesTo reports whether the `on:` block runs the workflow for a push to
// branch. Path filters are ignored: a push that changes other paths does not
// start it, but any push to the branch may.
func pushesTo(on *yaml.Node, branch string) bool {
	if on == nil {
		return false
	}
	switch on.Kind {
	case yaml.ScalarNode, yaml.SequenceNode:
		for _, ev := range scalars(on) {
			if ev == "push" {
				return true
			}
		}
		return false
	case yaml.MappingNode:
		push := field(on, "push")
		if push == nil {
			// `push:` with no value is a null node; a missing key is nil.
			for i := 0; i+1 < len(on.Content); i += 2 {
				if on.Content[i].Value == "push" {
					return true
				}
			}
			return false
		}
		if push.Kind != yaml.MappingNode {
			return true
		}
		if b := field(push, "branches"); b != nil {
			return refMatches(scalars(b), branch)
		}
		if b := field(push, "branches-ignore"); b != nil {
			for _, p := range scalars(b) {
				if globMatch(p, branch) {
					return false
				}
			}
			return true
		}
		// Only tag filters: a branch push does not start the workflow.
		return field(push, "tags") == nil && field(push, "tags-ignore") == nil
	}
	return false
}

// refMatches applies a `branches:` list: later patterns win, and a leading !
// removes a match.
func refMatches(patterns []string, name string) bool {
	matched := false
	for _, p := range patterns {
		if strings.HasPrefix(p, "!") {
			if globMatch(p[1:], name) {
				matched = false
			}
		} else if globMatch(p, name) {
			matched = true
		}
	}
	return matched
}

// globMatch implements GitHub's filter patterns for the practical subset:
// * (no slash), ** (anything), ? and + (zero-or-one and one-or-more of the
// previous character) and [] classes.
func globMatch(pattern, name string) bool {
	var re strings.Builder
	re.WriteString("^")
	rs := []rune(pattern)
	for i := 0; i < len(rs); i++ {
		switch r := rs[i]; r {
		case '*':
			if i+1 < len(rs) && rs[i+1] == '*' {
				re.WriteString(".*")
				i++
			} else {
				re.WriteString("[^/]*")
			}
		case '?', '+':
			re.WriteRune(r)
		case '[':
			j := i + 1
			for j < len(rs) && rs[j] != ']' {
				j++
			}
			if j < len(rs) {
				re.WriteString(string(rs[i : j+1]))
				i = j
			} else {
				re.WriteString(`\[`)
			}
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	re.WriteString("$")
	m, e := regexp.MatchString(re.String(), name)
	return e == nil && m
}

type job struct {
	name     string
	matrix   bool
	reusable bool
}

// produces matches a workflow's jobs against the watched checks. A check run
// is named by its job's name (default: the job id), `name (matrix values)` for
// a matrix job and `name / inner` for a reusable-workflow job. A job name made
// only of expressions cannot be resolved without running the workflow.
func produces(jobs *yaml.Node, watched []string, known bool) (string, []string) {
	if !known {
		return ProducesUnknown, nil
	}
	var js []job
	if jobs != nil && jobs.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(jobs.Content); i += 2 {
			j := job{name: jobs.Content[i].Value}
			body := jobs.Content[i+1]
			if n := field(body, "name"); n != nil && n.Kind == yaml.ScalarNode && n.Value != "" {
				j.name = n.Value
			}
			j.matrix = field(field(body, "strategy"), "matrix") != nil
			j.reusable = field(body, "uses") != nil
			js = append(js, j)
		}
	}
	var matched []string
	unknown := false
	for _, j := range js {
		re, literal := jobPattern(j)
		if !literal {
			// A name that is only expressions would match every check.
			unknown = true
			continue
		}
		for _, w := range watched {
			if re.MatchString(w) {
				matched = append(matched, w)
			}
		}
	}
	sort.Strings(matched)
	matched = dedupe(matched)
	switch {
	case len(matched) > 0:
		return ProducesYes, matched
	case unknown:
		return ProducesUnknown, nil
	}
	return ProducesNo, nil
}

var expr = regexp.MustCompile(`\$\{\{.*?\}\}`)

// jobPattern builds the regexp for the check names a job can produce.
// literal is false when the name has no text outside expressions.
func jobPattern(j job) (*regexp.Regexp, bool) {
	parts := expr.Split(j.name, -1)
	var b strings.Builder
	literal := false
	for i, p := range parts {
		if i > 0 {
			b.WriteString(".+")
		}
		if p != "" {
			literal = true
		}
		b.WriteString(regexp.QuoteMeta(p))
	}
	name := b.String()
	alt := name
	if j.matrix {
		alt += `(?: \(.*\))?`
	}
	if j.reusable {
		alt += `(?: / .*)?`
	}
	return regexp.MustCompile("^" + alt + "$"), literal || len(parts) == 1
}

func dedupe(s []string) []string {
	var out []string
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// Text renders one report for a terminal.
func (r Report) Text() string {
	var b strings.Builder
	head := r.Head
	if len(head) > 7 {
		head = head[:7]
	}
	fmt.Fprintf(&b, "%s %s@%s %s\n", r.Repo, r.Branch, head, strings.ToUpper(r.Verdict))
	fmt.Fprintf(&b, "  watched checks: %s\n", strings.Join(r.WatchedChecks, ", "))
	for _, w := range r.Workflows {
		state := "ok"
		if !w.Compliant {
			state = "WARN"
		}
		fmt.Fprintf(&b, "  push-%s workflow %s: %s; produces watched check: %s; cancel-in-progress: %s; group: %q per-ref=%t\n",
			r.Branch, w.File, state, w.Produces, w.Concurrency.CancelInProgress, w.Concurrency.Group, w.Concurrency.PerRef)
	}
	if len(r.Workflows) == 0 {
		fmt.Fprintf(&b, "  no workflow runs on push to %s\n", r.Branch)
	}
	fmt.Fprintf(&b, "  any_ref_workflow: set=%t exists=%t %s\n", r.AnyRef.Set, r.AnyRef.Exists, r.AnyRef.Workflow)
	for _, reason := range r.Reasons {
		fmt.Fprintf(&b, "  - %s\n", reason)
	}
	return b.String()
}
