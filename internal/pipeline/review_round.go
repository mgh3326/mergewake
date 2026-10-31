package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A review verdict covers a patch, not a SHA, and a review that never ends is
// no review: round one reads the whole diff, later rounds read only what
// changed since the last reviewed head with the earlier findings in context,
// a stated Acceptance contract closes the question "what is a defect", and
// after a budget of rounds only critical and high findings are reported.

// defaultBudgetRounds is the number of full review rounds before later rounds
// report only critical and high findings.
const defaultBudgetRounds = 3

// contractBudget bounds the Acceptance section carried into the prompt.
const contractBudget = 4 << 10

// headLister and rangeDiffer are optional GitHub capabilities: without them a
// review is round one over the whole diff, as before.
type headLister interface {
	ReviewedHeads(ctx context.Context, repo string, n int, login string) ([]github.ReviewedHead, error)
}
type rangeDiffer interface {
	CompareDiff(ctx context.Context, repo, base, head string) (string, error)
}

// roundPlan is what one Review call shows the reviewer and how it grades the
// answer.
type roundPlan struct {
	round int
	// base is the previously reviewed head whose interdiff diff holds; empty
	// when diff is the whole PR.
	base string
	diff string
	// quiet limits findings to critical and high: the budget is spent, or no
	// hunk changed since the last reviewed head.
	quiet bool
	empty bool
	// note says why a later round could not review a delta.
	note   string
	budget int
	// contract is the Acceptance section, contractSource where it came from.
	contract       string
	contractSource string
}

func (s *Service) budgetRounds() int {
	switch n := s.Config.Review.BudgetRounds; {
	case n == 0:
		return defaultBudgetRounds
	case n < 0:
		return 0
	default:
		return n
	}
}

// planRound decides the round, the diff the reviewer sees and the reporting
// budget. Round history is read from the reviews mergewake already posted, so
// every host agrees on it; anything unreadable falls back to a whole-diff
// round one with the reason in note.
func (s *Service) planRound(ctx context.Context, r config.Repo, pr github.Pull, full string) (rp roundPlan) {
	rp = roundPlan{round: 1, diff: full, budget: s.budgetRounds()}
	defer func() {
		if rp.budget > 0 && rp.round > rp.budget {
			rp.quiet = true
		}
	}()
	lister, ok := s.GH.(headLister)
	if !ok {
		return rp
	}
	heads, e := lister.ReviewedHeads(ctx, r.Name, pr.Number, s.Config.Review.Login)
	if e != nil {
		rp.note = "round history unreadable: " + e.Error()
		return rp
	}
	// Rounds count heads that carried changed lines; a head reviewed before
	// round counting existed adds one.
	var prior []github.ReviewedHead
	priorRound, curRound := 0, 0
	for _, h := range heads {
		round := priorRound + 1
		if h.Round > 0 {
			round = h.Round
		}
		if h.SHA == pr.Head.SHA {
			curRound = round
			break
		}
		prior, priorRound = append(prior, h), round
	}
	if len(prior) == 0 {
		if curRound > 0 {
			rp.round = curRound
		}
		return rp
	}
	rp.round = priorRound + 1
	if curRound > 0 {
		rp.round = curRound
	}
	last := prior[len(prior)-1].SHA
	differ, ok := s.GH.(rangeDiffer)
	if !ok {
		rp.note = "GitHub client cannot read the previous patch"
		return rp
	}
	old, e := differ.CompareDiff(ctx, r.Name, pr.Base.Ref, last)
	if e != nil || strings.TrimSpace(old) == "" {
		rp.note = "previous patch unreadable"
		if e != nil {
			rp.note += ": " + e.Error()
		}
		return rp
	}
	delta := interdiff(old, full)
	if delta == "" {
		// Nothing changed (a clean base merge or rebase): the round does not
		// count, and only a critical or high finding is worth reporting.
		rp.empty, rp.quiet, rp.base = true, true, last
		if curRound == 0 {
			rp.round = priorRound
		}
		return rp
	}
	rp.base, rp.diff = last, delta
	return rp
}

// diffFile is one file's block of a unified diff, kept as raw lines so a
// filtered diff reproduces the original text exactly.
type diffFile struct {
	key   string // the "diff --git" line
	head  []string
	hunks []diffHunk
}
type diffHunk struct {
	lines []string
	sig   string // the added and removed lines, without line numbers or context
}

func splitDiff(d string) []diffFile {
	var files []diffFile
	for _, line := range strings.SplitAfter(d, "\n") {
		switch {
		case line == "":
		case strings.HasPrefix(line, "diff --git "):
			files = append(files, diffFile{key: strings.TrimRight(line, "\r\n"), head: []string{line}})
		case len(files) == 0:
		case strings.HasPrefix(line, "@@ "):
			f := &files[len(files)-1]
			f.hunks = append(f.hunks, diffHunk{lines: []string{line}})
		default:
			f := &files[len(files)-1]
			if len(f.hunks) == 0 {
				f.head = append(f.head, line)
				continue
			}
			h := &f.hunks[len(f.hunks)-1]
			h.lines = append(h.lines, line)
			if line[0] == '+' || line[0] == '-' {
				h.sig += line
			}
		}
	}
	return files
}

// interdiff is what newer adds to older: the hunks of newer whose added and
// removed lines older does not carry for the same file. Both are PR-only
// patches (merge base to head), so a base merge that brings in main's changes
// leaves no trace in either, and a rebase that only moves line numbers or
// context leaves every hunk matching. A hunk that mixes old and new changes
// counts as changed and is kept whole. The result is "" when nothing changed.
func interdiff(older, newer string) string {
	prior := map[string]diffFile{}
	for _, f := range splitDiff(older) {
		prior[f.key] = f
	}
	var out strings.Builder
	for _, f := range splitDiff(newer) {
		old, seen := prior[f.key]
		if len(f.hunks) == 0 {
			// A file with no hunks (mode change, rename, binary) is unchanged
			// when its block is byte-identical.
			if !seen || len(old.hunks) != 0 || strings.Join(old.head, "") != strings.Join(f.head, "") {
				out.WriteString(strings.Join(f.head, ""))
			}
			continue
		}
		known := map[string]bool{}
		for _, h := range old.hunks {
			known[h.sig] = true
		}
		var kept []diffHunk
		for _, h := range f.hunks {
			if !known[h.sig] {
				kept = append(kept, h)
			}
		}
		if len(kept) == 0 {
			continue
		}
		out.WriteString(strings.Join(f.head, ""))
		for _, h := range kept {
			out.WriteString(strings.Join(h.lines, ""))
		}
	}
	return out.String()
}

// acceptanceSection returns the section headed "Acceptance" (also "Acceptance
// criteria", "Acceptance (...)"), heading included.
func acceptanceSection(body string) string {
	return markdownSection(body, func(h string) bool {
		h = strings.ToLower(strings.TrimSpace(h))
		rest, ok := strings.CutPrefix(h, "acceptance")
		if !ok {
			return false
		}
		r := []rune(rest)
		return len(r) == 0 || !unicode.IsLetter(r[0])
	})
}

// acceptance finds the PR's closed contract: its own Acceptance section, else
// the one in the body of the task the PR links by Mergewake-Task trailer.
func (s *Service) acceptance(ctx context.Context, r config.Repo, pr github.Pull) (text, source string, err error) {
	if sec := acceptanceSection(pr.Body); sec != "" {
		return clipContract(sec), "PR description", nil
	}
	reader, ok := s.Sink.(interface {
		TaskBody(context.Context, string) (string, error)
	})
	if !ok {
		return "", "", nil
	}
	task := trailer(pr.Body, "Mergewake-Task")
	if !integrations.TaskID.MatchString(task) {
		c, e := s.GH.Commit(ctx, r.Name, pr.Head.SHA)
		if e != nil {
			return "", "", e
		}
		if task = trailer(c.Commit.Message, "Mergewake-Task"); !integrations.TaskID.MatchString(task) {
			return "", "", nil
		}
	}
	body, e := reader.TaskBody(ctx, task)
	if e != nil {
		return "", "", e
	}
	if sec := acceptanceSection(body); sec != "" {
		return clipContract(sec), "linked task " + task, nil
	}
	return "", "", nil
}

func clipContract(s string) string {
	if len(s) <= contractBudget {
		return s
	}
	const notice = "\n[Acceptance section truncated at the size bound.]"
	return clip(s, contractBudget-len(notice)) + notice
}

func short7(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// roundNotes renders the instructions that tell the reviewer which round this
// is, what the diff is, and what to report.
func (rp roundPlan) notes() string {
	var b strings.Builder
	switch {
	case rp.empty:
		fmt.Fprintf(&b, "REVIEW ROUND %d. No hunk changed since the last reviewed head %s (a base merge or rebase). The diff below is the whole PR, for reference only.\n", rp.round, short7(rp.base))
	case rp.base != "":
		fmt.Fprintf(&b, "REVIEW ROUND %d (delta). The diff below is NOT the whole PR: it holds only the hunks changed since the last reviewed head %s. Review only these changed lines and report nothing about code outside them.\n", rp.round, short7(rp.base))
	case rp.round > 1:
		fmt.Fprintf(&b, "REVIEW ROUND %d. The diff below is the whole PR (the interdiff could not be built: %s).\n", rp.round, rp.note)
	}
	if rp.quiet {
		if rp.empty {
			b.WriteString("REVIEW BUDGET: report only a new critical or high finding; otherwise return {\"findings\":[]}.\n")
		} else {
			fmt.Fprintf(&b, "REVIEW BUDGET SPENT: this PR has had %d full review rounds. Report only critical or high findings; return no medium or low findings.\n", rp.budget)
		}
	}
	return b.String()
}

var (
	findingMarkers = regexp.MustCompile(`(?s)^(?:<!-- mergewake-[a-z]+(?::[a-z]+)? -->\s*)+(?:\*\*Advisory\*\*[^\n]*\n\s*)?`)
	severityOf     = regexp.MustCompile(`^<!-- mergewake-severity:([a-z]+) -->`)
)

// stripFinding removes the marker lines mergewake adds to a posted finding.
func stripFinding(body string) string { return findingMarkers.ReplaceAllString(body, "") }

// priorItem is a finding mergewake posted in an earlier round, with its
// disposition.
type priorItem struct {
	path        string
	line        int
	severity    string
	advisory    bool
	finding     string
	disposition string
}

// priorFindings lists the threads the review login started, newest first,
// each with the disposition it reached: resolved, answered, or still open.
func priorFindings(fb github.ReviewFeedback, own string) []priorItem {
	var out []priorItem
	for i := len(fb.Threads) - 1; i >= 0; i-- {
		t := fb.Threads[i]
		if len(t.Comments) == 0 || own == "" || !strings.EqualFold(t.Comments[0].Author, own) {
			continue
		}
		first := t.Comments[0]
		it := priorItem{path: t.Path, line: t.Line, finding: excerpt(stripFinding(first.Body), excerptLimit), advisory: strings.Contains(first.Body, github.AdvisoryMarker)}
		if m := severityOf.FindStringSubmatch(first.Body); m != nil {
			it.severity = m[1]
		}
		reply := ""
		for j := len(t.Comments) - 1; j > 0; j-- {
			c := t.Comments[j]
			if strings.TrimSpace(c.Body) != "" && c.Author != first.Author {
				reply = excerpt(c.Body, excerptLimit)
				break
			}
		}
		switch {
		case t.IsResolved && reply != "":
			it.disposition = "resolved; reply: " + reply
		case t.IsResolved:
			it.disposition = "resolved"
		case reply != "":
			it.disposition = "answered: " + reply
		default:
			it.disposition = "none yet (still open)"
		}
		out = append(out, it)
	}
	return out
}

// knownThread is a finding already on the PR: its path and text.
type knownThread struct{ path, body string }

func knownThreads(fb github.ReviewFeedback) []knownThread {
	var out []knownThread
	for _, t := range fb.Threads {
		if len(t.Comments) > 0 {
			out = append(out, knownThread{t.Path, stripFinding(t.Comments[0].Body)})
		}
	}
	return out
}

func knownFromFindings(fs []github.Finding) []knownThread {
	var out []knownThread
	for _, f := range fs {
		out = append(out, knownThread{f.Path, f.Body})
	}
	return out
}

func words(s string) map[string]bool {
	set := map[string]bool{}
	for _, w := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		if len(w) >= 3 {
			set[w] = true
		}
	}
	return set
}

// sameSubstance reports two finding texts that say the same thing: their word
// sets overlap by half, or a short one is almost contained in a longer one.
func sameSubstance(a, b string) bool {
	wa, wb := words(a), words(b)
	if len(wa) == 0 || len(wb) == 0 {
		return false
	}
	both := 0
	for w := range wa {
		if wb[w] {
			both++
		}
	}
	smaller := min(len(wa), len(wb))
	return 2*both >= len(wa)+len(wb)-both || (smaller >= 6 && 5*both >= 4*smaller)
}

// shaped is a pass's findings after the round rules, with what was removed.
type shaped struct {
	kept                         []github.Finding
	duplicates, advisory, budget int
}

func blockingSeverity(s string) bool { return s == "critical" || s == "high" || s == "blocking" }

// shapeFindings applies, in order: no duplicate of a finding already on the PR
// (same path and substance), the Acceptance contract (an outside finding is
// advisory and at most medium, unless critical), and the round budget (quiet
// rounds keep only critical and high).
func shapeFindings(in []github.Finding, rp roundPlan, known []knownThread) shaped {
	var out shaped
	for _, f := range in {
		dup := false
		for _, k := range known {
			if k.path == f.Path && sameSubstance(f.Body, k.body) {
				dup = true
				break
			}
		}
		if dup {
			out.duplicates++
			continue
		}
		if rp.contract != "" && f.Advisory && f.Severity != "critical" {
			switch f.Severity {
			case "", "low", "minor":
				f.Severity = "low"
			default:
				f.Severity = "medium"
			}
			out.advisory++
		} else {
			f.Advisory = false
		}
		if rp.quiet && !blockingSeverity(f.Severity) {
			out.budget++
			continue
		}
		out.kept = append(out.kept, f)
	}
	return out
}
