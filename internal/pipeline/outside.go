package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// An outside-diff finding is a critical, high or blocking finding a review
// pass could not anchor in the diff and so listed in its review body. The
// gate tracks each one across heads: it stays open until a person answers it
// or a later completed review does not report it again, and a finding
// answered or re-reviewed never comes back.

const (
	outsideOpen     = "open"
	outsideAnswered = "answered"
	// outsideReviewed is the re-reviewed terminal state: a later settled pass
	// reviewed the code and did not report the finding again.
	outsideReviewed = "re-reviewed"
)

// outsideFinding is one tracked outside-diff finding. Head is the head whose
// pass most recently reported it; State is open, answered or re-reviewed;
// FirstAt is when the finding was first posted, so an answer written before
// it cannot settle it.
type outsideFinding struct {
	Ref      string
	Aliases  []string
	Lines    []int
	Titles   []string
	Path     string
	Line     int
	Severity string
	Advisory bool
	Body     string
	Head     string
	State    string
	Via      string
	Answer   string
	FirstAt  time.Time
}

const outsideLineWindow = 3

// sameOutsideFinding joins a re-listing to the earlier finding only when its
// path and nearby line agree and its wording still describes the same issue.
// The small line window tolerates shifted line numbers; the text check avoids
// merging two distinct findings merely because they are nearby.
func sameOutsideFinding(a outsideFinding, b github.OutsideFinding) bool {
	if a.Path != b.Path || b.Line <= 0 {
		return false
	}
	dist, ok := outsideDistance(a, b.Line)
	if !ok || dist > outsideLineWindow {
		return false
	}
	return sameSubstance(a.Body, b.Body)
}

func outsideDistance(a outsideFinding, line int) (int, bool) {
	if a.Line <= 0 && len(a.Lines) == 0 {
		return 0, false
	}
	minDist := -1
	check := func(l int) {
		if l <= 0 {
			return
		}
		d := l - line
		if d < 0 {
			d = -d
		}
		if minDist == -1 || d < minDist {
			minDist = d
		}
	}
	check(a.Line)
	for _, l := range a.Lines {
		check(l)
	}
	if minDist == -1 {
		return 0, false
	}
	return minDist, true
}

func markOutsideDisputes(records []outsideFinding) {
	for i := range records {
		if records[i].State != outsideOpen {
			continue
		}
		for j := range records {
			if records[j].State != outsideAnswered || records[i].Path != records[j].Path {
				continue
			}
			dist, ok := outsideDistance(records[j], records[i].Line)
			if ok && dist <= outsideLineWindow && !sameSubstance(records[i].Body, records[j].Body) {
				records[i].Via = "dispute"
				break
			}
		}
	}
}

func hasOutsideDispute(records []outsideFinding) bool {
	for _, f := range records {
		if f.State == outsideOpen && f.Via == "dispute" {
			return true
		}
	}
	return false
}

// foldOutside replays the posted passes oldest first and returns every
// outside-diff finding they reported, each in its current state. Passes are
// grouped by head; a head's passes union into what that head's review
// reported. The gated head contributes only passes whose patch id matches —
// hk 3190's rule — while every earlier head, the inherited head included,
// contributes all of its passes, because its findings are exactly what must
// persist across the push.
//
// A finding stays open until a settled later head does not report it again:
// settled means the head is not the gated one, or the gated head's own review
// completed (settledCurrent). A settling pass could only stay silent on a
// finding it was shown — the prompt lists every finding still open and the
// pass receipt records which refs it rendered — so only a ref in a pass's
// OutsideShown can be re-reviewed away. Passes posted before outside findings
// entered prompts carry no OutsideShown and can clear nothing, and neither
// can a pass whose context budget dropped the finding. Terminal states are
// sticky: once answered or re-reviewed, a later report of the same ref does
// not reopen it.
func foldOutside(history []github.ReviewedPass, head, patch, inherited string, settledCurrent bool) []outsideFinding {
	var order []string
	groups := map[string][]github.ReviewedPass{}
	for _, p := range history {
		if _, ok := groups[p.Head]; !ok {
			order = append(order, p.Head)
		}
		groups[p.Head] = append(groups[p.Head], p)
	}
	var out []outsideFinding
	open := map[string]int{}
	closed := map[string]bool{}
	aliases := map[string]string{}
	for _, h := range order {
		passes := groups[h]
		// The inherited head's findings persist whatever its patch: a clean
		// base merge inherits a review of a different patch, and dropping its
		// passes would let the chain clear a finding it never settled. Only
		// the gated head's own passes are patch-filtered, and an inherited
		// head settles only when one of its passes carries the current patch.
		own := h == head
		settled := !(own || (inherited != "" && h == inherited)) || settledCurrent
		if inherited != "" && h == inherited && !own && patch != "" && settledCurrent {
			settled = false
			for _, p := range passes {
				if p.Patch == patch {
					settled = true
				}
			}
		}
		reported := map[string]github.OutsideFinding{}
		var repOrder []string
		shown := map[string]bool{}
		reportedAt := map[string]time.Time{}
		repLines := map[string][]int{}
		repTitles := map[string][]string{}
		repAliases := map[string][]string{}
		for _, p := range passes {
			if own && patch != "" && p.Patch != patch {
				continue
			}
			for _, ref := range strings.Fields(p.OutsideShown) {
				if canonical, ok := aliases[ref]; ok {
					ref = canonical
				}
				shown[ref] = true
			}
			for _, f := range p.Outside {
				if !f.Blocking() {
					continue
				}
				ref := f.Ref
				if canonical, ok := aliases[ref]; ok {
					ref = canonical
				} else {
					matched := false
					for i := range out {
						if sameOutsideFinding(out[i], f) {
							ref = out[i].Ref
							out[i].Lines = appendUniqueInt(out[i].Lines, f.Line)
							out[i].Titles = appendUnique(out[i].Titles, f.Body)
							if f.Ref != ref {
								out[i].Aliases = appendUnique(out[i].Aliases, f.Ref)
								aliases[f.Ref] = ref
							}
							matched = true
							break
						}
					}
					if !matched {
						for _, prevRef := range repOrder {
							prev := reported[prevRef]
							if sameOutsideFinding(outsideFinding{Path: prev.Path, Line: prev.Line, Lines: repLines[prevRef], Body: prev.Body, Titles: repTitles[prevRef]}, f) {
								ref = prevRef
								if f.Ref != ref {
									aliases[f.Ref] = ref
									repAliases[ref] = appendUnique(repAliases[ref], f.Ref)
								}
								matched = true
								break
							}
						}
					}
				}
				repLines[ref] = appendUniqueInt(repLines[ref], f.Line)
				repTitles[ref] = appendUnique(repTitles[ref], f.Body)
				if p.SubmittedAt != nil {
					if t, ok := reportedAt[ref]; !ok || p.SubmittedAt.Before(t) {
						reportedAt[ref] = *p.SubmittedAt
					}
				}
				if _, ok := reported[ref]; !ok {
					reported[ref] = f
					repOrder = append(repOrder, ref)
				}
			}
		}
		// A head that reports an open finding again keeps it open and moves
		// its head pointer to the latest reporter.
		for _, ref := range repOrder {
			if idx, ok := open[ref]; ok {
				out[idx].Head = h
			}
		}
		// A settled review that does not report an open finding re-reviews it,
		// but only a finding one of its passes was actually shown.
		if settled {
			for ref, idx := range open {
				if _, ok := reported[ref]; !ok && shown[ref] {
					out[idx].State = outsideReviewed
					out[idx].Via = "rereview"
					closed[ref] = true
					delete(open, ref)
				}
			}
		}
		for _, ref := range repOrder {
			if closed[ref] {
				continue
			}
			if idx, ok := open[ref]; ok {
				out[idx].Lines = appendUniqueInts(out[idx].Lines, repLines[ref])
				out[idx].Titles = appendUniques(out[idx].Titles, repTitles[ref])
				out[idx].Aliases = appendUniques(out[idx].Aliases, repAliases[ref])
				continue
			}
			f := reported[ref]
			out = append(out, outsideFinding{
				Ref: ref, Aliases: repAliases[ref], Path: f.Path, Line: f.Line, Lines: repLines[ref], Titles: repTitles[ref],
				Severity: f.Severity, Advisory: f.Advisory, Body: f.Body, Head: h, State: outsideOpen,
				FirstAt: reportedAt[ref],
			})
			open[ref] = len(out) - 1
		}
	}
	return out
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniques(values []string, additions []string) []string {
	for _, a := range additions {
		values = appendUnique(values, a)
	}
	return values
}

func appendUniqueInt(values []int, value int) []int {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueInts(values []int, additions []int) []int {
	for _, a := range additions {
		values = appendUniqueInt(values, a)
	}
	return values
}

// outsideLegacy counts the outside-diff hold that receipts written before the
// review body listed findings can still carry: OutsideBlocking on the current
// head's (or inherited head's) patch-matching passes, minus what the parsed
// list already accounts for. It keeps today's current-head-only semantics —
// an opaque count has no identity to track across heads.
func outsideLegacy(history []github.ReviewedPass, head, patch, inherited string) int {
	n := 0
	for _, p := range history {
		own := p.Head == head && (patch == "" || p.Patch == patch)
		inh := inherited != "" && p.Head == inherited && p.Patch == patch
		if !own && !inh {
			continue
		}
		parsed := 0
		for _, f := range p.Outside {
			if f.Blocking() {
				parsed++
			}
		}
		if d := p.OutsideBlocking - parsed; d > 0 {
			n += d
		}
	}
	return n
}

var (
	// outsideAnswerPrefix accepts one or more finding refs before an answer
	// form. Refs can be stable outside ids or path:line locations, optionally
	// quoted with backticks and separated from each other or the answer by
	// whitespace or punctuation.
	outsideAnswerPrefix = `\W*(?:(?:` + "`?od-[[:xdigit:]]+`?|`?[\\w./-]+:[0-9]+`?" + `)(?:\s*[:\-—|]\s*|\s+))*`
	// outsideFixForm is the fix disposition in a PR comment. The forms are
	// anchored to a line start so "do not dismiss:" or "not fixed in" mid-
	// sentence cannot pass for an answer.
	outsideFixForm = regexp.MustCompile(`(?im)^` + outsideAnswerPrefix + `fixed\s+in\b`)
	// outsideDismissForm is the dismiss disposition with its reason, bare
	// ("Dismiss: …") or under "Disposition:" ("Disposition: dismiss …").
	outsideDismissForm = regexp.MustCompile(`(?im)^` + outsideAnswerPrefix + `(?:disposition\s*:\s*)?dismiss\w*\s*:?\s*\S`)
	// outsideDeferForm is the defer disposition naming its task, in the form
	// deferTaskRef accepts (gate.go).
	outsideDeferForm = regexp.MustCompile(`(?im)^` + outsideAnswerPrefix + `disposition\s*:\s*defer\s+` + deferTaskRef)
)

// outsideTitle is the short text BLOCKED and answers show for a finding.
func outsideTitle(body string) string { return excerpt(body, 80) }

// outsideNames reports whether a comment names the finding: by its ref, its
// path:line, or its title when the title is long enough to identify it.
func outsideNames(comment string, f outsideFinding) bool {
	if strings.Contains(comment, f.Ref) {
		return true
	}
	for _, alias := range f.Aliases {
		if strings.Contains(comment, alias) {
			return true
		}
	}
	if f.Path != "" {
		if f.Line > 0 && outsidePathLine(comment, f.Path, f.Line) {
			return true
		}
		for _, line := range f.Lines {
			if line > 0 && outsidePathLine(comment, f.Path, line) {
				return true
			}
		}
	}
	title := outsideTitle(f.Body)
	if utf8.RuneCountInString(title) >= 12 && strings.Contains(comment, title) {
		return true
	}
	for _, body := range f.Titles {
		title = outsideTitle(body)
		if utf8.RuneCountInString(title) >= 12 && strings.Contains(comment, title) {
			return true
		}
	}
	return false
}

// outsidePathLine reports whether comment carries "path:line" as a whole
// token: not glued to more path on the left ("xdocs/a.go:3") nor to more of
// the number or a word on the right ("docs/a.go:30", "docs/a.go:3x").
func outsidePathLine(comment, path string, line int) bool {
	needle := path + ":" + strconv.Itoa(line)
	for at := 0; at+len(needle) <= len(comment); {
		i := strings.Index(comment[at:], needle)
		if i < 0 {
			return false
		}
		i += at
		end := i + len(needle)
		leftOK := i == 0 || !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./-", rune(comment[i-1]))
		rightOK := end == len(comment) || !isNameByte(comment[end])
		if leftOK && rightOK {
			return true
		}
		at = i + 1
	}
	return false
}

func isNameByte(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b == '_'
}

// outsideAnswerable accepts human comments unless the body marks them as
// generated by mergewake. The review login is allowed to answer; the content
// marker, not the account identity, separates notices from human replies.
func outsideAnswerable(c github.IssueComment) bool {
	if c.User.Type == "Bot" || strings.HasSuffix(c.User.Login, "[bot]") {
		return false
	}
	return !strings.Contains(c.Body, "<!-- mergewake")
}

// outsideAnswerLine reports whether the line matches one of the accepted outside
// answer forms (fix, dismiss or defer).
func outsideAnswerLine(line string) bool {
	return outsideDismissForm.MatchString(line) || outsideDeferForm.MatchString(line) || outsideFixForm.MatchString(line)
}

// outsideLineNames reports whether a single line names the finding by its ref,
// an alias, or its path:line. A title match is intentionally excluded on answer
// lines so that descriptive text does not accidentally bind.
func outsideLineNames(line string, f outsideFinding) bool {
	if strings.Contains(line, f.Ref) {
		return true
	}
	for _, alias := range f.Aliases {
		if strings.Contains(line, alias) {
			return true
		}
	}
	if f.Path != "" {
		if f.Line > 0 && outsidePathLine(line, f.Path, f.Line) {
			return true
		}
		for _, l := range f.Lines {
			if l > 0 && outsidePathLine(line, f.Path, l) {
				return true
			}
		}
	}
	return false
}

// outsideAnsweredBy reports whether a comment line carries an accepted answer for
// the finding: a fix naming a commit verify accepts, a dismiss with a reason,
// or a defer naming a task.
func outsideAnsweredBy(line string, f outsideFinding, verify func(sha, posted string) bool) bool {
	if outsideDismissForm.MatchString(line) || outsideDeferForm.MatchString(line) {
		return true
	}
	if !outsideFixForm.MatchString(line) {
		return false
	}
	for _, sha := range commitToken.FindAllString(line, -1) {
		if verify != nil && verify(sha, f.Head) {
			return true
		}
	}
	return false
}

// answerOutside marks every still-open finding a person's comment answers.
// verify is the fix verifier for "Fixed in <sha>" claims; nil refuses them.
func answerOutside(comments []github.IssueComment, records []outsideFinding, verify func(sha, posted string) bool) {
	for i := range records {
		if records[i].State != outsideOpen {
			continue
		}
		f := &records[i]
		for _, c := range comments {
			if !outsideAnswerable(c) {
				continue
			}
			// An answer written before the finding was first reported —
			// e.g. a dismissal of an earlier finding at the same path:line —
			// cannot settle this one.
			if !f.FirstAt.IsZero() && !c.CreatedAt.IsZero() && c.CreatedAt.Before(f.FirstAt) {
				continue
			}

			lines := strings.Split(c.Body, "\n")
			answered := false
			commentNamesFinding := false

			// Check each line: an answer line naming one or more findings answers
			// exactly those findings.
			for _, line := range lines {
				if outsideAnswerLine(line) {
					namesAnyFinding := false
					for _, other := range records {
						if outsideLineNames(line, other) {
							namesAnyFinding = true
							break
						}
					}
					if namesAnyFinding {
						if outsideLineNames(line, *f) && outsideAnsweredBy(line, *f, verify) {
							answered = true
							break
						}
						// This answer line named other finding(s) but not f;
						// do not fall back to comment-level matching for this line.
						continue
					}
					// Answer line named no finding; it can answer findings named elsewhere
					// in the comment.
					if !commentNamesFinding {
						commentNamesFinding = outsideNames(c.Body, *f)
					}
					if commentNamesFinding && outsideAnsweredBy(line, *f, verify) {
						answered = true
						break
					}
				}
			}

			if answered {
				f.State = outsideAnswered
				f.Via = "comment"
				f.Answer = excerpt(c.Body, excerptLimit)
				break
			}
		}
	}
}

// outsidePriorItems renders open and answered outside-diff findings as prior
// findings for the reviewer prompt. Re-reviewed findings need no reminder.
func outsidePriorItems(records []outsideFinding) []priorItem {
	var out []priorItem
	for _, f := range records {
		if f.State != outsideOpen && f.State != outsideAnswered {
			continue
		}
		disposition := "none yet (still open)"
		if f.State == outsideAnswered {
			disposition = "answered: " + f.Answer
		}
		item := priorItem{
			path: f.Path, line: f.Line, severity: f.Severity,
			finding: excerpt(f.Body, excerptLimit), disposition: disposition,
		}
		if f.State == outsideOpen {
			item.ref = f.Ref
		}
		out = append(out, item)
	}
	return out
}

// outsideGate folds the posted outside-diff findings into their states,
// applies the PR's comments as answers, and returns the records plus the
// opaque legacy count still holding the merge.
func (s *Service) outsideGate(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, patch, inherited string, settledCurrent bool, fixes *fixVerifierState) ([]outsideFinding, int, error) {
	records := foldOutside(history, pr.Head.SHA, patch, inherited, settledCurrent)
	legacy := outsideLegacy(history, pr.Head.SHA, patch, inherited)
	need := legacy > 0
	for _, f := range records {
		if f.State == outsideOpen {
			need = true
			break
		}
	}
	if need {
		comments, e := s.GH.IssueComments(github.FreshEvidence(ctx), r.Name, pr.Number)
		if e != nil {
			return nil, 0, e
		}
		answerOutside(comments, records, fixes.verified)
		if fixes.err != nil {
			return nil, 0, fixes.err
		}
		markOutsideDisputes(records)
		// Receipts from before findings carried identity still answer the
		// old way: a person's dismissal naming this head's SHA prefix.
		if legacy > 0 && outsideDismissed(comments, pr.Head.SHA, s.Config.Review.Login) {
			legacy = 0
		}
	}
	return records, legacy, nil
}

// openOutsideForPrompt lists the outside-diff findings still open for the
// reviewer prompt. The gated head's own in-progress passes cannot settle a
// re-review, so nothing here is inherited and the current review is not
// settled; answers apply with the real fix verifier, so an unverifiable
// "Fixed in" claim leaves the finding visible.
func (s *Service) outsideForPrompt(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, patch string) []outsideFinding {
	records := foldOutside(history, pr.Head.SHA, patch, "", false)
	comments, _ := s.issueComments(ctx, r.Name, pr.Number)
	// Fix claims verify for real here too: an unverifiable "Fixed in" must
	// not hide a finding from every later prompt, or its re-review exit
	// becomes unreachable and the hold never lifts on its own.
	answerOutside(comments, records, s.fixVerifier(ctx, r.Name, pr.Head.SHA).verified)
	markOutsideDisputes(records)
	return records
}

// openOutsideForPrompt keeps the gate's view limited to findings that can
// still hold a merge; answered findings are included only in reviewer context.
func (s *Service) openOutsideForPrompt(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, patch string) []outsideFinding {
	var open []outsideFinding
	for _, f := range s.outsideForPrompt(ctx, r, pr, history, patch) {
		if f.State == outsideOpen {
			open = append(open, f)
		}
	}
	return open
}

// outsideBlockedText is the BLOCKED notice's list of still-open outside-diff
// findings, so the author can answer each by name.
func outsideBlockedText(findings []outsideFinding) string {
	var open []outsideFinding
	for _, f := range findings {
		if f.State == outsideOpen {
			open = append(open, f)
		}
	}
	if len(open) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("outside-diff findings still open:")
	for i, f := range open {
		if i == 5 {
			fmt.Fprintf(&b, " +%d more", len(open)-5)
			break
		}
		fmt.Fprintf(&b, " %s %s:%d (%s) %s;", f.Ref, f.Path, f.Line, f.Severity, outsideTitle(f.Body))
	}
	return strings.TrimSuffix(b.String(), ";")
}

// outsideAuditFile is the per-PR record of outside-diff finding states the
// audit diffs against, so a gate pass logs only transitions.
func (s *Service) outsideAuditFile(repo string, n int) string {
	return filepath.Join(s.state(repo), "outside-findings", strconv.Itoa(n)+".json")
}

// auditOutsideFindings writes one audit line per outside-diff finding whose
// state changed since the last reporting pass: open when first seen, then
// answered or re-reviewed. A nil Outside means the gate did not evaluate
// outside findings this pass (pending checks, an early verdict) and nothing
// is audited.
func (s *Service) auditOutsideFindings(r config.Repo, pr github.Pull, v gateVerdict) error {
	if v.Outside == nil {
		return nil
	}
	path := s.outsideAuditFile(r.Name, pr.Number)
	prev := map[string]string{}
	_, _ = fsutil.ReadJSON(path, &prev)
	next := make(map[string]string, len(v.Outside))
	var errs []error
	changed := false
	for _, f := range v.Outside {
		next[f.Ref] = f.State
		if prev[f.Ref] == f.State {
			continue
		}
		changed = true
		if err := s.audit(r.Name, "outside-finding", map[string]any{
			"pr":       pr.Number,
			"ref":      f.Ref,
			"path":     f.Path,
			"line":     f.Line,
			"severity": f.Severity,
			"head":     f.Head,
			"state":    f.State,
			"via":      f.Via,
		}, false); err != nil {
			// A failed transition keeps its previous state — or none for a
			// new ref — so the next pass retries the audit line.
			errs = append(errs, err)
			delete(next, f.Ref)
		}
	}
	for ref, state := range prev {
		if _, ok := next[ref]; !ok {
			next[ref] = state
		}
	}
	if changed {
		if err := fsutil.WriteJSON(path, next); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
