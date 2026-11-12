package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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
// pass most recently reported it; State is open, answered or re-reviewed.
type outsideFinding struct {
	Ref      string
	Path     string
	Line     int
	Severity string
	Advisory bool
	Body     string
	Head     string
	State    string
	Via      string
}

// foldOutside replays the posted passes oldest first and returns every
// outside-diff finding they reported, each in its current state. Passes are
// grouped by head; a head's passes union into what that head's review
// reported. The current head (and the inherited head, when the gate adopted
// an earlier head's review) contributes only passes whose patch id matches —
// hk 3190's rule — while every earlier head contributes all of its passes,
// because its findings are exactly what must persist across the push.
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
	for _, h := range order {
		passes := groups[h]
		current := h == head || (inherited != "" && h == inherited)
		settled := !current || settledCurrent
		reported := map[string]github.OutsideFinding{}
		var repOrder []string
		shown := map[string]bool{}
		for _, p := range passes {
			if current && patch != "" && p.Patch != patch {
				continue
			}
			for _, ref := range strings.Fields(p.OutsideShown) {
				shown[ref] = true
			}
			for _, f := range p.Outside {
				if !f.Blocking() {
					continue
				}
				if _, ok := reported[f.Ref]; !ok {
					reported[f.Ref] = f
					repOrder = append(repOrder, f.Ref)
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
			if _, ok := open[ref]; ok {
				continue
			}
			f := reported[ref]
			out = append(out, outsideFinding{
				Ref: f.Ref, Path: f.Path, Line: f.Line, Severity: f.Severity,
				Advisory: f.Advisory, Body: f.Body, Head: h, State: outsideOpen,
			})
			open[ref] = len(out) - 1
		}
	}
	return out
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
	// outsideFixForm is the fix disposition in a PR comment.
	outsideFixForm = regexp.MustCompile(`(?i)\bfixed\s+in\b`)
	// outsideDismissForm is the dismiss disposition with its reason.
	outsideDismissForm = regexp.MustCompile(`(?i)\bdismiss\w*\s*:\s*\S`)
	// outsideDeferForm is the defer disposition naming its task.
	outsideDeferForm = regexp.MustCompile(`(?i)\bdisposition\s*:\s*defer\s+\S*\d\S*`)
)

// outsideTitle is the short text BLOCKED and answers show for a finding.
func outsideTitle(body string) string { return excerpt(body, 80) }

// outsideNames reports whether a comment names the finding: by its ref, its
// path:line, or its title when the title is long enough to identify it.
func outsideNames(comment string, f outsideFinding) bool {
	if strings.Contains(comment, f.Ref) {
		return true
	}
	if f.Path != "" && f.Line > 0 && outsidePathLine(comment, f.Path, f.Line) {
		return true
	}
	title := outsideTitle(f.Body)
	return utf8.RuneCountInString(title) >= 12 && strings.Contains(comment, title)
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

// outsideAnswerable is the person check an answer needs: not a bot, and not
// the review identity itself — the same trust bar as a round-hold release.
func outsideAnswerable(c github.IssueComment, reviewLogin string) bool {
	if c.User.Type == "Bot" || strings.HasSuffix(c.User.Login, "[bot]") {
		return false
	}
	return reviewLogin == "" || c.User.Login != reviewLogin
}

// outsideAnsweredBy reports whether a comment carries an accepted answer for
// the finding: a fix naming a commit verify accepts, a dismiss with a reason,
// or a defer naming a task.
func outsideAnsweredBy(body string, f outsideFinding, verify func(sha, posted string) bool) bool {
	if outsideDismissForm.MatchString(body) || outsideDeferForm.MatchString(body) {
		return true
	}
	if !outsideFixForm.MatchString(body) {
		return false
	}
	for _, sha := range commitToken.FindAllString(body, -1) {
		if verify != nil && verify(sha, f.Head) {
			return true
		}
	}
	return false
}

// answerOutside marks every still-open finding a person's comment answers.
// verify is the fix verifier for "Fixed in <sha>" claims; nil refuses them.
func answerOutside(comments []github.IssueComment, records []outsideFinding, verify func(sha, posted string) bool, reviewLogin string) {
	for i := range records {
		if records[i].State != outsideOpen {
			continue
		}
		f := &records[i]
		for _, c := range comments {
			if !outsideAnswerable(c, reviewLogin) || !outsideNames(c.Body, *f) {
				continue
			}
			if outsideAnsweredBy(c.Body, *f, verify) {
				f.State = outsideAnswered
				f.Via = "comment"
				break
			}
		}
	}
}

// outsidePriorItems renders still-open outside-diff findings as prior
// findings for the reviewer prompt, marked as outside the diff. They carry no
// thread disposition: the gate's own answer tracking owns them.
func outsidePriorItems(open []outsideFinding) []priorItem {
	var out []priorItem
	for _, f := range open {
		if f.State != outsideOpen {
			continue
		}
		out = append(out, priorItem{
			path: f.Path, line: f.Line, severity: f.Severity,
			outside: true, ref: f.Ref,
			finding: excerpt(f.Body, excerptLimit), disposition: priorOpen,
		})
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
		answerOutside(comments, records, fixes.verified, s.Config.Review.Login)
		if fixes.err != nil {
			return nil, 0, fixes.err
		}
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
// settled; answers apply with a permissive fix check — the gate verifies fix
// claims itself.
func (s *Service) openOutsideForPrompt(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, patch string) []outsideFinding {
	records := foldOutside(history, pr.Head.SHA, patch, "", false)
	comments, _ := s.issueComments(ctx, r.Name, pr.Number)
	answerOutside(comments, records, func(string, string) bool { return true }, s.Config.Review.Login)
	var open []outsideFinding
	for _, f := range records {
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
	for _, f := range v.Outside {
		next[f.Ref] = f.State
		if prev[f.Ref] == f.State {
			continue
		}
		errs = append(errs, s.audit(r.Name, "outside-finding", map[string]any{
			"pr":       pr.Number,
			"ref":      f.Ref,
			"path":     f.Path,
			"line":     f.Line,
			"severity": f.Severity,
			"head":     f.Head,
			"state":    f.State,
			"via":      f.Via,
		}, false))
	}
	for ref, state := range prev {
		if _, ok := next[ref]; !ok {
			next[ref] = state
		}
	}
	if len(errs) > 0 {
		if err := fsutil.WriteJSON(path, next); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
