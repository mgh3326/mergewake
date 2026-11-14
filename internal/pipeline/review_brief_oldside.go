package pipeline

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// A removed line of a private key shows nothing that says it belongs to a key
// when the BEGIN marker lies outside its hunk. The old-side file does: this
// file marks the private-key line ranges of a file's base text, so a removed
// line can be matched by its old-side line number instead of by its shape.

const (
	// oldSideFileCap bounds one old-side file; a larger one is not scanned.
	oldSideFileCap = 1 << 20
	// oldSideBudget bounds the old-side bytes read for one brief; past it the
	// remaining files keep the in-hunk rules only.
	oldSideBudget = 4 << 20
	// oldSideTimeout bounds all old-side reads of one brief.
	oldSideTimeout = 30 * time.Second
)

// Why a file's removed lines were withheld from the brief.
const (
	oldSideReadFailed = "read_failed"
	oldSideTooLarge   = "too_large"
	oldSideOverBudget = "over_budget"
	oldSideMisaligned = "misaligned"
	// oldSideNotLoaded: no loader or no reader was given, or the path is empty.
	oldSideNotLoaded = "not_loaded"
)

// errOldSideNoClient is the read error of a brief built without a GitHub client.
var errOldSideNoClient = errors.New("no GitHub client to read the base revision")

// oldSide is the base text of one file, with the lines that belong to a
// private key marked.
type oldSide struct {
	lines  []string
	secret []bool
}

// oldSideFallback is one file with removed lines withheld from the brief: why
// its old side was not used, and how much of the file was withheld.
type oldSideFallback struct {
	Path          string `json:"path"`
	Reason        string `json:"reason"`
	Coverage      string `json:"coverage"` // "none" or "partial"
	WithheldHunks int    `json:"withheld_hunks"`
	WithheldLines int    `json:"withheld_lines"`
}

// oldSideAudit counts the hunks of one file that have removed lines.
type oldSideAudit struct {
	reason        string
	covered       int
	withheldHunks int
	withheldLines int
}

// oldSideLoader reads and scans old-side files on demand, once per path, within
// the byte budget. The zero value reads nothing: every lookup is absent.
type oldSideLoader struct {
	read      func(path string) ([]byte, error)
	used      int
	files     map[string]*oldSide
	unscanned map[string]string
	audit     map[string]*oldSideAudit
}

func newOldSideLoader(read func(path string) ([]byte, error)) *oldSideLoader {
	return &oldSideLoader{read: read, files: map[string]*oldSide{}, unscanned: map[string]string{}, audit: map[string]*oldSideAudit{}}
}

// lookup returns the scanned old side of path, or nil when it is unavailable.
func (l *oldSideLoader) lookup(path string) *oldSide {
	if l == nil || l.read == nil || path == "" {
		return nil
	}
	if o, ok := l.files[path]; ok {
		return o
	}
	l.files[path] = nil
	if l.used >= oldSideBudget {
		l.unscanned[path] = oldSideOverBudget
		return nil
	}
	body, err := l.read(path)
	if err != nil {
		l.unscanned[path] = oldSideReadFailed
		return nil
	}
	l.used += len(body)
	if len(body) > oldSideFileCap {
		l.unscanned[path] = oldSideTooLarge
		return nil
	}
	o := scanOldSide(body)
	l.files[path] = o
	return o
}

// unscannedReason says why lookup found no old side for path.
func (l *oldSideLoader) unscannedReason(path string) string {
	if l != nil && l.unscanned[path] != "" {
		return l.unscanned[path]
	}
	return oldSideNotLoaded
}

// note records one hunk of path that has removed lines: covered when its old
// side matched, otherwise its lines withheld for reason.
func (l *oldSideLoader) note(path string, covered bool, reason string, lines int) {
	if l == nil {
		return
	}
	if l.audit == nil {
		l.audit = map[string]*oldSideAudit{}
	}
	a := l.audit[path]
	if a == nil {
		a = &oldSideAudit{}
		l.audit[path] = a
	}
	if covered {
		a.covered++
		return
	}
	a.withheldHunks++
	a.withheldLines += lines
	if a.reason == "" {
		a.reason = reason
	}
}

// skipped lists the files with withheld removed lines, sorted by path. A file
// is "partial" when some of its hunks were covered and "none" when none were.
func (l *oldSideLoader) skipped() []oldSideFallback {
	if l == nil {
		return nil
	}
	var out []oldSideFallback
	for p, a := range l.audit {
		if a.withheldHunks == 0 {
			continue
		}
		coverage := "none"
		if a.covered > 0 {
			coverage = "partial"
		}
		out = append(out, oldSideFallback{Path: p, Reason: a.reason, Coverage: coverage, WithheldHunks: a.withheldHunks, WithheldLines: a.withheldLines})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// withheldMarker stands in for a run of n removed lines whose old side was not
// checked: it keeps the removed-line style and says why the text is missing.
func withheldMarker(n int, reason string) string {
	return fmt.Sprintf("-[%d removed lines withheld: old side not scanned (%s)]\n", n, reason)
}

// withheldNotice tells the reviewer once that removed lines were withheld and
// why, so that a finding is not raised about them as missing.
func withheldNotice(files []oldSideFallback) string {
	var paths []string
	reasons := map[string]bool{}
	for _, f := range files {
		paths = append(paths, f.Path)
		reasons[f.Reason] = true
	}
	names := make([]string, 0, len(reasons))
	for r := range reasons {
		names = append(names, r)
	}
	sort.Strings(names)
	return fmt.Sprintf("\nSome removed lines were withheld from this delta, because their base-revision text could not be checked (paths: %s; reasons: %s). Each withheld run is shown as a marker line in place of the removed lines. Do not report those lines as missing.\n",
		strings.Join(paths, ", "), strings.Join(names, ", "))
}

func scanOldSide(body []byte) *oldSide {
	lines := strings.Split(string(body), "\n")
	if n := len(lines); n > 0 && lines[n-1] == "" {
		lines = lines[:n-1]
	}
	for i, l := range lines {
		// A CRLF file's lines carry no carriage return: every scan below reads
		// text, and none of them should have to know the line ending.
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	secret := make([]bool, len(lines))
	parsed := markScalarPrivateKeys(body, lines, secret)
	for i, m := range markPrivateKeyLines(lines, parsed) {
		secret[i] = secret[i] || m
	}
	return &oldSide{lines: lines, secret: secret}
}

// aligned reports whether the old and context lines of a hunk that starts at
// old-side line start are the lines the file has there. A hunk cut from another
// revision of the file fails this, and its line numbers are then not used.
func (o *oldSide) aligned(start int, hunk []string) bool {
	n := start
	for _, l := range hunk {
		if l == "" || (l[0] != '-' && l[0] != ' ') {
			continue
		}
		if n < 1 || n > len(o.lines) || strings.TrimRight(o.lines[n-1], "\r") != strings.TrimRight(l[1:], "\r\n") {
			return false
		}
		n++
	}
	return true
}

// secretAt reports whether old-side line n (1-based) is part of a private key.
func (o *oldSide) secretAt(n int) bool {
	return o != nil && n >= 1 && n <= len(o.secret) && o.secret[n-1]
}

var (
	pemKeyBegin = "-----" + "BEGIN "
	pemKeyEnd   = "-----" + "END "
	// pemHeaderLine is an RFC 1421 header inside a key block, such as
	// Proc-Type, DEK-Info, or the armor headers of a PGP block.
	pemHeaderLine = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]*: `)
	base64Only    = regexp.MustCompile(`^[A-Za-z0-9+/=]+$`)
	base64Run     = regexp.MustCompile(`[A-Za-z0-9+/]{24,}={0,2}`)
)

// markPrivateKeyLines marks the lines of PEM private-key blocks (any type whose
// name ends in PRIVATE KEY, so RSA, EC, OPENSSH and ENCRYPTED too, and the PGP
// private-key armor) and of base64-encoded PEM private keys, as found in
// Kubernetes secrets. Certificates and public keys are not marked.
func markPrivateKeyLines(lines []string, parsed bool) []bool {
	secret := make([]bool, len(lines))
	for i := 0; i < len(lines); {
		t := strings.TrimSpace(lines[i])
		switch {
		case privateKeyBegin.MatchString(t):
			i = markPEMBlock(lines, secret, i)
		case !parsed && base64PEMEnd(t) > 0:
			// A file that parses as YAML or JSON has its base64 keys marked by
			// scalar; one that does not is read by line.
			i = markWrappedBase64(lines, secret, i)
		default:
			i++
		}
	}
	return secret
}

// pemBlockSpan bounds how far a BEGIN marker looks for its END.
const pemBlockSpan = 4096

// markPEMBlock marks the block that begins at line i and returns the index of
// the first line after it. A block with its END marker is marked whole, whatever
// its lines look like (PEM readers accept spaces inside a body); one the file
// leaves unterminated ends at the first line that cannot be key text.
func markPEMBlock(lines []string, secret []bool, i int) int {
	label := privateKeyBegin.FindStringSubmatch(strings.TrimSpace(lines[i]))[1]
	endMarker := pemKeyEnd + label + "-----"
	for j := i + 1; j < len(lines) && j <= i+pemBlockSpan; j++ {
		if strings.HasPrefix(strings.TrimSpace(lines[j]), endMarker) {
			for k := i; k <= j; k++ {
				secret[k] = true
			}
			return j + 1
		}
	}
	secret[i] = true
	j := i + 1
	for ; j < len(lines); j++ {
		t := strings.TrimSpace(lines[j])
		if t != "" && !base64Only.MatchString(strings.NewReplacer(" ", "", "\t", "").Replace(t)) && !pemHeaderLine.MatchString(t) {
			break
		}
		secret[j] = true
	}
	for k := j - 1; k > i && strings.TrimSpace(lines[k]) == ""; k-- {
		secret[k] = false
	}
	return j
}

// markWrappedBase64 marks line i, which holds the start of a base64 private key
// that no YAML or JSON parse accounted for (a file that does not parse), and the
// base64-only lines that follow it.
func markWrappedBase64(lines []string, secret []bool, i int) int {
	secret[i] = true
	j := i + 1
	for ; j < len(lines); j++ {
		c := strings.TrimSpace(lines[j])
		if c == "" || !base64Only.MatchString(c) {
			break
		}
		secret[j] = true
	}
	return j
}

// base64PEMEnd returns the end offset of the first base64 run in line that
// decodes to a PEM private-key header, or 0 when there is none. Kubernetes
// secrets hold keys this way (`tls.key: LS0tLS1CRUdJTi…`). Only the first bytes
// are decoded, so no other base64 (a go.sum hash, a lockfile integrity value,
// a certificate) matches: only text that opens with the BEGIN marker does.
func base64PEMEnd(line string) int {
	_, end := base64PEMRun(line)
	return end
}

// base64PEMRun is base64PEMEnd with the run's start offset too.
func base64PEMRun(line string) (start, end int) {
	for _, loc := range base64Run.FindAllStringIndex(line, -1) {
		run := line[loc[0]:loc[1]]
		run = strings.TrimRight(run, "=")
		if len(run) > 256 {
			run = run[:256]
		}
		run = run[:len(run)/4*4]
		dec, err := base64.StdEncoding.DecodeString(run)
		if err != nil {
			continue
		}
		if startsPrivateKey(string(dec)) {
			return loc[0], loc[1]
		}
	}
	return 0, 0
}
