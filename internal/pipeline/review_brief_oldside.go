package pipeline

import (
	"bytes"
	"encoding/base64"
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

// Why a file's old side did not add coverage to the brief.
const (
	oldSideReadFailed = "read_failed"
	oldSideTooLarge   = "too_large"
	oldSideOverBudget = "over_budget"
	oldSideMisaligned = "misaligned"
)

// oldSide is the base text of one file, with the lines that belong to a
// private key marked.
type oldSide struct {
	lines  []string
	secret []bool
}

// oldSideFallback names a file whose old side added no coverage, and why.
type oldSideFallback struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// oldSideLoader reads and scans old-side files on demand, once per path, within
// the byte budget. The zero value and nil read nothing: every lookup is absent.
type oldSideLoader struct {
	read      func(path string) ([]byte, error)
	used      int
	files     map[string]*oldSide
	fallbacks map[string]string
}

func newOldSideLoader(read func(path string) ([]byte, error)) *oldSideLoader {
	return &oldSideLoader{read: read, files: map[string]*oldSide{}, fallbacks: map[string]string{}}
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
		l.fallbacks[path] = oldSideOverBudget
		return nil
	}
	body, err := l.read(path)
	if err != nil {
		l.fallbacks[path] = oldSideReadFailed
		return nil
	}
	l.used += len(body)
	if len(body) > oldSideFileCap {
		l.fallbacks[path] = oldSideTooLarge
		return nil
	}
	o := scanOldSide(body)
	l.files[path] = o
	return o
}

// misaligned records that a hunk of path does not match its old-side file, so
// the file's line numbers cannot be trusted.
func (l *oldSideLoader) misaligned(path string) {
	if l != nil && l.fallbacks[path] == "" {
		l.fallbacks[path] = oldSideMisaligned
	}
}

// skipped lists the files whose old side added no coverage, sorted by path.
func (l *oldSideLoader) skipped() []oldSideFallback {
	if l == nil || len(l.fallbacks) == 0 {
		return nil
	}
	out := make([]oldSideFallback, 0, len(l.fallbacks))
	for p, reason := range l.fallbacks {
		out = append(out, oldSideFallback{p, reason})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func scanOldSide(body []byte) *oldSide {
	lines := strings.Split(string(body), "\n")
	if n := len(lines); n > 0 && lines[n-1] == "" {
		lines = lines[:n-1]
	}
	return &oldSide{lines: lines, secret: markPrivateKeyLines(lines)}
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
func markPrivateKeyLines(lines []string) []bool {
	secret := make([]bool, len(lines))
	for i := 0; i < len(lines); {
		t := strings.TrimSpace(lines[i])
		switch {
		case strings.HasPrefix(t, pemKeyBegin) && strings.Contains(t, "PRIVATE KEY"):
			i = markPEMBlock(lines, secret, i)
		case base64PEMEnd(t) > 0:
			i = markWrappedBase64(lines, secret, i)
		default:
			i++
		}
	}
	return secret
}

// markPEMBlock marks the block that begins at line i and returns the index of
// the first line after it. The block ends at its END line, or, when the file
// ends it unterminated, at the first line that cannot be key text.
func markPEMBlock(lines []string, secret []bool, i int) int {
	secret[i] = true
	j := i + 1
	for ; j < len(lines); j++ {
		t := strings.TrimSpace(lines[j])
		if strings.HasPrefix(t, pemKeyEnd) {
			secret[j] = true
			return j + 1
		}
		if t != "" && !base64Only.MatchString(t) && !pemHeaderLine.MatchString(t) {
			break
		}
		secret[j] = true
	}
	for k := j - 1; k > i && strings.TrimSpace(lines[k]) == ""; k-- {
		secret[k] = false
	}
	return j
}

// markWrappedBase64 marks line i, which holds the start of a base64 private
// key, and the base64-only lines that carry on its wrapped value. The value ends
// where its decoded text reaches the END marker, however wide each chunk is; a
// value the file leaves unterminated ends at the first line that is not base64.
func markWrappedBase64(lines []string, secret []bool, i int) int {
	t := strings.TrimSpace(lines[i])
	secret[i] = true
	start, end := base64PEMRun(t)
	if strings.Trim(t[end:], "\"',; \t") != "" {
		return i + 1
	}
	value := t[start:end]
	if decodesToPEMEnd(value) {
		return i + 1
	}
	j := i + 1
	for ; j < len(lines); j++ {
		c := strings.TrimSpace(lines[j])
		if c == "" || !base64Only.MatchString(c) {
			break
		}
		secret[j] = true
		value += c
		if decodesToPEMEnd(value) {
			j++
			break
		}
	}
	return j
}

// decodesToPEMEnd reports whether the base64 text, cut to whole quanta, decodes
// to something that holds a PEM END marker.
func decodesToPEMEnd(value string) bool {
	value = strings.TrimRight(value, "=")
	dec, err := base64.StdEncoding.DecodeString(value[:len(value)/4*4])
	return err == nil && bytes.Contains(dec, []byte(pemKeyEnd))
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
		dec = bytes.TrimLeft(dec, " \t\r\n")
		if bytes.HasPrefix(dec, []byte(pemKeyBegin)) && bytes.Contains(dec, []byte("PRIVATE KEY")) {
			return loc[0], loc[1]
		}
	}
	return 0, 0
}
