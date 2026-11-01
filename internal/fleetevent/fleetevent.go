// Package fleetevent writes fleet-event v1 records: one JSON object per line,
// appended to a local, size-bounded file that Grafana Alloy tails and ships to
// Loki. The same shape is written by runtug, panewire and mergewake; the
// contract lives in panewire's deploy/alloy/README.md ("Fleet events").
//
// Every string field is redacted and bounded before it is written, so a caller
// may pass an error message or a stderr tail without scrubbing it first. Kind,
// outcome and reason become (or feed) Loki labels and are normalised to short
// lowercase codes to keep label cardinality low.
package fleetevent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Event is the fleet-event v1 record.
type Event struct {
	TS      string `json:"ts"`
	Host    string `json:"host"`
	Tool    string `json:"tool"`
	Kind    string `json:"kind"`
	Job     string `json:"job"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
	Detail  string `json:"detail"`
}

const (
	// MaxDetail bounds the detail field in bytes, after redaction.
	MaxDetail = 1024
	// maxCode bounds kind, outcome and reason.
	maxCode = 64
	// maxJob bounds the job field.
	maxJob = 128
	// DefaultMaxBytes is the size at which the file rotates to <path>.1.
	DefaultMaxBytes = 4 << 20
	// FileName is the events file inside a tool's events directory.
	FileName = "fleet-events.jsonl"
)

// Writer appends events to Path. The zero Path disables it. Emit never returns
// an error to the caller's control flow: telemetry must not change behaviour.
type Writer struct {
	Path     string
	Tool     string
	Host     string
	MaxBytes int64
	Now      func() time.Time

	mu sync.Mutex
}

// New returns a writer for <stateDir>/events/fleet-events.jsonl.
func New(stateDir, tool string, now func() time.Time) *Writer {
	path := ""
	if stateDir != "" {
		path = filepath.Join(stateDir, "events", FileName)
	}
	return &Writer{Path: path, Tool: tool, Host: ShortHost(), MaxBytes: DefaultMaxBytes, Now: now}
}

// ShortHost is the machine's hostname up to the first dot, lowercased.
func ShortHost() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	h, _, _ = strings.Cut(h, ".")
	return Code(h)
}

// Emit appends one event. It reports whether the line was written.
func (w *Writer) Emit(kind, job, outcome, reason, detail string) bool {
	if w == nil || w.Path == "" {
		return false
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	ev := Event{
		TS:      now().UTC().Format(time.RFC3339Nano),
		Host:    Code(w.Host),
		Tool:    Code(w.Tool),
		Kind:    Code(kind),
		Job:     Bound(Redact(job), maxJob),
		Outcome: Code(outcome),
		Reason:  Code(reason),
		Detail:  Bound(Redact(detail), MaxDetail),
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return false
	}
	line = append(line, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	// Group-readable (0750 dir, 0640 file): on Linux the alloy system user
	// tails these through a group or ACL grant. Every field is redacted.
	if os.MkdirAll(filepath.Dir(w.Path), 0o750) != nil {
		return false
	}
	w.rotate(int64(len(line)))
	f, err := os.OpenFile(w.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return false
	}
	// One write per line: O_APPEND keeps concurrent writers' lines whole.
	_, err = f.Write(line)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err == nil
}

// rotate renames the file to <path>.1 when the next line would cross
// MaxBytes. It is best effort across processes: two writers racing past the
// limit may drop one rotated generation, never a line of the live file.
func (w *Writer) rotate(next int64) {
	limit := w.MaxBytes
	if limit <= 0 {
		limit = DefaultMaxBytes
	}
	info, err := os.Stat(w.Path)
	if err != nil || info.Size()+next <= limit {
		return
	}
	_ = os.Rename(w.Path, w.Path+".1")
}

var codeRune = regexp.MustCompile(`[^a-z0-9_.:-]+`)

// Code normalises a low-cardinality field: lowercase, [a-z0-9_.:-] only,
// at most 64 bytes.
func Code(s string) string {
	s = codeRune.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), "_")
	return Bound(strings.Trim(s, "_"), maxCode)
}

// Bound returns the longest valid UTF-8 prefix of s within limit bytes.
func Bound(s string, limit int) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.ValidString(s[:limit]) {
		limit--
	}
	return s[:limit]
}

// Tail returns the last limit bytes of s on a UTF-8 boundary, trimmed.
func Tail(s string, limit int) string {
	s = strings.TrimSpace(strings.ToValidUTF8(s, "?"))
	if len(s) <= limit {
		return s
	}
	start := len(s) - limit
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

// secretPatterns matches credential-shaped substrings. It mirrors panewire's
// fleet-event redaction set: URL userinfo, bearer/basic credentials, provider
// token shapes, any NAME=value whose name holds key/token/secret/password,
// whole PEM private-key blocks (or a truncated tail of one), and email
// addresses.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+(:[^/\s@]*)?@`),
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)`),
	regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`(?i)basic\s+[A-Za-z0-9+/=]{8,}`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}`),
	regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{16,}`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\bgl(c|sa)_[A-Za-z0-9_=+/-]{16,}`),
	regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{8,}`),
	regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`),
	regexp.MustCompile(`eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}`),
	// A NAME=value whose name holds a credential word: a double- or single-
	// quoted value through its closing quote (escapes and spaces included,
	// or to the end when the tail cut the quote off), else the bare word.
	regexp.MustCompile(`(?i)[a-z0-9_.-]*(api[_-]?key|access[_-]?key|private[_-]?key|token|secret|password|passwd|credential|dsn)[a-z0-9_.-]*["']?\s*[:=]\s*("(?:[^"\\]|\\.)*"?|'(?:[^'\\]|\\.)*'?|[^\s"',;&]+)`),
	regexp.MustCompile(`(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b`),
}

// longBlob matches a long base64-alphabet run: a key or token body that no
// named pattern caught. Pure hex runs (commit SHAs, digests) are kept.
var longBlob = regexp.MustCompile(`[A-Za-z0-9+/_-]{48,}={0,2}`)

var hexOnly = regexp.MustCompile(`^[0-9a-fA-F]+$`)

// Redact replaces credential-shaped substrings and email addresses with
// [redacted]. URL userinfo keeps its scheme so the line still reads as a URL.
func Redact(s string) string {
	for i, p := range secretPatterns {
		if i == 0 {
			s = p.ReplaceAllString(s, "${1}[redacted]@")
			continue
		}
		s = p.ReplaceAllString(s, "[redacted]")
	}
	return longBlob.ReplaceAllStringFunc(s, func(m string) string {
		if hexOnly.MatchString(m) {
			return m
		}
		return "[redacted]"
	})
}
