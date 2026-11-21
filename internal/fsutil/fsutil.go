// Package fsutil holds the durable local-state file operations every role
// shares: atomic replacement, JSON receipts and append-only JSONL logs.
package fsutil

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Atomic writes beside the target, so rename never crosses filesystems.
func Atomic(name string, data []byte, mode os.FileMode) error {
	if e := os.MkdirAll(filepath.Dir(name), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(name), ".mergewake-*")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(mode); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, name); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(name))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

// WriteJSON atomically replaces name with indented JSON readable only by its owner.
func WriteJSON(name string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return Atomic(name, b, 0600)
}

// ReadJSON decodes name into v. A missing file reports false and no error;
// any other read or decode failure is returned.
func ReadJSON(name string, v any) (bool, error) {
	b, e := os.ReadFile(name)
	if errors.Is(e, fs.ErrNotExist) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return true, json.Unmarshal(b, v)
}

// AppendLine appends one JSON line to name, creating it and its directory
// owner-only, and syncs before returning so a logged decision survives a crash.
func AppendLine(name string, line []byte) error {
	return AppendLineMode(name, 0700, 0600, line)
}

// AppendLineMode is AppendLine with the directory and file modes given. The
// file is locked (flock) for the append, so writers in other processes do not
// interleave with it.
func AppendLineMode(name string, dirMode, fileMode os.FileMode, line []byte) error {
	if e := os.MkdirAll(filepath.Dir(name), dirMode); e != nil {
		return e
	}
	f, e := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_RDWR, fileMode)
	if e != nil {
		return e
	}
	defer f.Close()
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return AppendTo(f, line)
}

// Appender is the file surface AppendTo writes through. *os.File satisfies it.
type Appender interface {
	io.ReaderAt
	io.Writer
	Stat() (fs.FileInfo, error)
	Truncate(size int64) error
	Sync() error
}

// AppendTo appends line and a newline to f with one Write, so a line lands
// whole or not at all. When f does not end in a newline (a fragment a crash or
// a full disk left), the line starts with one, so the fragment stays its own
// line and no good record joins it. A write that fails part-way is cut back to
// the size f had before. The caller must be the only appender of f, or hold
// its lock, for that cut to be safe.
func AppendTo(f Appender, line []byte) error {
	fi, e := f.Stat()
	if e != nil {
		return e
	}
	size := fi.Size()
	buf := make([]byte, 0, len(line)+2)
	if size > 0 {
		var last [1]byte
		if _, e = f.ReadAt(last[:], size-1); e != nil {
			return e
		}
		if last[0] != '\n' {
			buf = append(buf, '\n')
		}
	}
	buf = append(buf, line...)
	buf = append(buf, '\n')
	n, e := f.Write(buf)
	if e == nil && n != len(buf) {
		e = io.ErrShortWrite
	}
	if e != nil {
		if terr := f.Truncate(size); terr != nil {
			return errors.Join(e, terr)
		}
		return e
	}
	return f.Sync()
}

// ErrStop ends a ScanJSONL walk early without an error.
var ErrStop = errors.New("fsutil: stop scan")

// JSONL walks the complete lines of one JSONL file in order. A last line with
// no newline is not complete: Next never yields it and it is not consumed, so
// a writer that finishes it later is read whole by the next walk. A line that
// is not JSON is audited with Skipped and passed over.
type JSONL struct {
	path     string
	br       *bufio.Reader
	line     int
	consumed int64
	err      error
}

// NewJSONL walks the lines of r, a part of the file at path that starts after
// before complete lines, so line numbers in the audit are the file's own.
func NewJSONL(path string, before int, r io.Reader) *JSONL {
	return &JSONL{path: path, br: bufio.NewReader(r), line: before}
}

// Next returns the next complete line that is JSON, without its newline, and
// true. It returns false at the end of the complete lines or on a read error,
// which Err reports.
func (j *JSONL) Next() ([]byte, bool) {
	for {
		b, e := j.br.ReadBytes('\n')
		if errors.Is(e, io.EOF) {
			return nil, false
		}
		if e != nil {
			j.err = e
			return nil, false
		}
		j.line++
		j.consumed += int64(len(b))
		line := b[:len(b)-1]
		if json.Valid(line) {
			return line, true
		}
		j.Skip(line)
	}
}

// Skip audits a complete line Next yielded that the caller could not decode:
// it is recorded with the line's number and passed over by the caller.
func (j *JSONL) Skip(line []byte) {
	skipAudit(j.path, fmt.Sprintf("line %d", j.line), line)
}

// SkipWindow audits a line of the file at path that a reader decoded from a
// window it read at byte start, where line numbers are not known.
func SkipWindow(path string, start int64, line []byte) {
	skipAudit(path, fmt.Sprintf("from byte %d", start), line)
}

// Line is the number of the last complete line of the file Next has read.
func (j *JSONL) Line() int { return j.line }

// Consumed is how many bytes of the walk's complete lines Next has read.
func (j *JSONL) Consumed() int64 { return j.consumed }

// Err reports the read error that ended the walk, if any.
func (j *JSONL) Err() error { return j.err }

// ScanJSONL decodes each complete line of the JSONL file at path into T and
// calls fn with it, in order. A missing file has no rows. A line that does not
// decode into T is skipped and recorded by Skipped; it never fails the scan.
// An error from fn stops the scan and is returned; ErrStop from fn ends it
// cleanly.
func ScanJSONL[T any](path string, fn func(T) error) error {
	f, e := os.Open(path)
	if errors.Is(e, fs.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	defer f.Close()
	j := NewJSONL(path, 0, f)
	for line, ok := j.Next(); ok; line, ok = j.Next() {
		var v T
		if json.Unmarshal(line, &v) != nil {
			j.Skip(line)
			continue
		}
		if e := fn(v); errors.Is(e, ErrStop) {
			return nil
		} else if e != nil {
			return e
		}
	}
	return j.Err()
}

// skipAudited keeps the audit rows this process wrote, so a line that every
// read hits is recorded once per process, not once per read.
var skipAudited sync.Map // key string → true

// skipAudit records a line of the file at path that did not decode: one row in
// ledger-audit.jsonl beside it, with the path, where the line is and its first
// bytes. The row is best effort. A full disk must not fail the read that found
// the line, so a write error only lets the next read retry the row.
func skipAudit(path, where string, line []byte) {
	sum := sha256.Sum256(line)
	key := fmt.Sprintf("%s|%s|%x", path, where, sum[:8])
	if _, dup := skipAudited.LoadOrStore(key, true); dup {
		return
	}
	head := line
	if len(head) > skipHeadBytes {
		head = head[:skipHeadBytes]
	}
	row, e := json.Marshal(skipRow{At: time.Now().UTC(), Path: path, Where: where, Head: string(head)})
	if e == nil {
		e = AppendLine(filepath.Join(filepath.Dir(path), AuditFile), row)
	}
	if e != nil {
		skipAudited.Delete(key)
	}
}

// AuditFile is the name of the audit log, beside each ledger it audits.
const AuditFile = "ledger-audit.jsonl"

const skipHeadBytes = 80

type skipRow struct {
	At    time.Time `json:"at"`
	Path  string    `json:"path"`
	Where string    `json:"where"`
	Head  string    `json:"head"`
}

// Lock takes an exclusive advisory lock on name, creating it and its directory
// owner-only. With wait it blocks until the holder releases; without, a held
// lock fails at once. The lock lives as long as the returned unlock is not
// called, and the kernel drops it if the process exits.
func Lock(name string, wait bool) (unlock func(), err error) {
	if e := os.MkdirAll(filepath.Dir(name), 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	how := syscall.LOCK_EX
	if !wait {
		how |= syscall.LOCK_NB
	}
	if e = syscall.Flock(int(f.Fd()), how); e != nil {
		f.Close()
		return nil, e
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// TryLockFile takes an exclusive advisory lock on f without blocking. The lock
// belongs to f's open file description: it lasts until f is closed, and a second
// open of the same file conflicts with it, even within this process. It fails
// with EWOULDBLOCK while another holder has the lock.
func TryLockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
