package fsutil

import (
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestReadJSONMissingPresentCorrupt(t *testing.T) {
	dir := t.TempDir()
	var v struct{ Commit string }
	if found, e := ReadJSON(filepath.Join(dir, "missing.json"), &v); found || e != nil {
		t.Fatalf("missing: found=%v err=%v", found, e)
	}
	name := filepath.Join(dir, "nested", "receipt.json")
	if e := WriteJSON(name, struct{ Commit string }{"abc"}); e != nil {
		t.Fatal(e)
	}
	if info, e := os.Stat(name); e != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("mode: %v %v", info, e)
	}
	if found, e := ReadJSON(name, &v); !found || e != nil || v.Commit != "abc" {
		t.Fatalf("present: found=%v err=%v v=%+v", found, e, v)
	}
	if e := os.WriteFile(name, []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	if found, e := ReadJSON(name, &v); !found || e == nil {
		t.Fatalf("corrupt receipt must fail: found=%v err=%v", found, e)
	}
}

func TestAppendLineCreatesAndAppends(t *testing.T) {
	name := filepath.Join(t.TempDir(), "state", "decisions.jsonl")
	for _, line := range []string{`{"a":1}`, `{"b":2}`} {
		if e := AppendLine(name, []byte(line)); e != nil {
			t.Fatal(e)
		}
	}
	b, e := os.ReadFile(name)
	if e != nil || string(b) != "{\"a\":1}\n{\"b\":2}\n" {
		t.Fatalf("%q %v", b, e)
	}
}

func TestLockExcludesUntilUnlocked(t *testing.T) {
	name := filepath.Join(t.TempDir(), "state", "lock")
	unlock, e := Lock(name, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := Lock(name, false); e == nil {
		t.Fatal("second non-blocking lock acquired a held lock")
	}
	unlock()
	again, e := Lock(name, true)
	if e != nil {
		t.Fatalf("lock not released: %v", e)
	}
	again()
}

// A file size limit makes the kernel accept part of an append and then fail
// it with EFBIG: a real short write, with no injected writer.
func TestAppendLineShortWriteLeavesFileAtOldSize(t *testing.T) {
	name := filepath.Join(t.TempDir(), "decisions.jsonl")
	if e := AppendLine(name, []byte(`{"a":1}`)); e != nil {
		t.Fatal(e)
	}
	before, e := os.Stat(name)
	if e != nil {
		t.Fatal(e)
	}
	var old syscall.Rlimit
	if e := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &old); e != nil {
		t.Fatal(e)
	}
	signal.Ignore(syscall.SIGXFSZ)
	defer signal.Reset(syscall.SIGXFSZ)
	lim := syscall.Rlimit{Cur: uint64(before.Size()) + 5, Max: old.Max}
	if e := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &lim); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &old) })
	if AppendLine(name, []byte(`{"b":2222222222222222222222222222}`)) == nil {
		t.Fatal("append past the size limit succeeded")
	}
	after, e := os.Stat(name)
	if e != nil {
		t.Fatal(e)
	}
	if after.Size() != before.Size() {
		t.Fatalf("file is %d bytes after a failed append, want its old %d", after.Size(), before.Size())
	}
}

func TestAppendLineStartsAFragmentOnItsOwnLine(t *testing.T) {
	name := filepath.Join(t.TempDir(), "events.jsonl")
	if e := os.WriteFile(name, []byte(`{"a":"cut`), 0600); e != nil {
		t.Fatal(e)
	}
	if e := AppendLine(name, []byte(`{"b":2}`)); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(name)
	if e != nil || string(b) != "{\"a\":\"cut\n{\"b\":2}\n" {
		t.Fatalf("%q %v", b, e)
	}
}

func TestScanJSONLSkipsBadLinesAndUnterminatedTail(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "decisions.jsonl")
	body := "{\"n\":1}\n{\"n\":\nnot json\n{\"n\":2}\n{\"n\":3"
	if e := os.WriteFile(name, []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
	var got []int
	if e := ScanJSONL(name, func(v struct {
		N int `json:"n"`
	}) error {
		got = append(got, v.N)
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("rows %v, want [1 2]: the cut tail is not a complete line", got)
	}
	audit, e := os.ReadFile(filepath.Join(dir, AuditFile))
	if e != nil {
		t.Fatalf("no audit row: %v", e)
	}
	if n := strings.Count(string(audit), "\n"); n != 2 {
		t.Fatalf("%d audit rows, want one per bad line (2):\n%s", n, audit)
	}
}

func TestScanJSONLMissingFileHasNoRows(t *testing.T) {
	err := ScanJSONL(filepath.Join(t.TempDir(), "absent.jsonl"), func(struct{}) error {
		t.Fatal("row from a missing file")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
